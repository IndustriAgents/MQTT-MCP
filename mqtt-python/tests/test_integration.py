"""End to end: a real broker, the mock edge node, and the real server over stdio.

Needs a broker (see conftest.py). Every test uses its own Sparkplug group and
topic prefix, so a shared broker is fine.
"""

from __future__ import annotations

import asyncio
import datetime
import ipaddress
import ssl
import time

import anyio
import pytest

from mqtt_mcp.sparkplug import decode_payload

from .conftest import call, eventually, mcp_session, parse_connect, unique

pytestmark = [pytest.mark.anyio, pytest.mark.integration]


def metrics_of(decoded: dict) -> dict:
    return {m["name"]: (m["datatype"], m["value"]) for m in decoded["metrics"]}


async def test_read_only_server_against_the_mock(broker, mock_node, observer, tmp_path):
    mock = mock_node()
    group = mock.group_id
    async with mcp_session(
        tmp_path,
        MQTT_BROKER_URL=broker.url,
        SPARKPLUG_GROUP_ID=group,
        SPARKPLUG_EDGE_NODE_ID="mcp-observer",
    ) as session:
        info = await call(session, "get_broker_info")
        assert info["data"]["connected"] is True
        assert info["data"]["writes_enabled"] is False

        sub = await call(session, "subscribe_topic", topic=f"spBv1.0/{group}/#", qos=1)
        assert sub["success"] is True and sub["data"]["granted_qos"] == 1
        listed = await call(session, "list_subscriptions")
        assert listed["data"] == [{"topic": f"spBv1.0/{group}/#", "qos": 1}]

        # Passive discovery of the mock's node and devices from broker traffic.
        async def discovered():
            nodes = (await call(session, "list_sparkplug_nodes"))["data"]["nodes"]
            if nodes and len(nodes[0]["devices"]) == 2 and nodes[0]["devices"][0]["metrics"]:
                return nodes
            return None

        (node,) = await eventually(discovered)
        assert (node["group_id"], node["edge_node_id"], node["online"]) == (
            group,
            "edge-node-1",
            True,
        )
        sensors = node["devices"][0]
        assert sensors["device_id"] == "device-1"
        assert sensors["metrics"]["temperature"]["datatype"] == "Float"
        assert 20.0 <= sensors["metrics"]["temperature"]["value"] <= 25.0

        # Received messages are returned, decoded.
        read = await call(
            session,
            "read_messages",
            topic_filter=f"spBv1.0/{group}/DDATA/+/device-1",
            limit=3,
        )
        assert read["success"] is True and read["data"]
        message = read["data"][-1]
        assert message["payload_encoding"] == "hex"
        assert set(metrics_of(message["sparkplug"])) == {"temperature", "pressure", "flow_rate"}
        decoded = await call(session, "decode_sparkplug_payload", payload_hex=message["payload"])
        assert decoded["data"] == message["sparkplug"]

        # Writes are refused and nothing reaches the broker.
        observer.subscribe(f"itest-ro/{group}/#")
        refused = await call(
            session, "publish_message", topic=f"itest-ro/{group}/x", payload="must-not-arrive"
        )
        assert refused["success"] is False
        command = await call(
            session,
            "publish_device_command",
            device_id="device-2",
            edge_node_id="edge-node-1",
            metrics=[{"name": "pump_speed", "value": 90, "type": "int32"}],
        )
        assert command["success"] is False
        await asyncio.sleep(1.0)
        assert not observer.find(lambda m: m.topic.startswith("itest-ro/"))
        assert not observer.find(lambda m: "/DCMD/" in m.topic and group in m.topic)
        latest = await call(
            session, "read_messages", topic_filter=f"spBv1.0/{group}/DDATA/+/device-2", limit=1
        )
        assert metrics_of(latest["data"][-1]["sparkplug"])["pump_speed"] == ("Int32", 0)

        # The mock shuts down once and its deaths mark the node offline.
        assert mock.stop() == 0
        assert mock.text().count("Shutting down...") == 1
        assert mock.text().count("Shutdown complete") == 1

        async def offline():
            nodes = (await call(session, "list_sparkplug_nodes"))["data"]["nodes"]
            node = nodes[0]
            if node["online"] is False and all(d["online"] is False for d in node["devices"]):
                return node
            return None

        node = await eventually(offline)
        assert node["last_message_type"] == "NDEATH"


async def test_writes_commands_edge_node_and_will(
    broker, mock_node, observer, proxy_factory, tmp_path
):
    mock = mock_node()
    group = mock.group_id
    proxy = proxy_factory().start()  # lets the test cut the server's connection
    observer.subscribe(f"spBv1.0/{group}/#")
    observer.subscribe(f"itest-rw/{group}/#")

    async with mcp_session(
        tmp_path,
        MQTT_BROKER_URL=f"mqtt://127.0.0.1:{proxy.port}",
        MQTT_WRITES_ENABLED="true",
        SPARKPLUG_GROUP_ID=group,
        SPARKPLUG_EDGE_NODE_ID="mcp-node",
    ) as session:
        # Plain publish, confirmed by the broker.
        sent = await call(
            session, "publish_message", topic=f"itest-rw/{group}/cmd", payload="hello", qos=1
        )
        assert sent["success"] is True
        assert "PUBACK" in sent["data"]["delivery"]
        received = await observer.wait_for(lambda m: m.topic == f"itest-rw/{group}/cmd")
        assert received.payload == b"hello"

        # A DCMD the mock acts on.
        await call(session, "subscribe_topic", topic=f"spBv1.0/{group}/#")
        command = await call(
            session,
            "publish_device_command",
            device_id="device-2",
            edge_node_id="edge-node-1",
            metrics=[
                {"name": "pump_speed", "value": 80, "type": "int32"},
                {"name": "enabled", "value": True, "type": "boolean"},
            ],
        )
        assert command["success"] is True
        assert command["data"]["topic"] == f"spBv1.0/{group}/DCMD/edge-node-1/device-2"

        async def pump_running():
            nodes = (await call(session, "list_sparkplug_nodes"))["data"]["nodes"]
            for node in nodes:
                for device in node["devices"]:
                    metrics = device["metrics"]
                    if (
                        device["device_id"] == "device-2"
                        and metrics.get("pump_speed", {}).get("value") == 80
                    ):
                        return metrics
            return None

        metrics = await eventually(pump_running)
        assert metrics["enabled"]["value"] is True
        dcmd = await observer.wait_for(lambda m: "/DCMD/" in m.topic)
        assert decode_payload(dcmd.payload)["seq"] is None  # commands carry no seq
        assert "Command: set device-2/pump_speed=80" in mock.text()

        # This server as an edge node: NBIRTH with negative ints returns promptly.
        with anyio.fail_after(15):
            birth = await call(
                session,
                "publish_node_birth",
                metrics=[
                    {"name": "offset", "value": -5, "type": "int32"},
                    {"name": "enabled", "value": True, "type": "boolean"},
                ],
            )
        assert birth["success"] is True, birth
        assert (birth["data"]["seq"], birth["data"]["bd_seq"]) == (0, 0)
        assert birth["meta"]["reconnected_with_will"] is True
        nbirth = await observer.wait_for(lambda m: m.topic == f"spBv1.0/{group}/NBIRTH/mcp-node")
        assert nbirth.qos == 0 and not nbirth.retain
        decoded = decode_payload(nbirth.payload)
        assert decoded["seq"] == 0
        assert metrics_of(decoded) == {
            "bdSeq": ("Int64", 0),
            "Node Control/Rebirth": ("Boolean", False),
            "offset": ("Int32", -5),
            "enabled": ("Boolean", True),
        }

        early = await call(session, "publish_device_data", device_id="dev-a", metrics=[])
        assert early["success"] is False
        dbirth = await call(
            session,
            "publish_device_birth",
            device_id="dev-a",
            metrics=[{"name": "level", "value": -1, "type": "int8"}],
        )
        assert dbirth["data"]["seq"] == 1
        ddata = await call(
            session,
            "publish_device_data",
            device_id="dev-a",
            metrics=[{"name": "level", "value": -2, "type": "int8"}],
        )
        assert ddata["data"]["seq"] == 2
        message = await observer.wait_for(
            lambda m: m.topic == f"spBv1.0/{group}/DDATA/mcp-node/dev-a"
        )
        assert metrics_of(decode_payload(message.payload)) == {"level": ("Int8", -2)}
        ndata = await call(
            session,
            "publish_node_data",
            metrics=[{"name": "offset", "value": -9_000_000_000, "type": "int64"}],
        )
        assert ndata["data"]["seq"] == 3

        # Cut the connection without a DISCONNECT: the broker publishes the NDEATH will.
        before = await call(session, "read_messages", limit=1)
        dropped_at = time.time()
        proxy.drop_connections()
        will = await observer.wait_for(lambda m: m.topic == f"spBv1.0/{group}/NDEATH/mcp-node")
        assert will.qos == 1
        will_payload = decode_payload(will.payload)
        assert will_payload["seq"] is None
        assert metrics_of(will_payload) == {"bdSeq": ("Int64", 0)}

        async def reconnected():
            info = (await call(session, "get_broker_info"))["data"]
            return info if info["connected"] and not info["sparkplug"]["node_born"] else None

        info = await eventually(reconnected, timeout=20)
        assert "NDEATH will" in info["sparkplug"]["node_lost"]
        assert info["subscriptions"] == [{"topic": f"spBv1.0/{group}/#", "qos": 0}]
        gone = await call(
            session, "publish_node_data", metrics=[{"name": "x", "value": 1, "type": "int32"}]
        )
        assert gone["success"] is False and "not born" in gone["error"]

        # The subscription was restored: new mock data arrives after the reconnect.
        async def fresh_data():
            read = await call(
                session,
                "read_messages",
                topic_filter=f"spBv1.0/{group}/DDATA/edge-node-1/+",
                since_id=before["meta"]["last_id"],
            )
            return [
                m
                for m in read["data"]
                if datetime.datetime.fromisoformat(m["received_at"]).timestamp() > dropped_at
            ]

        assert await eventually(fresh_data)

        # A new birth needs a new session, so it gets the next bdSeq.
        rebirth = await call(session, "publish_node_birth")
        assert rebirth["success"] is True and rebirth["data"]["bd_seq"] == 1
        death = await call(session, "publish_node_death")
        assert death["success"] is True and "PUBACK" in death["data"]["delivery"]
        last = await observer.wait_for(
            lambda m: (
                m.topic == f"spBv1.0/{group}/NDEATH/mcp-node"
                and metrics_of(decode_payload(m.payload)) == {"bdSeq": ("Int64", 1)}
            )
        )
        assert last.qos == 1


async def test_broker_down_at_startup_then_up(broker, observer, proxy_factory, tmp_path):
    proxy = proxy_factory()  # not listening yet: the "broker" is down
    client_id = unique("itest-client")
    topic = f"itest-late/{client_id}/value"
    async with mcp_session(
        tmp_path,
        MQTT_BROKER_URL=f"mqtt://127.0.0.1:{proxy.port}",
        MQTT_CLIENT_ID=client_id,
        MQTT_USERNAME="agent",  # no password
        MQTT_TOOL_TIMEOUT="1",
    ) as session:
        info = await call(session, "get_broker_info")
        assert info["data"]["connected"] is False
        assert "refused" in info["data"]["last_error"].lower()
        early = await call(session, "subscribe_topic", topic=topic)
        assert early["success"] is False and "Not connected" in early["error"]

        proxy.start()

        async def connected():
            return (await call(session, "get_broker_info"))["data"]["connected"]

        await eventually(connected, timeout=25)
        sub = await call(session, "subscribe_topic", topic=topic)
        assert sub["success"] is True
        observer.publish(topic, "42")

        async def got_it():
            return (await call(session, "read_messages", topic_filter=topic))["data"]

        (message,) = await eventually(got_it)
        assert (message["payload"], message["payload_encoding"]) == ("42", "utf-8")

    connect = parse_connect(proxy.first_packets[0])
    assert connect["client_id"] == client_id  # MQTT_CLIENT_ID is used
    assert connect["username"] == "agent"  # username without password is sent
    assert connect["password_flag"] is False
    assert connect["will"] is False  # no will until this server publishes an NBIRTH


def make_tls_files(directory):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    now = datetime.datetime.now(datetime.timezone.utc)
    validity = {
        "not_valid_before": now - datetime.timedelta(days=1),
        "not_valid_after": now + datetime.timedelta(days=1),
    }
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "mqtt-mcp test CA")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(validity["not_valid_before"])
        .not_valid_after(validity["not_valid_after"])
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False)
        .sign(ca_key, hashes.SHA256())
    )
    key = ec.generate_private_key(ec.SECP256R1())
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]))
        .issuer_name(ca_name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(validity["not_valid_before"])
        .not_valid_after(validity["not_valid_after"])
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False
        )
        .sign(ca_key, hashes.SHA256())
    )
    pem = serialization.Encoding.PEM
    ca_file = directory / "ca.pem"
    ca_file.write_bytes(ca_cert.public_bytes(pem))
    cert_file = directory / "server.pem"
    cert_file.write_bytes(cert.public_bytes(pem))
    key_file = directory / "server.key"
    key_file.write_bytes(
        key.private_bytes(pem, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    return ca_file, cert_file, key_file


@pytest.mark.parametrize(
    ("variant", "expect_connected"),
    [("verified", True), ("untrusted", False), ("insecure", True)],
)
async def test_mqtts_uses_tls(variant, expect_connected, broker, observer, proxy_factory, tmp_path):
    ca_file, cert_file, key_file = make_tls_files(tmp_path)
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(cert_file, key_file)
    proxy = proxy_factory(ssl_context=context).start()  # TLS in front of the plain broker
    env = {"MQTT_BROKER_URL": f"mqtts://127.0.0.1:{proxy.port}", "MQTT_TOOL_TIMEOUT": "2"}
    if variant == "verified":
        env["MQTT_TLS_CA_FILE"] = str(ca_file)
    elif variant == "insecure":
        env["MQTT_TLS_INSECURE"] = "true"
    topic = f"itest-tls/{unique(variant)}"

    async with mcp_session(tmp_path, **env) as session:
        if expect_connected:

            async def connected():
                info = (await call(session, "get_broker_info"))["data"]
                return info if info["connected"] else None

            info = await eventually(connected)
            assert info["tls"] is True
            assert info["tls_verification"] is (variant == "verified")
            assert info["broker"].startswith("mqtts://")
            assert (await call(session, "subscribe_topic", topic=topic))["success"] is True
            observer.publish(topic, "over-tls")

            async def got_it():
                return (await call(session, "read_messages", topic_filter=topic))["data"]

            (message,) = await eventually(got_it)
            assert message["payload"] == "over-tls"
        else:
            await asyncio.sleep(2)
            info = (await call(session, "get_broker_info"))["data"]
            assert info["connected"] is False
            assert "CERTIFICATE_VERIFY_FAILED" in (info["last_error"] or "")
            refused = await call(session, "subscribe_topic", topic=topic)
            assert refused["success"] is False
