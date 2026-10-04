"""Tool behaviour that needs no broker: write gate, envelopes, validation, descriptions.

The server runs in-process (mcp's in-memory transport) against a port where no
broker listens.
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
from contextlib import asynccontextmanager

import anyio
import paho.mqtt.client as mqtt
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from mqtt_mcp import mqtt_server
from mqtt_mcp.config import load_settings

from .conftest import free_port

pytestmark = pytest.mark.anyio

READ_TOOLS = {
    "get_broker_info",
    "subscribe_topic",
    "unsubscribe_topic",
    "list_subscriptions",
    "read_messages",
    "list_sparkplug_nodes",
    "decode_sparkplug_payload",
}
METRIC = [{"name": "m", "value": 1, "type": "int32"}]

WRITE_TOOLS = {
    "publish_message": {"topic": "a/b", "payload": "x"},
    "publish_node_birth": {"metrics": []},
    "publish_node_death": {},
    "publish_device_birth": {"device_id": "d", "metrics": []},
    "publish_device_death": {"device_id": "d"},
    "publish_node_data": {"metrics": [{"name": "m", "value": 1, "type": "int32"}]},
    "publish_device_data": {
        "device_id": "d",
        "metrics": [{"name": "m", "value": 1, "type": "int32"}],
    },
    "publish_node_command": {"metrics": [{"name": "m", "value": 1, "type": "int32"}]},
    "publish_device_command": {
        "device_id": "d",
        "metrics": [{"name": "m", "value": 1, "type": "int32"}],
    },
}


@asynccontextmanager
async def server(**env):
    env.setdefault("MQTT_BROKER_URL", f"mqtt://127.0.0.1:{free_port()}")
    env.setdefault("MQTT_TOOL_TIMEOUT", "0.5")
    mqtt_server.set_settings(load_settings(env))
    try:
        async with create_connected_server_and_client_session(
            mqtt_server.mcp._mcp_server
        ) as session:
            yield session
    finally:
        mqtt_server.set_settings(None)


async def call(session, name, **arguments):
    result = await session.call_tool(name, arguments)
    assert not result.isError, result.content
    envelope = result.structuredContent
    assert set(envelope) == {"success", "data", "error", "meta"}
    assert json.loads(result.content[0].text) == envelope
    return envelope


async def test_server_info_reports_this_package():
    import importlib.metadata

    mqtt_server.set_settings(load_settings({"MQTT_BROKER_URL": f"mqtt://127.0.0.1:{free_port()}"}))
    try:
        async with create_connected_server_and_client_session(
            mqtt_server.mcp._mcp_server
        ) as session:
            init = await session.initialize()
    finally:
        mqtt_server.set_settings(None)
    assert init.serverInfo.name == "MQTT-Control"
    assert init.serverInfo.version == importlib.metadata.version("mqtt-mcp")


async def test_tool_list_and_annotations():
    async with server() as session:
        tools = {t.name: t for t in (await session.list_tools()).tools}
    assert set(tools) == READ_TOOLS | set(WRITE_TOOLS)
    assert set(WRITE_TOOLS) == mqtt_server.WRITE_TOOLS  # the deadline's write list
    for name in READ_TOOLS:
        assert tools[name].annotations.readOnlyHint is True, name
    for name in WRITE_TOOLS:
        tool = tools[name]
        assert tool.annotations.readOnlyHint is False, name
        assert tool.annotations.destructiveHint is True, name
        assert tool.annotations.idempotentHint is False, name
        assert tool.description.startswith("WRITE:"), name
        assert "MQTT_WRITES_ENABLED=true" in tool.description, name


async def test_descriptions_do_not_overclaim():
    async with server() as session:
        tools = {t.name: t.description for t in (await session.list_tools()).tools}
    assert "seen in messages this server received" in tools["list_sparkplug_nodes"]
    assert "subscribe to 'spBv1.0/#'" in tools["list_sparkplug_nodes"]
    assert "does not mean the node applied it" in tools["publish_node_command"]
    for name in (
        "publish_node_birth",
        "publish_device_birth",
        "publish_node_data",
        "publish_device_data",
        "publish_node_command",
        "publish_device_command",
    ):
        text = tools[name]
        assert '{"name": str, "value": ..., "type": str}' in text, name
        for type_name in ("int32", "float", "boolean", "string", "uint64", "double"):
            assert type_name in text, (name, type_name)
        assert "{METRICS_HELP}" not in text


async def test_writes_are_disabled_by_default():
    async with server() as session:
        info = await call(session, "get_broker_info")
        assert info["data"]["writes_enabled"] is False
        for name, arguments in WRITE_TOOLS.items():
            result = await call(session, name, **arguments)
            assert result["success"] is False, name
            assert "MQTT_WRITES_ENABLED=true" in result["error"], name
            assert "Nothing was published" in result["error"], name
            assert result["meta"]["outcome"] == "not_sent", name
            assert result["meta"]["request_sent"] is False, name


async def test_dollar_filters_other_than_sys_need_writes():
    async with server() as session:
        for topic in ("$share/group/sensors/#", "$queue/sensors/#", "$exclusive/sensors/#"):
            result = await call(session, "subscribe_topic", topic=topic)
            assert result["success"] is False, topic
            assert "MQTT_WRITES_ENABLED" in result["error"], topic
        sys_topics = await call(session, "subscribe_topic", topic="$SYS/broker/version")
        assert "MQTT_WRITES_ENABLED" not in sys_topics["error"]  # fails only: not connected


async def test_not_connected_is_reported_not_faked():
    async with server(MQTT_WRITES_ENABLED="true") as session:
        info = await call(session, "get_broker_info")
        assert info["data"]["connected"] is False
        assert "Connection" in (info["data"]["last_error"] or "")
        result = await call(session, "publish_message", topic="a/b", payload="x")
        assert result["success"] is False
        assert "Not connected" in result["error"]
        result = await call(session, "subscribe_topic", topic="a/#")
        assert result["success"] is False
        listed = await call(session, "list_subscriptions")
        assert listed["data"] == []


BAD_ARGUMENTS = [
    ("publish_message", {"topic": "a/b", "payload": "x", "qos": "high"}, "Invalid arguments"),
    ("publish_message", {"topic": "a/b"}, "Invalid arguments"),
    ("publish_message", {"topic": "a/#", "payload": "x"}, "wildcards"),
    ("publish_message", {"topic": "a/b", "payload": "x", "qos": 3}, "qos must be 0, 1 or 2"),
    ("publish_message", {"topic": "", "payload": "x"}, "non-empty"),
    ("publish_device_data", {"device_id": "a/b", "metrics": []}, "must not contain"),
    ("publish_node_data", {"metrics": []}, "at least one metric"),
    ("publish_node_data", {"metrics": "temperature"}, "Invalid arguments"),
    (
        "publish_node_birth",
        {"metrics": [{"name": "m", "value": 1, "type": "complex"}]},
        "unsupported metric type",
    ),
    (
        "publish_node_birth",
        {"metrics": [{"name": "bdSeq", "value": 1, "type": "int64"}]},
        "added by the server",
    ),
    (
        "publish_node_command",
        {"metrics": [{"name": "m", "value": "on", "type": "boolean"}]},
        "true or false",
    ),
    (
        "publish_device_command",
        {"device_id": "d", "metrics": [{"name": "m", "value": 1}], "edge_node_id": "n+1"},
        "must not contain",
    ),
    ("subscribe_topic", {"topic": "a/#/b"}, "'#' must be the whole last level"),
    ("subscribe_topic", {"topic": "a/b+"}, "'+' must be a whole level"),
    ("subscribe_topic", {"topic": "a/b", "qos": -1}, "qos must be"),
    ("subscribe_topic", {"topic": 5}, "Invalid arguments"),
    # Strict types: a boolean is never an integer, an integer never a boolean.
    ("subscribe_topic", {"topic": "a/#", "qos": True}, "Invalid arguments"),
    ("publish_message", {"topic": "a/b", "payload": "x", "qos": True}, "Invalid arguments"),
    ("publish_message", {"topic": "a/b", "payload": "x", "retain": 1}, "Invalid arguments"),
    ("read_messages", {"limit": True}, "Invalid arguments"),
    ("read_messages", {"since_id": False}, "Invalid arguments"),
    ("read_messages", {"limit": "5"}, "Invalid arguments"),
    (
        "publish_node_command",
        {"metrics": [{"name": "m", "value": True, "type": "int32"}]},
        "must be an integer",
    ),
]


async def test_bad_arguments_return_success_false():
    async with server(MQTT_WRITES_ENABLED="true") as session:
        for tool, arguments, message in BAD_ARGUMENTS:
            result = await call(session, tool, **arguments)
            assert result["success"] is False, (tool, arguments)
            assert message in result["error"], (tool, arguments, result["error"])
            if tool.startswith("publish_"):
                assert result["meta"].get("outcome", "not_sent") == "not_sent", tool


async def test_unsubscribe_unknown_topic():
    async with server() as session:
        result = await call(session, "unsubscribe_topic", topic="never/subscribed")
    assert result["success"] is False
    assert "no subscription" in result["error"]


async def test_read_messages_validation_and_empty_buffer():
    async with server() as session:
        empty = await call(session, "read_messages")
        assert empty["success"] is True and empty["data"] == []
        assert "subscribe_topic first" in empty["meta"]["note"]
        assert (await call(session, "read_messages", limit=0))["success"] is False
        assert (await call(session, "read_messages", since_id=-1))["success"] is False
        bad_filter = await call(session, "read_messages", topic_filter="a/#/b")
        assert bad_filter["success"] is False


async def test_decode_sparkplug_payload():
    from .test_sparkplug import README_SAMPLE_HEX

    async with server() as session:
        ok = await call(session, "decode_sparkplug_payload", payload_hex=README_SAMPLE_HEX)
        assert ok["success"] is True
        assert ok["data"]["seq"] == 1
        assert ok["data"]["metrics"][0]["value"] == 23.1
        assert ok["data"]["metrics"][1]["value"] == -5
        spaced = await call(
            session, "decode_sparkplug_payload", payload_hex="0x " + README_SAMPLE_HEX[:10] + " "
        )
        assert spaced["success"] is False  # truncated payload
        bad_hex = await call(session, "decode_sparkplug_payload", payload_hex="zz")
        assert bad_hex["success"] is False and "not valid hex" in bad_hex["error"]
        garbage = await call(session, "decode_sparkplug_payload", payload_hex="ffffff")
        assert garbage["success"] is False
        assert "Not a valid Sparkplug B payload" in garbage["error"]


async def test_list_sparkplug_nodes_before_discovery():
    async with server() as session:
        result = await call(session, "list_sparkplug_nodes")
    assert result["success"] is True
    assert result["data"]["nodes"] == []
    assert result["data"]["this_server"]["node_born"] is False
    assert "spBv1.0/#" in result["meta"]["note"]


def _offline_manager():
    settings = load_settings({"MQTT_BROKER_URL": f"mqtt://127.0.0.1:{free_port()}"})
    manager = mqtt_server.MQTTClientManager(settings)
    manager.client = mqtt_server.build_client(settings)
    return manager


def test_unconfirmed_qos1_message_is_withdrawn():
    """A QoS 1 publish that paho queued while offline must not go out later."""
    manager = _offline_manager()
    info = manager.client.publish("a/b", b"x", qos=1)
    assert info.rc == mqtt.MQTT_ERR_NO_CONN
    assert info.mid in manager.client._out_messages  # paho would resend it on connect
    assert manager._withdraw(info.mid) is True  # never written: not_sent
    assert info.mid not in manager.client._out_messages
    assert manager._withdraw(info.mid) is None  # nothing left to withdraw


def _queued_packet(mid, pos):
    return {"command": 0x30, "mid": mid, "qos": 0, "pos": pos, "to_process": 1, "packet": b"x"}


def test_withdraw_tells_unsent_from_possibly_sent():
    manager = _offline_manager()
    queue = manager.client._out_packet
    queue.append(_queued_packet(7, 0))  # QoS 0, not started
    assert manager._withdraw(7) is True and not queue
    queue.append(_queued_packet(8, 3))  # QoS 0, partly written
    assert manager._withdraw(8) is False
    queue.clear()
    assert manager._withdraw(9) is None  # QoS 0 already written: nothing held
    info = manager.client.publish("a/b", b"x", qos=1)
    message = manager.client._out_messages[info.mid]
    message.state = mqtt.mqtt_ms_wait_for_puback  # in flight, packet already written
    assert manager._withdraw(info.mid) is False
    info = manager.client.publish("a/b", b"x", qos=1)
    manager.client._out_messages[info.mid].dup = True  # resent after a reconnect
    assert manager._withdraw(info.mid) is False


async def test_birth_refused_without_a_registered_will(monkeypatch):
    """If the CONNECT that carried the will was refused, no NBIRTH may go out."""
    published = []

    async def no_restart(self, bd_seq):  # reconnected, but the broker holds no will
        self.connected = True
        self.will_active = False

    async def connected(self):
        return None

    async def record(self, *args, **kwargs):
        published.append(args)
        return {"mid": 1, "generation": self.generation, "delivery": "x"}

    monkeypatch.setattr(mqtt_server.MQTTClientManager, "_restart_with_will", no_restart)
    monkeypatch.setattr(mqtt_server.MQTTClientManager, "require_connection", connected)
    monkeypatch.setattr(mqtt_server.MQTTClientManager, "publish", record)
    async with server(MQTT_WRITES_ENABLED="true") as session:
        result = await call(session, "publish_node_birth", metrics=[])
    assert result["success"] is False
    assert "no NDEATH will" in result["error"]
    assert result["meta"] == {"outcome": "not_sent", "request_sent": False}
    assert published == []


def test_refused_connect_keeps_the_will_for_the_next_attempt():
    from paho.mqtt.reasoncodes import ReasonCode

    manager = _offline_manager()
    client = manager.client
    client.will_set("spBv1.0/g/NDEATH/n", b"x", qos=1)
    manager._will_bd_seq = 4
    refused = ReasonCode(mqtt.CONNACK >> 4, identifier=0x87)  # not authorized
    manager._on_connect(client, None, None, refused, None)
    manager._on_disconnect(client, None, None, refused, None)
    assert manager._will_bd_seq == 4 and client._will is True
    assert "refused" in manager.last_refusal
    ok = ReasonCode(mqtt.CONNACK >> 4, identifier=0)
    manager._on_connect(client, None, None, ok, None)
    assert manager.will_active is True and manager.bd_seq == 4
    from mqtt_mcp import sparkplug_b_pb2 as pb

    manager.registry.observe("spBv1.0/g/NDATA/other", pb.Payload(seq=1).SerializeToString(), 0)
    lost = ReasonCode(mqtt.DISCONNECT >> 4, identifier=0x80)
    manager.node_born = True
    manager._on_disconnect(client, None, None, lost, None)
    assert manager.will_active is False and manager.node_born is False
    # Deaths published while this server was away are missed: online is now unknown.
    assert manager.registry.snapshot()[0]["online"] is None
    assert client._will is False  # the broker published it; the next birth needs a new one


async def test_message_written_before_a_drop_is_unknown(monkeypatch):
    """Sent, then the connection dropped: never reported as written or not_sent."""

    async def will_registered(self, bd_seq):
        self.connected, self.will_active, self.bd_seq = True, True, bd_seq

    async def connected(self):
        return None

    async def written_then_dropped(self, *args, **kwargs):
        result = {"mid": 1, "generation": self.generation, "delivery": "x"}
        self.generation += 1  # a reconnect happened right after the write
        return result

    monkeypatch.setattr(mqtt_server.MQTTClientManager, "_restart_with_will", will_registered)
    monkeypatch.setattr(mqtt_server.MQTTClientManager, "require_connection", connected)
    monkeypatch.setattr(mqtt_server.MQTTClientManager, "publish", written_then_dropped)
    async with server(MQTT_WRITES_ENABLED="true") as session:
        result = await call(session, "publish_node_birth", metrics=[])
        info = await call(session, "get_broker_info")
    assert result["success"] is False
    assert result["meta"] == {"outcome": "unknown", "request_sent": True}
    assert "may have been applied; check before retrying" in result["error"]
    assert info["data"]["sparkplug"]["node_born"] is False


async def test_every_tool_call_has_an_overall_deadline(monkeypatch):
    """A broker operation that never returns must not hang the call."""

    async def hang(self, *args, **kwargs):
        await asyncio.sleep(3600)

    monkeypatch.setattr(mqtt_server.MQTTClientManager, "publish", hang)
    monkeypatch.setattr(mqtt_server.MQTTClientManager, "subscribe", hang)
    async with server(MQTT_WRITES_ENABLED="true", MQTT_TOOL_TIMEOUT="0.1") as session:
        with anyio.fail_after(20):
            write = await call(session, "publish_message", topic="a/b", payload="x")
            read = await call(session, "subscribe_topic", topic="a/#")
    assert write["success"] is False
    assert write["meta"]["outcome"] == "unknown" and write["meta"]["request_sent"] is True
    assert "overall deadline" in write["error"]
    assert "may have been applied; check before retrying" in write["error"]
    assert read["success"] is False and "overall deadline" in read["error"]
    assert "outcome" not in read["meta"]


class Peer:
    """A TCP peer that answers every connection with garbage (or with nothing)."""

    def __init__(self, reply: bytes | None) -> None:
        self.reply = reply
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen()
        self.port = self.sock.getsockname()[1]
        self.connections: list[socket.socket] = []
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self) -> None:
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            self.connections.append(conn)
            if self.reply is not None:
                try:
                    conn.sendall(self.reply)
                except OSError:
                    pass

    def close(self) -> None:
        self.sock.close()
        for conn in self.connections:
            conn.close()


@pytest.mark.parametrize("reply", [b"\xf0\x05not-mqtt-at-all", None], ids=["garbage", "silent"])
async def test_a_peer_that_is_not_a_broker_gives_clean_failures(reply):
    peer = Peer(reply)
    try:
        async with server(
            MQTT_BROKER_URL=f"mqtt://127.0.0.1:{peer.port}",
            MQTT_WRITES_ENABLED="true",
            MQTT_TOOL_TIMEOUT="0.5",
        ) as session:
            with anyio.fail_after(30):
                info = await call(session, "get_broker_info")
                read = await call(session, "subscribe_topic", topic="a/#")
                write = await call(session, "publish_device_command", device_id="d", metrics=METRIC)
    finally:
        peer.close()
    assert info["data"]["connected"] is False
    assert read["success"] is False and "Not connected" in read["error"]
    assert write["success"] is False
    assert write["meta"] == {"outcome": "not_sent", "request_sent": False, "device_id": "d"}
