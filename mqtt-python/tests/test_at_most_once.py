"""At most once: a message handed to the server is sent once or never, whatever
happens to the call (client cancel, client disconnect, connection drop, session
restart). Needs a broker; every test watches the broker with an observer."""

from __future__ import annotations

import asyncio
import json
import time

import anyio
import pytest

from mqtt_mcp.sparkplug import decode_payload

from .conftest import call, eventually, mcp_session, unique, wait_connected_raw

pytestmark = [pytest.mark.anyio, pytest.mark.integration]

COMMAND = [{"name": "pump_speed", "value": 77, "type": "int32"}]


def _start_writes(srv, tag, group):
    """Four writes that will still be waiting for the broker."""
    return [
        srv.call_async("publish_message", topic=f"itest-once/{tag}/q1", payload="never", qos=1),
        srv.call_async("publish_message", topic=f"itest-once/{tag}/q0", payload="never", qos=0),
        srv.call_async(
            "publish_device_command",
            edge_node_id="edge-node-1",
            device_id="device-2",
            group_id=group,
            metrics=COMMAND,
        ),
        srv.call_async("publish_node_birth"),
    ]


async def test_nothing_is_sent_after_the_client_disconnects(
    broker, observer, proxy_factory, raw_server
):
    """Broker down, calls waiting, stdin closes, then the broker comes up."""
    proxy = proxy_factory()  # not listening yet: the broker is down
    tag, group = unique("eof"), unique("eofg")
    observer.subscribe(f"itest-once/{tag}/#")
    observer.subscribe(f"spBv1.0/{group}/#")
    srv = raw_server(
        MQTT_BROKER_URL=f"mqtt://127.0.0.1:{proxy.port}",
        MQTT_WRITES_ENABLED="true",
        MQTT_TOOL_TIMEOUT="5",
        SPARKPLUG_GROUP_ID=group,
    )
    _start_writes(srv, tag, group)
    await asyncio.sleep(0.5)
    srv.close_stdin()
    await asyncio.sleep(0.5)
    proxy.start()
    status = await asyncio.to_thread(srv.wait_exit, 30)
    await asyncio.sleep(3)
    assert observer.find(lambda m: tag in m.topic or group in m.topic) == []
    assert status == 0, srv.stderr()
    assert "Traceback" not in srv.stderr()


async def test_cancelled_calls_are_never_sent(broker, observer, proxy_factory, raw_server):
    """notifications/cancelled while the calls wait for the broker."""
    proxy = proxy_factory()
    tag, group = unique("cancel"), unique("cancelg")
    observer.subscribe(f"itest-once/{tag}/#")
    observer.subscribe(f"spBv1.0/{group}/#")
    srv = raw_server(
        MQTT_BROKER_URL=f"mqtt://127.0.0.1:{proxy.port}",
        MQTT_WRITES_ENABLED="true",
        MQTT_TOOL_TIMEOUT="5",
        SPARKPLUG_GROUP_ID=group,
    )
    calls = _start_writes(srv, tag, group)
    await asyncio.sleep(0.5)
    for request_id in calls:
        srv.cancel(request_id)
    await asyncio.sleep(0.5)
    proxy.start()
    await wait_connected_raw(srv)
    await asyncio.sleep(3)
    assert observer.find(lambda m: tag in m.topic or group in m.topic) == []


async def test_a_cancelled_publish_queued_behind_a_backlog_is_withdrawn(
    broker, observer, proxy_factory, raw_server
):
    """Handed to the MQTT client, still queued behind big messages, then cancelled."""
    proxy = proxy_factory().start()
    tag, group = unique("backlog"), unique("backlogg")
    observer.subscribe(f"itest-once/{tag}/#")
    observer.subscribe(f"spBv1.0/{group}/#")
    srv = raw_server(
        MQTT_BROKER_URL=f"mqtt://127.0.0.1:{proxy.port}",
        MQTT_WRITES_ENABLED="true",
        MQTT_TOOL_TIMEOUT="30",
        SPARKPLUG_GROUP_ID=group,
    )
    await wait_connected_raw(srv)
    proxy.pause_upstream = True  # the socket backs up
    big = "x" * 500_000  # below Mosquitto 2.1's default packet size limit
    for i in range(20):
        srv.call_async("publish_message", topic=f"itest-once/{tag}/big{i}", payload=big)
    await asyncio.sleep(2)
    small = srv.call_async(
        "publish_message", topic=f"itest-once/{tag}/small", payload="never", qos=1
    )
    command = srv.call_async(
        "publish_device_command",
        edge_node_id="edge-node-1",
        device_id="device-2",
        group_id=group,
        metrics=COMMAND,
    )
    await asyncio.sleep(0.5)
    srv.cancel(small)
    srv.cancel(command)
    await asyncio.sleep(0.5)
    proxy.pause_upstream = False  # the backlog drains
    await observer.wait_for(lambda m: m.topic == f"itest-once/{tag}/big19", timeout=60)
    await asyncio.sleep(2)
    assert observer.find(lambda m: m.topic.endswith("/small")) == []
    assert observer.find(lambda m: "/DCMD/" in m.topic) == []


async def test_a_connection_drop_during_a_publish_never_duplicates_it(
    broker, observer, proxy_factory, tmp_path
):
    """No PUBACK, then the connection drops while the call waits: paho would resend
    the message on reconnect; the server gives it up instead (outcome unknown)."""
    proxy = proxy_factory().start()
    topic = f"itest-once/{unique('drop')}/valve"
    observer.subscribe(topic)
    async with mcp_session(
        tmp_path,
        MQTT_BROKER_URL=f"mqtt://127.0.0.1:{proxy.port}",
        MQTT_WRITES_ENABLED="true",
        MQTT_TOOL_TIMEOUT="10",
    ) as session:

        async def connected():
            return (await call(session, "get_broker_info"))["data"]["connected"]

        await eventually(connected)
        proxy.hold_downstream = True  # the broker's PUBACK never arrives
        result = {}

        async def publish():
            result["envelope"] = await call(
                session, "publish_message", topic=topic, payload="open", qos=1
            )

        async with anyio.create_task_group() as tg:
            tg.start_soon(publish)
            await observer.wait_for(lambda m: m.topic == topic)  # the broker has it
            dropped_at = time.monotonic()
            proxy.hold_downstream = False
            proxy.drop_connections()
        envelope = result["envelope"]
        assert time.monotonic() - dropped_at < 5  # ended by the drop, not the 10 s timeout
        assert envelope["success"] is False
        assert envelope["meta"]["outcome"] == "unknown"
        assert envelope["meta"]["request_sent"] is True
        await eventually(connected, timeout=20)
        await asyncio.sleep(2)
    assert len(observer.find(lambda m: m.topic == topic)) == 1


async def test_a_session_restart_never_duplicates_an_inflight_publish(
    broker, observer, proxy_factory, tmp_path
):
    """publish_node_birth reconnects (to register the will) while a QoS 1 publish is
    still waiting for its PUBACK: the publish ends unknown and is not resent."""
    proxy = proxy_factory().start()
    group = unique("restart")
    topic = f"itest-once/{group}/valve"
    observer.subscribe(topic)
    async with mcp_session(
        tmp_path,
        MQTT_BROKER_URL=f"mqtt://127.0.0.1:{proxy.port}",
        MQTT_WRITES_ENABLED="true",
        MQTT_TOOL_TIMEOUT="10",
        SPARKPLUG_GROUP_ID=group,
    ) as session:

        async def connected():
            return (await call(session, "get_broker_info"))["data"]["connected"]

        await eventually(connected)
        proxy.hold_downstream = True
        result = {}

        async def publish():
            result["envelope"] = await call(
                session, "publish_message", topic=topic, payload="open", qos=1
            )

        async with anyio.create_task_group() as tg:
            tg.start_soon(publish)
            await observer.wait_for(lambda m: m.topic == topic)
            proxy.hold_downstream = False
            birth = await call(session, "publish_node_birth")
        assert birth["success"] is True and birth["meta"]["reconnected_with_will"] is True
        assert result["envelope"]["meta"]["outcome"] == "unknown"
        await asyncio.sleep(2)
    assert len(observer.find(lambda m: m.topic == topic)) == 1


async def test_node_death_reconnects_without_the_will(broker, observer, proxy_factory, tmp_path):
    """After an explicit NDEATH a later crash must not announce a second death."""
    proxy = proxy_factory().start()
    group = unique("death")
    ndeath = f"spBv1.0/{group}/NDEATH/mqtt-mcp"
    observer.subscribe(f"spBv1.0/{group}/#")
    async with mcp_session(
        tmp_path,
        MQTT_BROKER_URL=f"mqtt://127.0.0.1:{proxy.port}",
        MQTT_WRITES_ENABLED="true",
        SPARKPLUG_GROUP_ID=group,
    ) as session:
        assert (await call(session, "publish_node_birth"))["data"]["bd_seq"] == 0
        death = await call(session, "publish_node_death")
        assert death["success"] is True and "reconnect" not in death["meta"]
        info = (await call(session, "get_broker_info"))["data"]
        assert info["sparkplug"]["ndeath_will_registered"] is False
        await observer.wait_for(lambda m: m.topic == ndeath)
        proxy.drop_connections()  # a crash now: no will is registered

        async def connected():
            return (await call(session, "get_broker_info"))["data"]["connected"]

        await eventually(connected, timeout=20)
        await asyncio.sleep(1.5)
        assert len(observer.find(lambda m: m.topic == ndeath)) == 1
        rebirth = await call(session, "publish_node_birth")
        assert rebirth["data"]["bd_seq"] == 1  # a new session, a new bdSeq
        assert decode_payload(observer.find(lambda m: m.topic == ndeath)[0].payload)["seq"] is None


@pytest.mark.parametrize("writes", ["false", "true"])
async def test_a_server_without_a_born_node_publishes_nothing_at_exit(
    broker, observer, tmp_path, writes
):
    group = unique("quiet")
    observer.subscribe(f"spBv1.0/{group}/#")
    async with mcp_session(
        tmp_path,
        MQTT_BROKER_URL=broker.url,
        MQTT_WRITES_ENABLED=writes,
        SPARKPLUG_GROUP_ID=group,
    ) as session:

        async def connected():
            return (await call(session, "get_broker_info"))["data"]["connected"]

        await eventually(connected)
    await asyncio.sleep(2)
    assert observer.find(lambda m: group in m.topic) == []


async def test_the_client_disconnecting_cancels_running_calls_at_once(
    broker, proxy_factory, raw_server
):
    """Calls waiting for a broker that never comes do not hold the server up."""
    proxy = proxy_factory()  # never started
    tag, group = unique("prompt"), unique("promptg")
    srv = raw_server(
        MQTT_BROKER_URL=f"mqtt://127.0.0.1:{proxy.port}",
        MQTT_WRITES_ENABLED="true",
        MQTT_TOOL_TIMEOUT="30",
        SPARKPLUG_GROUP_ID=group,
    )
    _start_writes(srv, tag, group)
    await asyncio.sleep(0.5)
    closed_at = time.monotonic()
    srv.close_stdin()
    status = await asyncio.to_thread(srv.wait_exit, 40)
    assert time.monotonic() - closed_at < 8  # not after the 30 s deadline
    assert status == 0, srv.stderr()


async def test_a_cancel_racing_the_hand_over_never_reaches_the_broker(
    broker, observer, monkeypatch
):
    """The cancel race against a real broker: the connection check completes and the
    request is cancelled in the same loop step, 60 times; nothing may arrive."""
    from mcp.shared.exceptions import McpError
    from mcp.shared.memory import create_connected_server_and_client_session

    from mqtt_mcp import mqtt_server
    from mqtt_mcp.config import load_settings

    from .test_tools import _cancel_request_now

    topic = f"itest-once/{unique('race')}"
    observer.subscribe(f"{topic}/#")
    real_require = mqtt_server.MQTTClientManager.require_connection

    async def connected_then_cancelled(self):
        await real_require(self)  # already connected: returns without a checkpoint
        _cancel_request_now()

    monkeypatch.setattr(
        mqtt_server.MQTTClientManager, "require_connection", connected_then_cancelled
    )
    mqtt_server.set_settings(
        load_settings({"MQTT_BROKER_URL": broker.url, "MQTT_WRITES_ENABLED": "true"})
    )
    try:
        async with create_connected_server_and_client_session(
            mqtt_server.mcp._mcp_server
        ) as session:
            with anyio.fail_after(120):
                for i in range(30):
                    for qos in (0, 1):
                        with pytest.raises(McpError, match="cancelled"):
                            await session.call_tool(
                                "publish_message",
                                {"topic": f"{topic}/{i}-{qos}", "payload": "never", "qos": qos},
                            )
                await session.send_ping()
            await asyncio.sleep(1.5)
    finally:
        mqtt_server.set_settings(None)
    assert observer.find(lambda m: m.topic.startswith(topic)) == []


async def test_calls_whose_connection_arrives_after_their_deadline_send_nothing(
    broker, observer, proxy_factory, raw_server
):
    """20 concurrent writes wait for the connection; the broker's CONNACK is held
    back until just after their deadline. None may be handed over late."""
    deadline = 2.0
    for trial in range(2):
        tag = unique(f"boundary{trial}")
        observer.subscribe(f"itest-once/{tag}/#")
        proxy = proxy_factory().start()
        proxy.pause_downstream = True  # the CONNECT gets through, the CONNACK waits
        srv = raw_server(
            trace_deadlines=True,
            MQTT_BROKER_URL=f"mqtt://127.0.0.1:{proxy.port}",
            MQTT_WRITES_ENABLED="true",
            MQTT_TOOL_TIMEOUT=str(deadline),
        )
        calls = [
            srv.call_async(
                "publish_message", topic=f"itest-once/{tag}/late{i}", payload="late", qos=i % 2
            )
            for i in range(20)
        ]
        # Measure the actual deadlines after all server tasks have entered the
        # tool wrapper. Client enqueue time is earlier and is not an admission
        # barrier: under load the twentieth request can start >20 ms later.
        admitted = []
        admission_timeout = time.monotonic() + 10
        while len(admitted) != len(calls) and time.monotonic() < admission_timeout:
            if srv.deadlines_path.exists():
                lines = srv.deadlines_path.read_text().split("\n")[:-1]
                admitted = [json.loads(line) for line in lines]
            await asyncio.sleep(0.001)
        assert len(admitted) == len(calls), srv.stderr()
        assert {entry["topic"] for entry in admitted} == {
            f"itest-once/{tag}/late{i}" for i in range(20)
        }
        latest_deadline = max(entry["deadline"] for entry in admitted)
        while time.monotonic() < latest_deadline + 0.02:
            await asyncio.sleep(0.0005)
        proxy.pause_downstream = False  # connected, 20 ms after the deadline
        outcomes = []
        for request_id in calls:
            response = await asyncio.to_thread(srv.wait, request_id, 10)
            outcomes.append(response["result"]["structuredContent"]["meta"]["outcome"])
        await asyncio.sleep(1.0)
        arrived = [m.topic for m in observer.find(lambda m: m.topic.startswith("itest-once/"))]
        assert [t for t in arrived if tag in t] == [], f"trial {trial}"
        assert set(outcomes) == {"not_sent"}, outcomes
        srv.close_stdin()
        await asyncio.to_thread(srv.wait_exit, 30)
