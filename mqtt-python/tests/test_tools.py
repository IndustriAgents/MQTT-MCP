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
    "publish_node_command": {
        "edge_node_id": "n",
        "metrics": [{"name": "m", "value": 1, "type": "int32"}],
    },
    "publish_device_command": {
        "edge_node_id": "n",
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
    assert result.isError is (json.loads(result.content[0].text).get("success") is False), (
        result.content
    )
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
    assert init.serverInfo.version == importlib.metadata.version("industriagents-mqtt-mcp")


async def test_tool_list_and_annotations():
    async with server() as session:
        tools = {t.name: t for t in (await session.list_tools()).tools}
    assert set(tools) == READ_TOOLS | {"publish_message", "publish_sparkplug"}
    assert set(WRITE_TOOLS) == mqtt_server.WRITE_TOOLS  # the deadline's write list
    for name in READ_TOOLS:
        assert tools[name].annotations.readOnlyHint is True, name
    for name in ("publish_message", "publish_sparkplug"):
        tool = tools[name]
        assert tool.annotations.readOnlyHint is False, name
        assert tool.annotations.destructiveHint is True, name
        assert tool.annotations.idempotentHint is False, name
        assert "WRITE:" in tool.description, name
        assert "MQTT_WRITES_ENABLED=true" in tool.description, name


async def test_descriptions_do_not_overclaim():
    async with server() as session:
        tools = {t.name: t.description for t in (await session.list_tools()).tools}
    assert "seen in messages this server received" in tools["list_sparkplug_nodes"]
    assert "subscribe to 'spBv1.0/#'" in tools["list_sparkplug_nodes"]
    assert "does not mean the node applied it" in tools["publish_sparkplug"]
    for name in (
        "publish_sparkplug",
        "publish_sparkplug",
        "publish_sparkplug",
        "publish_sparkplug",
        "publish_sparkplug",
        "publish_sparkplug",
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
        {"edge_node_id": "n", "metrics": [{"name": "m", "value": "on", "type": "boolean"}]},
        "true or false",
    ),
    # Commands have no default target.
    ("publish_node_command", {"metrics": METRIC}, "Invalid arguments: edge_node_id"),
    (
        "publish_device_command",
        {"device_id": "d", "metrics": METRIC},
        "Invalid arguments: edge_node_id",
    ),
    (
        "publish_node_birth",
        {"metrics": [{"name": "m", "value": 1, "type": "int32"}] * 2},
        "must be unique",
    ),
    (
        "publish_device_birth",
        {"device_id": "d", "metrics": [{"name": "m", "value": 1, "type": "int32"}] * 2},
        "must be unique",
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
        {"edge_node_id": "n", "metrics": [{"name": "m", "value": True, "type": "int32"}]},
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

    monkeypatch.setattr(mqtt_server.MQTTClientManager, "restart_session", no_restart)
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

    monkeypatch.setattr(mqtt_server.MQTTClientManager, "restart_session", will_registered)
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

    monkeypatch.setattr(mqtt_server.MQTTClientManager, "require_connection", hang)
    monkeypatch.setattr(mqtt_server.MQTTClientManager, "subscribe", hang)
    async with server(MQTT_WRITES_ENABLED="true", MQTT_TOOL_TIMEOUT="0.1") as session:
        with anyio.fail_after(20):
            write = await call(session, "publish_message", topic="a/b", payload="x")
            read = await call(session, "subscribe_topic", topic="a/#")
    # Still waiting for the connection when the deadline hit: nothing was handed to
    # the MQTT client, so it is not_sent (and cannot be sent later).
    assert write["success"] is False
    assert write["meta"]["outcome"] == "not_sent" and write["meta"]["request_sent"] is False
    assert "deadline" in write["error"] and "will not be sent later" in write["error"]
    assert read["success"] is False and "deadline" in read["error"]
    assert "outcome" not in read["meta"]


async def test_deadline_after_the_hand_over_is_unknown(monkeypatch):
    """Handed to the MQTT client, then no confirmation before the deadline: unknown."""

    async def connected(self):
        return None

    async def never_confirmed(self, info, timeout):
        await asyncio.sleep(3600)

    def accepted(self, topic, payload=None, qos=0, retain=False, properties=None):
        info = mqtt.MQTTMessageInfo(4242)
        info.rc = mqtt.MQTT_ERR_SUCCESS
        return info

    monkeypatch.setattr(mqtt_server.MQTTClientManager, "require_connection", connected)
    monkeypatch.setattr(mqtt_server.MQTTClientManager, "_await_confirmation", never_confirmed)
    monkeypatch.setattr(mqtt_server._Client, "publish", accepted)
    async with server(MQTT_WRITES_ENABLED="true", MQTT_TOOL_TIMEOUT="0.1") as session:
        with anyio.fail_after(20):
            write = await call(session, "publish_message", topic="a/b", payload="x")
    assert write["success"] is False
    assert write["meta"]["outcome"] == "unknown" and write["meta"]["request_sent"] is True
    assert "may have been applied; check before retrying" in write["error"]


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
                write = await call(
                    session,
                    "publish_device_command",
                    edge_node_id="n",
                    device_id="d",
                    metrics=METRIC,
                )
    finally:
        peer.close()
    assert info["data"]["connected"] is False
    assert "MQTT broker" in info["data"]["last_error"]  # says why, not null
    assert read["success"] is False and "Not connected" in read["error"]
    assert write["success"] is False
    assert write["meta"] == {"outcome": "not_sent", "request_sent": False, "device_id": "d"}


def test_withdraw_detects_a_packet_written_during_removal():
    """paho pops the head packet, writes part of it and puts it back: a remove()
    that then succeeds has taken a partly written packet (rule 1a: unknown)."""
    import collections

    manager = _offline_manager()

    class RacyDeque(collections.deque):
        def remove(self, packet):
            packet["pos"] = 100  # written by paho's thread in the meantime
            super().remove(packet)

    manager.client._out_packet = RacyDeque([_queued_packet(7, 0)])
    dropped = []
    manager._shutdown_socket = lambda: dropped.append(True)
    assert manager._withdraw(7) is False
    assert dropped == [True]  # the stream lacks the rest of the packet: reconnect


def test_a_dropped_connection_gives_up_inflight_publishes_before_paho_resends():
    from paho.mqtt.reasoncodes import ReasonCode

    manager = _offline_manager()
    client = manager.client
    info = client.publish("a/b", b"x", qos=1)
    client._out_messages[info.mid].state = mqtt.mqtt_ms_wait_for_puback  # written, no PUBACK
    manager._inflight[info.mid] = mqtt_server._InFlight(info)
    lost = ReasonCode(mqtt.DISCONNECT >> 4, identifier=0x80)
    manager._on_disconnect(client, None, None, lost, None)
    flight = manager._inflight[info.mid]
    assert flight.lost is True and flight.withdrawn is False  # outcome unknown
    client._messages_reconnect_reset_out()  # what paho does on reconnect
    assert info.mid not in client._out_messages  # nothing left to resend


async def test_one_deadline_is_shared_by_every_step_of_a_call(monkeypatch):
    """Waiting for the connection uses up part of the call's deadline: the session
    restart in publish_node_birth only gets what is left (never a fresh timeout)."""
    import time as clock

    async def slow_connection(self):
        await asyncio.sleep(1.5)  # most of the 2 s deadline

    monkeypatch.setattr(mqtt_server.MQTTClientManager, "require_connection", slow_connection)
    async with server(MQTT_WRITES_ENABLED="true", MQTT_TOOL_TIMEOUT="2") as session:
        started = clock.monotonic()
        birth = await call(session, "publish_node_birth")
        elapsed = clock.monotonic() - started
    assert birth["success"] is False and birth["meta"]["outcome"] == "not_sent"
    # The restart gave up at the call's deadline, with its own message; a per-step
    # timeout would have run past it and been cut off by the call wrapper instead.
    assert "could not reconnect with the NDEATH will registered" in birth["error"]
    assert elapsed < 2.4


async def test_nothing_is_handed_to_paho_once_the_client_is_gone(monkeypatch):
    manager = _offline_manager()
    manager.connected = True  # pretend a broker is there

    async def connected(self):
        return None

    monkeypatch.setattr(mqtt_server.MQTTClientManager, "require_connection", connected)
    handed = []
    monkeypatch.setattr(manager.client, "publish", lambda *a, **k: handed.append(a))
    manager.close_for_disconnect()
    with pytest.raises(mqtt_server.PublishError) as raised:
        await manager.publish("a/b", b"x", qos=1)
    assert raised.value.outcome == "not_sent"
    assert handed == []  # paho never saw it


async def test_a_session_restart_does_not_reconnect_once_closing(monkeypatch):
    manager = _offline_manager()
    manager._loop = asyncio.get_running_loop()
    stopped = []
    monkeypatch.setattr(
        manager, "_stop_network", lambda *a: stopped.append(manager.close_for_disconnect())
    )
    with pytest.raises(mqtt_server.PublishError, match="shutting down"):
        await manager.restart_session(3)  # the client goes away during the restart
    assert stopped and manager.client._will is False  # no will set, no reconnect
    assert manager.client._thread is None  # paho's network loop was not started


async def test_stop_waits_for_a_running_session_restart(monkeypatch):
    manager = _offline_manager()
    manager._loop = asyncio.get_running_loop()
    import time as clock

    calls = []

    def stop_network(*args):
        calls.append(args)
        if len(calls) == 1:
            clock.sleep(0.5)  # only the restart's own stop is slow

    monkeypatch.setattr(manager, "_stop_network", stop_network)
    restart = asyncio.ensure_future(manager.restart_session(None))
    await asyncio.sleep(0.1)
    await manager.stop()
    assert manager._restart_task.done()  # stop() did not leave it running
    with pytest.raises(mqtt_server.PublishError):
        await restart  # it saw closing and did not reconnect


def _cancel_request_now():
    """What RequestResponder.cancel() does synchronously: cancel the request's scope.

    The reply ("Request cancelled") is sent from a task, as cancel() does after
    its first await; the caller continues in the same loop step, like a task
    whose wait (lock, connection) has just completed.
    """
    context = mqtt_server.mcp._mcp_server.request_context
    responder = context.session._in_flight[context.request_id]
    responder._cancel_scope.cancel()
    asyncio.get_running_loop().create_task(responder.cancel())


async def test_the_mcp_request_cancel_scope_is_captured(monkeypatch):
    """Fails if the mcp internals the cancel check relies on move (mcp<2 is pinned)."""
    from mcp.shared.session import RequestResponder

    seen = []

    async def record(self):
        seen.append(mqtt_server._send_state.get().request_scope)

    monkeypatch.setattr(mqtt_server.MQTTClientManager, "require_connection", record)
    monkeypatch.setattr(mqtt_server._Client, "publish", lambda *a, **k: mqtt.MQTTMessageInfo(1))
    async with server(MQTT_WRITES_ENABLED="true") as session:
        await call(session, "publish_message", topic="a/b", payload="x", qos=1)
    assert len(seen) == 1
    assert isinstance(seen[0], anyio.CancelScope)
    assert seen[0].cancel_called is False
    assert "_cancel_scope" in RequestResponder.__init__.__code__.co_names


async def test_a_cancel_in_the_same_step_as_the_connection_never_reaches_paho(monkeypatch):
    """The cancel race: the connection wait completes and the client's cancel arrives
    in the same loop step; anyio would not interrupt before the hand-over to paho."""
    from mcp.shared.exceptions import McpError

    handed = []

    async def connected_then_cancelled(self):
        _cancel_request_now()  # no checkpoint follows: the task runs on

    def spy(self, topic, payload=None, qos=0, retain=False, properties=None):
        handed.append(topic)
        info = mqtt.MQTTMessageInfo(1)
        info.rc = mqtt.MQTT_ERR_NO_CONN
        return info

    monkeypatch.setattr(
        mqtt_server.MQTTClientManager, "require_connection", connected_then_cancelled
    )
    monkeypatch.setattr(mqtt_server._Client, "publish", spy)
    calls = [
        ("publish_message", {"topic": "a/b", "payload": "x", "qos": 1}),
        ("publish_message", {"topic": "a/b", "payload": "x", "qos": 0}),
        ("publish_device_command", {"edge_node_id": "n", "device_id": "d", "metrics": METRIC}),
        ("publish_node_command", {"edge_node_id": "n", "metrics": METRIC}),
    ]
    async with server(MQTT_WRITES_ENABLED="true") as session:
        with anyio.fail_after(60):
            for _ in range(10):
                for name, arguments in calls:
                    with pytest.raises(McpError, match="cancelled"):
                        await session.call_tool(name, arguments)
                    await session.send_ping()  # one reply only: the server is still fine
            info = await call(session, "get_broker_info")
    assert handed == []
    assert info["success"] is True


async def test_a_call_that_finishes_after_its_cancel_does_not_reply_twice(monkeypatch, caplog):
    """The SDK replied "Request cancelled" (RequestResponder.cancel()); the tool then
    finishes anyway. A second reply would crash the SDK's task group; the outcome
    goes to stderr and the server keeps answering."""
    from mcp.shared.exceptions import McpError

    async def written_then_cancelled(self, topic, payload, qos=0, retain=False, **kwargs):
        _cancel_request_now()
        return {"mid": 1, "generation": self.generation, "delivery": "x"}

    monkeypatch.setattr(mqtt_server.MQTTClientManager, "publish", written_then_cancelled)
    async with server(MQTT_WRITES_ENABLED="true") as session:
        with anyio.fail_after(30), caplog.at_level("WARNING", logger="mqtt_mcp"):
            for _ in range(5):
                with pytest.raises(McpError, match="cancelled"):
                    await session.call_tool(
                        "publish_message", {"topic": "a/b", "payload": "x", "qos": 1}
                    )
                await session.send_ping()
            info = await call(session, "get_broker_info")
    assert info["success"] is True
    assert any(
        "cancelled by the MCP client but finished: written" in r.message for r in caplog.records
    )


async def test_a_connection_that_arrives_after_the_deadline_sends_nothing(monkeypatch):
    """The wait for the connection ends just after the call's deadline, connected:
    the message must not be handed to paho any more (not_sent)."""
    handed = []

    async def connected_too_late(self):
        while self.time_left() > 0:
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.02)  # the CONNACK came 20 ms after the deadline

    def spy(self, topic, payload=None, qos=0, retain=False, properties=None):
        handed.append(topic)
        info = mqtt.MQTTMessageInfo(1)
        info.rc = mqtt.MQTT_ERR_SUCCESS
        return info

    monkeypatch.setattr(mqtt_server.MQTTClientManager, "require_connection", connected_too_late)
    monkeypatch.setattr(mqtt_server._Client, "publish", spy)
    async with server(MQTT_WRITES_ENABLED="true", MQTT_TOOL_TIMEOUT="0.3") as session:
        for qos in (0, 1):
            result = await call(session, "publish_message", topic="a/b", payload="x", qos=qos)
            assert result["meta"] == {"outcome": "not_sent", "request_sent": False, "topic": "a/b"}
            assert "deadline" in result["error"]
    assert handed == []


async def test_closing_stdin_marks_the_manager_closing(monkeypatch):
    """The wiring from the stdio relay's end of file to the MQTT manager."""
    closed = []
    original = mqtt_server.MQTTClientManager.close_for_disconnect

    def record(self):
        closed.append(self)
        original(self)

    monkeypatch.setattr(mqtt_server.MQTTClientManager, "close_for_disconnect", record)
    try:
        async with server() as session:
            await call(session, "get_broker_info")
            mqtt_server.mcp.client_disconnected()  # what the relay calls at end of file
            assert len(closed) == 1 and closed[0].closing is True
    finally:
        mqtt_server.mcp.client_gone = False


async def test_a_seq_that_may_have_gone_out_is_never_reused():
    """_publish_edge cancelled after the hand-over: the seq counts as used."""
    manager = _offline_manager()
    manager.will_active = manager.node_born = True
    manager.seq = 5
    state = mqtt_server.SendState()
    token = mqtt_server._send_state.set(state)

    async def cancelled_after(outcome):
        async def publish(*args, **kwargs):
            state.sent, state.outcome = True, outcome
            raise asyncio.CancelledError

        manager.publish = publish
        with pytest.raises(asyncio.CancelledError):
            await mqtt_server._publish_edge(manager, "NDATA", METRIC)

    try:
        await cancelled_after("unknown")  # may have gone out with seq 6
        assert manager.seq == 6
        await cancelled_after("not_sent")  # withdrawn: seq 7 was never used
        assert manager.seq == 6
    finally:
        mqtt_server._send_state.reset(token)


async def test_commands_refuse_an_empty_target():
    async with server(MQTT_WRITES_ENABLED="true") as session:
        for arguments in (
            {"edge_node_id": "", "metrics": METRIC},
            {"edge_node_id": "n", "group_id": "", "metrics": METRIC},
        ):
            result = await call(session, "publish_node_command", **arguments)
            assert result["success"] is False and "non-empty" in result["error"], arguments
            assert result["meta"]["outcome"] == "not_sent"
        device = await call(
            session, "publish_device_command", edge_node_id="", device_id="d", metrics=METRIC
        )
        assert device["success"] is False and "edge_node_id" in device["error"]


def _birth_test_patches(monkeypatch, between):
    """Patch publish_node_birth's path so that ``between(manager)`` runs after the
    session restart has stopped the network and before it reconnects. Returns the
    list of topics handed to paho and a holder for the manager."""
    handed, holder = [], {}
    real_to_thread = asyncio.to_thread

    async def connected(self):
        return None

    async def stop_then(fn, *args):
        if "manager" in holder:  # later calls (shutdown) run normally
            return await real_to_thread(fn, *args)
        fn(*args)  # the restart's _stop_network
        holder["manager"] = fn.__self__
        await between(fn.__self__)
        await asyncio.sleep(0)  # like the real to_thread, end on a scheduling step

    def spy(self, topic, payload=None, qos=0, retain=False, properties=None):
        handed.append(topic)
        info = mqtt.MQTTMessageInfo(1)
        info.rc = mqtt.MQTT_ERR_NO_CONN
        return info

    monkeypatch.setattr(mqtt_server.MQTTClientManager, "require_connection", connected)
    monkeypatch.setattr(mqtt_server.MQTTClientManager, "_stop_network", lambda self, *a: None)
    monkeypatch.setattr(asyncio, "to_thread", stop_then)
    monkeypatch.setattr(mqtt_server._Client, "publish", spy)
    return handed, holder


async def test_a_birth_cancelled_during_the_session_restart_registers_no_will(monkeypatch):
    """Cancelled between stopping the network and reconnecting: reconnect without the
    NDEATH will, and no NBIRTH goes out."""
    from mcp.shared.exceptions import McpError

    async def cancel(manager):
        # What the SDK does on notifications/cancelled: RequestResponder.cancel()
        # cancels the request's scope and replies "Request cancelled".
        context = mqtt_server.mcp._mcp_server.request_context
        await context.session._in_flight[context.request_id].cancel()

    handed, holder = _birth_test_patches(monkeypatch, cancel)
    async with server(MQTT_WRITES_ENABLED="true") as session:
        with anyio.fail_after(20):
            with pytest.raises(McpError, match="cancelled"):
                await session.call_tool("publish_node_birth", {})
            await session.send_ping()
        manager = holder["manager"]
        assert manager.client._will is False  # no NDEATH will registered
        assert manager._will_bd_seq is None and manager.will_active is False
        assert manager.node_born is False
    assert not any("/NBIRTH/" in topic for topic in handed)


async def test_a_birth_whose_deadline_passes_during_the_restart_registers_no_will(monkeypatch):
    """The call's deadline passes between stopping the network and reconnecting:
    reconnect without the NDEATH will, no NBIRTH, outcome not_sent."""

    async def run_out_the_deadline(manager):
        await asyncio.sleep(manager.time_left() + 0.02)

    handed, holder = _birth_test_patches(monkeypatch, run_out_the_deadline)
    async with server(MQTT_WRITES_ENABLED="true", MQTT_TOOL_TIMEOUT="0.4") as session:
        with anyio.fail_after(20):
            birth = await call(session, "publish_node_birth")
        manager = holder["manager"]
        assert manager.client._will is False
        assert manager._will_bd_seq is None and manager.will_active is False
    assert birth["success"] is False
    assert birth["meta"]["outcome"] == "not_sent" and birth["meta"]["request_sent"] is False
    assert "no NDEATH will was registered" in birth["error"]
    assert not any("/NBIRTH/" in topic for topic in handed)
