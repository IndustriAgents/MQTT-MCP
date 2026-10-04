"""Tool behaviour that needs no broker: write gate, envelopes, validation, descriptions.

The server runs in-process (mcp's in-memory transport) against a port where no
broker listens.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager

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
        async with create_connected_server_and_client_session(mqtt_server.mcp) as session:
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
    from mqtt_mcp import __version__

    mqtt_server.set_settings(load_settings({"MQTT_BROKER_URL": f"mqtt://127.0.0.1:{free_port()}"}))
    try:
        async with create_connected_server_and_client_session(mqtt_server.mcp) as session:
            init = await session.initialize()
    finally:
        mqtt_server.set_settings(None)
    assert init.serverInfo.name == "MQTT-Control"
    assert init.serverInfo.version == __version__


async def test_tool_list_and_annotations():
    async with server() as session:
        tools = {t.name: t for t in (await session.list_tools()).tools}
    assert set(tools) == READ_TOOLS | set(WRITE_TOOLS)
    for name in READ_TOOLS:
        assert tools[name].annotations.readOnlyHint is True, name
    for name in WRITE_TOOLS:
        tool = tools[name]
        assert tool.annotations.readOnlyHint is False, name
        assert tool.annotations.destructiveHint is True, name
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


async def test_shared_subscriptions_need_writes():
    async with server() as session:
        result = await call(session, "subscribe_topic", topic="$share/group/sensors/#")
    assert result["success"] is False
    assert "MQTT_WRITES_ENABLED" in result["error"]


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
]


async def test_bad_arguments_return_success_false():
    async with server(MQTT_WRITES_ENABLED="true") as session:
        for tool, arguments, message in BAD_ARGUMENTS:
            result = await call(session, tool, **arguments)
            assert result["success"] is False, (tool, arguments)
            assert message in result["error"], (tool, arguments, result["error"])


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


def test_unconfirmed_qos1_message_is_withdrawn():
    """A QoS 1 publish that paho queued while offline must not go out later."""
    settings = load_settings({"MQTT_BROKER_URL": f"mqtt://127.0.0.1:{free_port()}"})
    manager = mqtt_server.MQTTClientManager(settings)
    manager.client = mqtt_server.build_client(settings)
    info = manager.client.publish("a/b", b"x", qos=1)
    assert info.rc == mqtt.MQTT_ERR_NO_CONN
    assert info.mid in manager.client._out_messages  # paho would resend it on connect
    assert manager._withdraw(info.mid) is True
    assert info.mid not in manager.client._out_messages
    assert manager._withdraw(info.mid) is False
