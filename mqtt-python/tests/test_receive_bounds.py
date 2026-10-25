"""Receive budgets cover decoded Sparkplug state, not just truncated raw bytes."""

import pytest

from mqtt_mcp import sparkplug_b_pb2 as pb
from mqtt_mcp.config import ConfigError, load_settings
from mqtt_mcp.inbox import MessageBuffer, SparkplugRegistry


def birth(value="value", alias=7):
    payload = pb.Payload(seq=0)
    payload.metrics.add(name="metric", datatype=pb.String, string_value=value, alias=alias)
    return payload.SerializeToString()


def test_buffer_evicts_on_byte_budget_and_reports_loss():
    buffer = MessageBuffer(1000, max_bytes=800)
    for i in range(10):
        buffer.add("x", b"x" * 200, 0, False, float(i))
    messages, meta = buffer.query(None, 100, 0)
    assert len(messages) < 10 and meta["missed_messages"]
    assert meta["dropped_total"] == 10 - len(messages)
    assert meta["buffered_bytes"] <= 800
    assert meta["max_buffer_bytes"] == 800


def test_buffer_accounts_decoded_metadata_and_drops_oversized_entry():
    buffer = MessageBuffer(5, max_bytes=500)
    buffer.add("x", b"x", 0, False, 0.0, sparkplug={"metrics": [{"value": "x" * 1000}]})
    messages, meta = buffer.query(None, 5, 0)
    assert messages == [] and meta["dropped_total"] == 1 and meta["missed_messages"]


def test_registry_lru_evicts_alias_types_with_the_node():
    registry = SparkplugRegistry(max_nodes=1)
    registry.observe("spBv1.0/g/NBIRTH/a", birth(), 0.0)
    registry.observe("spBv1.0/g/NBIRTH/b", birth(), 1.0)
    assert [n["edge_node_id"] for n in registry.snapshot()] == ["b"]
    assert not any(key[:2] == ("g", "a") for key in registry._types)
    assert registry.status()["evicted_nodes"] == 1
    assert registry.status()["complete"] is False


def test_registry_byte_limit_covers_devices_and_accumulated_metrics():
    registry = SparkplugRegistry(max_bytes=1100)
    registry.observe("spBv1.0/g/NBIRTH/a", birth("a" * 50), 0.0)
    for i in range(20):
        registry.observe(f"spBv1.0/g/DBIRTH/a/device{i}", birth("x" * 100), float(i))
        assert registry.status()["stored_bytes"] <= 1100
    assert registry.status()["evicted_nodes"] > 0


def test_registry_rejects_oversized_payload_before_decoding(monkeypatch):
    registry = SparkplugRegistry(max_payload=10)

    def fail(*args):
        pytest.fail("oversized payload was decoded")

    monkeypatch.setattr("mqtt_mcp.inbox.sparkplug.decode_payload", fail)
    decoded, error = registry.observe("spBv1.0/g/NBIRTH/a", b"x" * 11, 0.0)
    assert decoded is None and "decode limit" in error
    assert registry.status()["rejected_payloads"] == 1
    assert registry.snapshot() == []


@pytest.mark.parametrize("value", ["NaN", "inf", "-inf", "Infinity"])
def test_nonfinite_deadline_is_rejected(value):
    with pytest.raises(ConfigError):
        load_settings({"MQTT_TOOL_TIMEOUT": value})


@pytest.mark.parametrize(
    "url",
    [
        "mqtt://user:secret-value@host:bad",
        "unknown://user:secret-value@host",
        "mqtt://[secret-value",
        "mqtt://host/?token=secret-value",
    ],
)
def test_malformed_url_never_echoes_credentials(url):
    with pytest.raises(ConfigError) as error:
        load_settings({"MQTT_BROKER_URL": url})
    assert "secret-value" not in str(error.value)


@pytest.mark.anyio
async def test_diagnostics_expose_budget_loss_and_backend_support():
    from .test_tools import call, server

    async with server() as session:
        info = await call(session, "get_broker_info")
        assert info["data"]["capabilities"]["device_verified"] is False
        assert info["data"]["registry"]["max_bytes"] > 0
        nodes = await call(session, "list_sparkplug_nodes")
        assert nodes["meta"]["registry"]["complete"] is True


def test_invalid_payloads_and_connection_gaps_make_observation_incomplete():
    registry = SparkplugRegistry()
    registry.observe("spBv1.0/g/NBIRTH/a", birth(), 0.0)
    registry.observe("spBv1.0/g/NDATA/a", b"\xff", 1.0)
    assert registry.status()["rejected_payloads"] == 1
    assert registry.status()["complete"] is False
    registry.mark_unknown()
    assert registry.status()["connection_gaps"] == 1
    assert registry.snapshot()[0]["online"] is None
    assert registry.status()["scope"] == "observed_messages_since_start"


def test_read_only_policy_denies_subscription_mutation():
    from mqtt_mcp.domain.policy import Policy
    from mqtt_mcp.protocol.contract import envelope_of

    from .test_architecture_contract import boundary, run

    b, calls = boundary(Policy(read_only=True))
    for topic in ("plant/#", "$share/group/plant/#", "$exclusive/plant/x"):
        for name in ("subscribe_topic", "unsubscribe_topic"):
            result = envelope_of(run(b.call_tool(name, {"topic": topic})))
            assert result["success"] is False and result["meta"]["outcome"] == "not_sent"
    assert calls == []


def test_buffer_reports_rejected_ids_between_retained_messages():
    buffer = MessageBuffer(5, max_bytes=800)
    first = buffer.add("x", b"ok", 0, False, 0.0)
    rejected = buffer.add("x", b"x", 0, False, 1.0, sparkplug={"value": "x" * 1000})
    last = buffer.add("x", b"ok", 0, False, 2.0)
    messages, meta = buffer.query(None, 5, first.id)
    assert [m["id"] for m in messages] == [last.id]
    assert meta["missed_messages"] is True
    assert meta["dropped_total"] == 1
    assert "missed_messages" not in buffer.query(None, 5, rejected.id)[1]


@pytest.mark.anyio
async def test_wide_values_survive_local_mcp_decode_and_received_inbox():
    import json
    from types import SimpleNamespace

    from mqtt_mcp.application.read import read_messages

    from .conftest import check_envelope
    from .test_tools import _offline_manager, call, server

    payload = pb.Payload(timestamp=2**64 - 1, seq=255)
    payload.metrics.add(name="count", datatype=pb.UInt64, long_value=2**64 - 1, alias=2**64 - 1)
    encoded = payload.SerializeToString()
    async with server() as session:
        decoded = await call(session, "decode_sparkplug_payload", payload_hex=encoded.hex())
    metric = decoded["data"]["metrics"][0]
    assert metric["value"] == str(2**64 - 1) and metric["value_encoding"] == "decimal"
    assert metric["alias"] == str(2**64 - 1)
    assert decoded["data"]["timestamp"] == str(2**64 - 1)
    manager = _offline_manager()
    manager._on_message(
        manager.client,
        None,
        SimpleNamespace(
            topic="spBv1.0/g/NBIRTH/n",
            payload=encoded,
            qos=0,
            retain=False,
        ),
    )
    received = read_messages(manager)
    check_envelope("read_messages", received)
    assert received["data"][0]["sparkplug"]["metrics"][0] == metric
    json.dumps(received, allow_nan=False)


def test_receive_failure_redacts_error_and_records_loss(monkeypatch):
    import json
    from types import SimpleNamespace

    from .test_tools import _offline_manager

    manager = _offline_manager()

    def fail(*args):
        raise RuntimeError("private-payload-123")

    monkeypatch.setattr(manager.registry, "observe", fail)
    manager._on_message(
        manager.client,
        None,
        SimpleNamespace(
            topic="spBv1.0/g/NBIRTH/n",
            payload=b"ok",
            qos=0,
            retain=False,
        ),
    )
    messages, _ = manager.messages.query(None, 10, None)
    assert "private-payload-123" not in json.dumps(messages)
    assert manager.registry.status()["rejected_payloads"] == 1
    assert manager.registry.status()["complete"] is False
