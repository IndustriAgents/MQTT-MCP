"""Sparkplug current-value and edge-wide sequence semantics, without active rebirth."""

import copy

import pytest

from mqtt_mcp import sparkplug_b_pb2 as pb
from mqtt_mcp.inbox import SparkplugRegistry


def payload(seq=0, value=10, bd_seq=None, **flags):
    result = pb.Payload()
    if seq is not None:
        result.seq = seq
    result.metrics.add(name="reading", datatype=pb.Int32, int_value=value, alias=7, **flags)
    if bd_seq is not None:
        result.metrics.add(name="bdSeq", datatype=pb.UInt64, long_value=bd_seq)
    return result.SerializeToString()


def observe(registry, kind, seq=0, **kwargs):
    suffix = "/device" if kind.startswith("D") else ""
    return registry.observe(f"spBv1.0/g/{kind}/node{suffix}", payload(seq, **kwargs), 1000.0)


@pytest.mark.parametrize("flag", ["is_historical", "is_transient"])
@pytest.mark.parametrize("kind", ["NDATA", "DDATA"])
def test_noncurrent_metrics_remain_decoded_but_do_not_overwrite_current_tags(flag, kind):
    registry = SparkplugRegistry()
    observe(registry, "NBIRTH", 0, bd_seq=1)
    observe(registry, "DBIRTH", 1)
    decoded, error = observe(registry, kind, 2, value=99, **{flag: True})
    assert error is None and decoded["metrics"][0][flag] is True
    node = registry.snapshot()[0]
    assert node["metrics"]["reading"]["value"] == 10
    assert node["devices"][0]["metrics"]["reading"]["value"] == 10


def test_transient_birth_metadata_can_resolve_later_live_alias_without_retaining_transient_value():
    registry = SparkplugRegistry()
    observe(registry, "NBIRTH", is_transient=True)
    assert registry.snapshot()[0]["metrics"] == {}
    update = pb.Payload(seq=1)
    update.metrics.add(alias=7, int_value=20)
    registry.observe("spBv1.0/g/NDATA/node", update.SerializeToString(), 1001)
    assert registry.snapshot()[0]["metrics"]["reading"]["value"] == 20


def test_current_null_remains_an_explicit_null_metric():
    registry = SparkplugRegistry()
    observe(registry, "NBIRTH")
    observe(registry, "NDATA", 1, is_null=True)
    metric = registry.snapshot()[0]["metrics"]["reading"]
    assert metric["value"] is None and metric["is_null"] is True


def test_sequence_is_shared_across_node_and_device_messages_and_wraps():
    registry = SparkplugRegistry()
    observe(registry, "NBIRTH", 0, bd_seq=1)
    observe(registry, "DBIRTH", 1)
    for seq in range(2, 256):
        observe(registry, "NDATA", seq)
    observe(registry, "DDATA", 0)
    observe(registry, "DDEATH", 1)
    node = registry.snapshot()[0]
    assert node["last_seq"] == 1 and node["sequence_status"] == "in_order"
    assert registry.status()["sequence_discontinuities"] == 0
    observe(registry, "NDEATH", None, bd_seq=1)
    assert registry.snapshot()[0]["last_seq"] == 1
    assert registry.status()["invalid_sequences"] == 0


@pytest.mark.parametrize("seq", [1, 255])
def test_nonzero_birth_cannot_establish_a_valid_anchor_but_keeps_arrival_values(seq):
    registry = SparkplugRegistry()
    observe(registry, "NBIRTH", seq, value=12)
    node = registry.snapshot()[0]
    assert node["last_seq"] is None and node["sequence_status"] == "invalid"
    assert node["last_sequence_issue"] == {
        "reason": "nonzero_birth",
        "expected": 0,
        "received": seq,
    }
    assert node["metrics"]["reading"]["value"] == 12
    assert registry.status()["invalid_sequences"] == 1 and not registry.status()["complete"]


@pytest.mark.parametrize(
    "seq,reason",
    [(4, "unexpected_sequence"), (0, "repeated_sequence"), (255, "unexpected_sequence")],
)
def test_sequence_discontinuity_reports_observed_values_without_inventing_loss_count(seq, reason):
    registry = SparkplugRegistry()
    observe(registry, "NBIRTH", 0)
    observe(registry, "NDATA", seq, value=11)
    node = registry.snapshot()[0]
    assert node["sequence_status"] == "discontinuity"
    assert node["last_sequence_issue"] == {"reason": reason, "expected": 1, "received": seq}
    assert node["metrics"]["reading"]["value"] == 11
    assert registry.status()["complete"] is False
    observe(registry, "NBIRTH", 0)
    assert registry.snapshot()[0]["sequence_discontinuities"] == 0
    assert registry.snapshot()[0]["last_sequence_issue"] is None
    assert registry.status()["sequence_discontinuities"] == 1


@pytest.mark.parametrize("seq", [None, 256, 2**64 - 1])
def test_invalid_sequence_does_not_overwrite_last_valid_sequence(seq):
    registry = SparkplugRegistry()
    observe(registry, "NBIRTH", 0)
    observe(registry, "NDATA", seq)
    node = registry.snapshot()[0]
    assert node["last_seq"] == 0 and node["sequence_status"] == "invalid"
    assert registry.status()["invalid_sequences"] == 1 and not registry.status()["complete"]
    expected = str(seq) if isinstance(seq, int) and seq > 2**53 - 1 else seq
    assert node["last_sequence_issue"]["received"] == expected


@pytest.mark.parametrize("death_bd_seq", [None, 1, 256])
def test_unmatched_death_cannot_mutate_any_registry_state(death_bd_seq):
    registry = SparkplugRegistry()
    observe(registry, "NBIRTH", 0, bd_seq=2)
    observe(registry, "DBIRTH", 1)
    before = copy.deepcopy(registry.snapshot()), registry.status(), copy.deepcopy(registry._types)
    _, error = registry.observe("spBv1.0/g/NDEATH/node", payload(99, bd_seq=death_bd_seq), 9000)
    assert "ignored" in error
    assert (registry.snapshot(), registry.status(), registry._types) == before


def test_death_without_observed_birth_does_not_create_a_phantom_node():
    registry = SparkplugRegistry()
    observe(registry, "NDEATH", None, bd_seq=1)
    assert registry.snapshot() == []


@pytest.mark.anyio
async def test_observation_metadata_matches_actual_mcp_output_schemas(monkeypatch):
    from mqtt_mcp import mqtt_server

    from .test_tools import _offline_manager, call, server

    manager = _offline_manager()
    observe(manager.registry, "NBIRTH", 0, bd_seq=1)
    observe(manager.registry, "NDATA", 2, is_null=True)
    monkeypatch.setattr(mqtt_server, "_manager", lambda ctx: manager)
    async with server() as session:
        result = await call(session, "list_sparkplug_nodes")
    node = result["data"]["nodes"][0]
    assert node["sequence_status"] == "discontinuity"
    assert node["last_sequence_issue"] == {
        "reason": "unexpected_sequence",
        "expected": 1,
        "received": 2,
    }
    assert node["metrics"]["reading"]["is_null"]
