"""The mock edge node (mqtt-mock-server/mqtt_mock_server.py), without a broker."""

from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest

from mqtt_mcp import sparkplug
from mqtt_mcp.sparkplug import build_payload, decode_payload

from .conftest import MOCK_DIR, PROJECT


@pytest.fixture(scope="module")
def mock_module():
    sys.path.insert(0, str(MOCK_DIR))
    try:
        yield importlib.import_module("mqtt_mock_server")
    finally:
        sys.path.remove(str(MOCK_DIR))


class FakeClient:
    """Records publishes instead of sending them."""

    def __init__(self):
        self.published = []
        self.calls = []

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, qos, retain))
        return SimpleNamespace(
            rc=mqtt.MQTT_ERR_SUCCESS,
            wait_for_publish=lambda timeout: None,
            is_published=lambda: True,
        )

    def __getattr__(self, name):  # disconnect, loop_stop, will_set, ...
        return lambda *args, **kwargs: self.calls.append(name)

    def topics(self):
        return [topic for topic, *_ in self.published]

    def decoded(self, topic):
        return [decode_payload(p) for t, p, *_ in self.published if t == topic]


@pytest.fixture
def node(mock_module):
    config = mock_module.MockConfig(group_id="g", edge_node_id="n")
    client = FakeClient()
    return mock_module.SparkplugEdgeNode(client, config), client


def test_schema_module_is_identical_in_server_and_mock():
    server_copy = PROJECT / "src" / "mqtt_mcp" / "sparkplug_b_pb2.py"
    assert (MOCK_DIR / "sparkplug_b_pb2.py").read_bytes() == server_copy.read_bytes()


def test_births_follow_the_spec(node):
    sim, client = node
    sim.bd_seq = 3
    assert sim.publish_births() is True
    assert client.topics() == [
        "spBv1.0/g/NBIRTH/n",
        "spBv1.0/g/DBIRTH/n/device-1",
        "spBv1.0/g/DBIRTH/n/device-2",
    ]
    assert all(qos == 0 and retain is False for *_, qos, retain in client.published)
    (nbirth,) = client.decoded("spBv1.0/g/NBIRTH/n")
    assert nbirth["seq"] == 0
    metrics = {m["name"]: m for m in nbirth["metrics"]}
    assert metrics["bdSeq"]["value"] == 3 and metrics["bdSeq"]["datatype"] == "Int64"
    assert metrics["Node Control/Rebirth"]["value"] is False
    (dbirth,) = client.decoded("spBv1.0/g/DBIRTH/n/device-2")
    assert dbirth["seq"] == 2
    types = {m["name"]: m["datatype"] for m in dbirth["metrics"]}
    assert types == {"pump_speed": "Int32", "valve_position": "Int32", "enabled": "Boolean"}


def test_death_payload_has_bdseq_and_no_seq(node):
    sim, _ = node
    sim.bd_seq = 7
    decoded = decode_payload(sim.death_payload())
    assert decoded["seq"] is None
    assert [(m["name"], m["value"]) for m in decoded["metrics"]] == [("bdSeq", 7)]


def test_data_uses_one_sequence_for_node_and_devices(node):
    sim, client = node
    sim.publish_births()
    sim.publish_data()
    seqs = [decode_payload(p)["seq"] for _, p, *_ in client.published]
    assert seqs == [0, 1, 2, 3, 4, 5]


def test_dcmd_changes_device_2_and_reports_it(node):
    sim, client = node
    sim.publish_births()
    client.published.clear()
    command = build_payload(
        [
            {"name": "pump_speed", "value": 75, "type": "int32"},
            {"name": "enabled", "value": True, "type": "boolean"},
        ],
        seq=None,
    ).SerializeToString()
    done = sim.handle_command("spBv1.0/g/DCMD/n/device-2", command)
    assert done == ["set device-2/pump_speed=75", "set device-2/enabled=True"]
    (ddata,) = client.decoded("spBv1.0/g/DDATA/n/device-2")
    assert {m["name"]: m["value"] for m in ddata["metrics"]} == {
        "pump_speed": 75,
        "enabled": True,
    }
    pump = sim.standard_topics()["control/pump"]
    assert pump == {"status": "on", "speed": 75, "enabled": True}


@pytest.mark.parametrize(
    ("metric", "expected"),
    [
        ({"name": "pump_speed", "value": 150, "type": "int32"}, "outside 0..100"),
        ({"name": "valve_position", "value": -1, "type": "int32"}, "outside 0..100"),
        ({"name": "pump_speed", "value": "fast", "type": "string"}, "expected Int32"),
        ({"name": "serial", "value": 1, "type": "int32"}, "not writable"),
    ],
)
def test_invalid_dcmd_is_rejected(node, metric, expected):
    sim, client = node
    command = build_payload([metric], seq=None).SerializeToString()
    (line,) = sim.handle_command("spBv1.0/g/DCMD/n/device-2", command)
    assert expected in line
    assert sim.devices["device-2"]["metrics"]["pump_speed"]["value"] == 0


def test_sensor_device_is_read_only(node):
    sim, _ = node
    command = build_payload([{"name": "temperature", "value": 1.0, "type": "float"}], seq=None)
    (line,) = sim.handle_command("spBv1.0/g/DCMD/n/device-1", command.SerializeToString())
    assert "read-only" in line


def test_ncmd_rebirth(node):
    sim, client = node
    command = build_payload(
        [{"name": sparkplug.REBIRTH_METRIC, "value": True, "type": "boolean"}], seq=None
    )
    sim.handle_command("spBv1.0/g/NCMD/n", command.SerializeToString())
    assert client.topics()[0] == "spBv1.0/g/NBIRTH/n"


def test_shutdown_runs_once(mock_module, capsys):
    """SIGTERM used to run shutdown() twice (signal handler + update_loop's finally)."""
    edge = mock_module.MockEdgeNode(mock_module.MockConfig(group_id="g", edge_node_id="n"))
    fake = FakeClient()
    edge.client = fake
    edge.sparkplug.client = fake
    edge.connected = True
    edge.sparkplug.node_online = True
    edge.running = False  # what the signal handler now does
    edge.update_loop()  # exits at once and shuts down in finally
    edge.shutdown()  # a second call is a no-op
    out = capsys.readouterr().out
    assert out.count("Shutting down...") == 1
    assert out.count("Shutdown complete") == 1
    assert fake.topics() == [
        "spBv1.0/g/DDEATH/n/device-1",
        "spBv1.0/g/DDEATH/n/device-2",
        "spBv1.0/g/NDEATH/n",
    ]
    assert fake.calls.count("disconnect") == 1


def test_mock_reads_config_at_runtime(mock_module):
    config = mock_module.MockConfig.from_env(
        {"MQTT_BROKER_PORT": "18999", "SPARKPLUG_GROUP_ID": "x", "UPDATE_INTERVAL": "0.5"}
    )
    assert (config.broker_port, config.group_id, config.update_interval) == (18999, "x", 0.5)
    assert not hasattr(mock_module, "BROKER_HOST")
