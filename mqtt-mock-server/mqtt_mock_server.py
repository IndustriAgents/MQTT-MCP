#!/usr/bin/env python3
"""Simulated Sparkplug B edge node for testing the MQTT MCP server.

This is an MQTT client, not a broker: it connects to the broker at
MQTT_BROKER_HOST:MQTT_BROKER_PORT (default 127.0.0.1:1883), keeps retrying
until one answers, and then

- registers an NDEATH Last Will carrying a bdSeq, subscribes to its NCMD/DCMD
  topics and publishes NBIRTH (seq 0, bdSeq, Node Control/Rebirth) and a DBIRTH
  for each device; it does this again after every reconnect (new bdSeq);
- publishes NDATA and DDATA every UPDATE_INTERVAL seconds (device-1's sensor
  values vary over time);
- acts on NCMD "Node Control/Rebirth" and on DCMD writes to device-2's
  pump_speed, valve_position and enabled, and reports changes in a DDATA;
- publishes JSON on sensors/*, control/* and system/status that mirrors the
  simulated values;
- publishes DDEATH for each device and NDEATH once when stopped (SIGINT/SIGTERM).

Payloads are Sparkplug B protobuf, built with sparkplug_b_pb2 (the same file
as the MCP server's copy). The mock is not an MCP server, so it prints to
stdout.
"""

from __future__ import annotations

import json
import math
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass

import paho.mqtt.client as mqtt

import sparkplug_b_pb2 as pb

NAMESPACE = "spBv1.0"
BD_SEQ = "bdSeq"
REBIRTH = "Node Control/Rebirth"


@dataclass(frozen=True)
class MockConfig:
    broker_host: str = "127.0.0.1"
    broker_port: int = 1883
    group_id: str = "factory"
    edge_node_id: str = "edge-node-1"
    update_interval: float = 2.0

    @classmethod
    def from_env(cls, env=None) -> MockConfig:
        env = os.environ if env is None else env
        return cls(
            broker_host=env.get("MQTT_BROKER_HOST") or "127.0.0.1",
            broker_port=int(env.get("MQTT_BROKER_PORT") or 1883),
            group_id=env.get("SPARKPLUG_GROUP_ID") or "factory",
            edge_node_id=env.get("SPARKPLUG_EDGE_NODE_ID") or "edge-node-1",
            update_interval=float(env.get("UPDATE_INTERVAL") or 2.0),
        )


def log(message: str) -> None:
    print(message, flush=True)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _set_value(metric, datatype: int, value) -> None:
    metric.datatype = datatype
    if value is None:
        metric.is_null = True
    elif datatype in (pb.Int8, pb.Int16, pb.Int32):
        metric.int_value = value & 0xFFFFFFFF  # 32-bit two's complement
    elif datatype in (pb.UInt8, pb.UInt16, pb.UInt32):
        metric.int_value = value
    elif datatype == pb.Int64:
        metric.long_value = value & 0xFFFFFFFFFFFFFFFF
    elif datatype in (pb.UInt64, pb.DateTime):
        metric.long_value = value
    elif datatype == pb.Float:
        metric.float_value = value
    elif datatype == pb.Double:
        metric.double_value = value
    elif datatype == pb.Boolean:
        metric.boolean_value = value
    else:
        metric.string_value = str(value)


def _signed(value: int, bits: int) -> int:
    value &= (1 << bits) - 1
    return value - (1 << bits) if value >= 1 << (bits - 1) else value


def build_payload(metrics, seq: int | None):
    """metrics: iterable of (name, datatype, value)."""
    stamp = _now_ms()
    payload = pb.Payload()
    payload.timestamp = stamp
    for name, datatype, value in metrics:
        metric = payload.metrics.add()
        metric.name = name
        metric.timestamp = stamp
        _set_value(metric, datatype, value)
    if seq is not None:
        payload.seq = seq
    return payload


class SparkplugEdgeNode:
    """State and messages of one simulated edge node with two devices."""

    WRITABLE = {
        "device-2": {
            "pump_speed": (pb.Int32, 0, 100),
            "valve_position": (pb.Int32, 0, 100),
            "enabled": (pb.Boolean, None, None),
        }
    }

    def __init__(self, client: mqtt.Client, config: MockConfig) -> None:
        self.client = client
        self.group_id = config.group_id
        self.edge_node_id = config.edge_node_id
        self.lock = threading.RLock()
        self.seq = 0
        self.bd_seq = 0
        self.node_online = False
        self.start_time = time.time()
        self.devices = {
            "device-1": {
                "name": "Sensor Array 1",
                "metrics": {
                    "temperature": {"value": 22.5, "type": pb.Float, "unit": "°C"},
                    "pressure": {"value": 101.3, "type": pb.Float, "unit": "kPa"},
                    "flow_rate": {"value": 45.2, "type": pb.Float, "unit": "L/min"},
                },
            },
            "device-2": {
                "name": "Control Module 1",
                "metrics": {
                    "pump_speed": {"value": 0, "type": pb.Int32, "unit": "%"},
                    "valve_position": {"value": 50, "type": pb.Int32, "unit": "%"},
                    "enabled": {"value": False, "type": pb.Boolean, "unit": ""},
                },
            },
        }

    # -- topics and payloads ------------------------------------------------

    def topic(self, message_type: str, device_id: str | None = None) -> str:
        parts = [NAMESPACE, self.group_id, message_type, self.edge_node_id]
        if device_id is not None:
            parts.append(device_id)
        return "/".join(parts)

    def death_payload(self) -> bytes:
        """NDEATH: only the bdSeq metric, no seq (Sparkplug B 3.0)."""
        return build_payload([(BD_SEQ, pb.Int64, self.bd_seq)], seq=None).SerializeToString()

    def uptime(self) -> int:
        return int(time.time() - self.start_time)

    def _next_seq(self) -> int:
        self.seq = (self.seq + 1) % 256
        return self.seq

    def _publish(self, topic: str, payload, qos: int = 0) -> bool:
        data = payload if isinstance(payload, bytes) else payload.SerializeToString()
        result = self.client.publish(topic, data, qos=qos, retain=False)
        return result.rc == mqtt.MQTT_ERR_SUCCESS

    def _device_metrics(self, device_id: str, names=None):
        metrics = self.devices[device_id]["metrics"]
        return [
            (name, info["type"], info["value"])
            for name, info in metrics.items()
            if names is None or name in names
        ]

    # -- lifecycle messages ---------------------------------------------------

    def publish_births(self) -> bool:
        """NBIRTH (seq 0) followed by a DBIRTH for every device."""
        with self.lock:
            self.seq = 0
            metrics = [
                (BD_SEQ, pb.Int64, self.bd_seq),
                (REBIRTH, pb.Boolean, False),
                ("uptime", pb.Int64, self.uptime()),
                ("status", pb.String, "online"),
            ]
            ok = self._publish(self.topic("NBIRTH"), build_payload(metrics, seq=0))
            self.node_online = ok
            for device_id in self.devices:
                seq = self._next_seq()
                payload = build_payload(self._device_metrics(device_id), seq=seq)
                ok = self._publish(self.topic("DBIRTH", device_id), payload) and ok
            return ok

    def publish_deaths(self) -> None:
        """DDEATH for every device, then NDEATH; waits for the NDEATH PUBACK."""
        with self.lock:
            if not self.node_online:
                return
            for device_id in self.devices:
                payload = build_payload([], seq=self._next_seq())
                self._publish(self.topic("DDEATH", device_id), payload)
            info = self.client.publish(self.topic("NDEATH"), self.death_payload(), qos=1)
            self.node_online = False
        if info.rc == mqtt.MQTT_ERR_SUCCESS:
            try:
                info.wait_for_publish(2.0)
            except (RuntimeError, ValueError):
                pass

    # -- periodic data ----------------------------------------------------------

    def simulate(self) -> None:
        now = time.time()
        sensors = self.devices["device-1"]["metrics"]
        sensors["temperature"]["value"] = 22.5 + 2.0 * math.sin(now / 10.0)
        sensors["pressure"]["value"] = 101.3 + 1.0 * math.cos(now / 15.0)
        sensors["flow_rate"]["value"] = 45.2 + 5.0 * math.sin(now / 8.0)

    def publish_data(self) -> bool:
        with self.lock:
            if not self.node_online:
                return False
            self.simulate()
            node = [("uptime", pb.Int64, self.uptime()), ("status", pb.String, "online")]
            ok = self._publish(self.topic("NDATA"), build_payload(node, seq=self._next_seq()))
            for device_id in self.devices:
                payload = build_payload(self._device_metrics(device_id), seq=self._next_seq())
                ok = self._publish(self.topic("DDATA", device_id), payload) and ok
            return ok

    def standard_topics(self) -> dict[str, dict]:
        sensors = self.devices["device-1"]["metrics"]
        control = self.devices["device-2"]["metrics"]
        pump_on = control["enabled"]["value"] and control["pump_speed"]["value"] > 0
        return {
            "sensors/temperature": {
                "value": round(sensors["temperature"]["value"], 2),
                "unit": "°C",
            },
            "sensors/pressure": {"value": round(sensors["pressure"]["value"], 2), "unit": "kPa"},
            "sensors/humidity": {
                "value": round(55.0 + 5.0 * math.sin(time.time() / 20.0), 1),
                "unit": "%",
            },
            "control/pump": {
                "status": "on" if pump_on else "off",
                "speed": control["pump_speed"]["value"],
                "enabled": control["enabled"]["value"],
            },
            "control/valve": {"position": control["valve_position"]["value"], "status": "stable"},
            "system/status": {"uptime": self.uptime(), "status": "online"},
        }

    # -- commands ---------------------------------------------------------------

    def handle_command(self, topic: str, data: bytes) -> list[str]:
        """Apply an NCMD/DCMD addressed to this node; return what was done."""
        parts = topic.split("/")
        try:
            payload = pb.Payload.FromString(data)
        except Exception as exc:
            return [f"ignored {topic}: not a Sparkplug B payload ({exc})"]
        if parts[2] == "NCMD":
            return self._node_command(payload)
        return self._device_command(parts[4] if len(parts) > 4 else "", payload)

    def _node_command(self, payload) -> list[str]:
        done = []
        for metric in payload.metrics:
            if metric.name == REBIRTH and metric.WhichOneof("value") == "boolean_value":
                if metric.boolean_value:
                    self.publish_births()
                    done.append("rebirth: republished NBIRTH and DBIRTH")
            else:
                done.append(f"ignored NCMD metric {metric.name!r}")
        return done

    def _device_command(self, device_id: str, payload) -> list[str]:
        writable = self.WRITABLE.get(device_id)
        if device_id not in self.devices:
            return [f"ignored DCMD for unknown device {device_id!r}"]
        if writable is None:
            return [f"ignored DCMD for {device_id}: its metrics are read-only"]
        done, changed = [], []
        with self.lock:
            for metric in payload.metrics:
                spec = writable.get(metric.name)
                if spec is None:
                    done.append(f"ignored {device_id}/{metric.name}: not writable")
                    continue
                datatype, low, high = spec
                field = metric.WhichOneof("value")
                if datatype == pb.Boolean and field == "boolean_value":
                    value = metric.boolean_value
                elif datatype == pb.Int32 and field in ("int_value", "long_value"):
                    raw = getattr(metric, field)
                    value = _signed(raw, 32 if field == "int_value" else 64)
                    if not low <= value <= high:
                        done.append(
                            f"rejected {device_id}/{metric.name}={value}: outside {low}..{high}"
                        )
                        continue
                else:
                    done.append(
                        f"rejected {device_id}/{metric.name}: expected"
                        f" {pb.DataType.Name(datatype)}, got {field}"
                    )
                    continue
                self.devices[device_id]["metrics"][metric.name]["value"] = value
                changed.append(metric.name)
                done.append(f"set {device_id}/{metric.name}={value}")
            if changed and self.node_online:
                payload = build_payload(self._device_metrics(device_id, changed), self._next_seq())
                self._publish(self.topic("DDATA", device_id), payload)
        return done


class MockEdgeNode:
    """The MQTT client side: connection, callbacks, update loop and shutdown."""

    def __init__(self, config: MockConfig) -> None:
        self.config = config
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"mqtt-mock-{config.edge_node_id}-{os.getpid()}",
        )
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.reconnect_delay_set(min_delay=1, max_delay=10)
        self.sparkplug = SparkplugEdgeNode(self.client, config)
        self.client.will_set(
            self.sparkplug.topic("NDEATH"), self.sparkplug.death_payload(), qos=1, retain=False
        )
        self.running = True
        self.connected = False
        self.last_update = 0.0
        self._shutdown_lock = threading.Lock()
        self._shut_down = False

    def on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code.is_failure:
            self.connected = False
            log(f"Connection refused by the broker: {reason_code}")
            return
        self.connected = True
        log(f"Connected to broker (bdSeq {self.sparkplug.bd_seq})")
        client.subscribe(self.sparkplug.topic("NCMD"), qos=0)
        client.subscribe(self.sparkplug.topic("DCMD", "+"), qos=0)
        self.sparkplug.publish_births()

    def on_message(self, client, userdata, msg) -> None:
        try:
            for line in self.sparkplug.handle_command(msg.topic, msg.payload):
                log(f"Command: {line}")
        except Exception as exc:  # never kill the network thread
            log(f"Command on {msg.topic} failed: {exc}")

    def on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        self.connected = False
        self.sparkplug.node_online = False
        if self.running:
            # The broker publishes the NDEATH will; the next session needs a new bdSeq.
            self.sparkplug.bd_seq = (self.sparkplug.bd_seq + 1) % 256
            client.will_set(
                self.sparkplug.topic("NDEATH"), self.sparkplug.death_payload(), qos=1, retain=False
            )
            log(f"Disconnected ({reason_code}); reconnecting")

    def connect(self) -> None:
        """Start connecting; paho keeps retrying until the broker answers."""
        log(f"Connecting to MQTT broker at {self.config.broker_host}:{self.config.broker_port}...")
        self.client.connect_async(self.config.broker_host, self.config.broker_port, keepalive=60)
        self.client.loop_start()

    def publish_standard_topics(self) -> None:
        for topic, data in self.sparkplug.standard_topics().items():
            self.client.publish(topic, json.dumps(data), qos=1)

    def update_loop(self) -> None:
        iteration = 0
        try:
            while self.running:
                now = time.time()
                if (
                    self.connected
                    and self.sparkplug.node_online
                    and now - self.last_update >= self.config.update_interval
                ):
                    self.sparkplug.publish_data()
                    self.publish_standard_topics()
                    self.last_update = now
                    iteration += 1
                    log(f"Update cycle {iteration}")
                time.sleep(0.1)
        except KeyboardInterrupt:
            log("Shutdown requested...")
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Publish DDEATH/NDEATH and disconnect. Runs once, however often it is called."""
        with self._shutdown_lock:
            if self._shut_down:
                return
            self._shut_down = True
        log("Shutting down...")
        self.running = False
        if self.connected:
            self.sparkplug.publish_deaths()
        self.client.disconnect()
        self.client.loop_stop()
        log("Shutdown complete")


def main() -> None:
    config = MockConfig.from_env()
    log("Mock Sparkplug B edge node (an MQTT client; it needs a broker)")
    log(f"Broker: {config.broker_host}:{config.broker_port}")
    log(f"Sparkplug Group: {config.group_id}")
    log(f"Edge Node: {config.edge_node_id}")
    log("")

    node = MockEdgeNode(config)

    def request_stop(signum, frame) -> None:
        # Only ask the loop to stop; update_loop's finally runs shutdown() once.
        node.running = False

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    node.connect()
    node.update_loop()
    sys.exit(0)


if __name__ == "__main__":
    main()
