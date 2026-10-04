"""What the server has received: a bounded message buffer and a Sparkplug B
registry built from the Sparkplug messages seen on the broker.

Both are filled from paho's network thread and read from the MCP event loop,
so every access goes through a lock.
"""

from __future__ import annotations

import copy
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from google.protobuf.message import DecodeError
from paho.mqtt.client import topic_matches_sub

from . import sparkplug

#: Payload bytes kept per buffered message; longer payloads are truncated.
MAX_STORED_PAYLOAD = 64 * 1024


def _iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat(timespec="milliseconds")


@dataclass
class ReceivedMessage:
    id: int
    topic: str
    payload: bytes
    payload_size: int
    qos: int
    retain: bool
    received_at: float
    sparkplug: dict[str, Any] | None = None
    sparkplug_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "id": self.id,
            "topic": self.topic,
            "qos": self.qos,
            "retain": self.retain,
            "received_at": _iso(self.received_at),
            "payload_size": self.payload_size,
        }
        text = None
        if not self.topic.startswith(f"{sparkplug.NAMESPACE}/") or "/STATE/" in self.topic:
            try:
                text = self.payload.decode("utf-8")
            except UnicodeDecodeError:
                text = None
        if text is not None:
            out["payload"] = text
            out["payload_encoding"] = "utf-8"
        else:
            out["payload"] = self.payload.hex()
            out["payload_encoding"] = "hex"
        if self.payload_size > len(self.payload):
            out["payload_truncated"] = True
        if self.sparkplug is not None:
            out["sparkplug"] = self.sparkplug
        if self.sparkplug_error is not None:
            out["sparkplug_error"] = self.sparkplug_error
        return out


class MessageBuffer:
    """The last ``size`` messages received on this server's subscriptions."""

    def __init__(self, size: int) -> None:
        self._items: deque[ReceivedMessage] = deque(maxlen=size)
        self._lock = threading.Lock()
        self._next_id = 1
        self.size = size
        self.dropped = 0

    def add(self, topic: str, payload: bytes, qos: int, retain: bool, received_at: float, **extra):
        with self._lock:
            if len(self._items) == self._items.maxlen:
                self.dropped += 1
            message = ReceivedMessage(
                id=self._next_id,
                topic=topic,
                payload=payload[:MAX_STORED_PAYLOAD],
                payload_size=len(payload),
                qos=qos,
                retain=retain,
                received_at=received_at,
                **extra,
            )
            self._next_id += 1
            self._items.append(message)
            return message

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)

    def query(
        self, topic_filter: str | None, limit: int, since_id: int | None
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        with self._lock:
            items = list(self._items)
            dropped = self.dropped
            last_id = self._next_id - 1
        buffered = len(items)
        oldest_id = items[0].id if items else last_id + 1
        if topic_filter is not None:
            items = [m for m in items if topic_matches_sub(topic_filter, m.topic)]
        if since_id is not None:
            items = [m for m in items if m.id > since_id]
            selected = items[:limit]
        else:
            selected = items[-limit:]
        meta: dict[str, Any] = {
            "returned": len(selected),
            "has_more": len(items) > len(selected),
            "buffered": buffered,
            "buffer_size": self.size,
            "dropped_total": dropped,
            "last_id": selected[-1].id
            if selected
            else (since_id if since_id is not None else last_id),
        }
        if since_id is not None and since_id + 1 < oldest_id:
            # Messages after since_id have already been pushed out of the buffer.
            meta["missed_messages"] = True
        return [m.to_dict() for m in selected], meta


def _new_entity() -> dict[str, Any]:
    return {
        "online": None,
        "last_message_type": None,
        "last_seen": None,
        "metrics": {},
    }


@dataclass
class _Datatypes:
    by_name: dict[str, int] = field(default_factory=dict)
    alias_names: dict[int, str] = field(default_factory=dict)

    def as_hints(self) -> dict[str, int]:
        hints = dict(self.by_name)
        for alias, name in self.alias_names.items():
            if name in self.by_name:
                hints[f"alias:{alias}"] = self.by_name[name]
        return hints


class SparkplugRegistry:
    """Sparkplug B edge nodes and devices seen in messages this server received."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._nodes: dict[tuple[str, str], dict[str, Any]] = {}
        self._types: dict[tuple, _Datatypes] = {}

    def observe(
        self, topic: str, payload: bytes, received_at: float
    ) -> tuple[dict[str, Any] | None, str | None]:
        """Record a message; return (decoded Sparkplug payload, decode error)."""
        parts = sparkplug.parse_topic(topic)
        if parts is None or parts["message_type"] == "STATE":
            return None, None
        message_type = parts["message_type"]
        node_key = (parts["group_id"], parts["edge_node_id"])
        device_id = parts.get("device_id")
        type_key = node_key + ((device_id,) if device_id else ())
        with self._lock:
            hints = self._types.get(type_key, _Datatypes()).as_hints()
        try:
            decoded = sparkplug.decode_payload(payload, hints)
        except DecodeError as exc:
            return None, f"not a valid Sparkplug B payload: {exc}"
        if message_type in ("NCMD", "DCMD"):
            # Commands are addressed to the node; they say nothing about its state.
            return decoded, None

        with self._lock:
            node = self._nodes.get(node_key)
            if node is None:
                node = _new_entity()
                node.update(
                    {
                        "group_id": node_key[0],
                        "edge_node_id": node_key[1],
                        "bd_seq": None,
                        "last_seq": None,
                        "devices": {},
                    }
                )
                self._nodes[node_key] = node
            if device_id is None:
                entity = node
            else:
                entity = node["devices"].get(device_id)
                if entity is None:
                    entity = _new_entity()
                    entity["device_id"] = device_id
                    node["devices"][device_id] = entity
            entity["last_message_type"] = message_type
            entity["last_seen"] = _iso(received_at)
            if decoded.get("seq") is not None:
                node["last_seq"] = decoded["seq"]

            if message_type in ("NBIRTH", "DBIRTH"):
                types = _Datatypes()
                for metric in decoded["metrics"]:
                    if metric["name"] is None:
                        continue
                    try:
                        types.by_name[metric["name"]] = sparkplug.pb.DataType.Value(
                            metric["datatype"]
                        )
                    except (TypeError, ValueError):  # no datatype, or not a known one
                        pass
                    if "alias" in metric:
                        types.alias_names[metric["alias"]] = metric["name"]
                self._types[type_key] = types
                entity["metrics"] = {}
                entity["online"] = True
                if message_type == "NBIRTH":
                    node["devices"] = {}
                    node["bd_seq"] = _bd_seq(decoded)
                    for key in [k for k in self._types if k[:2] == node_key and len(k) == 3]:
                        del self._types[key]
            elif message_type == "NDEATH":
                bd_seq = _bd_seq(decoded)
                if node["bd_seq"] is not None and bd_seq is not None and bd_seq != node["bd_seq"]:
                    # A stale death certificate from an earlier session (spec: ignore it).
                    entity["stale_ndeath_ignored"] = True
                    return decoded, None
                node["online"] = False
                for device in node["devices"].values():
                    device["online"] = False
            elif message_type == "DDEATH":
                entity["online"] = False
            else:  # NDATA / DDATA: the sender is alive
                entity["online"] = True
                if device_id is not None and node["online"] is None:
                    node["online"] = True

            if message_type != "NDEATH":
                aliases = self._types.get(type_key, _Datatypes()).alias_names
                for metric in decoded["metrics"]:
                    name = metric["name"]
                    if name is None and "alias" in metric:
                        name = aliases.get(metric["alias"], f"alias:{metric['alias']}")
                    if name is None:
                        continue
                    entity["metrics"][name] = {
                        "value": metric["value"],
                        "datatype": metric["datatype"],
                        "timestamp": metric.get("timestamp"),
                    }
        return decoded, None

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            nodes = copy.deepcopy(list(self._nodes.values()))
        out = []
        for node in sorted(nodes, key=lambda n: (n["group_id"], n["edge_node_id"])):
            devices = node.pop("devices")
            node["devices"] = [devices[d] for d in sorted(devices)]
            out.append(node)
        return out


def _bd_seq(decoded: dict[str, Any]) -> int | None:
    for metric in decoded["metrics"]:
        if metric["name"] == sparkplug.BD_SEQ_METRIC and isinstance(metric["value"], int):
            return metric["value"]
    return None
