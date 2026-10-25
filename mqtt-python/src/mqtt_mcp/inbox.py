"""What the server has received: a bounded message buffer and a Sparkplug B
registry built from the Sparkplug messages seen on the broker.

Both are filled from paho's network thread and read from the MCP event loop,
so every access goes through a lock.
"""

from __future__ import annotations

import copy
import json
import threading
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from google.protobuf.message import DecodeError
from paho.mqtt.client import topic_matches_sub

from . import sparkplug

#: Payload bytes kept per buffered message; longer payloads are truncated.
MAX_STORED_PAYLOAD = 64 * 1024
MAX_BUFFER_BYTES = 8 * 1024 * 1024
MAX_REGISTRY_BYTES = 4 * 1024 * 1024
MAX_REGISTRY_NODES = 256
MAX_SPARKPLUG_PAYLOAD = 64 * 1024


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
    stored_bytes: int = 0

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

    def __init__(self, size: int, *, max_bytes: int = MAX_BUFFER_BYTES) -> None:
        if size < 1 or max_bytes < 1:
            raise ValueError("Message buffer limits must be positive")
        self._items: deque[ReceivedMessage] = deque()
        self.max_bytes = max_bytes
        self._bytes = 0
        self._lock = threading.Lock()
        self._next_id = 1
        self.size = size
        self.dropped = 0
        self._last_dropped_id = 0

    def add(self, topic: str, payload: bytes, qos: int, retain: bool, received_at: float, **extra):
        with self._lock:
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
            message.stored_bytes = len(json.dumps(message.to_dict()).encode("utf-8"))
            if message.stored_bytes > self.max_bytes:
                self.dropped += 1
                self._last_dropped_id = message.id
                return message
            while self._items and (
                len(self._items) >= self.size or self._bytes + message.stored_bytes > self.max_bytes
            ):
                evicted = self._items.popleft()
                self._bytes -= evicted.stored_bytes
                self._last_dropped_id = max(self._last_dropped_id, evicted.id)
                self.dropped += 1
            self._items.append(message)
            self._bytes += message.stored_bytes
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
            buffered_bytes = self._bytes
            last_dropped_id = self._last_dropped_id
        buffered = len(items)
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
            "buffered_bytes": buffered_bytes,
            "max_buffer_bytes": self.max_bytes,
            "dropped_total": dropped,
            "last_id": selected[-1].id
            if selected
            else (since_id if since_id is not None else last_id),
        }
        if since_id is not None and since_id < last_dropped_id:
            # Loss may be eviction or a rejected oversized message between retained IDs.
            meta["missed_messages"] = True
        if since_id is not None and since_id > last_id:
            meta["note"] = (
                f"since_id {since_id} is newer than the newest message ({last_id}); ids start"
                " again at 1 when the server restarts"
            )
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
    alias_names: dict[int | str, str] = field(default_factory=dict)

    def as_hints(self) -> dict[str, int]:
        hints = dict(self.by_name)
        for alias, name in self.alias_names.items():
            if name in self.by_name:
                hints[f"alias:{alias}"] = self.by_name[name]
        return hints


class SparkplugRegistry:
    """Sparkplug B edge nodes and devices seen in messages this server received."""

    def __init__(
        self,
        *,
        max_bytes: int = MAX_REGISTRY_BYTES,
        max_nodes: int = MAX_REGISTRY_NODES,
        max_payload: int = MAX_SPARKPLUG_PAYLOAD,
    ) -> None:
        if min(max_bytes, max_nodes, max_payload) < 1:
            raise ValueError("Registry limits must be positive")
        self._lock = threading.Lock()
        self._nodes: OrderedDict[tuple[str, str], dict[str, Any]] = OrderedDict()
        self._types: dict[tuple, _Datatypes] = {}
        self._sizes: dict[tuple[str, str], int] = {}
        self.max_bytes, self.max_nodes, self.max_payload = max_bytes, max_nodes, max_payload
        self._bytes = 0
        self.evicted_nodes = 0
        self.rejected_payloads = 0
        self.connection_gaps = 0

    def observe(
        self, topic: str, payload: bytes, received_at: float
    ) -> tuple[dict[str, Any] | None, str | None]:
        """Record a message; return (decoded Sparkplug payload, decode error)."""
        parts = sparkplug.parse_topic(topic)
        if parts is None or parts["message_type"] == "STATE":
            return None, None
        if len(payload) > self.max_payload:
            with self._lock:
                self.rejected_payloads += 1
            return None, "Sparkplug payload exceeds the decode limit; registry was not updated"
        message_type = parts["message_type"]
        node_key = (parts["group_id"], parts["edge_node_id"])
        device_id = parts.get("device_id")
        type_key = node_key + ((device_id,) if device_id else ())
        with self._lock:
            hints = self._types.get(type_key, _Datatypes()).as_hints()
        try:
            decoded = sparkplug.decode_payload(payload, hints)
        except DecodeError:
            with self._lock:
                self.rejected_payloads += 1
            return None, "not a valid Sparkplug B payload"
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
                    self._bound_node(node_key)
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
                    if "value_encoding" in metric:
                        entity["metrics"][name]["value_encoding"] = metric["value_encoding"]
            if not self._bound_node(node_key):
                return (
                    decoded,
                    "Sparkplug node exceeds registry byte limit; cached node was removed",
                )
        return decoded, None

    def _bound_node(self, node_key: tuple[str, str]) -> bool:
        """Account the full node and its alias tables after every mutation; lock held."""
        node = self._nodes[node_key]
        self._nodes.move_to_end(node_key)
        encoded = {
            "node": node,
            "types": [
                {"by_name": value.by_name, "alias_names": value.alias_names}
                for key, value in self._types.items()
                if key[:2] == node_key
            ],
        }
        size = len(json.dumps(encoded).encode("utf-8"))
        self._bytes += size - self._sizes.get(node_key, 0)
        self._sizes[node_key] = size
        if size > self.max_bytes:
            self._evict(node_key)
            return False
        while self._bytes > self.max_bytes or len(self._nodes) > self.max_nodes:
            self._evict(next(iter(self._nodes)))
        return True

    def _evict(self, key: tuple[str, str]) -> None:
        self._nodes.pop(key, None)
        self._bytes -= self._sizes.pop(key, 0)
        for type_key in [item for item in self._types if item[:2] == key]:
            del self._types[type_key]
        self.evicted_nodes += 1

    def record_rejection(self) -> None:
        """Record an update lost during unexpected receive processing."""
        with self._lock:
            self.rejected_payloads += 1

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "nodes": len(self._nodes),
                "stored_bytes": self._bytes,
                "max_bytes": self.max_bytes,
                "max_nodes": self.max_nodes,
                "max_payload_bytes": self.max_payload,
                "evicted_nodes": self.evicted_nodes,
                "rejected_payloads": self.rejected_payloads,
                "connection_gaps": self.connection_gaps,
                "scope": "observed_messages_since_start",
                "complete": not (
                    self.evicted_nodes or self.rejected_payloads or self.connection_gaps
                ),
            }

    def mark_unknown(self) -> None:
        """This server lost its connection: deaths published meanwhile are missed, so
        the online state of every node and device is unknown until it is heard from."""
        with self._lock:
            self.connection_gaps += 1
            for key in list(self._nodes):
                node = self._nodes.get(key)
                if node is None:
                    continue
                node["online"] = None
                for device in node["devices"].values():
                    device["online"] = None
                self._bound_node(key)

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
