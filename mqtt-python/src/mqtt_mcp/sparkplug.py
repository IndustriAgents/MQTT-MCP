"""Sparkplug B helpers: metric validation, payload encoding and decoding, topics.

Encoding and decoding go through the protobuf schema in ``sparkplug_b_pb2``,
so payloads use the field numbers of the Sparkplug B specification and can be
read by any Sparkplug B host application.

Signed integer conventions (the specification stores every integer in an
unsigned protobuf field and leaves the signed representation implicit):

* Int8, Int16 and Int32 values are written to ``int_value`` (uint32) as their
  32-bit two's complement, e.g. Int8 -1 is ``0xFFFFFFFF``. That is what the
  Java/C# Sparkplug libraries (and so Ignition) produce, and it reads back
  correctly whether a decoder truncates to the type width or reinterprets
  32 bits.
* Int64 values are written to ``long_value`` (uint64) as their 64-bit two's
  complement.
* When decoding, Int8/Int16/Int32/Int64 values are read back as signed numbers
  from the low 8/16/32/64 bits, which accepts both the sign-extended form above
  and the form Eclipse Tahu's Python library writes (two's complement in the
  type's own width, e.g. Int8 -1 as ``0xFF``).
"""

from __future__ import annotations

import math
import struct
import time
import uuid as uuid_module
from dataclasses import dataclass
from typing import Any

from . import sparkplug_b_pb2 as pb

NAMESPACE = "spBv1.0"
BD_SEQ_METRIC = "bdSeq"
REBIRTH_METRIC = "Node Control/Rebirth"

NODE_MESSAGE_TYPES = ("NBIRTH", "NDEATH", "NDATA", "NCMD")
DEVICE_MESSAGE_TYPES = ("DBIRTH", "DDEATH", "DDATA", "DCMD")

_FLOAT32_MAX = 3.4028234663852886e38


class MetricError(ValueError):
    """A metric given to a publish tool is invalid."""


@dataclass(frozen=True)
class _TypeSpec:
    datatype: int
    kind: str  # "int", "float", "bool", "str", "datetime", "bytes"
    field: str
    minimum: int | None = None
    maximum: int | None = None
    bits: int | None = None  # storage width for signed types (two's complement)


_TYPES: dict[str, _TypeSpec] = {
    "int8": _TypeSpec(pb.Int8, "int", "int_value", -(2**7), 2**7 - 1, 32),
    "int16": _TypeSpec(pb.Int16, "int", "int_value", -(2**15), 2**15 - 1, 32),
    "int32": _TypeSpec(pb.Int32, "int", "int_value", -(2**31), 2**31 - 1, 32),
    "int64": _TypeSpec(pb.Int64, "int", "long_value", -(2**63), 2**63 - 1, 64),
    "uint8": _TypeSpec(pb.UInt8, "int", "int_value", 0, 2**8 - 1),
    "uint16": _TypeSpec(pb.UInt16, "int", "int_value", 0, 2**16 - 1),
    "uint32": _TypeSpec(pb.UInt32, "int", "int_value", 0, 2**32 - 1),
    "uint64": _TypeSpec(pb.UInt64, "int", "long_value", 0, 2**64 - 1),
    "float": _TypeSpec(pb.Float, "float", "float_value"),
    "double": _TypeSpec(pb.Double, "float", "double_value"),
    "boolean": _TypeSpec(pb.Boolean, "bool", "boolean_value"),
    "string": _TypeSpec(pb.String, "str", "string_value"),
    "text": _TypeSpec(pb.Text, "str", "string_value"),
    "uuid": _TypeSpec(pb.UUID, "uuid", "string_value"),
    "datetime": _TypeSpec(pb.DateTime, "datetime", "long_value", 0, 2**64 - 1),
    "bytes": _TypeSpec(pb.Bytes, "bytes", "bytes_value"),
}

TYPE_ALIASES = {"int": "int32", "long": "int64", "bool": "boolean"}

#: Every ``type`` value the publish tools accept (case-insensitive).
ACCEPTED_TYPES = tuple(sorted(_TYPES)) + tuple(sorted(TYPE_ALIASES))

_SIGNED_BITS = {pb.Int8: 8, pb.Int16: 16, pb.Int32: 32, pb.Int64: 64}
_INT_TYPES = {pb.Int8, pb.Int16, pb.Int32, pb.UInt8, pb.UInt16, pb.UInt32}
_LONG_TYPES = {pb.Int64, pb.UInt64, pb.DateTime}
_STRING_TYPES = {pb.String, pb.Text, pb.UUID}


def now_ms() -> int:
    return int(time.time() * 1000)


def validate_id(value: Any, what: str) -> str:
    """Check a Sparkplug group, edge node or device ID and return it.

    The specification allows any non-empty UTF-8 string without ``+``, ``/``
    and ``#``.
    """
    if not isinstance(value, str) or not value:
        raise ValueError(f"{what} must be a non-empty string")
    bad = sorted({c for c in value if c in "+/#"})
    if bad:
        raise ValueError(f"{what} {value!r} must not contain {' '.join(bad)}")
    return value


def topic(group_id: str, message_type: str, edge_node_id: str, device_id: str | None = None) -> str:
    parts = [NAMESPACE, group_id, message_type, edge_node_id]
    if device_id is not None:
        parts.append(device_id)
    return "/".join(parts)


def parse_topic(mqtt_topic: str) -> dict[str, str] | None:
    """Split a Sparkplug B topic into its parts, or return None if it is not one."""
    parts = mqtt_topic.split("/")
    if len(parts) < 3 or parts[0] != NAMESPACE:
        return None
    if parts[1] == "STATE":
        return {"message_type": "STATE", "host_id": "/".join(parts[2:])}
    if len(parts) == 4 and parts[2] in NODE_MESSAGE_TYPES:
        return {"group_id": parts[1], "message_type": parts[2], "edge_node_id": parts[3]}
    if len(parts) == 5 and parts[2] in DEVICE_MESSAGE_TYPES:
        return {
            "group_id": parts[1],
            "message_type": parts[2],
            "edge_node_id": parts[3],
            "device_id": parts[4],
        }
    return None


def resolve_type(type_name: Any) -> str:
    if not isinstance(type_name, str) or not type_name.strip():
        raise MetricError(f"'type' must be one of: {', '.join(ACCEPTED_TYPES)}")
    key = type_name.strip().lower()
    key = TYPE_ALIASES.get(key, key)
    if key not in _TYPES:
        raise MetricError(
            f"unsupported metric type {type_name!r}; use one of: {', '.join(ACCEPTED_TYPES)}"
        )
    return key


def _coerce_int(value: Any, type_key: str, spec: _TypeSpec) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MetricError(f"a {type_key} value must be an integer, got {value!r}")
    if isinstance(value, float):
        if not value.is_integer():
            raise MetricError(f"a {type_key} value must be an integer, got {value!r}")
        value = int(value)
    if not spec.minimum <= value <= spec.maximum:
        raise MetricError(
            f"{value} is out of range for {type_key} ({spec.minimum} to {spec.maximum})"
        )
    return value


def build_metric(metric: Any, *, timestamp: int | None = None) -> pb.Payload.Metric:
    """Validate one ``{name, value, type}`` dict and return a protobuf Metric."""
    if not isinstance(metric, dict):
        raise MetricError(
            f"each metric must be an object with name, value and type, got {metric!r}"
        )
    unknown = sorted(set(metric) - {"name", "value", "type"})
    if unknown:
        raise MetricError(f"unknown metric field(s) {unknown}; allowed: name, value, type")
    name = metric.get("name")
    if not isinstance(name, str) or not name:
        raise MetricError("each metric needs a non-empty string 'name'")
    if "type" not in metric:
        raise MetricError(f"metric {name!r} needs a 'type' ({', '.join(ACCEPTED_TYPES)})")
    if "value" not in metric:
        raise MetricError(f"metric {name!r} needs a 'value' (use null for a null value)")
    try:
        type_key = resolve_type(metric["type"])
    except MetricError as exc:
        raise MetricError(f"metric {name!r}: {exc}") from None
    spec = _TYPES[type_key]
    value = metric["value"]

    out = pb.Payload.Metric()
    out.name = name
    out.timestamp = now_ms() if timestamp is None else timestamp
    out.datatype = spec.datatype
    if value is None:
        out.is_null = True
        return out

    try:
        if spec.kind in ("int", "datetime"):
            number = _coerce_int(value, type_key, spec)
            if spec.bits is not None:
                number &= (1 << spec.bits) - 1  # two's complement for negative values
            setattr(out, spec.field, number)
        elif spec.kind == "float":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise MetricError(f"a {type_key} value must be a number, got {value!r}")
            number = float(value)
            if not math.isfinite(number):
                raise MetricError(f"a {type_key} value must be finite, got {value!r}")
            if type_key == "float" and abs(number) > _FLOAT32_MAX:
                raise MetricError(f"{value} is out of range for a 32-bit float")
            setattr(out, spec.field, number)
        elif spec.kind == "bool":
            if not isinstance(value, bool):
                raise MetricError(f"a boolean value must be true or false, got {value!r}")
            out.boolean_value = value
        elif spec.kind == "str":
            if not isinstance(value, str):
                raise MetricError(f"a {type_key} value must be a string, got {value!r}")
            out.string_value = value
        elif spec.kind == "uuid":
            if not isinstance(value, str):
                raise MetricError(f"a uuid value must be a string, got {value!r}")
            try:
                uuid_module.UUID(value)
            except ValueError:
                raise MetricError(f"{value!r} is not a valid UUID") from None
            out.string_value = value
        elif spec.kind == "bytes":
            if not isinstance(value, str):
                raise MetricError(f"a bytes value must be a hex string, got {value!r}")
            try:
                out.bytes_value = bytes.fromhex(value)
            except ValueError:
                raise MetricError(f"a bytes value must be a hex string, got {value!r}") from None
    except MetricError as exc:
        raise MetricError(f"metric {name!r}: {exc}") from None
    return out


def build_payload(
    metrics: list[Any] | None,
    *,
    seq: int | None,
    extra_metrics: list[pb.Payload.Metric] = (),
) -> pb.Payload:
    """Build a Payload from metric dicts. ``seq=None`` leaves the seq field out."""
    if metrics is None:
        metrics = []
    if not isinstance(metrics, list):
        raise MetricError("'metrics' must be a list of {name, value, type} objects")
    stamp = now_ms()
    payload = pb.Payload()
    payload.timestamp = stamp
    for extra in extra_metrics:
        payload.metrics.append(extra)
    for metric in metrics:
        payload.metrics.append(build_metric(metric, timestamp=stamp))
    if seq is not None:
        payload.seq = seq
    return payload


def bd_seq_metric(bd_seq: int) -> pb.Payload.Metric:
    return build_metric({"name": BD_SEQ_METRIC, "value": bd_seq, "type": "int64"})


def rebirth_metric() -> pb.Payload.Metric:
    return build_metric({"name": REBIRTH_METRIC, "value": False, "type": "boolean"})


def _signed(value: int, bits: int) -> int:
    value &= (1 << bits) - 1
    return value - (1 << bits) if value >= 1 << (bits - 1) else value


def float32_repr(value: float) -> float:
    """The shortest decimal that is the same 32-bit float (23.100000381469727 -> 23.1)."""
    if not math.isfinite(value):
        return value
    target = struct.pack("<f", value)
    for digits in range(1, 10):
        candidate = float(f"{value:.{digits}g}")
        if struct.pack("<f", candidate) == target:
            return candidate
    return value


def metric_value(metric: pb.Payload.Metric, datatype: int | None = None) -> Any:
    """Return the Python value of a metric.

    ``datatype`` overrides the metric's own datatype field; DATA and CMD
    messages often omit it, and the birth certificate then supplies it.
    """
    if metric.is_null:
        return None
    if datatype is None and metric.HasField("datatype"):
        datatype = metric.datatype
    which = metric.WhichOneof("value")
    if which is None:
        return None
    raw = getattr(metric, which)
    if which in ("int_value", "long_value"):
        bits = _SIGNED_BITS.get(datatype)
        return _signed(raw, bits) if bits else raw
    if which == "float_value":
        return float32_repr(raw)
    if which == "bytes_value":
        return raw.hex()
    if which in ("dataset_value", "template_value", "extension_value"):
        from google.protobuf import json_format

        return json_format.MessageToDict(raw, preserving_proto_field_name=True)
    return raw


def datatype_name(datatype: int | None) -> str | None:
    if datatype is None:
        return None
    try:
        return pb.DataType.Name(datatype)
    except ValueError:
        return f"Unknown({datatype})"


def decode_metric(metric: pb.Payload.Metric, datatype: int | None = None) -> dict[str, Any]:
    own = metric.datatype if metric.HasField("datatype") else None
    effective = own if own is not None else datatype
    out: dict[str, Any] = {
        "name": metric.name if metric.HasField("name") else None,
        "datatype": datatype_name(effective),
        "value": metric_value(metric, effective),
    }
    if metric.HasField("alias"):
        out["alias"] = metric.alias
    if metric.HasField("timestamp"):
        out["timestamp"] = metric.timestamp
    if metric.is_null:
        out["is_null"] = True
    if metric.is_historical:
        out["is_historical"] = True
    if metric.is_transient:
        out["is_transient"] = True
    return out


def parse_payload(data: bytes) -> pb.Payload:
    payload = pb.Payload()
    payload.ParseFromString(data)
    return payload


def decode_payload(data: bytes, known_datatypes: dict[str, int] | None = None) -> dict[str, Any]:
    """Decode Sparkplug B payload bytes into a JSON-friendly dict.

    Raises ``google.protobuf.message.DecodeError`` if the bytes are not a
    Sparkplug B payload.
    """
    payload = parse_payload(data)
    known = known_datatypes or {}
    metrics = []
    for metric in payload.metrics:
        hint = None
        if not metric.HasField("datatype"):
            hint = known.get(metric.name) if metric.HasField("name") else None
            if hint is None and metric.HasField("alias"):
                hint = known.get(f"alias:{metric.alias}")
        metrics.append(decode_metric(metric, hint))
    out: dict[str, Any] = {
        "timestamp": payload.timestamp if payload.HasField("timestamp") else None,
        "seq": payload.seq if payload.HasField("seq") else None,
        "metrics": metrics,
    }
    if payload.HasField("uuid"):
        out["uuid"] = payload.uuid
    if payload.HasField("body"):
        out["body_hex"] = payload.body.hex()
    return out
