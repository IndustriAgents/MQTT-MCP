"""Sparkplug B schema, encoding and decoding (no broker needed)."""

from __future__ import annotations

import pytest
from google.protobuf.message import DecodeError

from mqtt_mcp import sparkplug
from mqtt_mcp import sparkplug_b_pb2 as pb
from mqtt_mcp.sparkplug import MetricError, build_metric, build_payload, decode_payload

from .conftest import REPO

# The sample in README.md: DDATA payload, timestamp 2026-01-01T00:00:00Z, seq 1,
# temperature (Float) 23.1 and offset (Int32) -5.
README_SAMPLE_HEX = (
    "0880d0eab6b733121b0a0b74656d70657261747572651880d0eab6b733200965cdccb84112170a06"
    "6f66667365741880d0eab6b733200350fbffffff0f1801"
)


def roundtrip(metric: dict) -> dict:
    payload = build_payload([metric], seq=3)
    return decode_payload(payload.SerializeToString())["metrics"][0]


# --- the schema matches the Sparkplug B specification (sparkplug_b.proto) ------


def test_payload_field_numbers_match_the_spec():
    fields = {f.name: f.number for f in pb.Payload.DESCRIPTOR.fields}
    assert fields == {"timestamp": 1, "metrics": 2, "seq": 3, "uuid": 4, "body": 5}
    assert pb.Payload.DESCRIPTOR.full_name == "org.eclipse.tahu.protobuf.Payload"


def test_metric_field_numbers_match_the_spec():
    # The old hand-rolled encoder put the timestamp in field 2 (alias) and the
    # values in fields 5, 9, 11 and 12; these are the numbers of sparkplug_b.proto.
    fields = {f.name: f.number for f in pb.Payload.Metric.DESCRIPTOR.fields}
    assert fields == {
        "name": 1,
        "alias": 2,
        "timestamp": 3,
        "datatype": 4,
        "is_historical": 5,
        "is_transient": 6,
        "is_null": 7,
        "metadata": 8,
        "properties": 9,
        "int_value": 10,
        "long_value": 11,
        "float_value": 12,
        "double_value": 13,
        "boolean_value": 14,
        "string_value": 15,
        "bytes_value": 16,
        "dataset_value": 17,
        "template_value": 18,
        "extension_value": 19,
    }
    oneof = pb.Payload.Metric.DESCRIPTOR.oneofs_by_name["value"]
    assert [f.number for f in oneof.fields] == list(range(10, 20))


def test_datatype_enum_matches_the_spec():
    expected = {
        "Int8": 1,
        "Int16": 2,
        "Int32": 3,
        "Int64": 4,
        "UInt8": 5,
        "UInt16": 6,
        "UInt32": 7,
        "UInt64": 8,
        "Float": 9,
        "Double": 10,
        "Boolean": 11,
        "String": 12,
        "DateTime": 13,
        "Text": 14,
        "UUID": 15,
        "DataSet": 16,
        "Bytes": 17,
        "File": 18,
        "Template": 19,
        "PropertySet": 20,
        "PropertySetList": 21,
        "DateTimeArray": 34,
    }
    for name, number in expected.items():
        assert pb.DataType.Value(name) == number
        assert getattr(pb, name) == number


def test_hand_assembled_wire_bytes_decode():
    """Bytes written from the spec's field numbers by hand, independent of our schema."""
    metric = (
        bytes([0x0A, 0x01]) + b"a"  # 1 name = "a"
        + bytes([0x20, 0x03])  # 4 datatype = Int32
        + bytes([0x50, 0xFB, 0xFF, 0xFF, 0xFF, 0x0F])  # 10 int_value = 0xFFFFFFFB
    )  # fmt: skip
    data = bytes([0x08, 0x01, 0x12, len(metric)]) + metric + bytes([0x18, 0x02])
    decoded = decode_payload(data)
    assert decoded["timestamp"] == 1
    assert decoded["seq"] == 2
    assert decoded["metrics"] == [{"name": "a", "datatype": "Int32", "value": -5}]


def test_readme_sample_decodes():
    decoded = decode_payload(bytes.fromhex(README_SAMPLE_HEX))
    assert decoded["seq"] == 1
    assert decoded["timestamp"] == 1767225600000
    values = {m["name"]: (m["datatype"], m["value"]) for m in decoded["metrics"]}
    assert values == {"temperature": ("Float", 23.1), "offset": ("Int32", -5)}
    assert README_SAMPLE_HEX in (REPO / "README.md").read_text()


def test_garbage_is_not_a_payload():
    with pytest.raises(DecodeError):
        decode_payload(b"\xff\xff\xff")


# --- encoding ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("type_name", "value", "datatype", "field"),
    [
        ("int8", -128, "Int8", "int_value"),
        ("int16", -1, "Int16", "int_value"),
        ("int32", -(2**31), "Int32", "int_value"),
        ("int", -5, "Int32", "int_value"),
        ("int64", -(2**63), "Int64", "long_value"),
        ("long", -1, "Int64", "long_value"),
        ("uint8", 255, "UInt8", "int_value"),
        ("uint16", 65535, "UInt16", "int_value"),
        ("uint32", 2**32 - 1, "UInt32", "int_value"),
        ("uint64", 2**64 - 1, "UInt64", "long_value"),
        ("float", 23.1, "Float", "float_value"),
        ("double", -1e300, "Double", "double_value"),
        ("boolean", True, "Boolean", "boolean_value"),
        ("bool", False, "Boolean", "boolean_value"),
        ("BOOLEAN", True, "Boolean", "boolean_value"),
        ("string", "online", "String", "string_value"),
        ("text", "long text", "Text", "string_value"),
        ("uuid", "123e4567-e89b-12d3-a456-426614174000", "UUID", "string_value"),
        ("datetime", 1767225600000, "DateTime", "long_value"),
        ("bytes", "00ff10", "Bytes", "bytes_value"),
    ],
)
def test_every_type_roundtrips(type_name, value, datatype, field):
    metric = build_metric({"name": "m", "value": value, "type": type_name})
    assert metric.WhichOneof("value") == field
    assert pb.DataType.Name(metric.datatype) == datatype
    decoded = roundtrip({"name": "m", "value": value, "type": type_name})
    assert decoded["datatype"] == datatype
    expected = (
        str(value)
        if isinstance(value, int) and not isinstance(value, bool) and abs(value) > 2**53 - 1
        else value
    )
    assert decoded["value"] == expected
    if expected != value:
        assert decoded["value_encoding"] == "decimal"


def test_negative_int32_uses_32_bit_twos_complement():
    # The old encoder looped forever on negative numbers.
    metric = build_metric({"name": "m", "value": -1, "type": "int32"})
    assert metric.int_value == 0xFFFFFFFF
    assert metric.SerializeToString().endswith(bytes([0x50, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F]))
    assert build_metric({"name": "m", "value": -1, "type": "int8"}).int_value == 0xFFFFFFFF
    assert build_metric({"name": "m", "value": -1, "type": "int64"}).long_value == 2**64 - 1


def test_tahu_python_style_int8_also_decodes():
    """Eclipse Tahu's Python library stores Int8 -1 as 0xFF; read it back as -1."""
    metric = pb.Payload.Metric(name="m", datatype=pb.Int8, int_value=0xFF)
    assert sparkplug.metric_value(metric) == -1


def test_boolean_is_a_boolean_not_a_string():
    # The old encoder sent type "boolean" as the string "True".
    metric = build_metric({"name": "enabled", "value": True, "type": "boolean"})
    assert metric.datatype == pb.Boolean
    assert metric.WhichOneof("value") == "boolean_value"


def test_millisecond_timestamps_are_encoded_correctly():
    # The old varint encoder truncated values >= 2**32 on some inputs (e.g. 2**32 -> 0x00).
    payload = build_payload([{"name": "t", "value": 2**32, "type": "uint64"}], seq=0)
    decoded = decode_payload(payload.SerializeToString())
    assert decoded["metrics"][0]["value"] == 2**32
    assert decoded["timestamp"] > 2**32


def test_null_value():
    decoded = roundtrip({"name": "m", "value": None, "type": "float"})
    assert decoded["value"] is None
    assert decoded["is_null"] is True


def test_seq_is_optional():
    assert decode_payload(build_payload([], seq=None).SerializeToString())["seq"] is None
    assert decode_payload(build_payload([], seq=0).SerializeToString())["seq"] == 0


@pytest.mark.parametrize(
    ("metric", "message"),
    [
        ({"name": "m", "value": 1, "type": "complex"}, "unsupported metric type"),
        ({"name": "m", "value": 1}, "needs a 'type'"),
        ({"name": "m", "type": "int32"}, "needs a 'value'"),
        ({"value": 1, "type": "int32"}, "non-empty string 'name'"),
        ({"name": "m", "value": 1, "type": "int32", "unit": "%"}, "unknown metric field"),
        ({"name": "m", "value": "7", "type": "int32"}, "must be an integer"),
        ({"name": "m", "value": True, "type": "int32"}, "must be an integer"),
        ({"name": "m", "value": 1.5, "type": "int32"}, "must be an integer"),
        ({"name": "m", "value": 128, "type": "int8"}, "out of range"),
        ({"name": "m", "value": -1, "type": "uint32"}, "out of range"),
        ({"name": "m", "value": 2**31, "type": "int32"}, "out of range"),
        ({"name": "m", "value": 1e39, "type": "float"}, "out of range"),
        ({"name": "m", "value": "1.0", "type": "double"}, "must be a number"),
        ({"name": "m", "value": "true", "type": "boolean"}, "true or false"),
        ({"name": "m", "value": 1, "type": "bool"}, "true or false"),
        ({"name": "m", "value": 5, "type": "string"}, "must be a string"),
        ({"name": "m", "value": "nope", "type": "uuid"}, "not a valid UUID"),
        ({"name": "m", "value": "xyz", "type": "bytes"}, "hex string"),
        ("temperature", "must be an object"),
    ],
)
def test_invalid_metrics_are_rejected(metric, message):
    with pytest.raises(MetricError, match=message):
        build_metric(metric)


def test_integral_float_is_accepted_for_int_types():
    assert build_metric({"name": "m", "value": 75.0, "type": "int32"}).int_value == 75


# --- topics -----------------------------------------------------------------------


def test_parse_topic():
    assert sparkplug.parse_topic("spBv1.0/g/NDATA/n") == {
        "group_id": "g",
        "message_type": "NDATA",
        "edge_node_id": "n",
    }
    assert sparkplug.parse_topic("spBv1.0/g/DCMD/n/d")["device_id"] == "d"
    assert sparkplug.parse_topic("spBv1.0/STATE/host")["message_type"] == "STATE"
    assert sparkplug.parse_topic("spBv1.0/g/NDATA/n/extra") is None
    assert sparkplug.parse_topic("sensors/temperature") is None


@pytest.mark.parametrize("bad", ["", "a/b", "a+", "#", 5])
def test_invalid_ids(bad):
    with pytest.raises(ValueError):
        sparkplug.validate_id(bad, "device_id")


def test_registry_tolerates_unknown_datatypes_and_aliases():
    from mqtt_mcp.inbox import SparkplugRegistry

    registry = SparkplugRegistry()
    birth = pb.Payload(seq=0)
    birth.metrics.add(name="odd", datatype=99, int_value=1)
    birth.metrics.add(name="level", alias=7, datatype=pb.Int16, int_value=0xFFFF)
    decoded, error = registry.observe("spBv1.0/g/NBIRTH/n", birth.SerializeToString(), 0.0)
    assert error is None
    assert decoded["metrics"][0]["datatype"] == "Unknown(99)"
    data = pb.Payload(seq=1)
    data.metrics.add(alias=7, int_value=0xFFFE)  # alias only, no datatype: Int16 from the birth
    registry.observe("spBv1.0/g/NDATA/n", data.SerializeToString(), 1.0)
    (node,) = registry.snapshot()
    assert node["metrics"]["level"]["value"] == -2
    assert node["online"] is True and node["last_seq"] == 1
    _, error = registry.observe("spBv1.0/g/NDATA/n", b"\xff\xff", 2.0)
    assert "not a valid Sparkplug B payload" in error


def test_registry_forgets_online_state_when_this_server_loses_its_connection():
    from mqtt_mcp.inbox import SparkplugRegistry

    registry = SparkplugRegistry()
    registry.observe("spBv1.0/g/NDATA/n", pb.Payload(seq=1).SerializeToString(), 0.0)
    registry.observe("spBv1.0/g/DDATA/n/d", pb.Payload(seq=2).SerializeToString(), 0.0)
    (node,) = registry.snapshot()
    assert node["online"] is True and node["devices"][0]["online"] is True
    registry.mark_unknown()  # deaths published while we were away are missed
    (node,) = registry.snapshot()
    assert node["online"] is None and node["devices"][0]["online"] is None


@pytest.mark.parametrize(
    "type_name,value",
    [
        ("int64", -(2**63)),
        ("uint64", 2**64 - 1),
        ("int64", 2**53),
        ("int64", -(2**53)),
        ("uint64", 2**53 - 1),
    ],
)
def test_wide_integer_json_values_are_lossless(type_name, value):
    import json

    decoded = roundtrip({"name": "wide", "value": str(value), "type": type_name})
    if abs(value) > 2**53 - 1:
        assert decoded["value"] == str(value) and decoded["value_encoding"] == "decimal"
    else:
        assert decoded["value"] == value and "value_encoding" not in decoded
    json.dumps(decoded, allow_nan=False)


@pytest.mark.parametrize(
    "value,expected",
    [(float("nan"), "NaN"), (float("inf"), "Infinity"), (-float("inf"), "-Infinity")],
)
def test_nonfinite_received_values_have_explicit_encoding(value, expected):
    import json

    payload = pb.Payload(seq=1)
    payload.metrics.add(name="bad", datatype=pb.Double, double_value=value)
    metric = decode_payload(payload.SerializeToString())["metrics"][0]
    assert metric["value"] == expected and metric["value_encoding"] == "nonfinite"
    json.dumps(metric, allow_nan=False)


def test_registry_preserves_wide_integer_encoding_after_alias_resolution():
    from mqtt_mcp.inbox import SparkplugRegistry

    registry = SparkplugRegistry()
    birth = pb.Payload(seq=0)
    birth.metrics.add(name="counter", alias=7, datatype=pb.UInt64, long_value=2**64 - 1)
    registry.observe("spBv1.0/g/NBIRTH/n", birth.SerializeToString(), 0.0)
    data = pb.Payload(seq=1)
    data.metrics.add(alias=7, long_value=2**64 - 2)
    decoded, _ = registry.observe("spBv1.0/g/NDATA/n", data.SerializeToString(), 1.0)
    assert decoded["metrics"][0]["value"] == str(2**64 - 2)
    metric = registry.snapshot()[0]["metrics"]["counter"]
    assert metric["value"] == str(2**64 - 2) and metric["value_encoding"] == "decimal"
