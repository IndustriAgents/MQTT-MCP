"""Sparkplug B payload schema (``org.eclipse.tahu.protobuf.Payload``).

This module offers the same message classes and ``DataType`` enum as the
``sparkplug_b_pb2`` module that ``protoc`` generates from the Sparkplug B
``sparkplug_b.proto`` (Sparkplug B 3.0, Eclipse Tahu): ``Payload``,
``Payload.Metric``, ``Payload.MetaData``, ``Payload.PropertyValue``,
``Payload.PropertySet``, ``Payload.PropertySetList``, ``Payload.DataSet`` and
``Payload.Template``, with the same package, message names, field names,
field numbers and wire format.

The difference is how the schema gets into Python. A protoc-generated
``*_pb2.py`` is tied to the protoc release that wrote it: newer protobuf
runtimes warn about older generated code and eventually refuse it. Here the
``FileDescriptorProto`` is built in Python at import time, which works with
every protobuf runtime from 5.27 on (the floor declared in pyproject.toml:
older upb runtimes do not load on Python 3.14, and 4.24 crashes when two
private pools hold this file, as when the server and mock copies share a
process).
The descriptor lives in a private pool, so it cannot clash with another copy
of the Sparkplug schema loaded into the default pool by some other package.

The same file is shipped in ``mqtt-mock-server/sparkplug_b_pb2.py``; a test
checks that the two copies stay identical.
"""

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.internal import enum_type_wrapper

_F = descriptor_pb2.FieldDescriptorProto
_PACKAGE = "org.eclipse.tahu.protobuf"
_FILE_NAME = "sparkplug_b.proto"
# Protobuf field numbers go up to 2**29 - 1; descriptor ranges are end-exclusive.
_EXTENSIONS_MAX = 2**29

_SCALARS = {
    "string": _F.TYPE_STRING,
    "bytes": _F.TYPE_BYTES,
    "bool": _F.TYPE_BOOL,
    "uint32": _F.TYPE_UINT32,
    "uint64": _F.TYPE_UINT64,
    "float": _F.TYPE_FLOAT,
    "double": _F.TYPE_DOUBLE,
}

# (enum name, number) pairs of the Sparkplug B DataType enum.
_DATA_TYPES = (
    ("Unknown", 0),
    ("Int8", 1),
    ("Int16", 2),
    ("Int32", 3),
    ("Int64", 4),
    ("UInt8", 5),
    ("UInt16", 6),
    ("UInt32", 7),
    ("UInt64", 8),
    ("Float", 9),
    ("Double", 10),
    ("Boolean", 11),
    ("String", 12),
    ("DateTime", 13),
    ("Text", 14),
    ("UUID", 15),
    ("DataSet", 16),
    ("Bytes", 17),
    ("File", 18),
    ("Template", 19),
    ("PropertySet", 20),
    ("PropertySetList", 21),
    ("Int8Array", 22),
    ("Int16Array", 23),
    ("Int32Array", 24),
    ("Int64Array", 25),
    ("UInt8Array", 26),
    ("UInt16Array", 27),
    ("UInt32Array", 28),
    ("UInt64Array", 29),
    ("FloatArray", 30),
    ("DoubleArray", 31),
    ("BooleanArray", 32),
    ("StringArray", 33),
    ("DateTimeArray", 34),
)


def _message(container, name, fields, oneof=None, extensions_from=None, nested=()):
    """Add a proto2 message to ``container``.

    ``fields`` is a sequence of ``(name, number, type, repeated)`` tuples, where
    ``type`` is a scalar name from ``_SCALARS`` or a fully qualified message
    name starting with ``"."``. Fields whose name is listed in ``oneof``
    (a ``(oneof name, field names)`` pair) belong to that oneof.
    """
    msg = container.add(name=name)
    oneof_fields = set()
    if oneof is not None:
        msg.oneof_decl.add(name=oneof[0])
        oneof_fields = set(oneof[1])
    for field_name, number, field_type, repeated in fields:
        field = msg.field.add(
            name=field_name,
            number=number,
            label=_F.LABEL_REPEATED if repeated else _F.LABEL_OPTIONAL,
        )
        if field_type.startswith("."):
            field.type = _F.TYPE_MESSAGE
            field.type_name = field_type
        else:
            field.type = _SCALARS[field_type]
        if field_name in oneof_fields:
            field.oneof_index = 0
    if extensions_from is not None:
        msg.extension_range.add(start=extensions_from, end=_EXTENSIONS_MAX)
    for build_nested in nested:
        build_nested(msg.nested_type)
    return msg


def _extension_only(name):
    def build(container):
        _message(container, name, (), extensions_from=1)

    return build


def _build_file() -> descriptor_pb2.FileDescriptorProto:
    p = f".{_PACKAGE}.Payload"
    file_proto = descriptor_pb2.FileDescriptorProto(
        name=_FILE_NAME, package=_PACKAGE, syntax="proto2"
    )
    file_proto.options.java_package = "org.eclipse.tahu.protobuf"
    file_proto.options.java_outer_classname = "SparkplugBProto"

    data_type = file_proto.enum_type.add(name="DataType")
    for enum_name, number in _DATA_TYPES:
        data_type.value.add(name=enum_name, number=number)

    def template(container):
        def parameter(inner):
            _message(
                inner,
                "Parameter",
                (
                    ("name", 1, "string", False),
                    ("type", 2, "uint32", False),
                    ("int_value", 3, "uint32", False),
                    ("long_value", 4, "uint64", False),
                    ("float_value", 5, "float", False),
                    ("double_value", 6, "double", False),
                    ("boolean_value", 7, "bool", False),
                    ("string_value", 8, "string", False),
                    (
                        "extension_value",
                        9,
                        f"{p}.Template.Parameter.ParameterValueExtension",
                        False,
                    ),
                ),
                oneof=(
                    "value",
                    (
                        "int_value",
                        "long_value",
                        "float_value",
                        "double_value",
                        "boolean_value",
                        "string_value",
                        "extension_value",
                    ),
                ),
                nested=(_extension_only("ParameterValueExtension"),),
            )

        _message(
            container,
            "Template",
            (
                ("version", 1, "string", False),
                ("metrics", 2, f"{p}.Metric", True),
                ("parameters", 3, f"{p}.Template.Parameter", True),
                ("template_ref", 4, "string", False),
                ("is_definition", 5, "bool", False),
            ),
            extensions_from=6,
            nested=(parameter,),
        )

    def data_set(container):
        def data_set_value(inner):
            _message(
                inner,
                "DataSetValue",
                (
                    ("int_value", 1, "uint32", False),
                    ("long_value", 2, "uint64", False),
                    ("float_value", 3, "float", False),
                    ("double_value", 4, "double", False),
                    ("boolean_value", 5, "bool", False),
                    ("string_value", 6, "string", False),
                    (
                        "extension_value",
                        7,
                        f"{p}.DataSet.DataSetValue.DataSetValueExtension",
                        False,
                    ),
                ),
                oneof=(
                    "value",
                    (
                        "int_value",
                        "long_value",
                        "float_value",
                        "double_value",
                        "boolean_value",
                        "string_value",
                        "extension_value",
                    ),
                ),
                nested=(_extension_only("DataSetValueExtension"),),
            )

        def row(inner):
            _message(
                inner,
                "Row",
                (("elements", 1, f"{p}.DataSet.DataSetValue", True),),
                extensions_from=2,
            )

        _message(
            container,
            "DataSet",
            (
                ("num_of_columns", 1, "uint64", False),
                ("columns", 2, "string", True),
                ("types", 3, "uint32", True),
                ("rows", 4, f"{p}.DataSet.Row", True),
            ),
            extensions_from=5,
            nested=(data_set_value, row),
        )

    def property_value(container):
        _message(
            container,
            "PropertyValue",
            (
                ("type", 1, "uint32", False),
                ("is_null", 2, "bool", False),
                ("int_value", 3, "uint32", False),
                ("long_value", 4, "uint64", False),
                ("float_value", 5, "float", False),
                ("double_value", 6, "double", False),
                ("boolean_value", 7, "bool", False),
                ("string_value", 8, "string", False),
                ("propertyset_value", 9, f"{p}.PropertySet", False),
                ("propertysets_value", 10, f"{p}.PropertySetList", False),
                (
                    "extension_value",
                    11,
                    f"{p}.PropertyValue.PropertyValueExtension",
                    False,
                ),
            ),
            oneof=(
                "value",
                (
                    "int_value",
                    "long_value",
                    "float_value",
                    "double_value",
                    "boolean_value",
                    "string_value",
                    "propertyset_value",
                    "propertysets_value",
                    "extension_value",
                ),
            ),
            nested=(_extension_only("PropertyValueExtension"),),
        )

    def property_set(container):
        _message(
            container,
            "PropertySet",
            (
                ("keys", 1, "string", True),
                ("values", 2, f"{p}.PropertyValue", True),
            ),
            extensions_from=3,
        )

    def property_set_list(container):
        _message(
            container,
            "PropertySetList",
            (("propertyset", 1, f"{p}.PropertySet", True),),
            extensions_from=2,
        )

    def meta_data(container):
        _message(
            container,
            "MetaData",
            (
                ("is_multi_part", 1, "bool", False),
                ("content_type", 2, "string", False),
                ("size", 3, "uint64", False),
                ("seq", 4, "uint64", False),
                ("file_name", 5, "string", False),
                ("file_type", 6, "string", False),
                ("md5", 7, "string", False),
                ("description", 8, "string", False),
            ),
            extensions_from=9,
        )

    def metric(container):
        _message(
            container,
            "Metric",
            (
                ("name", 1, "string", False),
                ("alias", 2, "uint64", False),
                ("timestamp", 3, "uint64", False),
                ("datatype", 4, "uint32", False),
                ("is_historical", 5, "bool", False),
                ("is_transient", 6, "bool", False),
                ("is_null", 7, "bool", False),
                ("metadata", 8, f"{p}.MetaData", False),
                ("properties", 9, f"{p}.PropertySet", False),
                ("int_value", 10, "uint32", False),
                ("long_value", 11, "uint64", False),
                ("float_value", 12, "float", False),
                ("double_value", 13, "double", False),
                ("boolean_value", 14, "bool", False),
                ("string_value", 15, "string", False),
                ("bytes_value", 16, "bytes", False),
                ("dataset_value", 17, f"{p}.DataSet", False),
                ("template_value", 18, f"{p}.Template", False),
                ("extension_value", 19, f"{p}.Metric.MetricValueExtension", False),
            ),
            oneof=(
                "value",
                (
                    "int_value",
                    "long_value",
                    "float_value",
                    "double_value",
                    "boolean_value",
                    "string_value",
                    "bytes_value",
                    "dataset_value",
                    "template_value",
                    "extension_value",
                ),
            ),
            nested=(_extension_only("MetricValueExtension"),),
        )

    _message(
        file_proto.message_type,
        "Payload",
        (
            ("timestamp", 1, "uint64", False),
            ("metrics", 2, f"{p}.Metric", True),
            ("seq", 3, "uint64", False),
            ("uuid", 4, "string", False),
            ("body", 5, "bytes", False),
        ),
        extensions_from=6,
        nested=(
            template,
            data_set,
            property_value,
            property_set,
            property_set_list,
            meta_data,
            metric,
        ),
    )
    return file_proto


_POOL = descriptor_pool.DescriptorPool()
_POOL.AddSerializedFile(_build_file().SerializeToString())
DESCRIPTOR = _POOL.FindFileByName(_FILE_NAME)

DataType = enum_type_wrapper.EnumTypeWrapper(DESCRIPTOR.enum_types_by_name["DataType"])
Payload = message_factory.GetMessageClass(DESCRIPTOR.message_types_by_name["Payload"])

# Module-level enum constants, as protoc generates them (Int32, Boolean, ...).
globals().update({name: number for name, number in _DATA_TYPES})

__all__ = ["DESCRIPTOR", "DataType", "Payload"] + [name for name, _ in _DATA_TYPES]
