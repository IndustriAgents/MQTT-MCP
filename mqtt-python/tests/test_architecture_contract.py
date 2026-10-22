"""Contract, authorization, audit and concurrency regressions (no device needed)."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator, ValidationError
from mcp.server.fastmcp import FastMCP

from mqtt_mcp.application.execution import Execution
from mqtt_mcp.domain.policy import Policy
from mqtt_mcp.protocol.contract import ContractBoundary, as_result, envelope_of, install_contract


def run(coro):
    return asyncio.run(coro)


def boundary(policy=None):
    calls = []

    async def call(name, arguments):
        calls.append((name, arguments))
        return as_result({"success": True, "data": arguments, "error": None, "meta": {}})

    b = ContractBoundary(SimpleNamespace(call_tool=call), policy or Policy())
    return b, calls


def sample(schema):
    if "const" in schema:
        return schema["const"]
    if "enum" in schema:
        return schema["enum"][0]
    if "anyOf" in schema:
        return sample(schema["anyOf"][0])
    kind = schema.get("type")
    if kind == "object":
        return {k: sample(schema["properties"][k]) for k in schema.get("required", [])}
    if kind == "array":
        return [sample(schema.get("items", {})) for _ in range(max(1, schema.get("minItems", 0)))]
    if kind in ("integer", "number"):
        return schema.get("minimum", 1)
    if kind == "boolean":
        return True
    if kind == "null":
        return None
    return "example"


def test_bundled_contract_matches_repository_and_is_valid():
    b, _ = boundary()
    root = Path(__file__).resolve().parents[2]
    assert b.contract == json.loads((root / "contract/tools.json").read_text())
    assert len(b.tools) == len(b.contract["tools"])
    for tool in b.tools.values():
        Draft202012Validator.check_schema(tool["inputSchema"])
        assert tool["description"]
    Draft202012Validator.check_schema(b.contract["resultSchema"])


def test_each_consolidated_mode_routes_once_with_selector_removed():
    b, calls = boundary()
    for name, route in b.contract["routes"].items():
        if "selector" not in route:
            continue
        for variant in b.tools[name]["inputSchema"]["oneOf"]:
            args = sample(variant)
            Draft202012Validator(b.tools[name]["inputSchema"]).validate(args)
            before = len(calls)
            value = envelope_of(run(b.call_tool(name, args)))
            assert value["success"], value
            assert len(calls) == before + 1
            selector = route["selector"]
            assert calls[-1] == (
                route["options"][args[selector]],
                {k: v for k, v in args.items() if k != selector},
            )


def test_unknown_mode_and_extra_arguments_never_dispatch():
    b, calls = boundary()
    for name, route in b.contract["routes"].items():
        if "selector" not in route:
            continue
        bad = {route["selector"]: "not-a-mode"}
        assert envelope_of(run(b.call_tool(name, bad)))["success"] is False
        good = sample(b.tools[name]["inputSchema"]["oneOf"][0])
        good["misspelled_parameter"] = 1
        assert envelope_of(run(b.call_tool(name, good)))["success"] is False
    assert calls == []


def test_legacy_tools_cannot_bypass_read_only_or_allowlist():
    b, calls = boundary(Policy(read_only=True))
    for name, canonical in b.contract["legacy"].items():
        if b.tools[canonical]["annotations"].get("readOnlyHint"):
            continue
        args = sample(b.contract["legacySchemas"][name])
        result = envelope_of(run(b.call_tool(name, args)))
        assert result["success"] is False
        assert result["meta"]["outcome"] == "not_sent"
    assert calls == []
    b, calls = boundary(Policy(allowed_tools=()))
    for name, schema in b.contract["legacySchemas"].items():
        assert envelope_of(run(b.call_tool(name, sample(schema))))["success"] is False
    assert calls == []


def test_instances_do_not_share_admission_or_policy():
    a = Execution(Policy(max_in_flight=1, read_only=True))
    b = Execution(Policy(max_in_flight=1))
    first = a.admit("read", "read", write=False, local=False)
    with pytest.raises(ValueError):
        a.admit("read", "read", write=False, local=False)
    status = a.admit("status", "status", write=False, local=True)
    with pytest.raises(ValueError):
        a.admit("status", "status", write=False, local=True)
    control = b.admit("write", "write", write=True, local=False)
    with pytest.raises(ValueError):
        b.admit("write", "write", write=True, local=False)
    a.finish(first, None)
    a.finish(status, None)
    b.finish(control, None)
    assert (a.active, a.reserved, a.controls, b.active, b.controls) == (0, 0, 0, 0, 0)


def test_audit_intent_precedes_dispatch_and_never_logs_payload(tmp_path):
    path = tmp_path / "audit.jsonl"
    e = Execution(Policy(audit_path=str(path)))
    ticket = e.admit("write", "write", write=True, local=False)
    assert json.loads(path.read_text())["phase"] == "intent"
    e.finish(
        ticket,
        {"success": False, "data": "secret", "meta": {"outcome": "unknown", "request_sent": True}},
    )
    entries = [json.loads(line) for line in path.read_text().splitlines()]
    assert entries[0]["call_id"] == entries[1]["call_id"]
    assert entries[1]["outcome"] == "unknown"
    assert "secret" not in path.read_text()
    e = Execution(Policy(audit_path=str(tmp_path / "missing" / "audit")))
    with pytest.raises(OSError):
        e.admit("write", "write", write=True, local=False)
    assert e.active == e.controls == 0


def test_wire_failure_sets_is_error_and_keeps_structured_envelope():
    b, _ = boundary(Policy(allowed_tools=()))
    name = next(iter(b.tools))
    args = sample(b.tools[name]["inputSchema"].get("oneOf", [b.tools[name]["inputSchema"]])[0])
    result = run(b.wire_call(name, args))
    assert result.isError is True
    assert result.structuredContent == json.loads(result.content[0].text)
    Draft202012Validator(b.contract["resultSchema"]).validate(result.structuredContent)


def test_sdk_callbacks_use_the_contract():
    server = FastMCP("contract-test")
    b = install_contract(server, Policy())
    assert [t.model_dump(exclude_none=True) for t in run(server.list_tools())] == b.contract[
        "tools"
    ]


@pytest.mark.parametrize(
    "value",
    [
        {"read_only": "true"},
        {"max_in_flight": True},
        {"max_in_flight": 0},
        {"allowed_tools": "write"},
        {"unknown": True},
        {"audit_path": ""},
    ],
)
def test_invalid_policy_fails_closed(value):
    with pytest.raises(ValueError):
        Policy.from_dict(value)


def test_factories_snapshot_configuration_without_global_cache(monkeypatch):
    from mqtt_mcp.server import create_server

    monkeypatch.setenv("MQTT_BROKER_URL", "mqtt://127.0.0.1:1883")
    first = create_server()
    monkeypatch.setenv("MQTT_BROKER_URL", "mqtt://127.0.0.1:1884")
    second = create_server()
    assert first.settings.port == 1883
    assert second.settings.port == 1884
    assert first._tool_manager is not second._tool_manager
    assert first.contract_boundary.execution is not second.contract_boundary.execution


def test_only_declared_local_status_tools_use_reserved_capacity():
    b, calls = boundary(Policy(max_in_flight=1))
    ticket = b.execution.admit("busy", "busy", write=False, local=False)
    local_names = set(b.contract["localStatusTools"])
    assert local_names <= b.tools.keys()
    for name, tool in b.tools.items():
        args = sample(tool["inputSchema"].get("oneOf", [tool["inputSchema"]])[0])
        result = envelope_of(run(b.call_tool(name, args)))
        assert result["success"] is (name in local_names), name
    b.execution.finish(ticket, None)


def _properties(schema):
    yield from schema.get("properties", {}).items()


def test_every_argument_is_described_for_agents():
    """Agents read property descriptions; a placeholder there teaches nothing."""
    b, _ = boundary()
    for name, tool in b.tools.items():
        for prop, spec in _properties(tool["inputSchema"]):
            text = spec.get("description", "")
            assert len(text) >= 20, (name, prop)
            assert "for the selected operation" not in text, (name, prop)
            assert "title" not in spec, (name, prop)


def test_documented_ranges_are_in_the_schema():
    b, _ = boundary()
    for name in ("subscribe_topic", "publish_message"):
        assert b.tools[name]["inputSchema"]["properties"]["qos"]["enum"] == [0, 1, 2]
    limit = b.tools["read_messages"]["inputSchema"]["properties"]["limit"]
    assert (limit["minimum"], limit["maximum"]) == (1, 1000)
    sparkplug = b.tools["publish_sparkplug"]["inputSchema"]
    assert sparkplug["properties"]["message_type"]["enum"] == list(
        b.contract["routes"]["publish_sparkplug"]["options"]
    )
    metric = sparkplug["properties"]["metrics"]["items"]
    assert metric["required"] == ["name", "value", "type"]
    assert metric["additionalProperties"] is False


def test_output_schemas_embed_the_declared_result_shapes_and_error_codes():
    b, _ = boundary()
    codes = [k for k in b.contract["errors"] if k != "$comment"]
    for name, tool in b.tools.items():
        output = tool["outputSchema"]
        Draft202012Validator.check_schema(output)
        shape = b.contract["routes"][name]["resultShape"]
        assert output["properties"]["data"]["anyOf"][1] == b.contract["resultShapes"][shape]
        assert output["properties"]["meta"]["properties"]["code"]["enum"] == codes
        ok = {"success": True, "data": None, "error": None, "meta": {}}
        Draft202012Validator(output).validate(ok)
        failed = {"success": False, "data": None, "error": "x", "meta": {}}
        with pytest.raises(ValidationError):
            Draft202012Validator(output).validate(failed)  # a failure must carry a code
        Draft202012Validator(output).validate({**failed, "meta": {"code": "invalid_arguments"}})


def test_every_refusal_carries_a_code():
    b, _ = boundary(Policy(allowed_tools=()))
    assert envelope_of(run(b.call_tool("get_broker_info", {})))["meta"]["code"] == "policy_denied"
    b, _ = boundary(Policy(read_only=True))
    result = envelope_of(run(b.call_tool("publish_message", {"topic": "a", "payload": "x"})))
    assert result["meta"]["code"] == "policy_denied"
    b, _ = boundary(Policy(max_in_flight=1))
    ticket = b.execution.admit("busy", "busy", write=False, local=False)
    result = envelope_of(run(b.call_tool("read_messages", {})))
    assert result["meta"]["code"] == "resource_limit"
    b.execution.finish(ticket, None)


MODE_ERRORS = [
    ({"message_type": "NCMD", "metrics": []}, "edge_node_id is required"),
    (
        {"message_type": "NCMD", "edge_node_id": "n", "metrics": []},
        "metrics must be a list of at least 1 item(s)",
    ),
    (
        {"message_type": "NDATA", "metrics": [{"name": "m", "value": 1}]},
        "metrics[0].type is required",
    ),
    (
        {"message_type": "NDATA", "metrics": [{"name": "m", "value": 1, "type": "int32", "u": 1}]},
        "unknown argument(s) in metrics[0]: u",
    ),
    (
        {"message_type": "NDEATH", "device_id": "d"},
        "unknown argument(s): device_id; accepted: message_type",
    ),
    ({"message_type": "BIRTH"}, "message_type must be NBIRTH, NDEATH, DBIRTH,"),
    ({"metrics": []}, "message_type is required and must be NBIRTH,"),
    ({"message_type": "DDEATH", "device_id": ""}, "device_id must be a non-empty string"),
]


@pytest.mark.parametrize(("arguments", "message"), MODE_ERRORS)
def test_mode_errors_name_the_argument_and_the_fix(arguments, message):
    b, calls = boundary()
    result = envelope_of(run(b.call_tool("publish_sparkplug", arguments)))
    assert result["success"] is False
    assert message in result["error"], result["error"]
    assert result["meta"] == {
        "tool": "publish_sparkplug",
        "code": "invalid_arguments",
        "outcome": "not_sent",
        "request_sent": False,
    }
    assert calls == []


CANONICAL_ERRORS = [
    ("read_messages", {"limit": 0}, "limit must be at least 1"),
    ("read_messages", {"limit": 1001}, "limit must be at most 1000"),
    ("read_messages", {"since_id": -1}, "since_id must be at least 0"),
    ("read_messages", {"limit": "5"}, "limit must be an integer"),
    ("subscribe_topic", {"topic": "a", "qos": 3}, "qos must be 0, 1 or 2"),
    ("subscribe_topic", {}, "topic is required"),
    (
        "publish_message",
        {"topic": "a", "payload": "x", "retain": 1},
        "retain must be true or false",
    ),
    ("get_broker_info", {"verbose": True}, "unknown argument(s): verbose; accepted: none"),
]


@pytest.mark.parametrize(("name", "arguments", "message"), CANONICAL_ERRORS)
def test_advertised_tools_are_held_to_their_contract_schema(name, arguments, message):
    b, calls = boundary()
    result = envelope_of(run(b.call_tool(name, arguments)))
    assert result["success"] is False and result["meta"]["code"] == "invalid_arguments"
    assert message in result["error"], result["error"]
    assert calls == []


def test_schema_errors_never_echo_argument_values():
    b, _ = boundary()
    secret = "hunter2-token"
    for arguments in (
        {"topic": "a", "payload": "x", "qos": secret},
        {"topic": "a", "payload": "x", secret: 1},
    ):
        result = envelope_of(run(b.call_tool("publish_message", arguments)))
        assert result["success"] is False
        if secret in arguments:  # an unknown argument's name is not a value
            continue
        assert secret not in result["error"]


def test_metric_types_stay_case_insensitive_at_the_boundary():
    """The README documents case-insensitive metric types; the schema must not narrow that."""
    b, calls = boundary()
    arguments = {
        "message_type": "DCMD",
        "edge_node_id": "n",
        "device_id": "d",
        "metrics": [{"name": "m", "value": 1, "type": "Int32"}],
    }
    assert envelope_of(run(b.call_tool("publish_sparkplug", arguments)))["success"] is True
    assert calls[-1][0] == "publish_device_command"
