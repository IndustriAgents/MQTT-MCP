"""Contract-driven MCP boundary. This module is owned by this protocol package."""

from __future__ import annotations

import json
import logging
from importlib.resources import files
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import best_match
from mcp.types import CallToolResult, TextContent, Tool

from ..application.execution import Execution
from .request_limits import RequestError, validate_request

log = logging.getLogger(__name__)

_KINDS = {
    "array": "a list",
    "boolean": "true or false",
    "integer": "an integer",
    "null": "null",
    "number": "a number",
    "object": "an object",
    "string": "a string",
}


def _choices(values) -> str:
    """'a', 'a or b', 'a, b or c'."""
    words = [v if isinstance(v, str) else json.dumps(v) for v in values]
    return words[0] if len(words) == 1 else ", ".join(words[:-1]) + " or " + words[-1]


def _where(error) -> str:
    path = ""
    for part in error.absolute_path:
        path += f"[{part}]" if isinstance(part, int) else (f".{part}" if path else str(part))
    return path


def describe(error) -> str:
    """Say what a schema violation means, naming the argument but never its value
    (values may contain credentials or payloads)."""
    where, kind, expected = _where(error), error.validator, error.validator_value
    if kind == "required":
        missing = [f"{where}.{k}" if where else k for k in expected if k not in error.instance]
        return f"{', '.join(missing)} {'is' if len(missing) == 1 else 'are'} required"
    if kind == "additionalProperties":
        allowed = sorted(error.schema.get("properties", {}))
        unknown = sorted(set(error.instance) - set(allowed))
        scope = f" in {where}" if where else ""
        return (
            f"unknown argument(s){scope}: {', '.join(unknown)}; "
            f"accepted: {', '.join(allowed) or 'none'}"
        )
    where = where or "arguments"
    if kind == "type":
        kinds = expected if isinstance(expected, list) else [expected]
        return f"{where} must be {_choices([_KINDS.get(k, k) for k in kinds])}"
    if kind in ("enum", "const"):
        return f"{where} must be {_choices(expected if kind == 'enum' else [expected])}"
    bounds = {
        "minimum": "at least {}",
        "maximum": "at most {}",
        "minItems": "a list of at least {} item(s)",
        "maxItems": "a list of at most {} item(s)",
        "minLength": "a string of at least {} character(s)",
    }
    if kind == "minLength" and expected == 1:
        return f"{where} must be a non-empty string"
    if kind in bounds:
        return f"{where} must be {bounds[kind].format(expected)}"
    return f"{where} does not match the documented shape ({kind})"


def envelope_of(result: Any) -> dict[str, Any] | None:
    if isinstance(result, CallToolResult):
        if isinstance(result.structuredContent, dict):
            return result.structuredContent
        result = result.content
    if isinstance(result, tuple):
        content, structured = result
        if isinstance(structured, dict):
            return structured
        result = content
    if isinstance(result, dict):
        return result
    if isinstance(result, (list, tuple)) and result:
        try:
            return json.loads(result[0].text)
        except (AttributeError, TypeError, ValueError):
            return None
    return None


def as_result(value: dict[str, Any]) -> tuple:
    return ([TextContent(type="text", text=json.dumps(value))], value)


class ContractBoundary:
    def __init__(self, server: Any, policy=None):
        package = __package__.split(".")[0]
        self.contract = json.loads(files(package).joinpath("_contract/tools.json").read_text())
        self.original_call = server.call_tool
        self.tools = {t["name"]: t for t in self.contract["tools"]}
        self.validators = {n: Draft202012Validator(t["inputSchema"]) for n, t in self.tools.items()}
        self.mode_validators = {
            (name, variant["properties"][route["selector"]]["const"]): Draft202012Validator(variant)
            for name, route in self.contract["routes"].items()
            if "selector" in route
            for variant in self.tools[name]["inputSchema"]["oneOf"]
        }
        self.legacy_validators = {
            n: Draft202012Validator(s) for n, s in self.contract["legacySchemas"].items()
        }
        self.execution = Execution.from_environment(self.contract["protocol"], policy)
        self.server = server

    async def list_tools(self):
        return [Tool(**t) for t in self.tools.values()]

    def refusal(self, name, reason, code="invalid_arguments"):
        canonical = self.contract["legacy"].get(name, name)
        write = not self.tools.get(canonical, {}).get("annotations", {}).get("readOnlyHint", False)
        meta = {"tool": name, "code": code}
        if write:
            meta.update(outcome="not_sent", request_sent=False)
        return as_result({"success": False, "data": None, "error": reason, "meta": meta})

    async def call_tool(self, name, arguments=None):
        arguments = {} if arguments is None else arguments
        try:
            validate_request(
                arguments, self.execution.policy.max_request_bytes, self.contract["limits"]
            )
        except RequestError as exc:
            return self.refusal(name, str(exc), exc.code)
        route = self.contract["routes"].get(name)
        canonical = self.contract["legacy"].get(name, name)
        handler, forwarded = name, arguments
        legacy_call = route is None
        validator = self.legacy_validators.get(name)
        problem = None
        if route:
            canonical = name
            validator = self.validators[name]
            if "selector" in route:
                selector = route["selector"]
                if isinstance(arguments, dict) and selector in arguments:
                    mode = arguments[selector]
                    handler = route["options"].get(mode) if isinstance(mode, str) else None
                    if handler is None:
                        problem = f"{selector} must be {_choices(route['options'])}"
                    else:
                        # The selected mode's own schema names the exact problem;
                        # the combined oneOf could only say that nothing matched.
                        validator = self.mode_validators[(name, mode)]
                    forwarded = {k: v for k, v in arguments.items() if k != selector}
                elif "legacyHandler" in route:
                    legacy_call = True
                    handler = route["legacyHandler"]
                    validator = self.legacy_validators[handler]
                else:
                    handler = None
                    problem = f"{selector} is required and must be {_choices(route['options'])}"
            else:
                handler = route["handler"]
        if validator is None:
            from mcp.server.fastmcp.exceptions import ToolError

            raise ToolError(f"Unknown tool: {name}")
        # Legacy aliases retain native coercion, but unknown keys must not be
        # silently discarded by the SDK before dispatch.
        if legacy_call and set(arguments) - set(validator.schema.get("properties", {})):
            return self.refusal(name, "Unknown argument(s) for legacy tool")
        if problem is None and not legacy_call:
            error = best_match(validator.iter_errors(arguments))
            problem = None if error is None else describe(error)
        if problem is not None:
            return self.refusal(name, f"Invalid arguments: {problem}")
        definition = self.tools[canonical]
        write = not definition.get("annotations", {}).get("readOnlyHint", False)
        local = canonical in self.contract["localStatusTools"]
        try:
            ticket = self.execution.admit(canonical, handler, write=write, local=local)
        except (ValueError, OSError) as exc:
            # An audit intent that cannot be persisted refuses the call (OSError).
            code = getattr(exc, "code", "audit_unavailable")
            return self.refusal(name, str(exc), code)
        result = None
        try:
            result = await self.original_call(handler, forwarded)
        except Exception as exc:
            # An arbitrary adapter/SDK failure cannot prove that native dispatch
            # did not happen. Never encourage a retry by guessing not_sent.
            log.error("Unexpected error while executing tool %s (%s)", name, type(exc).__name__)
            meta = {"tool": name, "code": "internal_error"}
            if write:
                meta.update(outcome="unknown", request_sent=None)
            result = as_result(
                {
                    "success": False,
                    "data": None,
                    "error": "Unexpected error while executing tool",
                    "meta": meta,
                }
            )
        finally:
            # Audit the observed result; never turn an audit failure after sending
            # into a not_sent result, and never repeat an uncertain operation.
            value = envelope_of(result)
            self.execution.finish(ticket, value)
        # Audit failures may annotate the result after the handler serialized
        # its text. Rebuild both views from the same observed envelope.
        return as_result(value) if value is not None else result

    async def wire_call(self, name, arguments=None):
        result = await self.call_tool(name, arguments)
        value = envelope_of(result)
        if value is None:
            return result
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(value))],
            structuredContent=value,
            isError=value.get("success") is False,
        )


def install_contract(server, policy=None):
    boundary = ContractBoundary(server, policy)
    server.contract_boundary = boundary
    server.list_tools = boundary.list_tools
    server.call_tool = boundary.call_tool
    # FastMCP binds callbacks at construction; update only here at the SDK edge.
    server._mcp_server.list_tools()(boundary.list_tools)
    server._mcp_server.call_tool(validate_input=False)(boundary.wire_call)
    return boundary
