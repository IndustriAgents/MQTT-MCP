import asyncio
import time
from collections.abc import Callable
from typing import Any

import anyio
import anyio.lowlevel
from mcp.server.fastmcp import FastMCP
from pydantic import ValidationError

from ..config import Settings
from ..domain.model import UNKNOWN_HINT, WRITE_TOOLS, _fail, logger
from ..infrastructure.control_context import SendState, _end_as_cancelled, _send_state
from ..state import get_settings


def call_deadline(settings: Settings) -> float:
    """Seconds one tool call may take, from its start: MQTT_TOOL_TIMEOUT.

    It is one absolute deadline per call, shared by every step (waiting for the
    connection or a lock, a session restart, the broker's acknowledgement), so
    no call can hang without an outcome. Once it has passed, nothing more is
    handed to the MQTT client for the call, and what is still queued is withdrawn.
    """
    return settings.tool_timeout


_REPORT_GRACE = 0.5
_UNDELIVERABLE = (
    anyio.ClosedResourceError,
    anyio.BrokenResourceError,
    BrokenPipeError,
    ConnectionResetError,
)


def _only_undeliverable(exc: BaseException) -> bool:
    if isinstance(exc, _UNDELIVERABLE):
        return True
    inner = getattr(exc, "exceptions", None)
    return bool(inner) and all(_only_undeliverable(e) for e in inner)


class EnvelopeFastMCP(FastMCP):
    """FastMCP with the suite's envelope for every outcome of a tool call.

    - FastMCP validates arguments against the tool's signature before the tool
      runs and turns a failure into a protocol-level tool error. Here such errors
      come back as ``{success: false, error: "Invalid arguments: ..."}``.
    - Every call has one absolute deadline (``call_deadline``). A write that
      runs out of time reports what it knows: "not_sent" if nothing was handed to
      the MQTT client (or it was withdrawn in time), else "unknown". A read gets
      a clean error. Nothing is handed to the MQTT client after the deadline.
    - When the MCP client disconnects (stdin closes), every running call is
      cancelled at once and nothing more is published; see run_stdio_async.
    """

    on_client_disconnect: Callable[[], None] | None = None
    client_gone: bool = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._active_calls: set[anyio.CancelScope] = set()

    async def run_stdio_async(self) -> None:
        """Serve stdio; when the client disconnects, stop every pending call first.

        FastMCP stops reading at end of file but lets running tool calls finish,
        so a publish still waiting for the broker would go out after the client
        is gone. Messages are relayed through a stream whose end marks the server
        as closing and cancels the running calls; a reply that can no longer be
        delivered then is a normal end (exit status 0), not a crash.
        """
        from mcp.server.stdio import stdio_server

        self.client_gone = False
        try:
            async with stdio_server() as (read_stream, write_stream):
                relay_send, relay_receive = anyio.create_memory_object_stream(0)
                async with anyio.create_task_group() as tg:
                    tg.start_soon(self._relay_until_eof, read_stream, relay_send)
                    await self._mcp_server.run(
                        relay_receive,
                        write_stream,
                        self._mcp_server.create_initialization_options(),
                    )
                    tg.cancel_scope.cancel()
        except BaseException as exc:
            if not (self.client_gone and _only_undeliverable(exc)):
                raise
            logger.info("Dropped replies for the disconnected client")

    async def _relay_until_eof(self, source: Any, sink: Any) -> None:
        try:
            async with sink:
                async for message in source:
                    await sink.send(message)
        finally:
            self.client_disconnected()

    def client_disconnected(self) -> None:
        """The MCP client is gone: send nothing more and cancel running calls."""
        if self.client_gone:
            return
        self.client_gone = True
        logger.info("MCP client disconnected; cancelling running tool calls")
        if self.on_client_disconnect is not None:
            self.on_client_disconnect()
        for scope in list(self._active_calls):
            scope.cancel()

    async def call_tool(self, name: str, arguments: dict[str, Any]):
        deadline = call_deadline(getattr(self, "settings", None) or get_settings())
        state = SendState(
            deadline=time.monotonic() + deadline, request_scope=self._request_cancel_scope()
        )
        token = _send_state.set(state)
        try:
            with anyio.CancelScope() as scope:
                self._active_calls.add(scope)
                try:
                    if self.client_gone:
                        scope.cancel()
                        await anyio.lowlevel.checkpoint()
                    result = await asyncio.wait_for(
                        self._call_enveloped(name, arguments), deadline + _REPORT_GRACE
                    )
                    if state.cancel_requested():
                        envelope = result[1] if isinstance(result, tuple) else result
                        outcome = (
                            envelope.get("meta", {}).get("outcome", envelope.get("success"))
                            if isinstance(envelope, dict)
                            else "a result"
                        )
                        await _end_as_cancelled(
                            f"{name} was cancelled by the MCP client but finished: {outcome}"
                        )
                    return result
                except asyncio.TimeoutError:
                    return self._ended_early(
                        name,
                        state,
                        f"did not finish within its deadline of {deadline:g}s",
                        "deadline_exceeded",
                    )
                finally:
                    self._active_calls.discard(scope)
            return self._ended_early(
                name, state, "was cancelled: the MCP client disconnected", "cancelled"
            )
        finally:
            _send_state.reset(token)

    def _request_cancel_scope(self) -> Any:
        """The cancel scope of the MCP request being served (mcp<2 internals, hence
        the pin; a test fails if they move), or None outside a request."""
        try:
            context = self._mcp_server.request_context
            responder = context.session._in_flight.get(context.request_id)
        except (LookupError, AttributeError):
            return None
        return getattr(responder, "_cancel_scope", None)

    def _ended_early(self, name: str, state: SendState, why: str, code: str) -> dict[str, Any]:
        if name not in WRITE_TOOLS:
            return _fail(f"{name} {why}; nothing was changed", code=code, tool=name)
        if not state.sent or state.outcome == "not_sent":
            return _fail(
                f"{name} {why}. Nothing was sent, and it will not be sent later.",
                code=code,
                tool=name,
                outcome="not_sent",
                request_sent=False,
            )
        return _fail(
            f"{name} {why}. {UNKNOWN_HINT}",
            code="outcome_unknown",
            tool=name,
            outcome="unknown",
            request_sent=True,
        )

    async def _call_enveloped(self, name: str, arguments: dict[str, Any]):
        tool = self._tool_manager.get_tool(name)
        if tool is not None:
            metadata = tool.fn_metadata
            try:
                metadata.arg_model.model_validate(metadata.pre_parse_json(arguments or {}))
            except ValidationError as exc:
                problems = "; ".join(
                    f"{'.'.join(str(p) for p in err['loc']) or 'arguments'}: {err['msg']}"
                    for err in exc.errors()
                )
                meta = {"outcome": "not_sent", "request_sent": False} if name in WRITE_TOOLS else {}
                return _fail(
                    f"Invalid arguments: {problems}", code="invalid_arguments", tool=name, **meta
                )
        # Errors raised by a running handler, including ValidationError, do not
        # prove that a publication was never sent. The contract preserves that.
        return await super().call_tool(name, arguments)
