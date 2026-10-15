import asyncio
import time
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

import anyio
import anyio.lowlevel

from ..domain.model import logger


@dataclass
class SendState:
    """What the current tool call has handed to the MQTT client (one per call).

    Shared through a context variable, so that a call that ends without a result
    (deadline, client cancel, client disconnect) can still say whether anything
    may have been sent.
    """

    sent: bool = False
    outcome: str | None = None
    deadline: float | None = None
    request_scope: Any = None

    def cancel_requested(self) -> bool:
        """The MCP client cancelled this call (the SDK has already replied)."""
        return self.request_scope is not None and bool(self.request_scope.cancel_called)

    def deadline_passed(self) -> bool:
        return self.deadline is not None and time.monotonic() >= self.deadline


async def _end_as_cancelled(message: str) -> None:
    """End a call the MCP client has cancelled, as a real cancellation.

    The SDK has already answered "Request cancelled"; a second answer (an
    envelope) would trip its "already completed" assertion. The outcome goes to
    stderr instead. The cancellation the SDK scheduled for this task arrives at
    the next suspension point and is then suppressed by the SDK, as for any
    cancelled request.
    """
    logger.warning("%s", message)
    give_up = time.monotonic() + 5.0
    while time.monotonic() < give_up:
        await asyncio.sleep(0.01)
    raise anyio.get_cancelled_exc_class()()


_send_state: ContextVar[SendState | None] = ContextVar("mqtt_mcp_send_state", default=None)
