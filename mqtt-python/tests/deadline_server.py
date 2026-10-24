"""Test-only launcher recording actual deadlines without replacing deadline checks."""

import json
import sys
from pathlib import Path

from mqtt_mcp.cli import main
from mqtt_mcp.infrastructure.control_context import _send_state
from mqtt_mcp.protocol.server import EnvelopeFastMCP

path = Path(sys.argv.pop(1))
original = EnvelopeFastMCP._call_enveloped


async def trace_deadline(self, name, arguments):
    if name == "publish_message":
        state = _send_state.get()
        with path.open("a") as output:
            output.write(
                json.dumps({"topic": arguments["topic"], "deadline": state.deadline}) + "\n"
            )
    return await original(self, name, arguments)


EnvelopeFastMCP._call_enveloped = trace_deadline
main()
