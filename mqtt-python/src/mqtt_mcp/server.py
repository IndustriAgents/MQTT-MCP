import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from mcp.server.fastmcp import Context, FastMCP

from . import __version__
from .adapters.mqtt_client import MQTTClientManager, build_client
from .adapters.mqtt_client import _Client as _Client
from .adapters.mqtt_client import _InFlight as _InFlight
from .application.diagnostics import decode_sparkplug_payload as decode_sparkplug_payload
from .application.diagnostics import get_broker_info as get_broker_info
from .application.diagnostics import list_sparkplug_nodes as list_sparkplug_nodes
from .application.diagnostics import list_subscriptions as list_subscriptions
from .application.execution import Execution
from .application.read import read_messages as read_messages
from .application.read import subscribe_topic as subscribe_topic
from .application.read import unsubscribe_topic as unsubscribe_topic
from .application.write import _edge_failure as _edge_failure
from .application.write import _publish_command as _publish_command
from .application.write import _publish_edge as _publish_edge
from .application.write import _write_failure as _write_failure
from .application.write import _writes_disabled as _writes_disabled
from .application.write import _written as _written
from .application.write import publish_device_birth as publish_device_birth
from .application.write import publish_device_command as publish_device_command
from .application.write import publish_device_data as publish_device_data
from .application.write import publish_device_death as publish_device_death
from .application.write import publish_message as publish_message
from .application.write import publish_node_birth as publish_node_birth
from .application.write import publish_node_command as publish_node_command
from .application.write import publish_node_data as publish_node_data
from .application.write import publish_node_death as publish_node_death
from .config import ConfigError, load_settings
from .domain.model import METRICS_HELP as METRICS_HELP
from .domain.model import OUTCOMES as OUTCOMES
from .domain.model import UNKNOWN_HINT as UNKNOWN_HINT
from .domain.model import WRITE_TOOLS as WRITE_TOOLS
from .domain.model import WRITES_DISABLED as WRITES_DISABLED
from .domain.model import PublishError as PublishError
from .domain.model import ToolInputError as ToolInputError
from .domain.model import _check_id as _check_id
from .domain.model import _check_metrics as _check_metrics
from .domain.model import _check_qos as _check_qos
from .domain.model import _check_topic_filter as _check_topic_filter
from .domain.model import _check_topic_name as _check_topic_name
from .domain.model import _check_unique_names as _check_unique_names
from .domain.model import _fail as _fail
from .domain.model import _ok as _ok
from .domain.model import logger as logger
from .infrastructure.control_context import SendState as SendState
from .infrastructure.control_context import _end_as_cancelled as _end_as_cancelled
from .infrastructure.control_context import _send_state as _send_state
from .protocol.hints import _LOCAL as _LOCAL
from .protocol.hints import _READ as _READ
from .protocol.hints import _WRITE as _WRITE
from .protocol.hints import _metrics_doc as _metrics_doc
from .protocol.server import _REPORT_GRACE as _REPORT_GRACE
from .protocol.server import _UNDELIVERABLE as _UNDELIVERABLE
from .protocol.server import EnvelopeFastMCP
from .protocol.server import _only_undeliverable as _only_undeliverable
from .protocol.server import call_deadline as call_deadline
from .protocol.tools import register_tools
from .state import _settings as _settings
from .state import get_settings
from .state import set_settings as set_settings


@asynccontextmanager
async def mqtt_lifespan(server: FastMCP) -> AsyncIterator[dict]:
    """Create the MQTT client on startup and disconnect it on shutdown."""
    manager = MQTTClientManager(server.settings or get_settings())
    if isinstance(server, EnvelopeFastMCP):
        server.on_client_disconnect = manager.close_for_disconnect
    await manager.start()
    try:
        yield {"mqtt_manager": manager}
    finally:
        await manager.stop()


def _manager(ctx: Context) -> MQTTClientManager:
    return ctx.request_context.lifespan_context["mqtt_manager"]


def main(env_file: str | None = None) -> None:
    """Load the configuration (after the .env file) and run the MCP server on stdio."""
    try:
        settings = get_settings(env_file)
        build_client(settings)
        # The module-level compatibility server predates --env-file. Resolve its
        # policy only after the same environment merge used by connection settings.
        mcp.contract_boundary.execution = Execution.from_environment("mqtt")
    except (ConfigError, OSError, ValueError) as exc:
        print(f"mqtt-mcp: configuration error: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    mcp.run(transport="stdio")


def context_for(ctx):
    return _manager(ctx)


def _make_server(settings=None):
    server = EnvelopeFastMCP(
        "MQTT-Control",
        instructions=(
            "MQTT and Sparkplug B access to one broker. "
            "Read-only by default: subscribe_topic, then "
            "read_messages to see traffic; "
            "list_sparkplug_nodes shows Sparkplug edge nodes "
            "seen on your subscriptions (subscribe to "
            "'spBv1.0/#' first). Publishing tools return "
            "success:false unless the server runs with "
            "MQTT_WRITES_ENABLED=true; check get_broker_info."
            " Every result is {success, data, error, meta}."
        ),
        lifespan=mqtt_lifespan,
    )
    server.settings = settings
    server._mcp_server.version = __version__
    register_tools(server, context_for)
    return server


def create_server(settings=None):
    """Build an independent instance with a configuration snapshot."""
    return _make_server(settings if settings is not None else load_settings())


mcp = _make_server()
