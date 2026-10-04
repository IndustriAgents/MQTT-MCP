"""MQTT + Sparkplug B MCP server (FastMCP, stdio).

Read-only by default: subscribing, reading received messages, listing the
Sparkplug B nodes seen on the broker and decoding payloads always work; every
tool that publishes is refused unless ``MQTT_WRITES_ENABLED=true``.

Every tool returns the suite's envelope ``{success, data, error, meta}``.
stdout carries MCP JSON-RPC only; logs go to stderr.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
import sys
import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import paho.mqtt.client as mqtt
from dotenv import find_dotenv, load_dotenv
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import ValidationError

from . import __version__, sparkplug
from .config import ConfigError, Settings, load_settings
from .inbox import MessageBuffer, SparkplugRegistry

logger = logging.getLogger("mqtt_mcp")

# ---------------------------------------------------------------------------
# Envelope and validation helpers
# ---------------------------------------------------------------------------


def _ok(data: Any = None, **meta: Any) -> dict[str, Any]:
    return {"success": True, "data": data, "error": None, "meta": meta}


def _fail(error: str, **meta: Any) -> dict[str, Any]:
    return {"success": False, "data": None, "error": error, "meta": meta}


class ToolInputError(ValueError):
    """An argument is invalid; reported as success:false."""


def _check_qos(qos: Any) -> int:
    if isinstance(qos, bool) or qos not in (0, 1, 2):
        raise ToolInputError(f"qos must be 0, 1 or 2, got {qos!r}")
    return int(qos)


def _check_topic_name(topic: Any) -> str:
    """A topic to publish to: no wildcards."""
    if not isinstance(topic, str) or not topic:
        raise ToolInputError("topic must be a non-empty string")
    if "+" in topic or "#" in topic:
        raise ToolInputError(f"cannot publish to {topic!r}: wildcards (+, #) are for subscriptions")
    if "\x00" in topic or len(topic.encode("utf-8")) > 65535:
        raise ToolInputError("topic must not contain NUL and must be at most 65535 bytes")
    return topic


def _check_topic_filter(topic: Any, what: str = "topic") -> str:
    """A subscription filter: '+' must fill a whole level, '#' only as the last level."""
    if not isinstance(topic, str) or not topic:
        raise ToolInputError(f"{what} must be a non-empty string")
    if "\x00" in topic or len(topic.encode("utf-8")) > 65535:
        raise ToolInputError(f"{what} must not contain NUL and must be at most 65535 bytes")
    levels = topic.split("/")
    for index, level in enumerate(levels):
        if "#" in level and (level != "#" or index != len(levels) - 1):
            raise ToolInputError(f"invalid {what} {topic!r}: '#' must be the whole last level")
        if "+" in level and level != "+":
            raise ToolInputError(f"invalid {what} {topic!r}: '+' must be a whole level")
    return topic


def _check_id(value: Any, what: str) -> str:
    try:
        return sparkplug.validate_id(value, what)
    except ValueError as exc:
        raise ToolInputError(str(exc)) from None


def _check_metrics(metrics: Any, *, required: bool) -> list[Any]:
    if metrics is None:
        metrics = []
    if not isinstance(metrics, list):
        raise ToolInputError("metrics must be a list of {name, value, type} objects")
    if required and not metrics:
        raise ToolInputError("at least one metric is required")
    return metrics


WRITES_DISABLED = (
    "Writes are disabled: this server is read-only unless MQTT_WRITES_ENABLED=true is set in its"
    " environment. Nothing was published."
)

METRICS_HELP = (
    'A list of {"name": str, "value": ..., "type": str} objects. type is one of'
    " int8, int16, int32 (or int), int64 (or long), uint8, uint16, uint32, uint64, float,"
    " double, boolean (or bool), string, text, uuid, datetime (milliseconds since the Unix"
    " epoch) or bytes (value as a hex string). The value must match the type and fit its"
    " range; negative values are fine for the signed int types; null sends a null metric."
    " An unknown type or a mismatching value is rejected with success:false, nothing is sent."
)


# ---------------------------------------------------------------------------
# MQTT client
# ---------------------------------------------------------------------------


class PublishError(RuntimeError):
    """A publish was not confirmed; the message says what is known."""


class _Client(mqtt.Client):
    """paho client that remembers why the last connection attempt failed."""

    last_connect_error: str | None = None

    def reconnect(self) -> mqtt.MQTTErrorCode:
        try:
            return super().reconnect()
        except OSError as exc:
            self.last_connect_error = f"{type(exc).__name__}: {exc}"
            raise


def build_client(settings: Settings) -> _Client:
    """Create a configured paho client (no network activity).

    Raises ``ssl.SSLError``/``OSError``/``ValueError`` if the TLS files cannot be
    loaded, which ``main()`` reports as a configuration error.
    """
    client = _Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=settings.client_id,
        protocol=mqtt.MQTTv311,
        clean_session=True,
    )
    client.reconnect_delay_set(min_delay=1, max_delay=10)
    if settings.username is not None:
        client.username_pw_set(settings.username, settings.password)
    if settings.tls:
        client.tls_set(
            ca_certs=settings.tls_ca_file,
            certfile=settings.tls_cert_file,
            keyfile=settings.tls_key_file,
            keyfile_password=settings.tls_key_password,
            cert_reqs=ssl.CERT_NONE if settings.tls_insecure else ssl.CERT_REQUIRED,
        )
    return client


class MQTTClientManager:
    """Owns the MQTT connection, subscriptions, received messages and the
    Sparkplug B edge-node state of this server."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client: _Client | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock = threading.RLock()  # state shared with paho's network thread
        self.connected = False
        self.last_error: str | None = None
        self.connections = 0
        self._expect_disconnect = False
        self.subscriptions: dict[str, int] = {}  # filter -> QoS granted by the broker
        self._pending: dict[int, asyncio.Future] = {}
        self._restoring: dict[int, str] = {}
        self.messages = MessageBuffer(settings.message_buffer_size)
        self.registry = SparkplugRegistry()

        # This server's own Sparkplug B edge node (group_id/edge_node_id from settings).
        self.sparkplug_lock = asyncio.Lock()
        self.will_active = False  # the broker holds our NDEATH will for this connection
        self._will_bd_seq: int | None = None  # bdSeq of the will configured on the client
        self.bd_seq: int | None = None
        self._next_bd_seq = 0
        self.node_born = False
        self.seq = 0
        self.node_lost: str | None = None
        self.device_births: dict[str, dict[str, Any]] = {}

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        """Start connecting in the background; retries until the broker answers."""
        self._loop = asyncio.get_running_loop()
        self.client = build_client(self.settings)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.on_subscribe = self._on_subscribe
        self.client.on_unsubscribe = self._on_unsubscribe
        if self.settings.tls and self.settings.tls_insecure:
            logger.warning(
                "MQTT_TLS_INSECURE=true: the broker certificate and hostname are NOT verified"
            )
        logger.info(
            "Connecting to %s as %s (writes %s)",
            self.settings.broker_url,
            self.settings.client_id,
            "enabled" if self.settings.writes_enabled else "disabled",
        )
        self.client.connect_async(
            self.settings.host, self.settings.port, keepalive=self.settings.keepalive
        )
        self.client.loop_start()
        await self.wait_connected(min(1.0, self.settings.tool_timeout))

    async def stop(self) -> None:
        """Publish NDEATH if this server's edge node is born, then disconnect."""
        if self.client is None:
            return
        if self.node_born and self.connected:
            try:
                await self.publish(*self._ndeath_message(), qos=1, timeout=2.0)
            except Exception as exc:  # best effort on shutdown
                logger.warning("Could not publish NDEATH on shutdown: %s", exc)
        with self._lock:
            self._expect_disconnect = True
        await asyncio.to_thread(self._disconnect_and_stop)
        self.connected = False

    def _disconnect_and_stop(self) -> None:
        assert self.client is not None
        self.client.disconnect()
        self.client.loop_stop()

    async def wait_connected(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while not self.connected and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        return self.connected

    async def require_connection(self) -> None:
        if not await self.wait_connected(self.settings.tool_timeout):
            reason = self.last_error or (self.client.last_connect_error if self.client else None)
            detail = f" (last error: {reason})" if reason else ""
            raise PublishError(
                f"Not connected to the MQTT broker at {self.settings.broker_url}{detail}."
                " The server keeps retrying in the background."
            )

    # -- paho callbacks (network thread) -----------------------------------

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code.is_failure:
            with self._lock:
                self.connected = False
                self.last_error = f"broker refused the connection: {reason_code}"
            logger.warning("Broker refused the connection: %s", reason_code)
            return
        with self._lock:
            self.connected = True
            self.last_error = None
            client.last_connect_error = None
            self.connections += 1
            # A will configured on the client went out with this CONNECT.
            self.will_active = self._will_bd_seq is not None
            if self.will_active:
                self.bd_seq = self._will_bd_seq
            restore = dict(self.subscriptions)
            for topic, qos in restore.items():
                result, mid = client.subscribe(topic, qos)
                if result == mqtt.MQTT_ERR_SUCCESS and mid is not None:
                    self._restoring[mid] = topic
        logger.info(
            "Connected to %s%s",
            self.settings.broker_url,
            f"; restoring {len(restore)} subscription(s)" if restore else "",
        )

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        with self._lock:
            was_connected = self.connected
            self.connected = False
            pending = list(self._pending.values())
            self._pending.clear()
            self._restoring.clear()
            if not self._expect_disconnect:
                self._connection_lost(client, reason_code, was_connected)
        for future in pending:
            self._resolve(future, None)

    def _connection_lost(self, client, reason_code, was_connected: bool) -> None:
        if was_connected:
            self.last_error = f"connection lost: {reason_code}"
            logger.warning("Connection to the broker lost (%s); reconnecting", reason_code)
        if self._will_bd_seq is not None:
            # The broker now publishes our NDEATH will. The node stays dead until a new
            # NBIRTH, which needs a fresh will (and bdSeq) on a new connection.
            client.will_clear()
            self._will_bd_seq = None
            self.will_active = False
            if self.node_born:
                self.node_lost = (
                    f"connection lost at {time.strftime('%Y-%m-%dT%H:%M:%S%z')}; the broker"
                    f" published the NDEATH will (bdSeq {self.bd_seq})"
                )
            self.node_born = False
            self.device_births.clear()

    def _on_message(self, client, userdata, msg) -> None:
        received_at = time.time()
        decoded, error = None, None
        try:
            decoded, error = self.registry.observe(msg.topic, msg.payload, received_at)
        except Exception as exc:  # never let a bad message kill the network thread
            error = f"{type(exc).__name__}: {exc}"
        self.messages.add(
            msg.topic,
            bytes(msg.payload),
            msg.qos,
            bool(msg.retain),
            received_at,
            sparkplug=decoded,
            sparkplug_error=error,
        )

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties) -> None:
        with self._lock:
            future = self._pending.pop(mid, None)
            topic = self._restoring.pop(mid, None)
            if topic is not None and reason_codes and reason_codes[0].is_failure:
                self.subscriptions.pop(topic, None)
                logger.warning("Broker refused to restore subscription %s", topic)
        if future is not None:
            self._resolve(future, list(reason_codes))

    def _on_unsubscribe(self, client, userdata, mid, reason_codes, properties) -> None:
        with self._lock:
            future = self._pending.pop(mid, None)
        if future is not None:
            self._resolve(future, list(reason_codes))

    def _resolve(self, future: asyncio.Future, value: Any) -> None:
        def setter() -> None:
            if not future.done():
                future.set_result(value)

        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(setter)

    # -- operations ----------------------------------------------------------

    async def _await_ack(self, call, what: str):
        """Run a paho subscribe/unsubscribe call and wait for the broker's ack."""
        assert self._loop is not None
        future = self._loop.create_future()
        with self._lock:
            result, mid = call()
            if result != mqtt.MQTT_ERR_SUCCESS or mid is None:
                raise PublishError(f"{what} was not sent: {mqtt.error_string(result)}")
            self._pending[mid] = future
        try:
            reason_codes = await asyncio.wait_for(future, self.settings.tool_timeout)
        except asyncio.TimeoutError:
            with self._lock:
                self._pending.pop(mid, None)
            raise PublishError(
                f"the broker did not acknowledge the {what} within {self.settings.tool_timeout}s"
            ) from None
        if reason_codes is None:
            raise PublishError(f"the connection was lost before the broker acknowledged the {what}")
        return reason_codes

    async def subscribe(self, topic: str, qos: int) -> int:
        await self.require_connection()
        assert self.client is not None
        codes = await self._await_ack(lambda: self.client.subscribe(topic, qos), "subscription")
        if not codes or codes[0].is_failure:
            raise PublishError(f"the broker refused the subscription to {topic!r}")
        granted = int(codes[0].value)
        with self._lock:
            self.subscriptions[topic] = granted
        return granted

    async def unsubscribe(self, topic: str) -> None:
        await self.require_connection()
        assert self.client is not None
        await self._await_ack(lambda: self.client.unsubscribe(topic), "unsubscribe")
        with self._lock:
            self.subscriptions.pop(topic, None)

    async def publish(
        self,
        topic: str,
        payload: bytes,
        qos: int = 0,
        retain: bool = False,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Publish and confirm: QoS 0 once written to the socket, QoS 1/2 on the broker's ack.

        Raises PublishError when that confirmation does not come; an unconfirmed
        QoS 1/2 message is withdrawn from paho's retry queue so that it is not sent
        later behind the caller's back.
        """
        await self.require_connection()
        assert self.client is not None
        timeout = self.settings.tool_timeout if timeout is None else timeout
        info = self.client.publish(topic, payload, qos=qos, retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            self._withdraw(info.mid)
            raise PublishError(f"not sent: {mqtt.error_string(info.rc)}")
        published = await asyncio.to_thread(_wait_for_publish, info, timeout)
        if published is not True:
            withdrawn = self._withdraw(info.mid)
            if qos == 0:
                raise PublishError(f"not written to the network within {timeout}s ({published})")
            raise PublishError(
                f"the broker did not acknowledge the QoS {qos} message within {timeout}s"
                f" ({published}); it may or may not have reached the broker"
                + (", and it will not be retried" if withdrawn else "")
            )
        return {
            "mid": info.mid,
            "delivery": (
                "written to the network (QoS 0: the broker does not acknowledge)"
                if qos == 0
                else f"acknowledged by the broker ({'PUBACK' if qos == 1 else 'PUBCOMP'})"
            ),
        }

    def _withdraw(self, mid: int) -> bool:
        """Remove an unconfirmed message from paho's outgoing queue (paho 2.x internals)."""
        client = self.client
        try:
            with client._out_message_mutex:
                message = client._out_messages.pop(mid, None)
                if message is None:
                    return False
                in_flight = message.state in (
                    mqtt.mqtt_ms_wait_for_puback,
                    mqtt.mqtt_ms_wait_for_pubrec,
                    mqtt.mqtt_ms_wait_for_pubrel,
                    mqtt.mqtt_ms_wait_for_pubcomp,
                    mqtt.mqtt_ms_resend_pubrel,
                    mqtt.mqtt_ms_send_pubrec,
                )
                if in_flight and client._inflight_messages > 0:
                    client._inflight_messages -= 1
                return True
        except AttributeError:  # paho internals changed; nothing withdrawn
            return False

    # -- this server as a Sparkplug B edge node --------------------------------

    def _ndeath_message(self) -> tuple[str, bytes]:
        payload = sparkplug.build_payload(
            [], seq=None, extra_metrics=[sparkplug.bd_seq_metric(self.bd_seq or 0)]
        )
        topic = sparkplug.topic(self.settings.group_id, "NDEATH", self.settings.edge_node_id)
        return topic, payload.SerializeToString()

    async def _restart_with_will(self, bd_seq: int) -> None:
        """Reconnect so that the broker holds an NDEATH will carrying ``bd_seq``."""
        assert self.client is not None
        payload = sparkplug.build_payload(
            [], seq=None, extra_metrics=[sparkplug.bd_seq_metric(bd_seq)]
        )
        topic = sparkplug.topic(self.settings.group_id, "NDEATH", self.settings.edge_node_id)
        with self._lock:
            self._expect_disconnect = True
        await asyncio.to_thread(self._disconnect_and_stop)
        with self._lock:
            self.client.will_set(topic, payload.SerializeToString(), qos=1, retain=False)
            self._will_bd_seq = bd_seq
            self._next_bd_seq = (bd_seq + 1) % 256  # every CONNECT with a will gets a new one
            self._expect_disconnect = False
        self.client.connect_async(
            self.settings.host, self.settings.port, keepalive=self.settings.keepalive
        )
        self.client.loop_start()
        if not await self.wait_connected(self.settings.tool_timeout):
            # The will stays configured: once the client connects, the broker holds it
            # (on_connect marks it active) and the next NBIRTH can use it.
            raise PublishError(
                "could not reconnect with the NDEATH will registered"
                f" ({self.last_error or self.client.last_connect_error or 'timeout'})"
            )

    def sparkplug_state(self) -> dict[str, Any]:
        return {
            "group_id": self.settings.group_id,
            "edge_node_id": self.settings.edge_node_id,
            "node_born": self.node_born,
            "bd_seq": self.bd_seq,
            "last_seq": self.seq if self.node_born else None,
            "ndeath_will_registered": self.will_active,
            "devices_born": sorted(self.device_births),
            "node_lost": self.node_lost,
        }


def _wait_for_publish(info: mqtt.MQTTMessageInfo, timeout: float) -> bool | str:
    try:
        info.wait_for_publish(timeout)
    except (RuntimeError, ValueError) as exc:
        return str(exc)
    return True if info.is_published() else "timed out"


# ---------------------------------------------------------------------------
# Settings, lifespan and the FastMCP server
# ---------------------------------------------------------------------------

_settings: Settings | None = None


def get_settings() -> Settings:
    """Load settings once: a ``.env`` file in (or above) the working directory is
    merged into the environment first; variables already set take precedence."""
    global _settings
    if _settings is None:
        load_dotenv(find_dotenv(usecwd=True), override=False)
        _settings = load_settings()
    return _settings


def set_settings(settings: Settings | None) -> None:
    """Use these settings instead of the environment (tests, embedding)."""
    global _settings
    _settings = settings


@asynccontextmanager
async def mqtt_lifespan(server: FastMCP) -> AsyncIterator[dict]:
    """Create the MQTT client on startup and disconnect it on shutdown."""
    manager = MQTTClientManager(get_settings())
    await manager.start()
    try:
        yield {"mqtt_manager": manager}
    finally:
        await manager.stop()


class EnvelopeFastMCP(FastMCP):
    """FastMCP that reports argument validation errors in the result envelope.

    FastMCP validates arguments against the tool's signature before the tool
    runs and turns a failure into a protocol-level tool error. Here such errors
    come back as ``{success: false, error: "Invalid arguments: ..."}``, like
    every other failure.
    """

    async def call_tool(self, name: str, arguments: dict[str, Any]):
        try:
            return await super().call_tool(name, arguments)
        except ToolError as exc:
            cause = exc.__cause__
            if isinstance(cause, ValidationError):
                problems = "; ".join(
                    f"{'.'.join(str(p) for p in err['loc']) or 'arguments'}: {err['msg']}"
                    for err in cause.errors()
                )
                return _fail(f"Invalid arguments: {problems}", tool=name)
            if cause is not None:
                logger.exception("Tool %s failed", name, exc_info=cause)
                return _fail(
                    f"Internal error in {name}: {type(cause).__name__}: {cause}", tool=name
                )
            raise


mcp = EnvelopeFastMCP(
    "MQTT-Control",
    instructions=(
        "MQTT and Sparkplug B access to one broker. Read-only by default: subscribe_topic, then"
        " read_messages to see traffic; list_sparkplug_nodes shows Sparkplug edge nodes seen on"
        " your subscriptions (subscribe to 'spBv1.0/#' first). Publishing tools return"
        " success:false unless the server runs with MQTT_WRITES_ENABLED=true; check"
        " get_broker_info. Every result is {success, data, error, meta}."
    ),
    lifespan=mqtt_lifespan,
)
# FastMCP 1.x reports the mcp SDK's version as serverInfo.version unless the
# low-level server is given one.
mcp._mcp_server.version = __version__

_READ = ToolAnnotations(readOnlyHint=True, openWorldHint=True)
_LOCAL = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
_WRITE = ToolAnnotations(
    readOnlyHint=False, destructiveHint=True, idempotentHint=False, openWorldHint=True
)


def _manager(ctx: Context) -> MQTTClientManager:
    return ctx.request_context.lifespan_context["mqtt_manager"]


def _metrics_doc(fn):
    """Put METRICS_HELP into the docstring where it says {METRICS_HELP}."""
    if fn.__doc__:  # None under python -OO
        fn.__doc__ = fn.__doc__.replace("{METRICS_HELP}", METRICS_HELP)
    return fn


# ---------------------------------------------------------------------------
# Read-only tools
# ---------------------------------------------------------------------------


@mcp.tool(annotations=_LOCAL)
def get_broker_info(ctx: Context) -> dict[str, Any]:
    """Report the broker connection and this server's configuration and state.

    data: broker URL (credentials never shown), whether TLS is on, the client ID,
    whether the server is connected right now and the last connection error,
    whether writes are enabled (MQTT_WRITES_ENABLED), the subscriptions held, the
    number of buffered received messages, and the state of this server's own
    Sparkplug B edge node (born or not, bdSeq, devices born). Does not contact
    the broker.
    """
    m = _manager(ctx)
    s = m.settings
    with m._lock:
        subscriptions = [{"topic": t, "qos": q} for t, q in sorted(m.subscriptions.items())]
        last_error = m.last_error or (m.client.last_connect_error if m.client else None)
    return _ok(
        {
            "broker": s.broker_url,
            "tls": s.tls,
            "tls_verification": (not s.tls_insecure) if s.tls else None,
            "client_id": s.client_id,
            "username": s.username,
            "connected": m.connected,
            "last_error": last_error,
            "writes_enabled": s.writes_enabled,
            "subscriptions": subscriptions,
            "buffered_messages": len(m.messages),
            "sparkplug": m.sparkplug_state(),
        }
    )


@mcp.tool(annotations=_READ)
async def subscribe_topic(ctx: Context, topic: str, qos: int = 0) -> dict[str, Any]:
    """Subscribe this server to an MQTT topic filter so that matching messages are kept.

    Received messages go into a buffer that read_messages returns (the newest
    MQTT_MESSAGE_BUFFER_SIZE, default 1000); Sparkplug B messages also update
    list_sparkplug_nodes. A broker sends retained messages for the filter right
    away. Subscriptions are restored automatically after a reconnect.

    Parameters:
        topic: topic filter; '+' matches one level, '#' the rest (e.g. 'sensors/#',
            'spBv1.0/#'). Shared subscriptions ($share/..., $queue/...) are refused
            unless writes are enabled, because they take messages away from other
            subscribers in the same share group.
        qos: maximum QoS for delivery to this server: 0, 1 or 2 (default 0).

    data: the filter and the QoS the broker granted (success only after SUBACK).
    """
    m = _manager(ctx)
    try:
        topic = _check_topic_filter(topic)
        qos = _check_qos(qos)
        if topic.startswith(("$share/", "$queue/")) and not m.settings.writes_enabled:
            raise ToolInputError(
                "shared subscriptions ($share/, $queue/) divert messages from other consumers;"
                " they are only allowed with MQTT_WRITES_ENABLED=true"
            )
        granted = await m.subscribe(topic, qos)
    except (ToolInputError, PublishError) as exc:
        return _fail(f"Could not subscribe to {topic!r}: {exc}", topic=topic)
    return _ok({"topic": topic, "granted_qos": granted}, requested_qos=qos)


@mcp.tool(annotations=_READ)
async def unsubscribe_topic(ctx: Context, topic: str) -> dict[str, Any]:
    """Remove one of this server's subscriptions (exactly as listed by list_subscriptions).

    Already buffered messages stay readable. Fails if this server holds no
    subscription with that exact filter.

    Parameters:
        topic: the subscribed topic filter, e.g. 'sensors/#'.
    """
    m = _manager(ctx)
    try:
        topic = _check_topic_filter(topic)
        if topic not in m.subscriptions:
            raise ToolInputError(
                f"this server holds no subscription to {topic!r}"
                f" (current: {sorted(m.subscriptions) or 'none'})"
            )
        await m.unsubscribe(topic)
    except (ToolInputError, PublishError) as exc:
        return _fail(f"Could not unsubscribe from {topic!r}: {exc}", topic=topic)
    return _ok({"topic": topic, "unsubscribed": True})


@mcp.tool(annotations=_LOCAL)
def list_subscriptions(ctx: Context) -> dict[str, Any]:
    """List the subscriptions this server holds, with the QoS the broker granted.

    data: a list of {topic, qos}. Only this server's own subscriptions; the
    broker's other clients are not visible. Does not contact the broker.
    """
    m = _manager(ctx)
    with m._lock:
        subs = [{"topic": t, "qos": q} for t, q in sorted(m.subscriptions.items())]
    return _ok(subs, count=len(subs), connected=m.connected)


@mcp.tool(annotations=_LOCAL)
def read_messages(
    ctx: Context,
    topic_filter: str | None = None,
    limit: int = 50,
    since_id: int | None = None,
) -> dict[str, Any]:
    """Return messages this server has received on its subscriptions.

    Only messages that arrived after subscribe_topic are available, and only the
    newest MQTT_MESSAGE_BUFFER_SIZE (default 1000) are kept. Each message has an
    increasing id, topic, qos, retain flag, received_at (UTC), payload (UTF-8 text,
    or hex for binary and Sparkplug payloads) and, for Sparkplug B topics, the
    decoded payload under 'sparkplug'.

    Parameters:
        topic_filter: only messages whose topic matches this filter (+/# allowed).
        limit: at most this many messages, 1-1000 (default 50).
        since_id: only messages with an id greater than this, oldest first; pass
            meta.last_id from the previous call to poll for new messages. Without
            since_id the newest `limit` messages are returned (oldest first).

    meta: returned, has_more, buffered, dropped_total, last_id, and
    missed_messages=true if messages after since_id were already dropped.
    """
    m = _manager(ctx)
    try:
        if topic_filter is not None:
            topic_filter = _check_topic_filter(topic_filter, "topic_filter")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ToolInputError(f"limit must be an integer from 1 to 1000, got {limit!r}")
        if since_id is not None and (
            isinstance(since_id, bool) or not isinstance(since_id, int) or since_id < 0
        ):
            raise ToolInputError(f"since_id must be a non-negative integer, got {since_id!r}")
    except ToolInputError as exc:
        return _fail(str(exc))
    messages, meta = m.messages.query(topic_filter, limit, since_id)
    if not m.subscriptions and not messages:
        meta["note"] = "This server holds no subscriptions; call subscribe_topic first."
    return _ok(messages, **meta)


@mcp.tool(annotations=_LOCAL)
def list_sparkplug_nodes(ctx: Context) -> dict[str, Any]:
    """List Sparkplug B edge nodes and devices seen in messages this server received.

    Discovery is passive: a node appears only after one of its NBIRTH, NDATA,
    NDEATH, DBIRTH, DDATA or DDEATH messages arrives on a subscription this
    server holds (subscribe to 'spBv1.0/#', or 'spBv1.0/<group>/#', first). Births
    are not retained, so a node that was born before the subscription shows up
    with its next data message; metrics that its data messages send without a
    datatype then show their raw stored value until the node is reborn.

    data.nodes: per node group_id, edge_node_id, online (true/false, or null if
    unknown), last_message_type, last_seen, bd_seq, the latest metric values, and
    its devices with the same fields. data.this_server: the edge node this server
    itself publishes as (see publish_node_birth). Does not contact the broker.
    """
    m = _manager(ctx)
    nodes = m.registry.snapshot()
    meta: dict[str, Any] = {"count": len(nodes)}
    if not any(t.startswith(("spBv1.0/", "#", "+/")) for t in m.subscriptions):
        meta["note"] = (
            "No subscription covers Sparkplug topics; subscribe to 'spBv1.0/#' to discover nodes."
        )
    return _ok({"nodes": nodes, "this_server": m.sparkplug_state()}, **meta)


@mcp.tool(annotations=_LOCAL)
def decode_sparkplug_payload(payload_hex: str) -> dict[str, Any]:
    """Decode a hex-encoded Sparkplug B protobuf payload (as read_messages shows it).

    Parameters:
        payload_hex: the payload bytes as hex (spaces and a 0x prefix are ignored).

    data: timestamp, seq (null if absent), and metrics with name, alias,
    timestamp, datatype and value. Signed integer types are shown as signed
    numbers when the metric carries its datatype; a metric without datatype
    (allowed in DATA and CMD messages) shows its raw stored value. Fails with
    success:false if the hex is invalid or the bytes are not a Sparkplug B payload.
    """
    if not isinstance(payload_hex, str):
        return _fail("payload_hex must be a string of hex digits")
    cleaned = "".join(payload_hex.split())
    if cleaned[:2].lower() == "0x":
        cleaned = cleaned[2:]
    try:
        data = bytes.fromhex(cleaned)
    except ValueError:
        return _fail("payload_hex is not valid hex (expected pairs of 0-9a-f digits)")
    try:
        decoded = sparkplug.decode_payload(data)
    except Exception as exc:  # DecodeError and friends
        return _fail(f"Not a valid Sparkplug B payload: {exc}", size=len(data))
    return _ok(decoded, size=len(data))


# ---------------------------------------------------------------------------
# Write tools (MQTT_WRITES_ENABLED)
# ---------------------------------------------------------------------------


def _writes_disabled(m: MQTTClientManager) -> dict[str, Any] | None:
    if not m.settings.writes_enabled:
        return _fail(WRITES_DISABLED, writes_enabled=False)
    return None


@mcp.tool(annotations=_WRITE)
async def publish_message(
    ctx: Context, topic: str, payload: str, qos: int = 0, retain: bool = False
) -> dict[str, Any]:
    """WRITE: publish a UTF-8 text message to an MQTT topic.

    Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
    nothing.

    Any subscriber gets the message, and on a plant network that can include
    devices that act on it, so this is gated like the Sparkplug commands. With
    retain=true the broker keeps the message and hands it to every future
    subscriber.

    Parameters:
        topic: topic name without wildcards, e.g. 'sensors/temp'.
        payload: message text (sent as UTF-8).
        qos: 0, 1 or 2 (default 0).
        retain: keep the message on the broker as the topic's retained message.

    success:true means QoS 0: written to the network (the broker does not
    acknowledge QoS 0); QoS 1/2: the broker acknowledged it. Otherwise
    success:false with the reason.
    """
    m = _manager(ctx)
    if (refused := _writes_disabled(m)) is not None:
        return refused
    try:
        topic = _check_topic_name(topic)
        qos = _check_qos(qos)
        if not isinstance(retain, bool):
            raise ToolInputError("retain must be true or false")
        if not isinstance(payload, str):
            raise ToolInputError("payload must be a string")
        result = await m.publish(topic, payload.encode("utf-8"), qos=qos, retain=retain)
    except (ToolInputError, PublishError) as exc:
        return _fail(f"Did not publish to {topic!r}: {exc}", topic=topic)
    size = len(payload.encode("utf-8"))
    return _ok({"topic": topic, "qos": qos, "retain": retain, "bytes": size, **result})


async def _publish_edge(
    m: MQTTClientManager,
    message_type: str,
    metrics: list[Any],
    device_id: str | None = None,
    *,
    extra: list | None = None,
    seq_mode: str = "next",
) -> dict[str, Any]:
    """Publish an NBIRTH/NDATA/DBIRTH/DDATA/DDEATH of this server's edge node.

    Must be called with ``m.sparkplug_lock`` held. ``seq_mode`` is "zero" for
    NBIRTH and "next" otherwise; the sequence number only advances when the
    message was actually sent.
    """
    seq = 0 if seq_mode == "zero" else (m.seq + 1) % 256
    payload = sparkplug.build_payload(metrics, seq=seq, extra_metrics=extra or [])
    topic = sparkplug.topic(m.settings.group_id, message_type, m.settings.edge_node_id, device_id)
    result = await m.publish(topic, payload.SerializeToString(), qos=0)
    m.seq = seq
    return {
        "topic": topic,
        "seq": seq,
        "bd_seq": m.bd_seq,
        "metrics": len(payload.metrics),
        **result,
    }


def _edge_failure(message_type: str, exc: Exception, **meta: Any) -> dict[str, Any]:
    return _fail(f"{message_type} not published: {exc}", **meta)


@mcp.tool(annotations=_WRITE)
@_metrics_doc
async def publish_node_birth(
    ctx: Context, metrics: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    """WRITE: make this server a Sparkplug B edge node and publish its NBIRTH.

    Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
    nothing.

    The node is SPARKPLUG_GROUP_ID/SPARKPLUG_EDGE_NODE_ID (see get_broker_info);
    it must not be the ID of a real edge node. If no NDEATH will is registered
    on the current connection yet, the server first reconnects with an NDEATH
    Last Will carrying a new bdSeq, so the broker announces the node's death if
    the server disappears. The NBIRTH has seq 0 and automatically includes the
    'bdSeq' and 'Node Control/Rebirth' (false) metrics. Publishing NBIRTH again
    is a rebirth: devices must then be born again. The server does not act on
    commands (including rebirth requests) sent to its own node. After an
    unexpected disconnect the node is dead and needs a new NBIRTH.

    Parameters:
        metrics: the node's metrics (optional). {METRICS_HELP}

    data: topic, seq, bd_seq, metric count and delivery (QoS 0, as the
    specification requires for NBIRTH).
    """
    m = _manager(ctx)
    if (refused := _writes_disabled(m)) is not None:
        return refused
    try:
        metrics = _check_metrics(metrics, required=False)
        sparkplug.build_payload(metrics, seq=0)  # validate before touching the connection
        reserved = {sparkplug.BD_SEQ_METRIC, sparkplug.REBIRTH_METRIC}
        clash = sorted({x.get("name") for x in metrics} & reserved)
        if clash:
            raise ToolInputError(f"{clash} are added by the server; leave them out")
        async with m.sparkplug_lock:
            await m.require_connection()
            reconnected = False
            if not m.will_active:
                await m._restart_with_will(m._next_bd_seq)
                reconnected = True
            extra = [sparkplug.bd_seq_metric(m.bd_seq), sparkplug.rebirth_metric()]
            data = await _publish_edge(m, "NBIRTH", metrics, extra=extra, seq_mode="zero")
            m.node_born = True
            m.node_lost = None
            m.device_births.clear()
    except (ToolInputError, sparkplug.MetricError, PublishError) as exc:
        return _edge_failure("NBIRTH", exc)
    return _ok(data, reconnected_with_will=reconnected)


@mcp.tool(annotations=_WRITE)
async def publish_node_death(ctx: Context) -> dict[str, Any]:
    """WRITE: publish this server's NDEATH (edge node goes offline).

    Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
    nothing.

    Only possible after publish_node_birth. The payload carries the bdSeq of the
    NBIRTH and no seq, as the specification requires; it is sent with QoS 1 and
    confirmed by the broker. All of the node's devices count as dead afterwards.
    """
    m = _manager(ctx)
    if (refused := _writes_disabled(m)) is not None:
        return refused
    try:
        async with m.sparkplug_lock:
            if not m.node_born:
                raise ToolInputError("this server's edge node is not born; nothing to kill")
            topic, payload = m._ndeath_message()
            result = await m.publish(topic, payload, qos=1)
            m.node_born = False
            m.device_births.clear()
    except (ToolInputError, PublishError) as exc:
        return _edge_failure("NDEATH", exc)
    return _ok({"topic": topic, "bd_seq": m.bd_seq, **result})


@mcp.tool(annotations=_WRITE)
@_metrics_doc
async def publish_device_birth(
    ctx: Context, device_id: str, metrics: list[dict[str, Any]]
) -> dict[str, Any]:
    """WRITE: publish a DBIRTH for a device of this server's edge node.

    Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
    nothing.

    Needs a born node (publish_node_birth first). Uses the node's next seq.

    Parameters:
        device_id: device ID (no '+', '/' or '#').
        metrics: all metrics of the device (may be empty). {METRICS_HELP}
    """
    m = _manager(ctx)
    if (refused := _writes_disabled(m)) is not None:
        return refused
    try:
        device_id = _check_id(device_id, "device_id")
        metrics = _check_metrics(metrics, required=False)
        sparkplug.build_payload(metrics, seq=0)
        async with m.sparkplug_lock:
            if not m.node_born:
                raise ToolInputError("publish_node_birth first: the edge node is not born")
            data = await _publish_edge(m, "DBIRTH", metrics, device_id)
            m.device_births[device_id] = {"born_at_seq": data["seq"]}
    except (ToolInputError, sparkplug.MetricError, PublishError) as exc:
        return _edge_failure("DBIRTH", exc, device_id=device_id)
    return _ok(data, device_id=device_id)


@mcp.tool(annotations=_WRITE)
async def publish_device_death(ctx: Context, device_id: str) -> dict[str, Any]:
    """WRITE: publish a DDEATH for a device born with publish_device_birth.

    Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
    nothing.

    Parameters:
        device_id: a device this server has published a DBIRTH for.
    """
    m = _manager(ctx)
    if (refused := _writes_disabled(m)) is not None:
        return refused
    try:
        device_id = _check_id(device_id, "device_id")
        async with m.sparkplug_lock:
            if device_id not in m.device_births:
                raise ToolInputError(f"device {device_id!r} has no DBIRTH from this server")
            data = await _publish_edge(m, "DDEATH", [], device_id)
            m.device_births.pop(device_id, None)
    except (ToolInputError, PublishError) as exc:
        return _edge_failure("DDEATH", exc, device_id=device_id)
    return _ok(data, device_id=device_id)


@mcp.tool(annotations=_WRITE)
@_metrics_doc
async def publish_node_data(ctx: Context, metrics: list[dict[str, Any]]) -> dict[str, Any]:
    """WRITE: publish NDATA (new values of this server's node metrics).

    Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
    nothing.

    Needs a born node (publish_node_birth first). Uses the node's next seq.

    Parameters:
        metrics: at least one metric. {METRICS_HELP}
    """
    m = _manager(ctx)
    if (refused := _writes_disabled(m)) is not None:
        return refused
    try:
        metrics = _check_metrics(metrics, required=True)
        sparkplug.build_payload(metrics, seq=0)
        async with m.sparkplug_lock:
            if not m.node_born:
                raise ToolInputError("publish_node_birth first: the edge node is not born")
            data = await _publish_edge(m, "NDATA", metrics)
    except (ToolInputError, sparkplug.MetricError, PublishError) as exc:
        return _edge_failure("NDATA", exc)
    return _ok(data)


@mcp.tool(annotations=_WRITE)
@_metrics_doc
async def publish_device_data(
    ctx: Context, device_id: str, metrics: list[dict[str, Any]]
) -> dict[str, Any]:
    """WRITE: publish DDATA (new values of a device born by this server).

    Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
    nothing.

    Needs a DBIRTH for the device (publish_device_birth). Uses the node's next seq.

    Parameters:
        device_id: a device this server has published a DBIRTH for.
        metrics: at least one metric. {METRICS_HELP}
    """
    m = _manager(ctx)
    if (refused := _writes_disabled(m)) is not None:
        return refused
    try:
        device_id = _check_id(device_id, "device_id")
        metrics = _check_metrics(metrics, required=True)
        sparkplug.build_payload(metrics, seq=0)
        async with m.sparkplug_lock:
            if device_id not in m.device_births:
                raise ToolInputError(
                    f"publish_device_birth first: device {device_id!r} has no DBIRTH"
                    " from this server"
                )
            data = await _publish_edge(m, "DDATA", metrics, device_id)
    except (ToolInputError, sparkplug.MetricError, PublishError) as exc:
        return _edge_failure("DDATA", exc, device_id=device_id)
    return _ok(data, device_id=device_id)


async def _publish_command(
    m: MQTTClientManager,
    message_type: str,
    metrics: Any,
    group_id: str | None,
    edge_node_id: str | None,
    device_id: str | None = None,
) -> dict[str, Any]:
    target_group = _check_id(group_id or m.settings.group_id, "group_id")
    target_node = _check_id(edge_node_id or m.settings.edge_node_id, "edge_node_id")
    metrics = _check_metrics(metrics, required=True)
    payload = sparkplug.build_payload(metrics, seq=None)  # commands carry no seq
    topic = sparkplug.topic(target_group, message_type, target_node, device_id)
    result = await m.publish(topic, payload.SerializeToString(), qos=0)
    return {"topic": topic, "metrics": len(payload.metrics), **result}


@mcp.tool(annotations=_WRITE)
@_metrics_doc
async def publish_node_command(
    ctx: Context,
    metrics: list[dict[str, Any]],
    edge_node_id: str | None = None,
    group_id: str | None = None,
) -> dict[str, Any]:
    """WRITE: send a Sparkplug B NCMD to an edge node.

    Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
    nothing.

    A command can change real equipment: the edge node applies the metrics it
    receives. success:true only means the NCMD was written to the network (QoS 0,
    as the specification requires); it does not mean the node applied it. Watch
    its NDATA (read_messages) to confirm. Example: {"name": "Node Control/Rebirth",
    "value": true, "type": "boolean"} asks a node to republish its births.

    Parameters:
        metrics: at least one metric to write. {METRICS_HELP}
        edge_node_id: target edge node (default: SPARKPLUG_EDGE_NODE_ID).
        group_id: target group (default: SPARKPLUG_GROUP_ID).
    """
    m = _manager(ctx)
    if (refused := _writes_disabled(m)) is not None:
        return refused
    try:
        data = await _publish_command(m, "NCMD", metrics, group_id, edge_node_id)
    except (ToolInputError, sparkplug.MetricError, PublishError) as exc:
        return _edge_failure("NCMD", exc)
    return _ok(data)


@mcp.tool(annotations=_WRITE)
@_metrics_doc
async def publish_device_command(
    ctx: Context,
    device_id: str,
    metrics: list[dict[str, Any]],
    edge_node_id: str | None = None,
    group_id: str | None = None,
) -> dict[str, Any]:
    """WRITE: send a Sparkplug B DCMD to a device of an edge node.

    Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
    nothing.

    A command can change real equipment: the edge node applies the metrics to
    the device. success:true only means the DCMD was written to the network (QoS
    0, as the specification requires); it does not mean the device applied it.
    Watch its DDATA (read_messages) to confirm.

    Parameters:
        device_id: target device ID.
        metrics: at least one metric to write. {METRICS_HELP}
        edge_node_id: edge node the device belongs to (default: SPARKPLUG_EDGE_NODE_ID).
        group_id: target group (default: SPARKPLUG_GROUP_ID).
    """
    m = _manager(ctx)
    if (refused := _writes_disabled(m)) is not None:
        return refused
    try:
        device_id = _check_id(device_id, "device_id")
        data = await _publish_command(m, "DCMD", metrics, group_id, edge_node_id, device_id)
    except (ToolInputError, sparkplug.MetricError, PublishError) as exc:
        return _edge_failure("DCMD", exc, device_id=device_id)
    return _ok(data, device_id=device_id)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Load the configuration (after .env) and run the MCP server on stdio."""
    try:
        settings = get_settings()
        build_client(settings)  # fail fast on unreadable TLS files
    except (ConfigError, OSError, ValueError) as exc:
        print(f"mqtt-mcp: configuration error: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
