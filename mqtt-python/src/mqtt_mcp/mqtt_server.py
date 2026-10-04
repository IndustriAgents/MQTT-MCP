"""MQTT + Sparkplug B MCP server (FastMCP, stdio).

Read-only by default: subscribing, reading received messages, listing the
Sparkplug B nodes seen on the broker and decoding payloads always work; every
tool that publishes is refused unless ``MQTT_WRITES_ENABLED=true``.

Every tool returns the suite's envelope ``{success, data, error, meta}``.
stdout carries MCP JSON-RPC only; logs go to stderr.
"""

import asyncio
import logging
import socket
import ssl
import sys
import threading
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

import anyio
import anyio.lowlevel
import paho.mqtt.client as mqtt
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import StrictBool, StrictInt, ValidationError

from . import __version__, sparkplug
from .config import ConfigError, Settings, load_settings, merge_env_file
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


def _check_unique_names(metrics: list[Any]) -> None:
    """A birth certificate defines each metric once (Sparkplug B)."""
    seen, duplicates = set(), set()
    for metric in metrics:
        name = metric.get("name") if isinstance(metric, dict) else None
        (duplicates if name in seen else seen).add(name)
    if duplicates:
        raise ToolInputError(
            f"metric names must be unique in a birth certificate; repeated: {sorted(duplicates)}"
        )


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

#: What a write tool knows about its request: written (sent and, for QoS 1/2,
#: acknowledged), rejected (refused by the device or broker; MQTT 3.1.1 gives a
#: broker no way to reject a publish, so this server never reports it),
#: unknown (it may have gone out; never retried), not_sent.
OUTCOMES = ("written", "rejected", "unknown", "not_sent")
UNKNOWN_HINT = "It may have been applied; check before retrying."


@dataclass
class SendState:
    """What the current tool call has handed to the MQTT client (one per call).

    Shared through a context variable, so that a call that ends without a result
    (deadline, client cancel, client disconnect) can still say whether anything
    may have been sent.
    """

    sent: bool = False  # a message was handed to the MQTT client
    outcome: str | None = None  # set when the publish ended without a result
    #: The call's one absolute deadline (time.monotonic()); every wait in the call
    #: (connection, session restart, acknowledgement) uses what is left of it.
    deadline: float | None = None


_send_state: ContextVar[SendState | None] = ContextVar("mqtt_mcp_send_state", default=None)


@dataclass
class _InFlight:
    """A publish handed to paho and not yet confirmed or given up."""

    info: mqtt.MQTTMessageInfo
    lost: bool = False  # the connection dropped before confirmation
    withdrawn: bool | None = None  # _withdraw's verdict when it was lost


class PublishError(RuntimeError):
    """A request did not complete; ``outcome`` says whether it may have gone out."""

    def __init__(self, message: str, outcome: str = "not_sent", request_sent: bool = False):
        super().__init__(message)
        self.outcome = outcome
        self.request_sent = request_sent


class _Client(mqtt.Client):
    """paho client that remembers why the last connection attempt failed."""

    last_connect_error: str | None = None
    tcp_connected_at: float | None = None  # monotonic time of the last TCP connect

    def reconnect(self) -> mqtt.MQTTErrorCode:
        self.tcp_connected_at = None
        try:
            result = super().reconnect()
        except OSError as exc:
            self.last_connect_error = f"{type(exc).__name__}: {exc}"
            raise
        self.tcp_connected_at = time.monotonic()
        return result


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
        self.generation = 0  # number of successful connections so far
        self.last_error: str | None = None
        self.last_refusal: str | None = None  # last CONNACK refusal (kept after reconnects)
        self._expect_disconnect = False
        #: Set when the MCP client is gone or the server stops: nothing more is sent.
        self.closing = False
        self._inflight: dict[int, _InFlight] = {}  # mid -> publish awaiting confirmation
        self._restart_task: asyncio.Task | None = None
        self.subscriptions: dict[str, int] = {}  # filter -> QoS granted by the broker
        self._pending: dict[int, tuple[asyncio.Future | None, str, str]] = {}
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
        await self.wait_for(lambda: self.connected, min(1.0, self.settings.tool_timeout))

    def close_for_disconnect(self) -> None:
        """The MCP client is gone: from now on nothing is sent for a tool call."""
        with self._lock:
            self.closing = True

    async def stop(self) -> None:
        """Stop sending, publish NDEATH if this server's edge node is born, disconnect.

        Pending publishes are withdrawn first, and a session restart that is still
        running is waited for (it does not reconnect once ``closing`` is set). If
        the NDEATH is not confirmed, the connection is dropped without a DISCONNECT
        so that the broker publishes the NDEATH will instead.
        """
        if self.client is None:
            return
        with self._lock:
            self.closing = True
        task = self._restart_task
        if task is not None and not task.done():
            try:
                await asyncio.wait_for(asyncio.shield(task), 2 * self.settings.tool_timeout + 1)
            except BaseException as exc:  # finished with an error, or still stuck
                logger.info("Session restart ended on shutdown: %s", exc)
        with self._lock:
            for mid in list(self._inflight):
                self._finish(mid)
        graceful = True
        if self.node_born and self.will_active and self.connected:
            try:
                await self.publish(*self._ndeath_message(), qos=1, timeout=2.0, shutdown=True)
            except PublishError as exc:
                logger.warning("NDEATH not confirmed on shutdown (%s); leaving it to the will", exc)
                graceful = False
        await asyncio.to_thread(self._stop_network, graceful, self.settings.tool_timeout)
        self.connected = False

    def _stop_network(self, graceful: bool, timeout: float) -> None:
        """Stop paho's network thread, bounded by ``timeout`` (runs in a worker thread).

        graceful: send DISCONNECT (the broker discards the will). Otherwise, or if
        the thread does not stop in time, shut the socket down so that the broker
        sees a lost connection (and publishes the will, if one is registered).
        """
        assert self.client is not None
        with self._lock:
            self._expect_disconnect = True
        if graceful:
            self.client.disconnect()
        stopper = threading.Thread(target=self.client.loop_stop, daemon=True)
        stopper.start()
        if not graceful:
            self._shutdown_socket()
        stopper.join(timeout)
        if stopper.is_alive():
            self._shutdown_socket()
            stopper.join(timeout)

    def _shutdown_socket(self) -> None:
        sock = self.client.socket() if self.client else None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def time_left(self) -> float:
        """Seconds left until the current tool call's deadline (MQTT_TOOL_TIMEOUT
        outside a tool call). Each step uses what is left; nothing restarts it."""
        state = _send_state.get()
        if state is not None and state.deadline is not None:
            return max(0.0, state.deadline - time.monotonic())
        return self.settings.tool_timeout

    async def wait_for(self, condition, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while not condition() and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        return bool(condition())

    def current_error(self) -> str | None:
        """Why the server is not connected right now, as far as it is known."""
        if self.connected:
            return None
        if self.last_error or (self.client and self.client.last_connect_error):
            return self.last_error or self.client.last_connect_error
        client = self.client
        if client is not None and client.socket() is not None and client.tcp_connected_at:
            waited = time.monotonic() - client.tcp_connected_at
            return (
                f"the TCP connection is open, but no CONNACK has arrived for {waited:.0f}s:"
                " the peer does not answer as an MQTT broker"
            )
        return None

    async def require_connection(self) -> None:
        if not await self.wait_for(lambda: self.connected, self.time_left()):
            reason = self.current_error()
            detail = f" (last error: {reason})" if reason else ""
            raise PublishError(
                f"Not connected to the MQTT broker at {self.settings.broker_url}{detail}."
                " Nothing was sent; the server keeps retrying the connection in the background."
            )

    # -- paho callbacks (network thread) -----------------------------------

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code.is_failure:
            with self._lock:
                self.connected = False
                self.last_error = f"broker refused the connection: {reason_code}"
                self.last_refusal = self.last_error
            logger.warning("Broker refused the connection: %s", reason_code)
            return
        with self._lock:
            self.connected = True
            self.generation += 1
            self.last_error = None
            client.last_connect_error = None
            # A will configured on the client went out with this CONNECT.
            self.will_active = self._will_bd_seq is not None
            if self.will_active:
                self.bd_seq = self._will_bd_seq
            restore = dict(self.subscriptions)
            for topic, qos in restore.items():
                result, mid = client.subscribe(topic, qos)
                if result == mqtt.MQTT_ERR_SUCCESS and mid is not None:
                    self._pending[mid] = (None, "restore", topic)
        logger.info(
            "Connected to %s%s",
            self.settings.broker_url,
            f"; restoring {len(restore)} subscription(s)" if restore else "",
        )

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        with self._lock:
            was_connected = self.connected
            self.connected = False
            pending = [future for future, _, _ in self._pending.values() if future is not None]
            self._pending.clear()
            # paho resends unacknowledged QoS 1/2 messages when it reconnects (and
            # this runs before it does): withdraw every unconfirmed publish now, so a
            # message is never sent twice. The waiting calls end as unknown/not_sent.
            for mid, flight in self._inflight.items():
                if not flight.lost:
                    flight.withdrawn = self._withdraw(mid)
                    flight.lost = True
            if not self._expect_disconnect:
                self._connection_lost(client, reason_code, was_connected)

        for future in pending:
            self._resolve(future, None)

    def _connection_lost(self, client, reason_code, was_connected: bool) -> None:
        self.will_active = False
        if not was_connected:
            # A refused or failed CONNECT: the broker never held our will, so keep it
            # configured for the next attempt.
            if self.last_error is None and not client.last_connect_error:
                self.last_error = (
                    f"the connection closed before the broker accepted it ({reason_code}):"
                    " the peer does not answer as an MQTT broker"
                )
            return
        self.last_error = f"connection lost: {reason_code}"
        logger.warning("Connection to the broker lost (%s); reconnecting", reason_code)
        # Messages published while we are away (deaths included) are missed.
        self.registry.mark_unknown()
        if self._will_bd_seq is not None:
            # The broker now publishes our NDEATH will. The node stays dead until a new
            # NBIRTH, which needs a fresh will (and bdSeq) on a new connection.
            client.will_clear()
            self._will_bd_seq = None
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
            future, kind, topic = self._pending.pop(mid, (None, "", ""))
            if kind:
                refused = not reason_codes or reason_codes[0].is_failure
                if refused:
                    self.subscriptions.pop(topic, None)
                    if kind == "restore":
                        logger.warning("Broker refused to restore subscription %s", topic)
                else:
                    # Recorded here, under the lock, so a reconnect cannot miss it.
                    self.subscriptions[topic] = int(reason_codes[0].value)
        if future is not None:
            self._resolve(future, list(reason_codes))

    def _on_unsubscribe(self, client, userdata, mid, reason_codes, properties) -> None:
        with self._lock:
            future, kind, topic = self._pending.pop(mid, (None, "", ""))
            if kind == "unsubscribe":
                self.subscriptions.pop(topic, None)
        if future is not None:
            self._resolve(future, list(reason_codes))

    def _resolve(self, future: asyncio.Future, value: Any) -> None:
        def setter() -> None:
            if not future.done():
                future.set_result(value)

        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(setter)

    # -- operations ----------------------------------------------------------

    async def _await_ack(self, call, kind: str, topic: str):
        """Run a paho subscribe/unsubscribe call and wait for the broker's ack."""
        assert self._loop is not None
        future = self._loop.create_future()
        with self._lock:
            result, mid = call()
            if result != mqtt.MQTT_ERR_SUCCESS or mid is None:
                raise PublishError(f"the {kind} was not sent: {mqtt.error_string(result)}")
            self._pending[mid] = (future, kind, topic)
        try:
            reason_codes = await asyncio.wait_for(future, self.time_left())
        except asyncio.TimeoutError:
            with self._lock:
                self._pending.pop(mid, None)
            raise PublishError(
                f"the broker did not acknowledge the {kind} before the call's deadline",
                outcome="unknown",
                request_sent=True,
            ) from None
        if reason_codes is None:
            raise PublishError(
                f"the connection was lost before the broker acknowledged the {kind}",
                outcome="unknown",
                request_sent=True,
            )
        return reason_codes

    async def subscribe(self, topic: str, qos: int) -> int:
        await self.require_connection()
        assert self.client is not None
        codes = await self._await_ack(lambda: self.client.subscribe(topic, qos), "subscribe", topic)
        if not codes or codes[0].is_failure:
            raise PublishError(f"the broker refused the subscription to {topic!r}")
        return int(codes[0].value)

    async def unsubscribe(self, topic: str) -> None:
        await self.require_connection()
        assert self.client is not None
        await self._await_ack(lambda: self.client.unsubscribe(topic), "unsubscribe", topic)

    async def publish(
        self,
        topic: str,
        payload: bytes,
        qos: int = 0,
        retain: bool = False,
        timeout: float | None = None,
        *,
        shutdown: bool = False,
    ) -> dict[str, Any]:
        """Publish once and confirm: QoS 0 when written to the socket, QoS 1/2 on the
        broker's acknowledgement. Never retried, and never sent twice.

        Raises PublishError otherwise, with the outcome: "not_sent" if nothing can
        have left, "unknown" if it may have. If the call ends for any other reason
        (deadline, client cancel, client gone), the message is withdrawn from
        paho's queues before the exception propagates, so it cannot go out later,
        and the outcome is recorded in the call's SendState.
        """
        await self.require_connection()
        assert self.client is not None
        timeout = self.time_left() if timeout is None else timeout
        state = _send_state.get()
        with self._lock:
            # No await between this check and the hand-over to paho.
            if self.closing and not shutdown:
                raise PublishError(
                    "the server is shutting down (the MCP client is gone); nothing was sent"
                )
            generation = self.generation
            info = self.client.publish(topic, payload, qos=qos, retain=retain)
            self._inflight[info.mid] = _InFlight(info)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            # Nothing was written (no connection); QoS 1/2 would be queued for later.
            if self._finish(info.mid) is not False:
                raise PublishError(f"not sent: {mqtt.error_string(info.rc)}")
            if state is not None:
                state.sent = True
            raise PublishError(
                f"not sent now ({mqtt.error_string(info.rc)}), and it could not be removed"
                f" from the client's queue, so it may go out after a reconnect. {UNKNOWN_HINT}",
                outcome="unknown",
                request_sent=True,
            )
        if state is not None:
            state.sent = True
        try:
            problem = await self._await_confirmation(info, timeout)
        except BaseException:
            # Cancelled (deadline, client cancel, client gone): make sure it cannot go
            # out later, and record what is known for the caller.
            withdrawn = self._finish(info.mid)
            if state is not None:
                state.outcome = (
                    "not_sent" if withdrawn is True and self.generation == generation else "unknown"
                )
            raise
        if problem is None:
            with self._lock:
                self._inflight.pop(info.mid, None)
            return {
                "mid": info.mid,
                "generation": generation,
                "delivery": (
                    "written to the network; QoS 0 is not acknowledged, so whether it reached"
                    " the broker or had any effect on a device is unknown"
                    if qos == 0
                    else f"acknowledged by the broker ({'PUBACK' if qos == 1 else 'PUBCOMP'});"
                    " whether it had any effect on a device is not known"
                ),
            }
        withdrawn = self._finish(info.mid)
        if withdrawn is True and (problem == "connection lost" or self.generation == generation):
            raise PublishError(f"{problem} before it was written; nothing was sent")
        what = (
            "was not confirmed as written"
            if qos == 0
            else f"was not acknowledged by the broker (QoS {qos})"
        )
        raise PublishError(
            f"the message {what} ({problem}) and may have reached the broker. This server"
            f" does not send it again. {UNKNOWN_HINT}",
            outcome="unknown",
            request_sent=True,
        )

    async def _await_confirmation(self, info: mqtt.MQTTMessageInfo, timeout: float) -> str | None:
        """None once paho reports the message published, else why not."""
        deadline = time.monotonic() + timeout
        flight = self._inflight.get(info.mid)
        while True:
            try:
                if info.is_published():
                    return None
            except (RuntimeError, ValueError) as exc:
                return str(exc)
            if flight is not None and flight.lost:
                return "connection lost"
            if time.monotonic() >= deadline:
                return f"no confirmation within {timeout:g}s"
            await asyncio.sleep(0.01)

    def _finish(self, mid: int) -> bool | None:
        """Stop tracking a publish and withdraw it from paho (see _withdraw)."""
        with self._lock:
            flight = self._inflight.pop(mid, None)
            if flight is not None and flight.lost:
                return flight.withdrawn  # already withdrawn when the connection dropped
            return self._withdraw(mid)

    def _withdraw(self, mid: int) -> bool | None:
        """Remove an unconfirmed publish from paho's outgoing queues (paho 2.x internals).

        Returns True if no byte of it can have left the client (it was only
        queued, and is now removed), False if it may have been written (fully or
        partly), None if paho held nothing for it.
        """
        client = self.client
        removed_unsent = partial = False
        try:
            # paho's network thread pops a packet off this deque while it writes it, so
            # a packet still in it with pos 0 has not been started.
            for packet in list(client._out_packet):
                if packet.get("mid") != mid or packet.get("command", 0) & 0xF0 != 0x30:
                    continue
                if packet.get("pos", 0) == 0:
                    try:
                        client._out_packet.remove(packet)
                    except ValueError:  # taken for writing just now
                        partial = True
                        continue
                    if packet.get("pos", 0) != 0:
                        # paho took it, wrote part of it and put it back before our
                        # remove(): bytes went out, and the stream now lacks the rest.
                        partial = True
                        self._shutdown_socket()
                    else:
                        removed_unsent = True
                else:
                    partial = True
            with client._out_message_mutex:
                message = client._out_messages.pop(mid, None)
                if message is not None and message.state not in (
                    mqtt.mqtt_ms_publish,
                    mqtt.mqtt_ms_queued,
                ):
                    if client._inflight_messages > 0:
                        client._inflight_messages -= 1
        except (AttributeError, RuntimeError):  # paho internals changed, or a race
            return False
        if partial:
            return False
        if message is None:  # QoS 0, or nothing queued
            return True if removed_unsent else None
        if message.dup:  # an earlier copy went out before a reconnect
            return False
        if message.state in (mqtt.mqtt_ms_publish, mqtt.mqtt_ms_queued):
            return True  # waiting for a connection or an in-flight slot: never written
        return removed_unsent  # in flight: written unless its packet was still queued

    # -- this server as a Sparkplug B edge node --------------------------------

    def _ndeath_message(self) -> tuple[str, bytes]:
        payload = sparkplug.build_payload(
            [], seq=None, extra_metrics=[sparkplug.bd_seq_metric(self.bd_seq or 0)]
        )
        topic = sparkplug.topic(self.settings.group_id, "NDEATH", self.settings.edge_node_id)
        return topic, payload.SerializeToString()

    async def restart_session(self, bd_seq: int | None) -> None:
        """Reconnect with an NDEATH will carrying ``bd_seq``, or without a will (None).

        Runs as its own task, shielded from the calling tool's cancellation, so the
        client is never left half-restarted; ``stop()`` waits for it. It does not
        reconnect once ``closing`` is set.
        """
        self._restart_task = asyncio.ensure_future(self._restart(bd_seq))
        # Its error is reported to the caller; if the caller is gone, swallow it here.
        self._restart_task.add_done_callback(lambda t: t.cancelled() or t.exception())
        await asyncio.shield(self._restart_task)

    async def _restart(self, bd_seq: int | None) -> None:
        assert self.client is not None
        if self.closing:
            raise PublishError("the server is shutting down; the session was not restarted")
        topic = sparkplug.topic(self.settings.group_id, "NDEATH", self.settings.edge_node_id)
        payload = None
        if bd_seq is not None:
            payload = sparkplug.build_payload(
                [], seq=None, extra_metrics=[sparkplug.bd_seq_metric(bd_seq)]
            ).SerializeToString()
        await asyncio.to_thread(self._stop_network, True, self.settings.tool_timeout)
        with self._lock:
            if self.closing:  # checked under the lock, right before reconnecting
                raise PublishError("the server is shutting down; the session was not restarted")
            if payload is None:
                self.client.will_clear()
                self._will_bd_seq = None
            else:
                self.client.will_set(topic, payload, qos=1, retain=False)
                self._will_bd_seq = bd_seq
                self._next_bd_seq = (bd_seq + 1) % 256  # every CONNECT with a will gets one
            self.last_refusal = None
            self._expect_disconnect = False
            self.client.connect_async(
                self.settings.host, self.settings.port, keepalive=self.settings.keepalive
            )
            self.client.loop_start()
        wanted = (
            (lambda: self.connected and self.will_active) if payload else (lambda: self.connected)
        )
        if not await self.wait_for(wanted, self.time_left()):
            reason = self.last_refusal or self.current_error() or "timeout"
            what = "with the NDEATH will registered" if payload else "without a will"
            raise PublishError(f"could not reconnect {what} ({reason})")

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


# ---------------------------------------------------------------------------
# Settings, lifespan and the FastMCP server
# ---------------------------------------------------------------------------

_settings: Settings | None = None


def get_settings(env_file: str | None = None) -> Settings:
    """Load settings once, after merging a .env file into the environment.

    Variables already set in the environment take precedence. ``env_file`` is
    the explicit ``--env-file``; without it only ``mqtt-python/.env`` of a source
    checkout is read (never the working directory), and only for MQTT_* and
    SPARKPLUG_* variables other than MQTT_WRITES_ENABLED (warnings on stderr say
    what was ignored).
    """
    global _settings
    if _settings is None:
        for warning in merge_env_file(env_file):
            logger.warning(warning)
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
    if isinstance(server, EnvelopeFastMCP):
        server.on_client_disconnect = manager.close_for_disconnect
    await manager.start()
    try:
        yield {"mqtt_manager": manager}
    finally:
        await manager.stop()


def call_deadline(settings: Settings) -> float:
    """Seconds one tool call may take, from its start: MQTT_TOOL_TIMEOUT.

    It is one absolute deadline per call, shared by every step (waiting for the
    connection or a lock, a session restart, the broker's acknowledgement), so
    no call can hang without an outcome and none is sent after its deadline.
    """
    return settings.tool_timeout


#: Extra time the call wrapper gives the steps to report their own result once
#: the deadline has passed (the steps themselves stop at the deadline).
_REPORT_GRACE = 0.5


#: Tools that publish (all gated by MQTT_WRITES_ENABLED).
WRITE_TOOLS = frozenset(
    {
        "publish_message",
        "publish_node_birth",
        "publish_node_death",
        "publish_device_birth",
        "publish_device_death",
        "publish_node_data",
        "publish_device_data",
        "publish_node_command",
        "publish_device_command",
    }
)


#: Errors that only mean a reply could not be delivered to a client that is gone.
_UNDELIVERABLE = (
    anyio.ClosedResourceError,
    anyio.BrokenResourceError,
    BrokenPipeError,
    ConnectionResetError,
)


def _only_undeliverable(exc: BaseException) -> bool:
    if isinstance(exc, _UNDELIVERABLE):
        return True
    inner = getattr(exc, "exceptions", None)  # an exception group
    return bool(inner) and all(_only_undeliverable(e) for e in inner)


class EnvelopeFastMCP(FastMCP):
    """FastMCP with the suite's envelope for every outcome of a tool call.

    - FastMCP validates arguments against the tool's signature before the tool
      runs and turns a failure into a protocol-level tool error. Here such errors
      come back as ``{success: false, error: "Invalid arguments: ..."}``.
    - Every call has one absolute deadline (``call_deadline``). A write that
      runs out of time reports what it knows: "not_sent" if nothing was handed to
      the MQTT client (or it was withdrawn in time), else "unknown". A read gets
      a clean error. Nothing is sent after the deadline.
    - When the MCP client disconnects (stdin closes), every running call is
      cancelled at once and nothing more is published; see run_stdio_async.
    """

    #: Called once when the client's stdin reaches end of file (set by the lifespan).
    on_client_disconnect: Callable[[], None] | None = None
    #: True once the client's stdin has reached end of file.
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
        deadline = call_deadline(get_settings())
        state = SendState(deadline=time.monotonic() + deadline)
        token = _send_state.set(state)
        try:
            with anyio.CancelScope() as scope:
                self._active_calls.add(scope)
                try:
                    if self.client_gone:
                        scope.cancel()
                        await anyio.lowlevel.checkpoint()
                    return await asyncio.wait_for(
                        self._call_enveloped(name, arguments), deadline + _REPORT_GRACE
                    )
                except asyncio.TimeoutError:
                    return self._ended_early(
                        name, state, f"did not finish within its deadline of {deadline:g}s"
                    )
                finally:
                    self._active_calls.discard(scope)
            # Only reached when client_disconnected() cancelled this call.
            return self._ended_early(name, state, "was cancelled: the MCP client disconnected")
        finally:
            _send_state.reset(token)

    def _ended_early(self, name: str, state: SendState, why: str) -> dict[str, Any]:
        if name not in WRITE_TOOLS:
            return _fail(f"{name} {why}; nothing was changed", tool=name)
        if not state.sent or state.outcome == "not_sent":
            return _fail(
                f"{name} {why}. Nothing was sent, and it will not be sent later.",
                tool=name,
                outcome="not_sent",
                request_sent=False,
            )
        return _fail(
            f"{name} {why}. {UNKNOWN_HINT}", tool=name, outcome="unknown", request_sent=True
        )

    async def _call_enveloped(self, name: str, arguments: dict[str, Any]):
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
        last_error = m.current_error()
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
async def subscribe_topic(ctx: Context, topic: str, qos: StrictInt = 0) -> dict[str, Any]:
    """Subscribe this server to an MQTT topic filter so that matching messages are kept.

    Received messages go into a buffer that read_messages returns (the newest
    MQTT_MESSAGE_BUFFER_SIZE, default 1000); Sparkplug B messages also update
    list_sparkplug_nodes. A broker sends retained messages for the filter right
    away. Subscriptions are restored automatically after a reconnect.

    Parameters:
        topic: topic filter; '+' matches one level, '#' the rest (e.g. 'sensors/#',
            'spBv1.0/#'). Filters starting with '$' other than '$SYS/' are broker
            extensions such as shared ($share/, $queue/) or exclusive ($exclusive/)
            subscriptions that change what other clients receive; they are refused
            unless writes are enabled.
        qos: maximum QoS for delivery to this server: 0, 1 or 2 (default 0).

    data: the filter and the QoS the broker granted (success only after SUBACK).
    """
    m = _manager(ctx)
    try:
        topic = _check_topic_filter(topic)
        qos = _check_qos(qos)
        if (
            topic.startswith("$")
            and not topic.startswith("$SYS/")
            and not m.settings.writes_enabled
        ):
            raise ToolInputError(
                "filters starting with '$' (other than $SYS/) are broker extensions such as"
                " shared ($share/, $queue/) or exclusive ($exclusive/) subscriptions that"
                " change what other clients receive; they are only allowed with"
                " MQTT_WRITES_ENABLED=true"
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
    limit: StrictInt = 50,
    since_id: StrictInt | None = None,
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
    missed_messages=true if messages after since_id were already dropped. Ids
    start again at 1 when the server restarts.
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
    success:false if the hex is invalid or the bytes cannot be parsed as the
    Sparkplug B protobuf (protobuf cannot tell every other byte string apart, so
    success does not prove the bytes came from a Sparkplug node).
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
        return _fail(WRITES_DISABLED, writes_enabled=False, outcome="not_sent", request_sent=False)
    return None


def _written(data: dict[str, Any], **meta: Any) -> dict[str, Any]:
    """success:true for a write: the message went out (and QoS 1/2 was acknowledged)."""
    data = {k: v for k, v in data.items() if k != "generation"}
    return _ok(data, outcome="written", request_sent=True, **meta)


def _write_failure(what: str, exc: Exception, **meta: Any) -> dict[str, Any]:
    """success:false for a write, with what is known about the request."""
    if isinstance(exc, PublishError):
        outcome, request_sent = exc.outcome, exc.request_sent
    else:  # invalid arguments or state: refused before anything was sent
        outcome, request_sent = "not_sent", False
    return _fail(f"{what}: {exc}", outcome=outcome, request_sent=request_sent, **meta)


@mcp.tool(annotations=_WRITE)
async def publish_message(
    ctx: Context, topic: str, payload: str, qos: StrictInt = 0, retain: StrictBool = False
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

    Sent at most once: success:true (meta.outcome "written") means QoS 0: written
    to the network (the broker does not acknowledge QoS 0); QoS 1/2: the broker
    acknowledged it (an MQTT 3.1.1 broker acknowledges even a publish its ACL
    drops). meta.outcome "not_sent" means nothing left the server. "unknown"
    means it may have reached the broker: the server does not send it again, so
    check before retrying. If the connection drops while the call waits for the
    acknowledgement, the message is given up (never resent on reconnect).
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
        return _write_failure(f"Publish to {topic!r} failed", exc, topic=topic)
    size = len(payload.encode("utf-8"))
    return _written({"topic": topic, "qos": qos, "retain": retain, "bytes": size, **result})


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

    Must be called with ``m.sparkplug_lock`` held, on a connection that carries
    the node's NDEATH will. ``seq_mode`` is "zero" for NBIRTH and "next"
    otherwise; seq advances when the message was sent or may have been sent.
    The caller may update the node state only if this returns: if the
    connection dropped meanwhile, the broker has published the NDEATH will and
    this raises.
    """
    if not m.will_active or (message_type != "NBIRTH" and not m.node_born):
        raise PublishError(
            "the edge node is not born on this connection (no NDEATH will is registered);"
            " publish_node_birth first"
        )
    seq = 0 if seq_mode == "zero" else (m.seq + 1) % 256
    payload = sparkplug.build_payload(metrics, seq=seq, extra_metrics=extra or [])
    topic = sparkplug.topic(m.settings.group_id, message_type, m.settings.edge_node_id, device_id)
    try:
        result = await m.publish(topic, payload.SerializeToString(), qos=0)
    except PublishError as exc:
        if exc.outcome == "unknown":
            m.seq = seq  # it may have gone out with this seq; never reuse it
        raise
    except BaseException:  # cancelled: publish() recorded whether it may have gone out
        state = _send_state.get()
        if state is not None and state.sent and state.outcome != "not_sent":
            m.seq = seq
        raise
    m.seq = seq
    if m.generation != result["generation"] or not m.will_active:
        raise PublishError(
            f"{message_type} was written to the network, but the connection dropped right after"
            " it, so the broker may not have received it (QoS 0), and it has published the"
            f" NDEATH will: the node is dead; publish_node_birth again. {UNKNOWN_HINT}",
            outcome="unknown",
            request_sent=True,
        )
    return {
        "topic": topic,
        "seq": seq,
        "bd_seq": m.bd_seq,
        "metrics": len(payload.metrics),
        **result,
    }


def _edge_failure(message_type: str, exc: Exception, **meta: Any) -> dict[str, Any]:
    return _write_failure(f"{message_type} failed", exc, **meta)


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
        _check_unique_names(metrics)
        reserved = {sparkplug.BD_SEQ_METRIC, sparkplug.REBIRTH_METRIC}
        clash = sorted({x.get("name") for x in metrics} & reserved)
        if clash:
            raise ToolInputError(f"{clash} are added by the server; leave them out")
        async with m.sparkplug_lock:
            await m.require_connection()
            reconnected = False
            if not m.will_active:
                await m.restart_session(m._next_bd_seq)
                reconnected = True
            with m._lock:
                will_ok = m.connected and m.will_active and m.bd_seq is not None
            if not will_ok:
                raise PublishError(
                    "no NDEATH will is registered with the broker for this connection"
                    f" ({m.last_refusal or m.current_error() or 'connection changed'});"
                    " NBIRTH not sent"
                )
            extra = [sparkplug.bd_seq_metric(m.bd_seq), sparkplug.rebirth_metric()]
            data = await _publish_edge(m, "NBIRTH", metrics, extra=extra, seq_mode="zero")
            m.node_born = True
            m.node_lost = None
            m.device_births.clear()
    except (ToolInputError, sparkplug.MetricError, PublishError) as exc:
        return _edge_failure("NBIRTH", exc)
    return _written(data, reconnected_with_will=reconnected)


@mcp.tool(annotations=_WRITE)
async def publish_node_death(ctx: Context) -> dict[str, Any]:
    """WRITE: publish this server's NDEATH (edge node goes offline).

    Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
    nothing.

    Only possible after publish_node_birth. The payload carries the bdSeq of the
    NBIRTH and no seq, as the specification requires; it is sent with QoS 1 and
    confirmed by the broker. All of the node's devices count as dead afterwards.
    The server then reconnects without the NDEATH will, so that the broker does
    not announce the death a second time; the next NBIRTH uses a new bdSeq.
    """
    m = _manager(ctx)
    if (refused := _writes_disabled(m)) is not None:
        return refused
    meta: dict[str, Any] = {}
    try:
        async with m.sparkplug_lock:
            if not m.node_born:
                raise ToolInputError("this server's edge node is not born; nothing to kill")
            topic, payload = m._ndeath_message()
            bd_seq = m.bd_seq
            result = await m.publish(topic, payload, qos=1)
            m.node_born = False
            m.device_births.clear()
            try:
                await m.restart_session(None)
            except PublishError as exc:
                meta["reconnect"] = f"the NDEATH was published, but {exc}"
    except (ToolInputError, PublishError) as exc:
        return _edge_failure("NDEATH", exc)
    return _written({"topic": topic, "bd_seq": bd_seq, **result}, **meta)


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
        _check_unique_names(metrics)
        async with m.sparkplug_lock:
            if not m.node_born:
                raise ToolInputError("publish_node_birth first: the edge node is not born")
            data = await _publish_edge(m, "DBIRTH", metrics, device_id)
            m.device_births[device_id] = {"born_at_seq": data["seq"]}
    except (ToolInputError, sparkplug.MetricError, PublishError) as exc:
        return _edge_failure("DBIRTH", exc, device_id=device_id)
    return _written(data, device_id=device_id)


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
            if not m.node_born or device_id not in m.device_births:
                raise ToolInputError(f"device {device_id!r} has no DBIRTH from this server")
            data = await _publish_edge(m, "DDEATH", [], device_id)
            m.device_births.pop(device_id, None)
    except (ToolInputError, PublishError) as exc:
        return _edge_failure("DDEATH", exc, device_id=device_id)
    return _written(data, device_id=device_id)


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
    return _written(data)


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
            if not m.node_born or device_id not in m.device_births:
                raise ToolInputError(
                    f"publish_device_birth first: device {device_id!r} has no DBIRTH"
                    " from this server"
                )
            data = await _publish_edge(m, "DDATA", metrics, device_id)
    except (ToolInputError, sparkplug.MetricError, PublishError) as exc:
        return _edge_failure("DDATA", exc, device_id=device_id)
    return _written(data, device_id=device_id)


async def _publish_command(
    m: MQTTClientManager,
    message_type: str,
    metrics: Any,
    group_id: str | None,
    edge_node_id: str,
    device_id: str | None = None,
) -> dict[str, Any]:
    target_group = _check_id(group_id or m.settings.group_id, "group_id")
    target_node = _check_id(edge_node_id, "edge_node_id")
    metrics = _check_metrics(metrics, required=True)
    payload = sparkplug.build_payload(metrics, seq=None)  # commands carry no seq
    topic = sparkplug.topic(target_group, message_type, target_node, device_id)
    result = await m.publish(topic, payload.SerializeToString(), qos=0)
    return {"topic": topic, "metrics": len(payload.metrics), **result}


@mcp.tool(annotations=_WRITE)
@_metrics_doc
async def publish_node_command(
    ctx: Context,
    edge_node_id: str,
    metrics: list[dict[str, Any]],
    group_id: str | None = None,
) -> dict[str, Any]:
    """WRITE: send a Sparkplug B NCMD to an edge node.

    Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
    nothing.

    A command can change real equipment: the edge node applies the metrics it
    receives. It is sent at most once. success:true (meta.outcome "written") only
    means the NCMD was written to the network (QoS 0, as the specification
    requires); it does not mean the node applied it. Watch its NDATA
    (read_messages) to confirm. meta.outcome "unknown" means it may have gone out:
    check before retrying. Example: {"name": "Node Control/Rebirth",
    "value": true, "type": "boolean"} asks a node to republish its births.

    Parameters:
        metrics: at least one metric to write. {METRICS_HELP}
        edge_node_id: target edge node (required; there is no default target).
        group_id: target group (default: SPARKPLUG_GROUP_ID).
    """
    m = _manager(ctx)
    if (refused := _writes_disabled(m)) is not None:
        return refused
    try:
        data = await _publish_command(m, "NCMD", metrics, group_id, edge_node_id)
    except (ToolInputError, sparkplug.MetricError, PublishError) as exc:
        return _edge_failure("NCMD", exc)
    return _written(data)


@mcp.tool(annotations=_WRITE)
@_metrics_doc
async def publish_device_command(
    ctx: Context,
    edge_node_id: str,
    device_id: str,
    metrics: list[dict[str, Any]],
    group_id: str | None = None,
) -> dict[str, Any]:
    """WRITE: send a Sparkplug B DCMD to a device of an edge node.

    Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
    nothing.

    A command can change real equipment: the edge node applies the metrics to
    the device. It is sent at most once. success:true (meta.outcome "written")
    only means the DCMD was written to the network (QoS 0, as the specification
    requires); it does not mean the device applied it. Watch its DDATA
    (read_messages) to confirm. meta.outcome "unknown" means it may have gone out:
    check before retrying.

    Parameters:
        device_id: target device ID.
        metrics: at least one metric to write. {METRICS_HELP}
        edge_node_id: edge node the device belongs to (required; no default target).
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
    return _written(data, device_id=device_id)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(env_file: str | None = None) -> None:
    """Load the configuration (after the .env file) and run the MCP server on stdio."""
    try:
        settings = get_settings(env_file)
        build_client(settings)  # fail fast on unreadable TLS files
    except (ConfigError, OSError, ValueError) as exc:
        print(f"mqtt-mcp: configuration error: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
