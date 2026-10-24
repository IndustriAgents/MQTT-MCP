import asyncio
import socket
import ssl
import threading
import time
from dataclasses import dataclass
from typing import Any

import paho.mqtt.client as mqtt

from .. import sparkplug
from ..config import Settings
from ..domain.model import UNKNOWN_HINT, PublishError, logger
from ..inbox import MessageBuffer, SparkplugRegistry
from ..infrastructure.control_context import SendState, _end_as_cancelled, _send_state


@dataclass
class _InFlight:
    """A publish handed to paho and not yet confirmed or given up."""

    info: mqtt.MQTTMessageInfo
    lost: bool = False
    withdrawn: bool | None = None


class _Client(mqtt.Client):
    """paho client that remembers why the last connection attempt failed."""

    last_connect_error: str | None = None
    tcp_connected_at: float | None = None

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
        self._lock = threading.RLock()
        self.connected = False
        self.generation = 0
        self.last_error: str | None = None
        self.last_refusal: str | None = None
        self._expect_disconnect = False
        self.closing = False
        self._inflight: dict[int, _InFlight] = {}
        self._restart_task: asyncio.Task | None = None
        self.subscriptions: dict[str, int] = {}
        self._pending: dict[int, tuple[asyncio.Future | None, str, str]] = {}
        self.messages = MessageBuffer(settings.message_buffer_size)
        self.registry = SparkplugRegistry()
        self.sparkplug_lock = asyncio.Lock()
        self.will_active = False
        self._will_bd_seq: int | None = None
        self.bd_seq: int | None = None
        self._next_bd_seq = 0
        self.node_born = False
        self.seq = 0
        self.node_lost: str | None = None
        self.device_births: dict[str, dict[str, Any]] = {}

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
        if task is not None and (not task.done()):
            await asyncio.wait({task}, timeout=2 * self.settings.tool_timeout + 1)
            if not task.done():
                logger.warning("The session restart did not finish before shutdown")
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
                "the TCP connection is open, but no CONNACK has "
                "arrived for "
                f"{waited:.0f}"
                "s: the peer does not answer as an MQTT broker"
            )
        return None

    async def require_connection(self) -> None:
        if not await self.wait_for(lambda: self.connected, self.time_left()):
            reason = self.current_error()
            detail = f" (last error: {reason})" if reason else ""
            raise PublishError(
                "Not connected to the MQTT broker at "
                f"{self.settings.broker_url}"
                f"{detail}"
                ". Nothing was sent; the server keeps retrying "
                "the connection in the background."
            )

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
            if self.last_error is None and (not client.last_connect_error):
                self.last_error = (
                    "the connection closed before the broker accepted"
                    " it ("
                    f"{reason_code}"
                    "): the peer does not answer as an MQTT broker"
                )
            return
        self.last_error = f"connection lost: {reason_code}"
        logger.warning("Connection to the broker lost (%s); reconnecting", reason_code)
        self.registry.mark_unknown()
        if self._will_bd_seq is not None:
            client.will_clear()
            self._will_bd_seq = None
            if self.node_born:
                self.node_lost = (
                    "connection lost at "
                    f"{time.strftime('%Y-%m-%dT%H:%M:%S%z')}"
                    "; the broker published the NDEATH will (bdSeq "
                    f"{self.bd_seq}"
                    ")"
                )
        self.node_born = False
        self.device_births.clear()

    def _on_message(self, client, userdata, msg) -> None:
        received_at = time.time()
        decoded, error = (None, None)
        try:
            decoded, error = self.registry.observe(msg.topic, msg.payload, received_at)
        except Exception as exc:
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

        if self._loop is not None and (not self._loop.is_closed()):
            self._loop.call_soon_threadsafe(setter)

    async def _await_ack(self, call, kind: str, topic: str):
        """Run a paho subscribe/unsubscribe call and wait for the broker's ack."""
        assert self._loop is not None
        future = self._loop.create_future()
        state = _send_state.get()
        cancelled = False
        with self._lock:
            if self.closing:
                raise PublishError("the server is shutting down; nothing was sent")
            if state is not None and state.cancel_requested():
                cancelled = True
            else:
                if state is not None and state.deadline_passed():
                    raise PublishError("the call's deadline has passed; nothing was sent")
                result, mid = call()
                if result != mqtt.MQTT_ERR_SUCCESS or mid is None:
                    raise PublishError(f"the {kind} was not sent: {mqtt.error_string(result)}")
                self._pending[mid] = (future, kind, topic)
        if cancelled:
            await _end_as_cancelled(f"The {kind} was cancelled; nothing was sent")
        try:
            reason_codes = await asyncio.wait_for(future, self.time_left())
        except asyncio.TimeoutError:
            raise PublishError(
                f"the broker did not acknowledge the {kind} before the call's deadline",
                outcome="unknown",
                request_sent=True,
            ) from None
        finally:
            # Cancellation is just as final as a timeout for this waiter. Do not
            # retain cancelled futures indefinitely when the broker never acks.
            with self._lock:
                pending = self._pending.get(mid)
                if pending is not None and pending[0] is future:
                    self._pending.pop(mid)
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
        cancelled = False
        with self._lock:
            if self.closing and (not shutdown):
                raise PublishError(
                    "the server is shutting down (the MCP client is gone); nothing was sent"
                )
            if state is not None and state.cancel_requested():
                cancelled = True
            else:
                if state is not None and state.deadline_passed():
                    raise PublishError("the call's deadline has passed; nothing was sent")
                generation = self.generation
                info = self.client.publish(topic, payload, qos=qos, retain=retain)
                self._inflight[info.mid] = _InFlight(info)
        if cancelled:
            await _end_as_cancelled(
                f"Publish to {topic!r}: the MCP client cancelled the call; nothing was sent"
            )
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            if self._finish(info.mid) is not False:
                raise PublishError(f"not sent: {mqtt.error_string(info.rc)}")
            if state is not None:
                state.sent = True
            raise PublishError(
                (
                    "not sent now ("
                    f"{mqtt.error_string(info.rc)}"
                    "), and it could not be removed from the client's"
                    " queue, so it may go out after a reconnect. "
                    f"{UNKNOWN_HINT}"
                ),
                outcome="unknown",
                request_sent=True,
            )
        if state is not None:
            state.sent = True
        try:
            problem = await self._await_confirmation(info, timeout)
        except BaseException:
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
                ("delivery"): (
                    "written to the network; QoS 0 is not "
                    "acknowledged, so whether it reached the broker "
                    "or had any effect on a device is unknown"
                )
                if qos == 0
                else (
                    "acknowledged by the broker ("
                    f"{('PUBACK' if qos == 1 else 'PUBCOMP')}"
                    "); whether it had any effect on a device is not "
                    "known"
                ),
            }
        withdrawn = self._finish(info.mid)
        if withdrawn is True and (problem == "connection lost" or self.generation == generation):
            if problem == "connection lost":
                raise PublishError(
                    "the connection was lost before it was written; nothing was sent"
                )
            raise PublishError(
                "it was not on the network by the call's deadline"
                " ("
                f"{problem}"
                "); it was withdrawn and nothing was sent"
            )
        what = (
            "was not confirmed as written"
            if qos == 0
            else f"was not acknowledged by the broker (QoS {qos})"
        )
        raise PublishError(
            (
                "the message "
                f"{what}"
                " ("
                f"{problem}"
                ") and may have reached the broker. This server "
                "does not send it again. "
                f"{UNKNOWN_HINT}"
            ),
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
                return flight.withdrawn
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
            for packet in list(client._out_packet):
                if packet.get("mid") != mid or packet.get("command", 0) & 240 != 48:
                    continue
                if packet.get("pos", 0) == 0:
                    try:
                        client._out_packet.remove(packet)
                    except ValueError:
                        partial = True
                        continue
                    if packet.get("pos", 0) != 0:
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
        except (AttributeError, RuntimeError):
            return False
        if partial:
            return False
        if message is None:
            return True if removed_unsent else None
        if message.dup:
            return False
        if message.state in (mqtt.mqtt_ms_publish, mqtt.mqtt_ms_queued):
            return True
        return removed_unsent

    def _ndeath_message(self) -> tuple[str, bytes]:
        payload = sparkplug.build_payload(
            [], seq=None, extra_metrics=[sparkplug.bd_seq_metric(self.bd_seq or 0)]
        )
        topic = sparkplug.topic(self.settings.group_id, "NDEATH", self.settings.edge_node_id)
        return (topic, payload.SerializeToString())

    async def restart_session(self, bd_seq: int | None) -> None:
        """Reconnect with an NDEATH will carrying ``bd_seq``, or without a will (None).

        Runs as its own task, shielded from the calling tool's cancellation, so the
        client is never left half-restarted; ``stop()`` waits for it. It does not
        reconnect once ``closing`` is set.
        """
        task = asyncio.ensure_future(self._restart(bd_seq, _send_state.get()))
        self._restart_task = task
        task.add_done_callback(lambda t: t.cancelled() or t.exception())
        await asyncio.wait({task})
        if task.cancelled():
            raise PublishError("the session restart was cancelled")
        if task.exception() is not None:
            raise task.exception()

    async def _restart(self, bd_seq: int | None, state: SendState | None = None) -> None:
        assert self.client is not None
        if self.closing:
            raise PublishError("the server is shutting down; the session was not restarted")
        topic = sparkplug.topic(self.settings.group_id, "NDEATH", self.settings.edge_node_id)
        payload = None
        if bd_seq is not None:
            payload = sparkplug.build_payload(
                [], seq=None, extra_metrics=[sparkplug.bd_seq_metric(bd_seq)]
            ).SerializeToString()
        await asyncio.to_thread(self._stop_network, True, max(self.time_left(), 1.0))
        with self._lock:
            if self.closing:
                raise PublishError("the server is shutting down; the session was not restarted")
            cancelled = None
            if state is not None and payload is not None:
                if state.cancel_requested():
                    cancelled = "the MCP client cancelled the call"
                elif state.deadline_passed():
                    cancelled = "the call's deadline has passed"
            if cancelled is not None:
                payload = None
            if payload is None:
                self.client.will_clear()
                self._will_bd_seq = None
            else:
                self.client.will_set(topic, payload, qos=1, retain=False)
                self._will_bd_seq = bd_seq
                self._next_bd_seq = (bd_seq + 1) % 256
            self.last_refusal = None
            self._expect_disconnect = False
            self.client.connect_async(
                self.settings.host, self.settings.port, keepalive=self.settings.keepalive
            )
            self.client.loop_start()
        if cancelled is not None:
            raise PublishError(f"{cancelled}; no NDEATH will was registered")
        wanted = (
            (lambda: self.connected and self.will_active) if payload else lambda: self.connected
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
