from __future__ import annotations

from typing import Any, Protocol


class MQTTSessionPort(Protocol):
    _expect_disconnect: Any
    _inflight: Any
    _lock: Any
    _loop: Any
    _next_bd_seq: Any
    _pending: Any
    _restart_task: Any
    _will_bd_seq: Any
    bd_seq: Any
    client: Any
    closing: Any
    connected: Any
    device_births: Any
    generation: Any
    last_error: Any
    last_refusal: Any
    messages: Any
    node_born: Any
    node_lost: Any
    registry: Any
    seq: Any
    settings: Any
    sparkplug_lock: Any
    subscriptions: Any
    will_active: Any

    async def start(self) -> Any: ...

    def close_for_disconnect(self) -> Any: ...

    async def stop(self) -> Any: ...

    def _stop_network(self, graceful: Any, timeout: Any) -> Any: ...

    def _shutdown_socket(self) -> Any: ...

    def time_left(self) -> Any: ...

    async def wait_for(self, condition, timeout: Any) -> Any: ...

    def current_error(self) -> Any: ...

    async def require_connection(self) -> Any: ...

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> Any: ...

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> Any: ...

    def _connection_lost(self, client, reason_code, was_connected: Any) -> Any: ...

    def _on_message(self, client, userdata, msg) -> Any: ...

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties) -> Any: ...

    def _on_unsubscribe(self, client, userdata, mid, reason_codes, properties) -> Any: ...

    def _resolve(self, future: Any, value: Any) -> Any: ...

    async def _await_ack(self, call, kind: Any, topic: Any) -> Any: ...

    async def subscribe(self, topic: Any, qos: Any) -> Any: ...

    async def unsubscribe(self, topic: Any) -> Any: ...

    async def publish(
        self,
        topic: Any,
        payload: Any,
        qos: Any = ...,
        retain: Any = ...,
        timeout: Any = ...,
        *,
        shutdown: Any = ...,
    ) -> Any: ...

    async def _await_confirmation(self, info: Any, timeout: Any) -> Any: ...

    def _finish(self, mid: Any) -> Any: ...

    def _withdraw(self, mid: Any) -> Any: ...

    def _ndeath_message(self) -> Any: ...

    async def restart_session(self, bd_seq: Any) -> Any: ...

    async def _restart(self, bd_seq: Any, state: Any = ...) -> Any: ...

    def sparkplug_state(self) -> Any: ...
