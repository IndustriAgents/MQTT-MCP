"""Subscription cancellation must release pending state and fence new sends."""

import asyncio
import time

import anyio
import paho.mqtt.client as mqtt
import pytest

from mqtt_mcp.adapters.mqtt_client import MQTTClientManager
from mqtt_mcp.config import load_settings
from mqtt_mcp.domain.model import PublishError
from mqtt_mcp.infrastructure.control_context import SendState, _send_state

pytestmark = pytest.mark.anyio


def manager():
    value = MQTTClientManager(
        load_settings({"MQTT_BROKER_URL": "mqtt://127.0.0.1:1", "MQTT_TOOL_TIMEOUT": "0.1"})
    )
    value.connected = True
    value._loop = asyncio.get_running_loop()
    return value


@pytest.mark.parametrize("kind", ["subscribe", "unsubscribe"])
async def test_cancelled_ack_wait_releases_pending_future(kind):
    value = manager()
    if kind == "unsubscribe":
        value.subscriptions["test/#"] = 0
    sent = asyncio.Event()

    def native_call():
        sent.set()
        return mqtt.MQTT_ERR_SUCCESS, 1

    task = asyncio.create_task(value._await_ack(native_call, kind, "test/#"))
    await sent.wait()
    assert len(value._pending) == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert value._pending == {}
    assert value.subscription_state()["uncertain_topics"] == ["test/#"]


@pytest.mark.parametrize("kind", ["subscribe", "unsubscribe"])
@pytest.mark.parametrize("reason", ["closing", "deadline", "cancelled"])
async def test_stopped_call_does_not_send_subscription_packet(kind, reason):
    value = manager()
    calls = []

    def native_call():
        calls.append(kind)
        return mqtt.MQTT_ERR_SUCCESS, 1

    state = SendState(deadline=time.monotonic() + 10)
    token = _send_state.set(state)
    try:
        if reason == "cancelled":
            with anyio.CancelScope() as scope:
                state.request_scope = scope
                scope.cancel()
                await value._await_ack(native_call, kind, "test/#")
        else:
            value.closing = reason == "closing"
            if reason == "deadline":
                state.deadline = time.monotonic() - 1
            with pytest.raises(PublishError) as caught:
                await value._await_ack(native_call, kind, "test/#")
            assert caught.value.outcome == "not_sent"
    finally:
        _send_state.reset(token)
    assert calls == []
    assert value._pending == {}
