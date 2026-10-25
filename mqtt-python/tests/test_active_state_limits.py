"""Caller-created MQTT state stays bounded across admission, callbacks and loss."""

import asyncio
from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest

from mqtt_mcp.adapters.mqtt_client import MQTTClientManager
from mqtt_mcp.application import read, write
from mqtt_mcp.config import load_settings
from mqtt_mcp.domain import limits
from mqtt_mcp.domain.model import PublishError
from mqtt_mcp.infrastructure.control_context import SendState, _send_state

pytestmark = pytest.mark.anyio


def manager():
    result = MQTTClientManager(
        load_settings(
            {
                "MQTT_BROKER_URL": "mqtt://127.0.0.1:1",
                "MQTT_WRITES_ENABLED": "true",
                "MQTT_TOOL_TIMEOUT": "0.1",
            }
        )
    )
    result.connected = result.node_born = result.will_active = True
    result._loop = asyncio.get_running_loop()
    result.client = SimpleNamespace(last_connect_error=None, subscribe=lambda *args: (0, 100))
    return result


def ack(value, mid, *, refused=False):
    value._on_subscribe(
        value.client,
        None,
        mid,
        [SimpleNamespace(is_failure=refused, value=128 if refused else 1)],
        None,
    )


def sent_result(value):
    return {
        "topic": "spBv1.0/g/DBIRTH/n/d",
        "seq": 1,
        "generation": value.generation,
        "bd_seq": 0,
        "metrics": 0,
        "mid": 1,
        "delivery": "written",
    }


async def test_pending_subscription_reserves_capacity_before_native_send(monkeypatch):
    monkeypatch.setattr(limits, "MAX_SUBSCRIPTIONS", 1)
    value = manager()
    sent = asyncio.Event()
    calls = []

    def send():
        calls.append("first")
        sent.set()
        return 0, 1

    first = asyncio.create_task(value._await_ack(send, "subscribe", "first/#"))
    await sent.wait()
    with pytest.raises(PublishError, match="capacity") as error:
        await value._await_ack(lambda: calls.append("second"), "subscribe", "second/#")
    assert error.value.outcome == "not_sent" and calls == ["first"]
    ack(value, 1)
    await first
    assert value.subscription_state()["count"] == 1
    assert value._reserved_subscriptions == set()


async def test_subscription_encoded_budget_accounts_unicode_before_send(monkeypatch):
    value = manager()
    topic = "水/#"
    monkeypatch.setattr(limits, "MAX_SUBSCRIPTION_BYTES", limits.subscription_bytes(topic) - 1)
    with pytest.raises(PublishError, match="capacity"):
        await value._await_ack(lambda: pytest.fail("must not send"), "subscribe", topic)
    assert value.subscription_state()["count"] == 0


async def test_subscription_unknown_holds_budget_late_ack_fenced_and_explicit_unsubscribe_clears(
    monkeypatch,
):
    monkeypatch.setattr(limits, "MAX_SUBSCRIPTIONS", 1)
    value = manager()
    with pytest.raises(PublishError) as error:
        await value._await_ack(lambda: (0, 1), "subscribe", "x/#")
    assert error.value.outcome == "unknown"
    assert value._pending == {} and value.subscription_state()["uncertain_topics"] == ["x/#"]
    ack(value, 1)  # Expired callback cannot promote unknown state to confirmed.
    assert value.subscriptions == {}
    with pytest.raises(PublishError, match="uncertain"):
        await value._await_ack(lambda: pytest.fail("must not replay"), "subscribe", "x/#")
    with pytest.raises(PublishError, match="capacity"):
        await value._await_ack(lambda: pytest.fail("must not exceed budget"), "subscribe", "y/#")

    def unsubscribe(topic):
        value._loop.call_soon(value._on_unsubscribe, value.client, None, 2, [], None)
        return 0, 2

    value.client.unsubscribe = unsubscribe
    result = await read.unsubscribe_topic(value, "x/#")
    assert result["success"] and value.subscription_state()["count"] == 0


@pytest.mark.parametrize("failure", ["native", "refused"])
async def test_subscription_known_failures_release_reservations(failure):
    value = manager()
    if failure == "native":
        with pytest.raises(PublishError):
            await value._await_ack(lambda: (mqtt.MQTT_ERR_NO_CONN, None), "subscribe", "x/#")
    else:
        task = asyncio.create_task(value._await_ack(lambda: (0, 1), "subscribe", "x/#"))
        await asyncio.sleep(0)
        ack(value, 1, refused=True)
        await task
    assert value.subscription_state()["count"] == 0 and value._pending == {}


async def test_reconnect_restores_only_confirmed_topics_without_double_charging(monkeypatch):
    monkeypatch.setattr(limits, "MAX_SUBSCRIPTIONS", 2)
    value = manager()
    value.subscriptions["known/#"] = 1
    value._uncertain_subscriptions.add("unknown/#")
    value._reserved_subscriptions.add("unknown/#")
    sent = []
    value.client.subscribe = lambda topic, qos: (sent.append(topic) or 0, 1)
    value._expect_disconnect = True
    value._on_disconnect(value.client, None, None, 0, None)
    value._on_connect(value.client, None, None, SimpleNamespace(is_failure=False), None)
    assert sent == ["known/#"]
    state = value.subscription_state()
    assert state["count"] == state["pending"] == 1
    assert state["lost_subscriptions"] == 1
    with pytest.raises(PublishError, match="pending"):
        await value._await_ack(lambda: pytest.fail("duplicate restore"), "subscribe", "known/#")
    ack(value, 1)
    assert value.subscription_state()["pending"] == 0


async def test_failed_restore_is_reported_and_releases_capacity():
    value = manager()
    value.subscriptions["known/#"] = 1
    value.client.subscribe = lambda *args: (mqtt.MQTT_ERR_NO_CONN, None)
    value._on_connect(value.client, None, None, SimpleNamespace(is_failure=False), None)
    assert value.subscription_state()["count"] == 0
    assert value.subscription_state()["lost_subscriptions"] == 1


async def test_concurrent_device_births_cannot_overbook_count(monkeypatch):
    monkeypatch.setattr(limits, "MAX_PUBLISHER_DEVICES", 1)
    value = manager()
    entered, release = asyncio.Event(), asyncio.Event()
    sent = []

    async def publish(m, kind, metrics, device_id):
        sent.append(device_id)
        entered.set()
        await release.wait()
        return sent_result(m)

    monkeypatch.setattr(write, "_publish_edge", publish)
    first = asyncio.create_task(write.publish_device_birth(value, "first", []))
    await entered.wait()
    assert value.publisher_device_state()["pending"] == 1
    second = asyncio.create_task(write.publish_device_birth(value, "second", []))
    release.set()
    assert (await first)["success"]
    refused = await second
    assert refused["meta"]["code"] == "resource_limit" and refused["meta"]["outcome"] == "not_sent"
    assert sent == ["first"] and value.publisher_device_state()["pending"] == 0


async def test_device_birth_byte_capacity_refuses_before_publish(monkeypatch):
    value = manager()
    monkeypatch.setattr(
        limits, "MAX_PUBLISHER_DEVICE_BYTES", limits.publisher_device_bytes("水") - 1
    )

    async def publish(*args, **kwargs):
        pytest.fail("must not send")

    monkeypatch.setattr(write, "_publish_edge", publish)
    result = await write.publish_device_birth(value, "水", [])
    assert result["meta"]["code"] == "resource_limit"
    assert value.publisher_device_state()["count"] == 0


@pytest.mark.parametrize("sent", [False, True])
async def test_cancelled_device_birth_releases_waiter_and_accounts_uncertainty(monkeypatch, sent):
    value = manager()
    entered = asyncio.Event()

    async def publish(*args):
        _send_state.get().sent = sent
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(write, "_publish_edge", publish)
    token = _send_state.set(SendState())
    try:
        task = asyncio.create_task(write.publish_device_birth(value, "d", []))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        _send_state.reset(token)
    state = value.publisher_device_state()
    assert state["pending"] == 0 and value.device_births == {}
    assert state["uncertain_devices"] == (["d"] if sent else [])
    assert state["count"] == int(sent)


async def test_unknown_device_birth_does_not_replay_and_death_releases_capacity(monkeypatch):
    monkeypatch.setattr(limits, "MAX_PUBLISHER_DEVICES", 1)
    value = manager()
    calls = []

    async def publish(m, kind, metrics, device_id):
        calls.append(kind)
        if kind == "DBIRTH":
            raise PublishError("ack lost", outcome="unknown", request_sent=True)
        return sent_result(m)

    monkeypatch.setattr(write, "_publish_edge", publish)
    failed = await write.publish_device_birth(value, "d", [])
    assert failed["meta"]["outcome"] == "unknown"
    refused = await write.publish_device_birth(value, "d", [])
    assert refused["meta"]["outcome"] == "not_sent" and calls == ["DBIRTH"]
    assert (await write.publish_device_birth(value, "other", []))["meta"][
        "code"
    ] == "resource_limit"
    assert calls == ["DBIRTH"]
    assert (await write.publish_device_death(value, "d"))["success"]
    assert value.publisher_device_state()["count"] == 0


async def test_disconnect_during_device_birth_cannot_commit_old_state(monkeypatch):
    value = manager()

    async def publish(m, *args):
        result = sent_result(m)
        m.generation += 1
        m.clear_device_births()
        return result

    monkeypatch.setattr(write, "_publish_edge", publish)
    failed = await write.publish_device_birth(value, "d", [])
    assert failed["meta"]["outcome"] == "unknown"
    assert value.publisher_device_state()["count"] == 0


async def test_unexpected_native_subscription_failure_keeps_uncertainty():
    value = manager()

    def fail():
        raise RuntimeError("native call failed after possible queueing")

    with pytest.raises(RuntimeError):
        await value._await_ack(fail, "subscribe", "x/#")
    assert value._pending == {}
    assert value.subscription_state()["uncertain_topics"] == ["x/#"]


async def test_failed_device_birth_before_send_releases_capacity(monkeypatch):
    value = manager()

    async def fail(*args):
        raise PublishError("not sent")

    monkeypatch.setattr(write, "_publish_edge", fail)
    result = await write.publish_device_birth(value, "d", [])
    assert result["meta"]["outcome"] == "not_sent"
    assert value.publisher_device_state()["count"] == 0


async def test_rejected_subscription_replacement_does_not_free_possible_old_state():
    value = manager()
    value.subscriptions["old/#"] = 1

    def subscribe(topic, qos):
        value._loop.call_soon(lambda: ack(value, 1, refused=True))
        return 0, 1

    value.client.subscribe = subscribe
    result = await read.subscribe_topic(value, "old/#", 2)
    assert result["meta"]["outcome"] == "rejected" and result["meta"]["request_sent"] is True
    assert value.subscription_state()["count"] == 1
    assert value.subscription_state()["uncertain_topics"] == ["old/#"]
