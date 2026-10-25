from typing import Any

from pydantic import StrictInt

from ..domain.model import (
    PublishError,
    ToolInputError,
    _check_qos,
    _check_topic_filter,
    _fail,
    _ok,
    error_code,
)
from ..domain.ports import MQTTSessionPort


async def subscribe_topic(ctx: MQTTSessionPort, topic: str, qos: StrictInt = 0) -> dict[str, Any]:
    """Subscribe this server to an MQTT topic filter so that matching messages are kept."""
    m = ctx
    try:
        topic = _check_topic_filter(topic)
        qos = _check_qos(qos)
        if (
            topic.startswith("$")
            and (not topic.startswith("$SYS/"))
            and (not m.settings.writes_enabled)
        ):
            raise ToolInputError(
                "filters starting with '$' (other than $SYS/) are"
                " broker extensions such as shared ($share/, "
                "$queue/) or exclusive ($exclusive/) "
                "subscriptions that change what other clients "
                "receive; they are only allowed with "
                "MQTT_WRITES_ENABLED=true",
                code="writes_disabled",
            )
        granted = await m.subscribe(topic, qos)
    except (ToolInputError, PublishError) as exc:
        return _fail(
            f"Could not subscribe to {topic!r}: {exc}",
            code=error_code(exc),
            topic=topic,
            outcome=getattr(exc, "outcome", "not_sent"),
            request_sent=getattr(exc, "request_sent", False),
        )
    return _ok({"topic": topic, "granted_qos": granted}, requested_qos=qos)


async def unsubscribe_topic(ctx: MQTTSessionPort, topic: str) -> dict[str, Any]:
    """Remove one of this server's subscriptions (exactly as listed by list_subscriptions)."""
    m = ctx
    try:
        topic = _check_topic_filter(topic)
        if topic not in m.subscriptions and topic not in m._uncertain_subscriptions:
            raise ToolInputError(
                "this server holds no subscription to "
                f"{topic!r}"
                " (current: "
                f"{sorted(m.subscriptions) or 'none'}"
                ")",
                code="not_subscribed",
            )
        await m.unsubscribe(topic)
    except (ToolInputError, PublishError) as exc:
        return _fail(
            f"Could not unsubscribe from {topic!r}: {exc}",
            code=error_code(exc),
            topic=topic,
            outcome=getattr(exc, "outcome", "not_sent"),
            request_sent=getattr(exc, "request_sent", False),
        )
    return _ok({"topic": topic, "unsubscribed": True})


def read_messages(
    ctx: MQTTSessionPort,
    topic_filter: str | None = None,
    limit: StrictInt = 50,
    since_id: StrictInt | None = None,
) -> dict[str, Any]:
    """Return messages this server has received on its subscriptions."""
    m = ctx
    try:
        if topic_filter is not None:
            topic_filter = _check_topic_filter(topic_filter, "topic_filter")
        if isinstance(limit, bool) or not isinstance(limit, int) or (not 1 <= limit <= 1000):
            raise ToolInputError(f"limit must be an integer from 1 to 1000, got {limit!r}")
        if since_id is not None and (
            isinstance(since_id, bool) or not isinstance(since_id, int) or since_id < 0
        ):
            raise ToolInputError(f"since_id must be a non-negative integer, got {since_id!r}")
    except ToolInputError as exc:
        return _fail(str(exc), code=exc.code)
    messages, meta = m.messages.query(topic_filter, limit, since_id)
    if not m.subscriptions and (not messages):
        meta["note"] = "This server holds no subscriptions; call subscribe_topic first."
    return _ok(messages, **meta)
