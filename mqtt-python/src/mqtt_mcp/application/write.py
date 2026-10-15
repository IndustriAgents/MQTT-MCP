from typing import Any

from pydantic import StrictBool, StrictInt

from .. import sparkplug
from ..domain.model import (
    UNKNOWN_HINT,
    WRITES_DISABLED,
    PublishError,
    ToolInputError,
    _check_id,
    _check_metrics,
    _check_qos,
    _check_topic_name,
    _check_unique_names,
    _fail,
    _ok,
)
from ..domain.ports import MQTTSessionPort
from ..infrastructure.control_context import _send_state


def _writes_disabled(m: Any) -> dict[str, Any] | None:
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
        outcome, request_sent = (exc.outcome, exc.request_sent)
    else:
        outcome, request_sent = ("not_sent", False)
    return _fail(f"{what}: {exc}", outcome=outcome, request_sent=request_sent, **meta)


async def publish_message(
    ctx: MQTTSessionPort, topic: str, payload: str, qos: StrictInt = 0, retain: StrictBool = False
) -> dict[str, Any]:
    """WRITE: publish a UTF-8 text message to an MQTT topic."""
    m = ctx
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
    m: Any,
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
    if not m.will_active or (message_type != "NBIRTH" and (not m.node_born)):
        raise PublishError(
            "the edge node is not born on this connection (no"
            " NDEATH will is registered); publish_node_birth "
            "first"
        )
    seq = 0 if seq_mode == "zero" else (m.seq + 1) % 256
    payload = sparkplug.build_payload(metrics, seq=seq, extra_metrics=extra or [])
    topic = sparkplug.topic(m.settings.group_id, message_type, m.settings.edge_node_id, device_id)
    try:
        result = await m.publish(topic, payload.SerializeToString(), qos=0)
    except PublishError as exc:
        if exc.outcome == "unknown":
            m.seq = seq
        raise
    except BaseException:
        state = _send_state.get()
        if state is not None and state.sent and (state.outcome != "not_sent"):
            m.seq = seq
        raise
    m.seq = seq
    if m.generation != result["generation"] or not m.will_active:
        raise PublishError(
            (
                f"{message_type}"
                " was written to the network, but the connection "
                "dropped right after it, so the broker may not "
                "have received it (QoS 0), and it has published "
                "the NDEATH will: the node is dead; "
                "publish_node_birth again. "
                f"{UNKNOWN_HINT}"
            ),
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


async def publish_node_birth(
    ctx: MQTTSessionPort, metrics: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    """WRITE: make this server a Sparkplug B edge node and publish its NBIRTH."""
    m = ctx
    if (refused := _writes_disabled(m)) is not None:
        return refused
    try:
        metrics = _check_metrics(metrics, required=False)
        sparkplug.build_payload(metrics, seq=0)
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
                will_ok = m.connected and m.will_active and (m.bd_seq is not None)
            if not will_ok:
                raise PublishError(
                    "no NDEATH will is registered with the broker for"
                    " this connection ("
                    f"{m.last_refusal or m.current_error() or 'connection changed'}"
                    "); NBIRTH not sent"
                )
            extra = [sparkplug.bd_seq_metric(m.bd_seq), sparkplug.rebirth_metric()]
            data = await _publish_edge(m, "NBIRTH", metrics, extra=extra, seq_mode="zero")
            m.node_born = True
            m.node_lost = None
            m.device_births.clear()
    except (ToolInputError, sparkplug.MetricError, PublishError) as exc:
        return _edge_failure("NBIRTH", exc)
    return _written(data, reconnected_with_will=reconnected)


async def publish_node_death(ctx: MQTTSessionPort) -> dict[str, Any]:
    """WRITE: publish this server's NDEATH (edge node goes offline)."""
    m = ctx
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


async def publish_device_birth(
    ctx: MQTTSessionPort, device_id: str, metrics: list[dict[str, Any]]
) -> dict[str, Any]:
    """WRITE: publish a DBIRTH for a device of this server's edge node."""
    m = ctx
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


async def publish_device_death(ctx: MQTTSessionPort, device_id: str) -> dict[str, Any]:
    """WRITE: publish a DDEATH for a device born with publish_device_birth."""
    m = ctx
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


async def publish_node_data(ctx: MQTTSessionPort, metrics: list[dict[str, Any]]) -> dict[str, Any]:
    """WRITE: publish NDATA (new values of this server's node metrics)."""
    m = ctx
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


async def publish_device_data(
    ctx: MQTTSessionPort, device_id: str, metrics: list[dict[str, Any]]
) -> dict[str, Any]:
    """WRITE: publish DDATA (new values of a device born by this server)."""
    m = ctx
    if (refused := _writes_disabled(m)) is not None:
        return refused
    try:
        device_id = _check_id(device_id, "device_id")
        metrics = _check_metrics(metrics, required=True)
        sparkplug.build_payload(metrics, seq=0)
        async with m.sparkplug_lock:
            if not m.node_born or device_id not in m.device_births:
                raise ToolInputError(
                    "publish_device_birth first: device "
                    f"{device_id!r}"
                    " has no DBIRTH from this server"
                )
            data = await _publish_edge(m, "DDATA", metrics, device_id)
    except (ToolInputError, sparkplug.MetricError, PublishError) as exc:
        return _edge_failure("DDATA", exc, device_id=device_id)
    return _written(data, device_id=device_id)


async def _publish_command(
    m: Any,
    message_type: str,
    metrics: Any,
    group_id: str | None,
    edge_node_id: str,
    device_id: str | None = None,
) -> dict[str, Any]:
    target_group = _check_id(m.settings.group_id if group_id is None else group_id, "group_id")
    target_node = _check_id(edge_node_id, "edge_node_id")
    metrics = _check_metrics(metrics, required=True)
    payload = sparkplug.build_payload(metrics, seq=None)
    topic = sparkplug.topic(target_group, message_type, target_node, device_id)
    result = await m.publish(topic, payload.SerializeToString(), qos=0)
    return {"topic": topic, "metrics": len(payload.metrics), **result}


async def publish_node_command(
    ctx: MQTTSessionPort,
    edge_node_id: str,
    metrics: list[dict[str, Any]],
    group_id: str | None = None,
) -> dict[str, Any]:
    """WRITE: send a Sparkplug B NCMD to an edge node."""
    m = ctx
    if (refused := _writes_disabled(m)) is not None:
        return refused
    try:
        data = await _publish_command(m, "NCMD", metrics, group_id, edge_node_id)
    except (ToolInputError, sparkplug.MetricError, PublishError) as exc:
        return _edge_failure("NCMD", exc)
    return _written(data)


async def publish_device_command(
    ctx: MQTTSessionPort,
    edge_node_id: str,
    device_id: str,
    metrics: list[dict[str, Any]],
    group_id: str | None = None,
) -> dict[str, Any]:
    """WRITE: send a Sparkplug B DCMD to a device of an edge node."""
    m = ctx
    if (refused := _writes_disabled(m)) is not None:
        return refused
    try:
        device_id = _check_id(device_id, "device_id")
        data = await _publish_command(m, "DCMD", metrics, group_id, edge_node_id, device_id)
    except (ToolInputError, sparkplug.MetricError, PublishError) as exc:
        return _edge_failure("DCMD", exc, device_id=device_id)
    return _written(data, device_id=device_id)
