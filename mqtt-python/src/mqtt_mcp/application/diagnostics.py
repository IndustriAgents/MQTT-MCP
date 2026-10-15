from typing import Any

from .. import sparkplug
from ..domain.model import _fail, _ok
from ..domain.ports import MQTTSessionPort


def get_broker_info(ctx: MQTTSessionPort) -> dict[str, Any]:
    """Report the broker connection and this server's configuration and state."""
    m = ctx
    s = m.settings
    with m._lock:
        subscriptions = [{"topic": t, "qos": q} for t, q in sorted(m.subscriptions.items())]
        last_error = m.current_error()
    return _ok(
        {
            "broker": s.broker_url,
            "tls": s.tls,
            "tls_verification": not s.tls_insecure if s.tls else None,
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


def list_subscriptions(ctx: MQTTSessionPort) -> dict[str, Any]:
    """List the subscriptions this server holds, with the QoS the broker granted."""
    m = ctx
    with m._lock:
        subs = [{"topic": t, "qos": q} for t, q in sorted(m.subscriptions.items())]
    return _ok(subs, count=len(subs), connected=m.connected)


def list_sparkplug_nodes(ctx: MQTTSessionPort) -> dict[str, Any]:
    """List Sparkplug B edge nodes and devices seen in messages this server received."""
    m = ctx
    nodes = m.registry.snapshot()
    meta: dict[str, Any] = {"count": len(nodes)}
    if not any(t.startswith(("spBv1.0/", "#", "+/")) for t in m.subscriptions):
        meta["note"] = (
            "No subscription covers Sparkplug topics; subscribe to 'spBv1.0/#' to discover nodes."
        )
    return _ok({"nodes": nodes, "this_server": m.sparkplug_state()}, **meta)


def decode_sparkplug_payload(payload_hex: str) -> dict[str, Any]:
    """Decode a hex-encoded Sparkplug B protobuf payload (as read_messages shows it)."""
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
    except Exception as exc:
        return _fail(f"Not a valid Sparkplug B payload: {exc}", size=len(data))
    return _ok(decoded, size=len(data))
