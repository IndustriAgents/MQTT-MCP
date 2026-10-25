"""Budgets for caller-created active state; encoded sizes exclude heap overhead."""

import json

MAX_SUBSCRIPTIONS = 256
MAX_SUBSCRIPTION_BYTES = 256 * 1024
MAX_PUBLISHER_DEVICES = 256
MAX_PUBLISHER_DEVICE_BYTES = 64 * 1024


def subscription_bytes(topic: str) -> int:
    # QoS is always one digit; include identity and retained state, not only text.
    return len(json.dumps([topic, 2], ensure_ascii=False).encode("utf-8"))


def publisher_device_bytes(device_id: str) -> int:
    # Upper bound for the retained sequence (0..255), including identity.
    return len(json.dumps([device_id, {"born_at_seq": 255}], ensure_ascii=False).encode("utf-8"))
