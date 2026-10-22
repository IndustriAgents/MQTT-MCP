import logging
from typing import Any

from .. import sparkplug

logger = logging.getLogger("mqtt_mcp")


def _ok(data: Any = None, **meta: Any) -> dict[str, Any]:
    return {"success": True, "data": data, "error": None, "meta": meta}


def _fail(error: str, **meta: Any) -> dict[str, Any]:
    return {"success": False, "data": None, "error": error, "meta": meta}


class ToolInputError(ValueError):
    """An argument is invalid, or the call does not fit the current state.

    Reported as success:false with ``meta.code`` (one of the contract's ``errors``).
    """

    def __init__(self, message: str, code: str = "invalid_arguments"):
        super().__init__(message)
        self.code = code


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
    seen, duplicates = (set(), set())
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
    if required and (not metrics):
        raise ToolInputError("at least one metric is required")
    return metrics


WRITES_DISABLED = (
    "Writes are disabled: this server is read-only "
    "unless MQTT_WRITES_ENABLED=true is set in its "
    "environment. Nothing was published."
)
METRICS_HELP = (
    'A list of {"name": str, "value": ..., "type": '
    "str} objects. type is one of int8, int16, int32 "
    "(or int), int64 (or long), uint8, uint16, "
    "uint32, uint64, float, double, boolean (or "
    "bool), string, text, uuid, datetime "
    "(milliseconds since the Unix epoch) or bytes "
    "(value as a hex string). The value must match "
    "the type and fit its range; negative values are "
    "fine for the signed int types; null sends a null"
    " metric. An unknown type or a mismatching value "
    "is rejected with success:false, nothing is sent."
)
OUTCOMES = ("written", "rejected", "unknown", "not_sent")
UNKNOWN_HINT = "It may have been applied; check before retrying."


class PublishError(RuntimeError):
    """A request did not complete; ``outcome`` says whether it may have gone out."""

    def __init__(
        self,
        message: str,
        outcome: str = "not_sent",
        request_sent: bool = False,
        code: str | None = None,
    ):
        super().__init__(message)
        self.outcome = outcome
        self.request_sent = request_sent
        self.code = code or ("outcome_unknown" if outcome == "unknown" else "send_failed")


def error_code(exc: Exception) -> str:
    """The contract error code for a failure (``invalid_arguments`` unless it says)."""
    return getattr(exc, "code", "invalid_arguments")


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
