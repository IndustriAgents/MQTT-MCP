"""Configuration of the MQTT MCP server.

Nothing here runs at import time: ``load_settings()`` reads a mapping (the
process environment by default) when it is called, which ``main()`` does after
``load_dotenv()`` has merged a ``.env`` file into the environment.
"""

from __future__ import annotations

import os
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit

from .sparkplug import validate_id

DEFAULT_BROKER_URL = "mqtt://127.0.0.1:1883"
DEFAULT_PORTS = {"mqtt": 1883, "mqtts": 8883}

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}

#: Environment variables the server reads, with a one-line description each
#: (used by ``mqtt-mcp --help``).
ENV_VARS = {
    "MQTT_BROKER_URL": f"mqtt://host[:port] or mqtts://host[:port] (default {DEFAULT_BROKER_URL};"
    " mqtts defaults to port 8883)",
    "MQTT_CLIENT_ID": "MQTT client ID (default: mqtt-mcp-<random hex>, unique per process)",
    "MQTT_USERNAME": "broker username (may be used without a password)",
    "MQTT_PASSWORD": "broker password (requires MQTT_USERNAME)",
    "MQTT_KEEPALIVE": "keep-alive interval in seconds (default 60)",
    "MQTT_TLS_CA_FILE": "CA bundle that signs the broker certificate (mqtts only;"
    " default: the system trust store)",
    "MQTT_TLS_CERT_FILE": "client certificate for mutual TLS (mqtts only)",
    "MQTT_TLS_KEY_FILE": "private key of the client certificate (mqtts only)",
    "MQTT_TLS_KEY_PASSWORD": "password of an encrypted MQTT_TLS_KEY_FILE",
    "MQTT_TLS_INSECURE": "true disables certificate and hostname verification"
    " (testing only; default false)",
    "MQTT_WRITES_ENABLED": "true enables every tool that publishes (default false: read-only)",
    "MQTT_TOOL_TIMEOUT": "seconds a tool waits for the connection and broker acknowledgements"
    " (default 5)",
    "MQTT_MESSAGE_BUFFER_SIZE": "received messages kept for read_messages (default 1000)",
    "SPARKPLUG_GROUP_ID": "Sparkplug B group ID of this server's edge node (default factory)",
    "SPARKPLUG_EDGE_NODE_ID": "Sparkplug B edge node ID of this server (default edge-node-1)",
}


class ConfigError(ValueError):
    """The environment holds an invalid setting."""


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    tls: bool
    client_id: str
    username: str | None
    password: str | None
    keepalive: int
    tls_ca_file: str | None
    tls_cert_file: str | None
    tls_key_file: str | None
    tls_key_password: str | None
    tls_insecure: bool
    writes_enabled: bool
    tool_timeout: float
    message_buffer_size: int
    group_id: str
    edge_node_id: str

    @property
    def broker_url(self) -> str:
        """The broker URL without credentials, for display."""
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"{'mqtts' if self.tls else 'mqtt'}://{host}:{self.port}"


def _get(env: Mapping[str, str], name: str) -> str | None:
    """Return a variable's value, treating unset and empty (or blank) alike."""
    value = env.get(name)
    if value is None or not value.strip():
        return None
    return value.strip()


def parse_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    value = _get(env, name)
    if value is None:
        return default
    lowered = value.lower()
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False
    raise ConfigError(f"{name}={value!r} is not a boolean; use true or false")


def _parse_number(env, name, default, kind, minimum, maximum=None):
    value = _get(env, name)
    if value is None:
        return default
    try:
        number = kind(value)
    except ValueError:
        raise ConfigError(f"{name}={value!r} is not a valid {kind.__name__}") from None
    if number < minimum or (maximum is not None and number > maximum):
        upper = f" and at most {maximum}" if maximum is not None else ""
        raise ConfigError(f"{name}={value!r} must be at least {minimum}{upper}")
    return number


def parse_broker_url(url: str) -> tuple[str, int, bool]:
    """Parse ``mqtt://host[:port]`` / ``mqtts://host[:port]`` into (host, port, tls).

    A bare ``host[:port]`` is taken as ``mqtt://``. Credentials in the URL are
    refused (use MQTT_USERNAME / MQTT_PASSWORD), as are other schemes.
    """
    raw = url.strip()
    if "://" not in raw:
        raw = f"mqtt://{raw}"
    try:
        parts = urlsplit(raw)
        port = parts.port
    except ValueError as exc:
        raise ConfigError(f"MQTT_BROKER_URL={url!r} is not a valid URL: {exc}") from None
    scheme = parts.scheme.lower()
    if scheme not in DEFAULT_PORTS:
        raise ConfigError(
            f"MQTT_BROKER_URL={url!r}: unsupported scheme {parts.scheme!r};"
            " use mqtt:// (plain TCP) or mqtts:// (TLS)"
        )
    if parts.username is not None or parts.password is not None:
        raise ConfigError(
            "MQTT_BROKER_URL must not contain credentials; set MQTT_USERNAME and MQTT_PASSWORD"
        )
    if not parts.hostname:
        raise ConfigError(f"MQTT_BROKER_URL={url!r} has no host")
    if parts.path not in ("", "/") or parts.query or parts.fragment:
        raise ConfigError(f"MQTT_BROKER_URL={url!r} must not have a path, query or fragment")
    if port is None:
        port = DEFAULT_PORTS[scheme]
    if not 1 <= port <= 65535:
        raise ConfigError(f"MQTT_BROKER_URL={url!r}: port must be between 1 and 65535")
    return parts.hostname, port, scheme == "mqtts"


def _existing_file(env: Mapping[str, str], name: str) -> str | None:
    path = _get(env, name)
    if path is not None and not os.path.isfile(path):
        raise ConfigError(f"{name}={path!r} does not exist or is not a file")
    return path


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Build Settings from ``env`` (default: ``os.environ``) or raise ConfigError."""
    if env is None:
        env = os.environ

    host, port, tls = parse_broker_url(_get(env, "MQTT_BROKER_URL") or DEFAULT_BROKER_URL)

    username = _get(env, "MQTT_USERNAME")
    password = env.get("MQTT_PASSWORD") or None  # a password may contain spaces
    if password is not None and username is None:
        raise ConfigError("MQTT_PASSWORD is set but MQTT_USERNAME is not (MQTT requires both)")

    tls_ca_file = _existing_file(env, "MQTT_TLS_CA_FILE")
    tls_cert_file = _existing_file(env, "MQTT_TLS_CERT_FILE")
    tls_key_file = _existing_file(env, "MQTT_TLS_KEY_FILE")
    tls_key_password = env.get("MQTT_TLS_KEY_PASSWORD") or None
    tls_insecure = parse_bool(env, "MQTT_TLS_INSECURE", False)
    tls_options = [
        name
        for name, value in (
            ("MQTT_TLS_CA_FILE", tls_ca_file),
            ("MQTT_TLS_CERT_FILE", tls_cert_file),
            ("MQTT_TLS_KEY_FILE", tls_key_file),
            ("MQTT_TLS_KEY_PASSWORD", tls_key_password),
            ("MQTT_TLS_INSECURE", tls_insecure or None),
        )
        if value
    ]
    if tls_options and not tls:
        raise ConfigError(
            f"{', '.join(tls_options)} set, but MQTT_BROKER_URL is not mqtts://;"
            " TLS is only used with an mqtts:// URL"
        )
    if tls_key_file and not tls_cert_file:
        raise ConfigError("MQTT_TLS_KEY_FILE is set but MQTT_TLS_CERT_FILE is not")
    if tls_key_password and not tls_key_file:
        raise ConfigError("MQTT_TLS_KEY_PASSWORD is set but MQTT_TLS_KEY_FILE is not")

    client_id = _get(env, "MQTT_CLIENT_ID") or f"mqtt-mcp-{secrets.token_hex(4)}"
    if len(client_id.encode("utf-8")) > 65535:
        raise ConfigError("MQTT_CLIENT_ID is too long")

    try:
        group_id = validate_id(_get(env, "SPARKPLUG_GROUP_ID") or "factory", "SPARKPLUG_GROUP_ID")
        edge_node_id = validate_id(
            _get(env, "SPARKPLUG_EDGE_NODE_ID") or "edge-node-1", "SPARKPLUG_EDGE_NODE_ID"
        )
    except ValueError as exc:
        raise ConfigError(str(exc)) from None

    return Settings(
        host=host,
        port=port,
        tls=tls,
        client_id=client_id,
        username=username,
        password=password,
        keepalive=_parse_number(env, "MQTT_KEEPALIVE", 60, int, 0, 65535),
        tls_ca_file=tls_ca_file,
        tls_cert_file=tls_cert_file,
        tls_key_file=tls_key_file,
        tls_key_password=tls_key_password,
        tls_insecure=tls_insecure,
        writes_enabled=parse_bool(env, "MQTT_WRITES_ENABLED", False),
        tool_timeout=_parse_number(env, "MQTT_TOOL_TIMEOUT", 5.0, float, 0.1, 300),
        message_buffer_size=_parse_number(env, "MQTT_MESSAGE_BUFFER_SIZE", 1000, int, 1, 1_000_000),
        group_id=group_id,
        edge_node_id=edge_node_id,
    )
