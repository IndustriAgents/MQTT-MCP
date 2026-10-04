"""Configuration: read after load_dotenv, never at import; TLS, credentials, client ID."""

from __future__ import annotations

import os
import ssl

import pytest

from mqtt_mcp import mqtt_server
from mqtt_mcp.config import ConfigError, load_settings, parse_broker_url
from mqtt_mcp.mqtt_server import build_client


@pytest.fixture(autouse=True)
def fresh_settings():
    """Forget cached settings and undo whatever load_dotenv put into os.environ."""
    saved = dict(os.environ)
    mqtt_server.set_settings(None)
    yield
    mqtt_server.set_settings(None)
    os.environ.clear()
    os.environ.update(saved)


def test_defaults():
    s = load_settings({})
    assert (s.host, s.port, s.tls) == ("127.0.0.1", 1883, False)
    assert s.writes_enabled is False
    assert s.username is None and s.password is None
    assert (s.group_id, s.edge_node_id) == ("factory", "edge-node-1")
    assert s.keepalive == 60 and s.tool_timeout == 5.0 and s.message_buffer_size == 1000
    assert s.broker_url == "mqtt://127.0.0.1:1883"


def test_default_client_id_is_unique_per_process():
    a, b = load_settings({}).client_id, load_settings({}).client_id
    assert a.startswith("mqtt-mcp-") and b.startswith("mqtt-mcp-") and a != b


def test_client_id_is_used():
    s = load_settings({"MQTT_CLIENT_ID": "line-3-agent"})
    assert s.client_id == "line-3-agent"
    assert build_client(s)._client_id == b"line-3-agent"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("mqtt://broker.local:1884", ("broker.local", 1884, False)),
        ("mqtt://broker.local", ("broker.local", 1883, False)),
        ("mqtts://broker.local", ("broker.local", 8883, True)),
        ("MQTTS://broker.local:9999", ("broker.local", 9999, True)),
        ("10.0.0.5:1885", ("10.0.0.5", 1885, False)),
        ("mqtt://[::1]:1883", ("::1", 1883, False)),
    ],
)
def test_parse_broker_url(url, expected):
    assert parse_broker_url(url) == expected


@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("ws://broker:80", "unsupported scheme"),
        ("mqtt://user:secret@broker", "must not contain credentials"),
        ("mqtt://broker:notaport", "not a valid URL"),
        ("mqtt://broker:0", "between 1 and 65535"),
        ("mqtt://broker/path", "path"),
        ("mqtt://", "no host"),
    ],
)
def test_invalid_broker_urls(url, message):
    with pytest.raises(ConfigError, match=message):
        load_settings({"MQTT_BROKER_URL": url})


def test_username_without_password_is_used():
    s = load_settings({"MQTT_USERNAME": "agent"})
    client = build_client(s)
    assert client._username == b"agent"
    assert client._password is None


def test_username_and_password():
    client = build_client(load_settings({"MQTT_USERNAME": "agent", "MQTT_PASSWORD": "pw"}))
    assert (client._username, client._password) == (b"agent", b"pw")


def test_password_without_username_is_an_error():
    with pytest.raises(ConfigError, match="MQTT_USERNAME"):
        load_settings({"MQTT_PASSWORD": "pw"})


def test_mqtts_sets_up_verified_tls():
    client = build_client(load_settings({"MQTT_BROKER_URL": "mqtts://broker.example"}))
    context = client._ssl_context
    assert context is not None
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True


def test_mqtt_has_no_tls():
    assert build_client(load_settings({}))._ssl_context is None


def test_tls_insecure_disables_verification():
    s = load_settings({"MQTT_BROKER_URL": "mqtts://b", "MQTT_TLS_INSECURE": "true"})
    context = build_client(s)._ssl_context
    assert context.verify_mode == ssl.CERT_NONE
    assert context.check_hostname is False


def test_tls_options_require_mqtts(tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("x")
    with pytest.raises(ConfigError, match="not mqtts"):
        load_settings({"MQTT_TLS_CA_FILE": str(ca)})
    with pytest.raises(ConfigError, match="not mqtts"):
        load_settings({"MQTT_TLS_INSECURE": "true"})


def test_tls_files_must_exist(tmp_path):
    with pytest.raises(ConfigError, match="does not exist"):
        load_settings(
            {"MQTT_BROKER_URL": "mqtts://b", "MQTT_TLS_CA_FILE": str(tmp_path / "missing.pem")}
        )


def test_tls_key_needs_cert(tmp_path):
    key = tmp_path / "key.pem"
    key.write_text("x")
    with pytest.raises(ConfigError, match="MQTT_TLS_CERT_FILE"):
        load_settings({"MQTT_BROKER_URL": "mqtts://b", "MQTT_TLS_KEY_FILE": str(key)})


@pytest.mark.parametrize(("raw", "expected"), [("true", True), ("ON", True), ("0", False)])
def test_writes_enabled_parsing(raw, expected):
    assert load_settings({"MQTT_WRITES_ENABLED": raw}).writes_enabled is expected


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("MQTT_WRITES_ENABLED", "yes please"),
        ("MQTT_KEEPALIVE", "abc"),
        ("MQTT_KEEPALIVE", "-1"),
        ("MQTT_TOOL_TIMEOUT", "0"),
        ("MQTT_MESSAGE_BUFFER_SIZE", "0"),
        ("SPARKPLUG_GROUP_ID", "plant/1"),
        ("SPARKPLUG_EDGE_NODE_ID", "node#1"),
    ],
)
def test_invalid_values(name, value):
    with pytest.raises(ConfigError, match=name):
        load_settings({name: value})


def test_empty_values_count_as_unset():
    s = load_settings({"MQTT_BROKER_URL": "", "MQTT_USERNAME": " ", "MQTT_WRITES_ENABLED": ""})
    assert s.broker_url == "mqtt://127.0.0.1:1883"
    assert s.username is None and s.writes_enabled is False


def test_configuration_is_read_when_the_server_starts_not_at_import(monkeypatch):
    # mqtt_server is already imported; changing the environment now must count.
    monkeypatch.setenv("MQTT_BROKER_URL", "mqtt://late-host:1999")
    monkeypatch.setenv("MQTT_WRITES_ENABLED", "true")
    s = mqtt_server.get_settings()
    assert (s.host, s.port, s.writes_enabled) == ("late-host", 1999, True)
    assert not hasattr(mqtt_server, "MQTT_HOST")


def test_dotenv_is_loaded_before_reading_settings(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text(
        "MQTT_BROKER_URL=mqtt://dotenv-host:1777\nSPARKPLUG_GROUP_ID=from-dotenv\n"
    )
    monkeypatch.chdir(tmp_path)
    for name in ("MQTT_BROKER_URL", "SPARKPLUG_GROUP_ID"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SPARKPLUG_EDGE_NODE_ID", "from-env")
    s = mqtt_server.get_settings()
    assert (s.host, s.port) == ("dotenv-host", 1777)
    assert s.group_id == "from-dotenv"
    assert s.edge_node_id == "from-env"


def test_real_environment_wins_over_dotenv(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text("MQTT_BROKER_URL=mqtt://dotenv-host:1777\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MQTT_BROKER_URL", "mqtt://env-host:1666")
    assert mqtt_server.get_settings().host == "env-host"


def test_implicit_dotenv_cannot_open_the_write_gate(tmp_path, monkeypatch, caplog):
    project = tmp_path / "client-project" / "sub"
    project.mkdir(parents=True)
    # Found automatically, from a parent of the working directory.
    (tmp_path / "client-project" / ".env").write_text(
        "MQTT_WRITES_ENABLED=true\nMQTT_BROKER_URL=mqtt://dotenv-host:1777\n"
    )
    monkeypatch.chdir(project)
    monkeypatch.delenv("MQTT_WRITES_ENABLED", raising=False)
    monkeypatch.delenv("MQTT_BROKER_URL", raising=False)
    with caplog.at_level("WARNING", logger="mqtt_mcp"):
        s = mqtt_server.get_settings()
    assert s.writes_enabled is False
    assert s.host == "dotenv-host"  # connection settings still come from the file
    assert "MQTT_WRITES_ENABLED" not in os.environ
    assert any("Ignoring MQTT_WRITES_ENABLED" in r.getMessage() for r in caplog.records)


def test_explicit_env_file_may_open_the_write_gate(tmp_path, monkeypatch):
    env_file = tmp_path / "deliberate.env"
    env_file.write_text("MQTT_WRITES_ENABLED=true\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MQTT_WRITES_ENABLED", raising=False)
    assert mqtt_server.get_settings(str(env_file)).writes_enabled is True


def test_real_environment_may_open_the_write_gate(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text("MQTT_WRITES_ENABLED=false\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MQTT_WRITES_ENABLED", "true")
    assert mqtt_server.get_settings().writes_enabled is True


def test_merge_env_file_without_touching_os_environ(tmp_path, monkeypatch):
    from mqtt_mcp.config import merge_env_file

    (tmp_path / ".env").write_text("MQTT_WRITES_ENABLED=1\nMQTT_KEEPALIVE=30\n")
    monkeypatch.chdir(tmp_path)
    environ: dict[str, str] = {}
    warnings = merge_env_file(None, environ)
    assert environ == {"MQTT_KEEPALIVE": "30"}
    assert len(warnings) == 1 and "MQTT_WRITES_ENABLED" in warnings[0]
    environ = {}
    assert merge_env_file(str(tmp_path / ".env"), environ) == []
    assert environ == {"MQTT_WRITES_ENABLED": "1", "MQTT_KEEPALIVE": "30"}
    with pytest.raises(ConfigError, match="does not exist"):
        merge_env_file(str(tmp_path / "missing.env"), {})
