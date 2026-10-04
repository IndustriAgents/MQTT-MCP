"""The console script: --help/--version, configuration errors, .env handling, and a
stdout that carries nothing but JSON-RPC."""

from __future__ import annotations

import json
import subprocess
import sys

from .conftest import SERVER_EXE, free_port, server_env
from .test_sparkplug import README_SAMPLE_HEX

METRIC = [{"name": "m", "value": -1, "type": "int32"}]

#: One call for every tool the server lists (checked against tools/list).
EVERY_TOOL = {
    "get_broker_info": {},
    "subscribe_topic": {"topic": "a/#"},
    "unsubscribe_topic": {"topic": "a/#"},
    "list_subscriptions": {},
    "read_messages": {},
    "list_sparkplug_nodes": {},
    "decode_sparkplug_payload": {"payload_hex": README_SAMPLE_HEX},
    "publish_message": {"topic": "a/b", "payload": "x", "qos": 1},
    "publish_node_birth": {"metrics": METRIC},
    "publish_node_death": {},
    "publish_device_birth": {"device_id": "d", "metrics": METRIC},
    "publish_device_death": {"device_id": "d"},
    "publish_node_data": {"metrics": METRIC},
    "publish_device_data": {"device_id": "d", "metrics": METRIC},
    "publish_node_command": {"edge_node_id": "n", "metrics": METRIC},
    "publish_device_command": {"edge_node_id": "n", "device_id": "d", "metrics": METRIC},
}

STRAY_PRINT = (
    "import sys\n"
    "from mqtt_mcp import cli, mqtt_server\n"
    "original = mqtt_server._ok\n"
    "def noisy(*args, **kwargs):\n"
    "    print('stray debug output')\n"
    "    return original(*args, **kwargs)\n"
    "mqtt_server._ok = noisy\n"
    "sys.argv = ['mqtt-mcp']\n"
    "cli.main()\n"
)


def run(args, tmp_path, timeout=30, **env):
    return subprocess.run(
        [str(SERVER_EXE), *args],
        cwd=tmp_path,
        env=server_env(**env),
        capture_output=True,
        text=True,
        timeout=timeout,
        stdin=subprocess.DEVNULL,
    )


def drive(command, cwd, env, calls):
    """Speak raw JSON-RPC to the server on stdio.

    Returns (responses by id, tool names from tools/list, stdout lines that are
    not JSON-RPC, stderr).
    """
    proc = subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    requests = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "raw-test", "version": "0"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
    ]
    for number, (name, arguments) in enumerate(calls.items(), start=3):
        requests.append(
            {
                "jsonrpc": "2.0",
                "id": number,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            }
        )
    responses, stray = {}, []

    def take(line):
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            stray.append(line)
            return
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
            stray.append(line)
        elif "id" in message:
            responses[message["id"]] = message

    try:
        for request in requests:
            proc.stdin.write(json.dumps(request) + "\n")
            proc.stdin.flush()
            if "id" not in request:
                continue
            while request["id"] not in responses:
                line = proc.stdout.readline()
                assert line, f"server closed stdout; stderr:\n{proc.stderr.read()}"
                take(line)
        proc.stdin.close()
        assert proc.wait(30) == 0
        for line in proc.stdout.read().splitlines(keepends=True):
            take(line)
        stderr = proc.stderr.read()
    finally:
        if proc.poll() is None:
            proc.kill()
    listed = {tool["name"] for tool in responses[2]["result"]["tools"]}
    return responses, listed, stray, stderr


def offline_env(**overrides):
    return server_env(
        MQTT_BROKER_URL=f"mqtt://127.0.0.1:{free_port()}",
        MQTT_TOOL_TIMEOUT="0.5",
        MQTT_WRITES_ENABLED="true",
        MQTT_USERNAME="agent",
        **overrides,
    )


def test_help_lists_the_configuration(tmp_path):
    result = run(["--help"], tmp_path)
    assert result.returncode == 0
    assert "waits silently for a client on stdin" in result.stdout
    assert "--env-file" in result.stdout
    for name in ("MQTT_BROKER_URL", "MQTT_WRITES_ENABLED", "MQTT_TLS_CA_FILE", "MQTT_CLIENT_ID"):
        assert name in result.stdout


def test_version(tmp_path):
    result = run(["--version"], tmp_path)
    assert result.returncode == 0
    assert result.stdout.startswith("mqtt-mcp ")


def test_unknown_option_fails(tmp_path):
    result = run(["--bogus"], tmp_path)
    assert result.returncode == 2
    assert result.stdout == ""


def test_configuration_error_exits_without_touching_stdout(tmp_path):
    result = run([], tmp_path, MQTT_BROKER_URL="ws://broker:80")
    assert result.returncode == 2
    assert result.stdout == ""
    assert "configuration error" in result.stderr
    assert "unsupported scheme" in result.stderr


def test_missing_env_file_is_a_configuration_error(tmp_path):
    result = run(["--env-file", str(tmp_path / "nope.env")], tmp_path)
    assert result.returncode == 2
    assert "does not exist" in result.stderr


def test_stdout_carries_only_json_rpc(tmp_path):
    """Every tool, broker unreachable (so errors get logged), PYTHONUNBUFFERED=1."""
    env = offline_env()
    assert env["PYTHONUNBUFFERED"] == "1"
    responses, listed, stray, stderr = drive([str(SERVER_EXE)], tmp_path, env, EVERY_TOOL)
    assert listed == set(EVERY_TOOL), "add new tools to EVERY_TOOL"
    assert stray == []
    for number, name in enumerate(EVERY_TOOL, start=3):
        envelope = responses[number]["result"]["structuredContent"]
        assert set(envelope) == {"success", "data", "error", "meta"}, name
    info = responses[3]["result"]["structuredContent"]
    assert info["data"]["connected"] is False and info["data"]["username"] == "agent"
    assert "Connecting to" in stderr  # logs go to stderr


def test_stdout_check_catches_a_stray_print(tmp_path):
    """The check above must fail if anything prints to stdout."""
    _, _, stray, _ = drive(
        [sys.executable, "-c", STRAY_PRINT], tmp_path, offline_env(), {"get_broker_info": {}}
    )
    assert any("stray debug output" in line for line in stray)


def test_dotenv_in_the_working_directory_is_not_read(tmp_path):
    """The working directory is the MCP client's project: its .env is never loaded."""
    port = free_port()
    (tmp_path / ".env").write_text(
        f"MQTT_BROKER_URL=mqtt://127.0.0.1:{port}\nMQTT_WRITES_ENABLED=true\n"
    )
    env = server_env(MQTT_TOOL_TIMEOUT="0.5")
    del env["MQTT_WRITES_ENABLED"], env["MQTT_BROKER_URL"]  # only the .env would set them
    calls = {"get_broker_info": {}, "publish_message": {"topic": "a/b", "payload": "x"}}
    responses, _, stray, stderr = drive([str(SERVER_EXE)], tmp_path, env, calls)
    assert stray == []
    info = responses[3]["result"]["structuredContent"]["data"]
    assert info["broker"] != f"mqtt://127.0.0.1:{port}"
    assert info["writes_enabled"] is False
    published = responses[4]["result"]["structuredContent"]
    assert published["success"] is False and "Writes are disabled" in published["error"]
    assert str(tmp_path) not in stderr


def test_explicit_env_file_can_enable_writes(tmp_path):
    env_file = tmp_path / "plant.env"
    env_file.write_text("MQTT_WRITES_ENABLED=true\n")
    env = server_env(MQTT_TOOL_TIMEOUT="0.5", MQTT_BROKER_URL=f"mqtt://127.0.0.1:{free_port()}")
    del env["MQTT_WRITES_ENABLED"]
    responses, _, _, stderr = drive(
        [str(SERVER_EXE), "--env-file", str(env_file)], tmp_path, env, {"get_broker_info": {}}
    )
    assert responses[3]["result"]["structuredContent"]["data"]["writes_enabled"] is True
    assert "Ignoring" not in stderr
