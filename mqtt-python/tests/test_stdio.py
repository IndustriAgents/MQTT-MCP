"""The console script: --help/--version, config errors, and a clean stdout channel."""

from __future__ import annotations

import json
import subprocess

from .conftest import SERVER_EXE, free_port, server_env


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


def test_help_lists_the_configuration(tmp_path):
    result = run(["--help"], tmp_path)
    assert result.returncode == 0
    assert "waits silently for a client on stdin" in result.stdout
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


def test_stdout_carries_only_json_rpc(tmp_path):
    """Drive the server with raw JSON-RPC (broker unreachable, so errors get logged)."""
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
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "get_broker_info", "arguments": {}},
        },
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {"name": "subscribe_topic", "arguments": {"topic": "a/#"}},
        },
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {"name": "publish_message", "arguments": {"topic": "a", "payload": "x"}},
        },
    ]
    proc = subprocess.Popen(
        [str(SERVER_EXE)],
        cwd=tmp_path,
        env=server_env(
            MQTT_BROKER_URL=f"mqtt://127.0.0.1:{free_port()}",
            MQTT_TOOL_TIMEOUT="1",
            MQTT_USERNAME="agent",
        ),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    responses = {}
    lines = []
    try:
        for request in requests:
            proc.stdin.write(json.dumps(request) + "\n")
            proc.stdin.flush()
            if "id" not in request:
                continue
            while request["id"] not in responses:
                line = proc.stdout.readline()
                assert line, f"server closed stdout; stderr:\n{proc.stderr.read()}"
                lines.append(line)
                message = json.loads(line)  # every stdout line must be JSON
                assert message["jsonrpc"] == "2.0"
                if "id" in message:
                    responses[message["id"]] = message
        proc.stdin.close()
        assert proc.wait(15) == 0
        rest = proc.stdout.read()
    finally:
        if proc.poll() is None:
            proc.kill()
    for line in rest.splitlines():
        assert json.loads(line)["jsonrpc"] == "2.0"
    assert "result" in responses[1] and "result" in responses[2]
    info = responses[3]["result"]["structuredContent"]
    assert info["success"] is True and info["data"]["connected"] is False
    assert info["data"]["username"] == "agent"
    assert responses[4]["result"]["structuredContent"]["success"] is False
    gated = responses[5]["result"]["structuredContent"]
    assert gated["success"] is False and "MQTT_WRITES_ENABLED" in gated["error"]
    stderr = proc.stderr.read()
    assert "Connecting to" in stderr  # logs go to stderr
