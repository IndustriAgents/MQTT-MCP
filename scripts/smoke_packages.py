import json
import os
import queue
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

repo = Path(__file__).resolve().parents[1]
package = json.loads((repo / "npm/package.json").read_text())
stem = (
    package["name"]
    .removeprefix("industriagents-")
    .removesuffix("-mcp")
)
mode = sys.argv[1]
wheel_name = package["name"].replace("-", "_") + "-" + package["version"] + "-py3-none-any.whl"
wheel = repo / "npm/vendor" / wheel_name
temporary = tempfile.TemporaryDirectory(prefix="protocol-package-smoke-")
artifact_root = Path(temporary.name)
shutil.copy2(wheel, artifact_root / wheel.name)
wheel = artifact_root / wheel.name
if mode == "npm":
    tarball = repo / "npm" / (package["name"] + "-" + package["version"] + ".tgz")
    subprocess.run(
        [
            "npm",
            "install",
            "--prefix",
            str(artifact_root),
            "--ignore-scripts",
            "--no-audit",
            "--no-fund",
            str(tarball),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    launcher = (
        artifact_root
        / "node_modules"
        / ("industriagents-" + stem + "-mcp")
        / "bin/cli.mjs"
    )
cmd = (
    ["uv", "tool", "run", "--isolated", "--from", str(wheel), stem + "-mcp"]
    if mode == "python"
    else ["node", "--", str(launcher)]
)
# Never inherit deployment endpoints, maps, credentials or policy into artifact checks.
protocol_prefixes = ("BACNET_", "DNP3_", "MQTT_", "SPARKPLUG_", "MODBUS_", "S7_", "S7COMM_",
                     "ENIP_", "ETHERNETIP_", "ETHERCAT_", "PROFIBUS_", "PROFINET_")
env = {key: value for key, value in os.environ.items()
       if not key.upper().startswith(protocol_prefixes) and not key.upper().endswith("_MAP_FILE")}
with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as reservation:
    reservation.bind(("127.0.0.1", 0))
    bacnet_port = reservation.getsockname()[1]
env.update(
    {
        "BACNET_INTERFACE": "127.0.0.1",
        "BACNET_PORT": str(bacnet_port),
        "DNP3_HOST": "127.0.0.1",
        "DNP3_PORT": "1",
        "ETHERCAT_BACKEND": "mock",
        "PROFINET_BACKEND": "none",
        "PROFIBUS_BACKEND": "mock",
        "MQTT_BROKER_URL": "mqtt://127.0.0.1:1",
        "MODBUS_HOST": "127.0.0.1",
        "MODBUS_PORT": "1",
        "S7_HOST": "127.0.0.1",
        "S7_PORT": "1",
        "ENIP_HOST": "127.0.0.1",
    }
)
for k in list(env):
    if any(
        w in k
        for w in [
            "WRITES_ENABLED",
            "SYSTEM_CMDS_ENABLED",
            "CONFIG_CMDS_ENABLED",
            "STATE_CHANGE_ENABLED",
            "POLICY_FILE",
        ]
    ):
        env.pop(k, None)
proc = subprocess.Popen(
    cmd,
    cwd="/tmp",
    env=env,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    bufsize=1,
)
q = queue.Queue()
errors = []


def stdout():
    for line in proc.stdout:
        try:
            q.put(json.loads(line))
        except ValueError:
            q.put({"invalid_stdout": line})
    q.put({"eof": True})


threading.Thread(target=stdout, daemon=True).start()
threading.Thread(target=lambda: errors.extend(proc.stderr), daemon=True).start()


def send(value):
    proc.stdin.write(json.dumps(value) + "\n")
    proc.stdin.flush()


def response(id):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        msg = q.get(timeout=max(0.01, deadline - time.monotonic()))
        if "eof" in msg or "invalid_stdout" in msg:
            raise AssertionError((msg, "".join(errors)[-1500:]))
        if msg.get("id") == id:
            return msg
    raise TimeoutError()


try:
    send(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "artifact-smoke", "version": "1"},
            },
        }
    )
    init = response(1)
    assert init["result"]["serverInfo"]["version"] == package["version"], init
    send({"jsonrpc": "2.0", "method": "notifications/initialized"})
    send({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    tools = response(2)["result"]["tools"]
    contract = json.loads((repo / "contract/tools.json").read_text())
    assert [t["name"] for t in tools] == [t["name"] for t in contract["tools"]]
    for actual, expected in zip(tools, contract["tools"]):
        for field in ("description", "inputSchema", "outputSchema"):
            assert actual.get(field) == expected.get(field), (actual["name"], field)
        for field, value in expected.get("annotations", {}).items():
            assert actual.get("annotations", {}).get(field) == value, (actual["name"], field)
    route = next((name, r) for name, r in contract["routes"].items() if "selector" in r)
    send(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": route[0],
                "arguments": {route[1]["selector"]: "invalid-mode"},
            },
        }
    )
    result = response(3)["result"]
    assert result["isError"] is True and result["structuredContent"]["success"] is False
    assert json.loads(result["content"][0]["text"]) == result["structuredContent"]
    send({"jsonrpc":"2.0", "id":4, "method":"tools/call",
          "params":{"name":"get_broker_info", "arguments":{}}})
    status = response(4)["result"]
    observed = status["structuredContent"]
    assert observed["success"] and not status.get("isError", False), status
    assert json.loads(status["content"][0]["text"]) == observed
    capabilities = observed["data"]["capabilities"]
    assert capabilities["scope"] == "backend_support" and capabilities["device_verified"] is False
    assert set(capabilities["operations"]) == {tool["name"] for tool in contract["tools"]}
    assert capabilities["simulated"] is False
    print(repo.name, mode, "OK", len(tools), "tools and capabilities", flush=True)
finally:
    proc.stdin.close()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.terminate()
        proc.wait(timeout=10)
    if proc.returncode not in (0, -15):
        raise AssertionError((proc.returncode, "".join(errors)[-1200:]))

    temporary.cleanup()
