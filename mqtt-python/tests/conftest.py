"""Shared fixtures and helpers.

Unit tests need nothing. Integration tests (``-m integration``) need an MQTT
broker: set ``MQTT_TEST_BROKER_URL`` (e.g. ``mqtt://127.0.0.1:1883``) to use a
running one, or put ``mosquitto`` on PATH and the tests start their own on a
free port. Without either they are skipped, unless ``MQTT_TEST_REQUIRE_BROKER=1``
(set in CI) turns that into a failure.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt
import pytest

from mqtt_mcp.config import parse_broker_url

PROJECT = Path(__file__).resolve().parents[1]
REPO = PROJECT.parent
MOCK_DIR = REPO / "mqtt-mock-server"
MOCK_SCRIPT = MOCK_DIR / "mqtt_mock_server.py"
BIN = Path(sys.executable).parent
SERVER_EXE = BIN / "mqtt-mcp"

#: Every variable the server reads; tests set all of them so that a stray .env
#: file can never change what is under test (load_dotenv does not override).
SERVER_VARS = (
    "MQTT_BROKER_URL",
    "MQTT_CLIENT_ID",
    "MQTT_USERNAME",
    "MQTT_PASSWORD",
    "MQTT_KEEPALIVE",
    "MQTT_TLS_CA_FILE",
    "MQTT_TLS_CERT_FILE",
    "MQTT_TLS_KEY_FILE",
    "MQTT_TLS_KEY_PASSWORD",
    "MQTT_TLS_INSECURE",
    "MQTT_WRITES_ENABLED",
    "MQTT_TOOL_TIMEOUT",
    "MQTT_MESSAGE_BUFFER_SIZE",
    "SPARKPLUG_GROUP_ID",
    "SPARKPLUG_EDGE_NODE_ID",
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_port(host: str, port: int, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"nothing listens on {host}:{port}")


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Broker
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Broker:
    host: str
    port: int

    @property
    def url(self) -> str:
        return f"mqtt://{self.host}:{self.port}"


@pytest.fixture(scope="session")
def broker():
    url = os.environ.get("MQTT_TEST_BROKER_URL")
    if url:
        host, port, tls = parse_broker_url(url)
        assert not tls, "MQTT_TEST_BROKER_URL must be a plain mqtt:// broker"
        wait_for_port(host, port)
        yield Broker(host, port)
        return
    exe = shutil.which("mosquitto")
    if exe is None:
        message = "no MQTT broker: set MQTT_TEST_BROKER_URL or install mosquitto"
        if os.environ.get("MQTT_TEST_REQUIRE_BROKER") == "1":
            pytest.fail(message)
        pytest.skip(message)
    port = free_port()
    with tempfile.TemporaryDirectory() as tmp:
        conf = Path(tmp) / "mosquitto.conf"
        conf.write_text(f"listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\n")
        proc = subprocess.Popen(
            [exe, "-c", str(conf)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        try:
            wait_for_port("127.0.0.1", port)
            yield Broker("127.0.0.1", port)
        finally:
            proc.terminate()
            try:
                proc.wait(10)
            except subprocess.TimeoutExpired:
                proc.kill()


# ---------------------------------------------------------------------------
# TCP/TLS proxy in front of the broker: lets tests start "the broker" late,
# cut connections without a DISCONNECT, terminate TLS, and see CONNECT packets.
# ---------------------------------------------------------------------------


class Proxy:
    def __init__(
        self,
        target: Broker,
        port: int | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self.target = target
        self.port = port or free_port()
        self.ssl_context = ssl_context
        self.first_packets: list[bytes] = []
        #: While true, bytes from the broker to the client are dropped (no acks arrive).
        self.hold_downstream = False
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._server = None
        self._writers: set[asyncio.StreamWriter] = set()

    def _run(self, coro, timeout: float = 10.0):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    def start(self) -> Proxy:
        async def start():
            self._server = await asyncio.start_server(
                self._handle, "127.0.0.1", self.port, ssl=self.ssl_context
            )

        self._run(start())
        return self

    def drop_connections(self) -> None:
        """Close every open connection abruptly (no MQTT DISCONNECT)."""

        async def drop():
            for writer in list(self._writers):
                writer.transport.abort()
            self._writers.clear()

        self._run(drop())

    def stop(self) -> None:
        async def stop():
            if self._server is not None:
                self._server.close()
                await self._server.wait_closed()
                self._server = None

        self.drop_connections()
        self._run(stop())

    def close(self) -> None:
        self.stop()
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(5)

    async def _handle(self, reader, writer):
        try:
            up_reader, up_writer = await asyncio.open_connection(self.target.host, self.target.port)
        except OSError:
            writer.close()
            return
        self._writers.update({writer, up_writer})
        first = True

        async def pipe(src, dst, upstream):
            nonlocal first
            try:
                while data := await src.read(65536):
                    if upstream and first:
                        self.first_packets.append(data)
                        first = False
                    if not upstream and self.hold_downstream:
                        continue
                    dst.write(data)
                    await dst.drain()
            except (OSError, asyncio.IncompleteReadError, ssl.SSLError):
                pass
            finally:
                dst.close()

        await asyncio.gather(pipe(reader, up_writer, True), pipe(up_reader, writer, False))
        self._writers.discard(writer)
        self._writers.discard(up_writer)


def parse_connect(packet: bytes) -> dict[str, Any]:
    """Decode the fields of an MQTT 3.1.1 CONNECT packet."""
    assert packet[0] == 0x10, f"not a CONNECT packet: {packet[:4].hex()}"
    i = 1
    while packet[i] & 0x80:  # skip the remaining-length varint
        i += 1
    i += 1

    def string(pos):
        n = int.from_bytes(packet[pos : pos + 2], "big")
        return packet[pos + 2 : pos + 2 + n], pos + 2 + n

    protocol, i = string(i)
    level, flags = packet[i], packet[i + 1]
    i += 4  # level, flags, keepalive
    client_id, i = string(i)
    out = {
        "protocol": protocol.decode(),
        "level": level,
        "client_id": client_id.decode(),
        "clean_session": bool(flags & 0x02),
        "will": bool(flags & 0x04),
        "username": None,
        "password_flag": bool(flags & 0x40),
    }
    if flags & 0x04:
        will_topic, i = string(i)
        _, i = string(i)
        out["will_topic"] = will_topic.decode()
    if flags & 0x80:
        username, i = string(i)
        out["username"] = username.decode()
    return out


@pytest.fixture
def proxy_factory(broker):
    proxies: list[Proxy] = []

    def make(**kwargs) -> Proxy:
        proxy = Proxy(broker, **kwargs)
        proxies.append(proxy)
        return proxy

    yield make
    for proxy in proxies:
        proxy.close()


# ---------------------------------------------------------------------------
# Observer: a plain paho client that records what reaches the broker
# ---------------------------------------------------------------------------


@dataclass
class Observer:
    client: mqtt.Client
    messages: list[mqtt.MQTTMessage] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def on_message(self, client, userdata, msg) -> None:
        with self.lock:
            self.messages.append(msg)

    def subscribe(self, topic: str) -> None:
        done = threading.Event()
        self.client.on_subscribe = lambda *a: done.set()
        self.client.subscribe(topic, qos=1)
        assert done.wait(5), f"observer could not subscribe to {topic}"

    def find(self, predicate: Callable[[mqtt.MQTTMessage], bool]) -> list[mqtt.MQTTMessage]:
        with self.lock:
            return [m for m in self.messages if predicate(m)]

    async def wait_for(
        self, predicate: Callable[[mqtt.MQTTMessage], bool], timeout: float = 10.0
    ) -> mqtt.MQTTMessage:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            found = self.find(predicate)
            if found:
                return found[-1]
            await asyncio.sleep(0.05)
        raise AssertionError("observer did not receive the expected message")

    def publish(self, topic: str, payload: bytes | str, qos: int = 1) -> None:
        self.client.publish(topic, payload, qos=qos).wait_for_publish(5)


@pytest.fixture
def observer(broker):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=unique("observer"))
    obs = Observer(client)
    client.on_message = obs.on_message
    connected = threading.Event()
    client.on_connect = lambda *a: connected.set()
    client.connect(broker.host, broker.port)
    client.loop_start()
    assert connected.wait(5), "observer could not connect"
    yield obs
    client.disconnect()
    client.loop_stop()


# ---------------------------------------------------------------------------
# Mock edge node subprocess
# ---------------------------------------------------------------------------


@dataclass
class MockProcess:
    proc: subprocess.Popen
    group_id: str
    edge_node_id: str
    output: list[str] = field(default_factory=list)

    def stop(self) -> int:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
        try:
            return self.proc.wait(15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            return self.proc.wait(5)

    def text(self) -> str:
        return "".join(self.output)


@pytest.fixture
def mock_node(broker):
    """Start the mock edge node (the real script) with a unique Sparkplug group."""
    started: list[MockProcess] = []

    def start(group_id: str | None = None, edge_node_id: str = "edge-node-1") -> MockProcess:
        group_id = group_id or unique("itest")
        env = dict(os.environ)
        env.update(
            MQTT_BROKER_HOST=broker.host,
            MQTT_BROKER_PORT=str(broker.port),
            SPARKPLUG_GROUP_ID=group_id,
            SPARKPLUG_EDGE_NODE_ID=edge_node_id,
            UPDATE_INTERVAL="0.3",
            PYTHONUNBUFFERED="1",
        )
        proc = subprocess.Popen(
            [sys.executable, str(MOCK_SCRIPT)],
            cwd=MOCK_DIR,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        mock = MockProcess(proc, group_id, edge_node_id)

        def collect() -> None:
            for line in proc.stdout:
                mock.output.append(line)

        threading.Thread(target=collect, daemon=True).start()
        started.append(mock)
        return mock

    yield start
    for mock in started:
        mock.stop()


# ---------------------------------------------------------------------------
# The real MCP server over stdio, driven with the mcp client SDK
# ---------------------------------------------------------------------------


def server_env(**overrides: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k not in SERVER_VARS}
    env.update({name: "" for name in SERVER_VARS})
    env.update({"MQTT_TOOL_TIMEOUT": "5", "PYTHONUNBUFFERED": "1"})
    env.update(overrides)
    return env


@asynccontextmanager
async def mcp_session(tmp_path: Path, **env: str):
    """Run ``mqtt-mcp`` as an MCP client would and yield an initialized session.

    The working directory is an empty temporary directory, so no .env file is
    picked up. The server's stderr is written to tmp_path/server-stderr.log.
    """
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    params = StdioServerParameters(
        command=str(SERVER_EXE), args=[], env=server_env(**env), cwd=str(tmp_path)
    )
    with open(tmp_path / "server-stderr.log", "a") as errlog:
        async with stdio_client(params, errlog=errlog) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session


async def call(session, name: str, **arguments: Any) -> dict[str, Any]:
    """Call a tool and return its {success, data, error, meta} envelope."""
    result = await session.call_tool(name, arguments)
    assert not result.isError, f"{name} raised a protocol-level error: {result.content}"
    envelope = result.structuredContent
    if envelope is None:
        envelope = json.loads(result.content[0].text)
    assert set(envelope) == {"success", "data", "error", "meta"}, envelope
    return envelope


async def eventually(check: Callable[[], Any], timeout: float = 15.0, interval: float = 0.2):
    """Await an async ``check`` until it returns a truthy value."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = await check()
        if last:
            return last
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s (last: {last!r})")
