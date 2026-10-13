# MQTT MCP Server (Python/FastMCP)

Python implementation of an MQTT + Sparkplug B MCP server using FastMCP.

This is the server package of the [MQTT MCP](../README.md) repository. The repository README covers the quick start with a broker and the mock edge node, the write gate, and the [Known limitations](../README.md#known-limitations); see [CONTRIBUTING.md](../CONTRIBUTING.md) for development.

**Read-only by default:** the publishing tools return `success: false` and send nothing unless `MQTT_WRITES_ENABLED=true`.

## Installation

```bash
uv sync
```

Or with pip:
```bash
pip install -e .
```

## Usage

### Local Development

```bash
export MQTT_BROKER_URL=mqtt://127.0.0.1:1883
export SPARKPLUG_GROUP_ID=factory
export SPARKPLUG_EDGE_NODE_ID=mcp-server
uv run mqtt-mcp
```

The server then waits silently for an MCP client on stdin: it speaks MCP (JSON-RPC) on stdio and logs to stderr. Normally your MCP client starts it (see [Usage with Claude Desktop](#usage-with-claude-desktop)); to try it by hand, run `npx @modelcontextprotocol/inspector uv --directory mqtt-python run mqtt-mcp` from the repository root. `uv run mqtt-mcp --help` lists the settings and `--version` prints the version.

At startup the server merges a `.env` file into the environment: the one given with `--env-file PATH`, or else `.env` in this directory (`mqtt-python/.env`, found next to the package source of a checkout). The working directory and its parents are never searched, because the working directory is often the MCP client's project; an installed package has no automatic `.env`. Variables already set in the environment take precedence. The automatic file may set only `MQTT_*` and `SPARKPLUG_*` variables, and not `MQTT_WRITES_ENABLED` (matched in any letter case, with or without `export `); anything else is ignored with a warning on stderr. Only the real environment or `--env-file` can enable writes. [`.env.example`](.env.example) lists every setting. An invalid setting stops the server with a `configuration error` on stderr (exit code 2).

## Configuration

Environment variables:

- `MQTT_BROKER_URL` - Broker address (default: `mqtt://127.0.0.1:1883`). `mqtts://` uses TLS (default port 8883)
- `MQTT_CLIENT_ID` - MQTT client identifier (default: `mqtt-mcp-<random hex>`, unique per process)
- `MQTT_USERNAME` - Optional username (sent with or without a password)
- `MQTT_PASSWORD` - Optional password (needs `MQTT_USERNAME`)
- `MQTT_KEEPALIVE` - Keep-alive interval in seconds (default: `60`)
- `MQTT_TLS_CA_FILE` - CA file for verifying the broker (`mqtts://` only; default: system trust store)
- `MQTT_TLS_CERT_FILE` / `MQTT_TLS_KEY_FILE` / `MQTT_TLS_KEY_PASSWORD` - Client certificate for mutual TLS
- `MQTT_TLS_INSECURE` - `true` disables certificate and hostname verification (testing only)
- `MQTT_WRITES_ENABLED` - `true` enables the publishing tools and `$`-prefixed subscription filters (default: `false`). Only from the real environment or `--env-file`
- `MQTT_TOOL_TIMEOUT` - Deadline of one tool call in seconds, shared by all its steps (default: `10`, at most `50`)
- `MQTT_MESSAGE_BUFFER_SIZE` - Received messages kept for `read_messages` (default: `1000`)
- `SPARKPLUG_GROUP_ID` - Sparkplug B group ID (default: `factory`)
- `SPARKPLUG_EDGE_NODE_ID` - This server's own Sparkplug B edge node ID (default: `mqtt-mcp`)

## Tools

Every tool returns `{ "success": bool, "data": ..., "error": str | null, "meta": {...} }`. Invalid arguments come back as `success: false` with the reason; types are strict (`true` is not a number, `1` is not a boolean).

Every tool call has one deadline, `MQTT_TOOL_TIMEOUT` (default 10 s, at most 50 s), from its start; waiting for the connection, a session restart and the broker's acknowledgement all share it. After it, nothing more is handed to the MQTT client for the call; a write still in the client's queue is withdrawn (`not_sent`) and never sent, one the client had started writing is `unknown`; a read gets a clean error. A cancelled call is answered only with the SDK's "Request cancelled"; its real outcome is logged to stderr, and a cancelled write whose message was already on the socket may have been applied, so read the state back before retrying. The same applies when the MCP client cancels a call or disconnects: when stdin closes, every running call is cancelled, nothing more is published and the server exits with status 0.

Write tools add `meta.outcome` (`written`, `not_sent`, `unknown`; `rejected` would need an explicit broker refusal, which MQTT 3.1.1 does not have) and `meta.request_sent`. Once a message may have left, a failure is `unknown`, never `not_sent`; and `written` for QoS 0 only means written to the network, its effect is unknown. Each publish goes out at most once: a late or lost confirmation is reported as `unknown` ("may have been applied; check before retrying"), never retried, and withdrawn from the client's queue; if the connection drops while a call waits for its PUBACK, the message is given up before the MQTT client could resend it on reconnect.

### Read-only tools

#### `get_broker_info`
Broker URL, TLS, client ID, username, whether the server is connected and the last connection error, whether writes are enabled, subscriptions, number of buffered messages, and the state of this server's own Sparkplug edge node.

#### `subscribe_topic`
Subscribe to a topic filter. Succeeds only after the broker's SUBACK and reports the granted QoS. Subscriptions are restored after a reconnect. Filters starting with `$` other than `$SYS/` (shared `$share/`, `$queue/`, exclusive `$exclusive/`) need writes enabled, because they change what other clients receive.

**Parameters:**
- `topic` (string, required) - Topic filter (supports `+`/`#` wildcards)
- `qos` (number, optional, 0-2) - Maximum QoS (default: 0)

#### `unsubscribe_topic`
Remove one of this server's subscriptions (fails if the server holds no subscription with that exact filter).

**Parameters:**
- `topic` (string, required) - The subscribed filter

#### `list_subscriptions`
The subscriptions this server holds, with the QoS the broker granted.

#### `read_messages`
Messages received on the subscriptions (newest `MQTT_MESSAGE_BUFFER_SIZE` kept). Each has `id`, `topic`, `qos`, `retain`, `received_at`, `payload` (UTF-8 text, or hex for binary and Sparkplug payloads), `payload_encoding`, and for Sparkplug topics the decoded payload under `sparkplug`.

**Parameters:**
- `topic_filter` (string, optional) - Only messages whose topic matches this filter
- `limit` (number, optional, 1-1000) - At most this many (default: 50)
- `since_id` (number, optional) - Only messages after this id, oldest first; pass `meta.last_id` from the previous call to poll. Without it, the newest `limit` messages are returned

#### `list_sparkplug_nodes`
Sparkplug B edge nodes and devices seen in messages received on the subscriptions (subscribe to `spBv1.0/#` first): online state, last message type and time, bdSeq, latest metric values. Also reports this server's own edge node.

#### `decode_sparkplug_payload`
Decode a hex-encoded Sparkplug B payload: timestamp, seq, and metrics with name, alias, timestamp, datatype and value.

**Parameters:**
- `payload_hex` (string, required) - Hex-encoded protobuf data (spaces and `0x` are ignored)

### Write tools (need `MQTT_WRITES_ENABLED=true`)

Sparkplug metrics are lists of `{"name": str, "value": ..., "type": str}` where `type` is one of `int8`, `int16`, `int32` (or `int`), `int64` (or `long`), `uint8`, `uint16`, `uint32`, `uint64`, `float`, `double`, `boolean` (or `bool`), `string`, `text`, `uuid`, `datetime` (ms since the epoch) or `bytes` (hex). The value must match the type and its range; `null` sends a null metric. Anything else is rejected and nothing is sent.

#### `publish_message`
Publish UTF-8 text. Success means: QoS 0 written to the network; QoS 1/2 acknowledged by the broker (an MQTT 3.1.1 broker also acknowledges a publish its ACL drops).

**Parameters:**
- `topic` (string, required) - Topic name (no wildcards)
- `payload` (string, required) - Message text
- `qos` (number, optional, 0-2) - Quality of Service level (default: 0)
- `retain` (boolean, optional) - Retain message on broker (default: false)

#### `publish_node_command` / `publish_device_command`
Send NCMD / DCMD (QoS 0, no `seq`, as Sparkplug requires). Success means the command was sent, not that the node applied it.

**Parameters:**
- `edge_node_id` (string, required) - Target edge node (there is no default target)
- `device_id` (string, required for DCMD) - Target device
- `metrics` (array, required) - At least one metric
- `group_id` (string, optional) - Target group (default: `SPARKPLUG_GROUP_ID`)

#### `publish_node_birth`
Make this server the edge node `SPARKPLUG_GROUP_ID`/`SPARKPLUG_EDGE_NODE_ID` and publish its NBIRTH (`seq` 0, with `bdSeq` and `Node Control/Rebirth` added; metric names must be unique). The first birth on a connection reconnects with an NDEATH Last Will carrying a new bdSeq.

**Parameters:**
- `metrics` (array, optional) - Node metrics

#### `publish_node_death`
Publish this server's NDEATH (bdSeq, no seq, QoS 1). Needs a born node. Afterwards the server reconnects without the will, so a later crash does not announce a second death.

#### `publish_device_birth` / `publish_device_death`
DBIRTH / DDEATH for a device of this server's node. DBIRTH needs a born node; DDEATH a device born by this server.

**Parameters:**
- `device_id` (string, required) - Device identifier
- `metrics` (array, required for DBIRTH, may be empty) - Device metrics

#### `publish_node_data` / `publish_device_data`
NDATA / DDATA with at least one metric. Need a born node (and, for DDATA, a born device).

**Parameters:**
- `device_id` (string, required for DDATA) - Device identifier
- `metrics` (array, required) - Updated metrics

## Usage with Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "mqtt-mcp-python": {
      "command": "uv",
      "args": ["--directory", "/absolute/path/to/MQTT-MCP/mqtt-python", "run", "mqtt-mcp"],
      "env": {
        "MQTT_BROKER_URL": "mqtt://127.0.0.1:1883",
        "SPARKPLUG_GROUP_ID": "factory",
        "SPARKPLUG_EDGE_NODE_ID": "mcp-server"
      }
    }
  }
}
```

Add `"MQTT_WRITES_ENABLED": "true"` to `env` only if the agent may publish.

## Dependencies

- `mcp[cli]>=1.12.3,<2` - MCP framework with FastMCP (1.12.0-1.12.2 break the session when a client cancels two running calls; mcp 2.x removed `mcp.server.fastmcp`, and the server uses FastMCP internals, so it needs 1.x)
- `anyio>=4.7` - task groups and cancel scopes (stdio relay, call cancellation)
- `pydantic>=2.8,<3` (`>=2.12` on Python 3.14) - strict argument types
- `paho-mqtt>=2.0,<3` - MQTT client (2.x callback API)
- `protobuf>=5.27` - Protobuf runtime for the Sparkplug B schema
- `python-dotenv>=1.0` - `.env` loading

Sparkplug B payloads are encoded and decoded with `mqtt_mcp/sparkplug_b_pb2.py`, which provides the same classes as the `sparkplug_b_pb2` module protoc generates from `sparkplug_b.proto` but builds the descriptor at import time, so it works with any protobuf runtime from 5.27 on (protoc-generated code is tied to the protoc version that wrote it).

Development dependencies (`uv sync` installs them): `pytest`, `ruff`, and `cryptography` (test TLS certificates).

## Architecture

The server uses FastMCP's lifespan context manager to:
1. Read the configuration (after `.env`) and start connecting to the broker in the background; paho-mqtt retries until the broker answers and after every drop
2. Share the client, the received-message buffer and the Sparkplug registry across all tool handlers
3. Publish NDEATH (if this server's edge node is born) and disconnect on shutdown

Received messages are stored by paho's network thread; Sparkplug messages also update the registry behind `list_sparkplug_nodes`.

## Development

```bash
uv sync                      # includes pytest and ruff
uv run ruff check . && uv run ruff format --check .
uv run pytest                # unit tests; integration tests need a broker:
MQTT_TEST_BROKER_URL=mqtt://127.0.0.1:1883 uv run pytest
uv run mqtt-mcp              # run the server
uv run mqtt-mcp --env-file ../plant.env   # settings (writes included) from an explicit file

# the lowest declared dependency versions, as CI's lowest-direct job does
uv venv /tmp/mqtt-lowest && uv pip install --python /tmp/mqtt-lowest --resolution lowest-direct -e . --group dev
MQTT_TEST_BROKER_URL=mqtt://127.0.0.1:1883 /tmp/mqtt-lowest/bin/python -m pytest
```
