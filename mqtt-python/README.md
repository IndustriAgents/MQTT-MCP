# MQTT MCP Server (Python/FastMCP)

Python implementation of an MQTT + Sparkplug B MCP server using FastMCP.

This is the server package of the [MQTT MCP](../README.md) repository. The repository README covers the quick start with a broker and the mock edge node, and the [Known limitations](../README.md#known-limitations); see [CONTRIBUTING.md](../CONTRIBUTING.md) for development.

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
export MQTT_CLIENT_ID=mqtt-mcp-python
export SPARKPLUG_GROUP_ID=factory
export SPARKPLUG_EDGE_NODE_ID=edge-node-1
uv run mqtt-mcp
```

### With uv

From the repository root:

```bash
cd mqtt-python
export MQTT_BROKER_URL=mqtt://127.0.0.1:1883
uv run mqtt-mcp
```

## Configuration

Environment variables:

- `MQTT_BROKER_URL` - MQTT broker address (default: `mqtt://127.0.0.1:1883`). Plain TCP only: an `mqtts://` prefix is accepted, but TLS is not set up
- `MQTT_CLIENT_ID` - MQTT client identifier (default: `mqtt-mcp-python`). Currently ignored: the client connects with an empty ID and the broker assigns one
- `MQTT_USERNAME` - Optional authentication username (used only together with `MQTT_PASSWORD`)
- `MQTT_PASSWORD` - Optional authentication password
- `MQTT_KEEPALIVE` - Keep-alive interval in seconds (default: `60`)
- `SPARKPLUG_GROUP_ID` - Sparkplug B group ID (default: `factory`)
- `SPARKPLUG_EDGE_NODE_ID` - Sparkplug B edge node ID (default: `edge-node-1`)

## Tools

### Standard MQTT Tools

#### `publish_message`
Publish a message to any MQTT topic.

**Parameters:**
- `topic` (string, required) - Topic name
- `payload` (string, required) - Message payload
- `qos` (number, optional, 0-2) - Quality of Service level (default: 0)
- `retain` (boolean, optional) - Retain message on broker (default: false)

#### `subscribe_topic`
Subscribe to an MQTT topic pattern. The subscription is registered with the broker; received messages are not stored or returned yet.

**Parameters:**
- `topic` (string, required) - Topic pattern (supports +/# wildcards)
- `qos` (number, optional, 0-2) - Quality of Service level (default: 0)

#### `unsubscribe_topic`
Unsubscribe from an MQTT topic.

**Parameters:**
- `topic` (string, required) - Topic name

#### `list_subscriptions`
List currently active subscriptions.

#### `get_broker_info`
Get connection info and status of the MQTT broker.

### Sparkplug B Tools

#### `publish_node_birth`
Publish a Sparkplug B Node Birth (NBIRTH) certificate.

**Parameters:**
- `metrics` (array, optional) - Node metrics:
  - `name` (string, required) - Metric name
  - `value` (number/string/boolean, required) - Metric value
  - `type` (string, required) - Data type: "int32" (or "int"), "float", "bool", "string". Any other value, including "boolean", is sent as a string

#### `publish_node_death`
Publish a Sparkplug B Node Death (NDEATH) certificate.

#### `publish_device_birth`
Publish a Sparkplug B Device Birth (DBIRTH) certificate.

**Parameters:**
- `device_id` (string, required) - Device identifier
- `metrics` (array, required) - Device metrics

#### `publish_device_death`
Publish a Sparkplug B Device Death (DDEATH) certificate.

**Parameters:**
- `device_id` (string, required) - Device identifier

#### `publish_node_data`
Publish Sparkplug B Node Data (NDATA) with updated metrics.

**Parameters:**
- `metrics` (array, required) - Updated node metrics

#### `publish_device_data`
Publish Sparkplug B Device Data (DDATA) with updated metrics.

**Parameters:**
- `device_id` (string, required) - Device identifier
- `metrics` (array, required) - Updated device metrics

#### `publish_node_command`
Publish a Sparkplug B Node Command (NCMD).

**Parameters:**
- `metrics` (array, required) - Command metrics

#### `publish_device_command`
Publish a Sparkplug B Device Command (DCMD).

**Parameters:**
- `device_id` (string, required) - Device identifier
- `metrics` (array, required) - Command metrics

#### `list_sparkplug_nodes`
List the Sparkplug B birth certificates this server has published. It does not discover other edge nodes on the broker.

#### `decode_sparkplug_payload`
Decode a hex-encoded Sparkplug B protobuf payload (for debugging). Currently fails with `name 'sparkplug_b_pb2' is not defined`.

**Parameters:**
- `payload_hex` (string, required) - Hex-encoded protobuf data

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
        "MQTT_CLIENT_ID": "mqtt-mcp-python",
        "SPARKPLUG_GROUP_ID": "factory",
        "SPARKPLUG_EDGE_NODE_ID": "edge-node-1"
      }
    }
  }
}
```

## Dependencies

- `mcp[cli]>=1.9.1,<2` - MCP framework with FastMCP (mcp 2.x removed `mcp.server.fastmcp`, so the server needs 1.x; `uv.lock` pins 1.21.1)
- `paho-mqtt>=1.6.1` - MQTT client (the code uses the paho-mqtt 2.x callback API; `uv.lock` pins 2.1.0)
- `protobuf>=4.24.0` - Protobuf runtime

Sparkplug B payloads are encoded by a small built-in encoder, not by the official `sparkplug_b.proto` bindings.

## Architecture

The server uses FastMCP's lifespan context manager to:
1. Create and connect MQTT client on startup
2. Share the client across all tool handlers
3. Properly disconnect on shutdown

Most tools are async and integrate with the FastMCP framework's event loop; `list_subscriptions`, `get_broker_info`, `list_sparkplug_nodes` and `decode_sparkplug_payload` are plain functions.

## Development

```bash
# Install development dependencies
uv sync

# Run the server
uv run mqtt-mcp

# Format code (ruff is not a project dependency, so run it through uvx)
uvx ruff format .
```

