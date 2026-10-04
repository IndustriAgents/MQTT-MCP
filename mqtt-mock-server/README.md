# Mock Sparkplug B Edge Node

A simulated Sparkplug B edge node with two devices and periodic metric updates, for testing the MQTT MCP server without real plant.

The mock is an MQTT **client**, not a broker: it connects to the broker at `MQTT_BROKER_HOST:MQTT_BROKER_PORT` (default `127.0.0.1:1883`) and publishes there, so run a broker as well (see the [repository README](../README.md#1-start-an-mqtt-broker) for Homebrew, Debian/Ubuntu and Docker). If the broker is not up yet, the mock keeps retrying.

## Features

- Publishes to an MQTT broker you run (no broker built in)
- Sparkplug B edge node with:
  - An NDEATH Last Will carrying `bdSeq`, registered on every connect (bdSeq goes up by one per new connection)
  - NBIRTH (`seq` 0, with `bdSeq` and `Node Control/Rebirth`) and a DBIRTH per device after every (re)connect
  - Periodic NDATA and DDATA updates; one `seq` counter for the node and its devices
  - Commands: NCMD `Node Control/Rebirth` republishes the births; DCMD sets device-2's `pump_speed`, `valve_position` and `enabled`
  - Sparkplug B protobuf payloads with a `datatype` on every metric (`sparkplug_b_pb2.py`, the same file as the server's)
- Standard MQTT topics with JSON that mirrors the simulated values
- Configurable via environment variables
- On Ctrl+C or SIGTERM, publishes DDEATH for each device and NDEATH, once, then disconnects

## Quick Start

With a broker listening on `127.0.0.1:1883`, from the repository root:

```bash
cd mqtt-mock-server
uv sync
uv run mqtt-mock-server
```

## Configuration

Environment variables (read when the mock starts):

- `MQTT_BROKER_HOST` - Broker address (default: `127.0.0.1`)
- `MQTT_BROKER_PORT` - Broker port (default: `1883`)
- `SPARKPLUG_GROUP_ID` - Sparkplug B group ID (default: `factory`)
- `SPARKPLUG_EDGE_NODE_ID` - Edge node ID (default: `edge-node-1`)
- `UPDATE_INTERVAL` - Metric update interval in seconds (default: `2.0`)

Example with custom configuration:

```bash
MQTT_BROKER_HOST=localhost \
MQTT_BROKER_PORT=1883 \
SPARKPLUG_GROUP_ID=production \
SPARKPLUG_EDGE_NODE_ID=edge-1 \
UPDATE_INTERVAL=1.0 \
uv run mqtt-mock-server
```

## Topic Map

### Sparkplug B Topics

**Node Topics:**
- `spBv1.0/{GROUP_ID}/NBIRTH/{EDGE_NODE_ID}` - Node birth certificate
- `spBv1.0/{GROUP_ID}/NDEATH/{EDGE_NODE_ID}` - Node death certificate (Last Will, and on shutdown)
- `spBv1.0/{GROUP_ID}/NDATA/{EDGE_NODE_ID}` - Node data updates
- `spBv1.0/{GROUP_ID}/NCMD/{EDGE_NODE_ID}` - Node commands (subscribed; `Node Control/Rebirth` is acted on)

**Device Topics:**
- `spBv1.0/{GROUP_ID}/DBIRTH/{EDGE_NODE_ID}/{DEVICE_ID}` - Device birth
- `spBv1.0/{GROUP_ID}/DDEATH/{EDGE_NODE_ID}/{DEVICE_ID}` - Device death
- `spBv1.0/{GROUP_ID}/DDATA/{EDGE_NODE_ID}/{DEVICE_ID}` - Device data updates
- `spBv1.0/{GROUP_ID}/DCMD/{EDGE_NODE_ID}/{DEVICE_ID}` - Device commands (subscribed; device-2 metrics are writable)

### Standard MQTT Topics

JSON payloads, published every `UPDATE_INTERVAL` seconds with QoS 1:

- `sensors/temperature` - device-1's temperature (°C)
- `sensors/pressure` - device-1's pressure (kPa)
- `sensors/humidity` - a humidity value that varies slowly (%)
- `control/pump` - device-2's pump: `status` ("on" when enabled and speed > 0), `speed`, `enabled`
- `control/valve` - device-2's valve position and status
- `system/status` - uptime and status

## Simulated Devices

### Device 1: Sensor Array
- **Topic:** `spBv1.0/factory/D{BIRTH|DATA}/edge-node-1/device-1`
- **Metrics (read-only; DCMD writes are rejected):**
  - `temperature` (Float, °C) - Varies sinusoidally
  - `pressure` (Float, kPa) - Varies with cosine
  - `flow_rate` (Float, L/min) - Varies sinusoidally

### Device 2: Control Module
- **Topic:** `spBv1.0/factory/D{BIRTH|DATA|CMD}/edge-node-1/device-2`
- **Metrics (writable with DCMD; they change only when commanded):**
  - `pump_speed` (Int32, %) - 0 at start, settable 0-100
  - `valve_position` (Int32, %) - 50 at start, settable 0-100
  - `enabled` (Boolean) - false at start

A DCMD with a value of the wrong datatype or out of range is rejected and logged. Accepted changes are published at once in a DDATA.

## Node Metrics

- `bdSeq` (Int64) - birth/death sequence of the current connection (NBIRTH only)
- `Node Control/Rebirth` (Boolean) - always false in the NBIRTH; send true in an NCMD to request a rebirth
- `uptime` (Int64, seconds) - Time since the mock started
- `status` (String) - always "online" while the node is alive (its death is signalled by NDEATH)

## Testing with MCP Servers

Once a broker is running on port 1883, start the mock and connect your MCP server to the same broker. Both terminals start at the repository root:

```bash
# Terminal 1 - Start the mock edge node
cd mqtt-mock-server
uv run mqtt-mock-server

# Terminal 2 - Start the MCP server (Python); it waits silently for an MCP client on stdin
cd mqtt-python
export MQTT_BROKER_URL=mqtt://127.0.0.1:1883
export MQTT_WRITES_ENABLED=true   # only needed for commands
uv run mqtt-mcp
```

Then use the MCP tools to:
- Check the broker connection (`get_broker_info`)
- Subscribe to `spBv1.0/factory/#` and see the node and its devices (`list_sparkplug_nodes`, `read_messages`)
- Send `publish_device_command` to `edge_node_id` `edge-node-1`, `device_id` `device-2` and watch the mock apply it

## Common Test Operations

### Watch device metrics

Through the MCP server: `subscribe_topic` with `spBv1.0/factory/DDATA/edge-node-1/+`, then `read_messages` (payloads come back decoded). Directly on the broker:

```bash
mosquitto_sub -h 127.0.0.1 -t 'spBv1.0/factory/DDATA/edge-node-1/+' -v      # binary payloads
docker exec mqtt-broker mosquitto_sub -h 127.0.0.1 -t 'sensors/#' -v        # Docker broker
```

### Monitor all standard topics

Subscribe to:
```
sensors/#
control/#
system/#
```

## Architecture

The mock:
1. Connects to the MQTT broker at `MQTT_BROKER_HOST:MQTT_BROKER_PORT` with an NDEATH Last Will (retrying until the broker answers)
2. Subscribes to its NCMD and DCMD topics
3. Publishes NBIRTH, then DBIRTH for each device (device-1, device-2)
4. Every UPDATE_INTERVAL seconds:
   - Publishes NDATA with node metrics
   - Publishes DDATA for each device with current metrics
   - Publishes the standard MQTT topics
5. Applies NCMD/DCMD as they arrive
6. After an unexpected disconnect, reconnects with the next bdSeq and publishes its births again
7. On shutdown, publishes DDEATH for each device, then NDEATH, and disconnects

## Limitations

- This is a mock edge node that publishes data; it is not an MQTT broker and needs one to connect to
- No authentication or TLS; plain MQTT 3.1.1 to the configured host and port
- Only device-2's three metrics are writable; there is no alias, property, template or dataset support
- Designed for local testing only

## Dependencies

- `paho-mqtt>=2.0,<3` - MQTT client (2.x callback API)
- `protobuf>=4.24` - Protobuf runtime for `sparkplug_b_pb2.py`

Development dependency: `ruff` (`uv sync` installs it).
