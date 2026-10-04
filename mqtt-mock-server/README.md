# MQTT Mock Server with Sparkplug B Simulation

Local MQTT mock for testing and development. It simulates a Sparkplug B edge node with two devices and periodic metric updates.

The mock is an MQTT **client**, not a broker: it connects to the broker at `MQTT_BROKER_HOST:MQTT_BROKER_PORT` (default `127.0.0.1:1883`) and publishes there, so start a broker first (for example `mosquitto -v`). See the [repository README](../README.md) for the full quick start.

## Features

- Publishes to an MQTT broker you run (no broker built in)
- Sparkplug B edge node with:
  - Node birth/death certificates (NBIRTH/NDEATH)
  - Multiple simulated devices with DBIRTH/DDEATH
  - Periodic NDATA and DDATA updates
  - Sequence numbers: one counter for the node and its devices (NBIRTH goes out with `seq` 1 rather than 0, and no `bdSeq` metric is sent)
  - Proper timestamps
- Standard MQTT topics for basic testing
- Configurable via environment variables
- Graceful shutdown with proper cleanup

## Quick Start

With a broker listening on `127.0.0.1:1883`, from the repository root:

```bash
cd mqtt-mock-server
uv sync
uv run mqtt-mock-server
```

## Configuration

Environment variables:

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
- `spBv1.0/{GROUP_ID}/NDEATH/{EDGE_NODE_ID}` - Node death certificate
- `spBv1.0/{GROUP_ID}/NDATA/{EDGE_NODE_ID}` - Node data updates
- `spBv1.0/{GROUP_ID}/NCMD/{EDGE_NODE_ID}` - Node commands (not subscribed: the mock does not act on commands)

**Device Topics:**
- `spBv1.0/{GROUP_ID}/DBIRTH/{EDGE_NODE_ID}/{DEVICE_ID}` - Device birth
- `spBv1.0/{GROUP_ID}/DDEATH/{EDGE_NODE_ID}/{DEVICE_ID}` - Device death
- `spBv1.0/{GROUP_ID}/DDATA/{EDGE_NODE_ID}/{DEVICE_ID}` - Device data updates
- `spBv1.0/{GROUP_ID}/DCMD/{EDGE_NODE_ID}/{DEVICE_ID}` - Device commands (not subscribed: the mock does not act on commands)

### Standard MQTT Topics

JSON payloads with fixed values, except the uptime in `system/status`:

- `sensors/temperature` - Temperature reading (°C)
- `sensors/pressure` - Pressure reading (kPa)
- `sensors/humidity` - Humidity reading (%)
- `control/pump` - Pump status and speed
- `control/valve` - Valve position and status
- `system/status` - System uptime and health

## Simulated Devices

### Device 1: Sensor Array
- **Topic:** `spBv1.0/factory/D{BIRTH|DATA}/edge-node-1/device-1`
- **Metrics:**
  - `temperature` (float, °C) - Varies sinusoidally
  - `pressure` (float, kPa) - Varies with cosine
  - `flow_rate` (float, L/min) - Varies sinusoidally

### Device 2: Control Module
- **Topic:** `spBv1.0/factory/D{BIRTH|DATA}/edge-node-1/device-2`
- **Metrics:**
  - `pump_speed` (int, %) - fixed at 0
  - `valve_position` (int, %) - fixed at 50
  - `enabled` (bool) - fixed at off

## Node Metrics

- `uptime` (int, seconds) - Time since node birth
- `status` (string) - Node status ("online", "offline", etc.)

## Testing with MCP Servers

Once a broker is running on port 1883, start the mock and connect your MCP server to the same broker. Both terminals start at the repository root:

```bash
# Terminal 1 - Start mock server
cd mqtt-mock-server
uv run mqtt-mock-server

# Terminal 2 - Start MCP server (Python)
cd mqtt-python
export MQTT_BROKER_URL=mqtt://127.0.0.1:1883
uv run mqtt-mcp
```

Then use MCP tools to:
- Check the broker connection (`get_broker_info`)
- Subscribe to topics (the server registers the subscription but does not return received messages yet)
- Publish messages and Sparkplug B commands (the mock does not act on commands)

To see what the mock publishes, watch the broker directly, for example `mosquitto_sub -h 127.0.0.1 -t '#' -v` (Sparkplug B payloads are binary).

## Common Test Operations

### Watch device metrics

The MCP server cannot read metric values yet. Publishing to a `DDATA` topic does not read anything; it would inject a message as if it came from the device. Watch the broker instead:

```bash
mosquitto_sub -h 127.0.0.1 -t 'spBv1.0/factory/DDATA/edge-node-1/+' -v
```

### Monitor all device data

Subscribe to:
```
spBv1.0/factory/DDATA/edge-node-1/+
```

### Monitor all standard topics

Subscribe to:
```
sensors/#
control/#
system/#
```

## Architecture

The mock server:
1. Connects to the MQTT broker at `MQTT_BROKER_HOST:MQTT_BROKER_PORT`
2. Publishes NBIRTH to announce the edge node
3. Publishes DBIRTH for each device (device-1, device-2)
4. Every UPDATE_INTERVAL seconds:
   - Publishes NDATA with node metrics
   - Publishes DDATA for each device with updated metrics
   - Publishes standard MQTT topic updates
5. On shutdown, publishes DDEATH for each device, then NDEATH for the node

## Limitations

- This is a mock edge node that publishes data; it is not an MQTT broker and needs one to connect to
- It subscribes to nothing, so NCMD/DCMD commands are not acted on
- Payloads use a hand-rolled protobuf encoder whose metric field numbers do not match `sparkplug_b.proto`, so standard Sparkplug B decoders cannot read the values
- QoS 0 (at-most-once delivery) is simplified
- No authentication or encryption
- Designed for local testing only

## Dependencies

- `paho-mqtt>=1.6.1` - MQTT client (the code uses the paho-mqtt 2.x callback API; `uv.lock` pins 2.1.0)
- `protobuf>=4.24.0` - Protobuf runtime (Sparkplug B payloads are encoded by hand, without the official bindings)
