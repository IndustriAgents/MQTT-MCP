# Contributing to MQTT MCP

Thanks for taking an interest. This repository holds an MCP server for MQTT and
Sparkplug B, plus a mock Sparkplug B edge node that lets you exercise it
without real plant.

## Where changes go

This server is also shipped inside the
[IndustriConnect](https://github.com/IndustriAgents/IndustriConnect) suite,
which bundles one MCP server per industrial protocol. There it appears as the
`MQTT-Project/` folder, which is a git submodule pointing at this repository's
`main` branch.

**Open issues and pull requests here, in IndustriAgents/MQTT-MCP, not in
IndustriConnect.** IndustriConnect only records which commit of this repository
`MQTT-Project/` points at. Edits made to that folder from inside IndustriConnect
do not reach this repository, and the next submodule sync replaces the pointer.
When `main` moves here, a workflow notifies IndustriConnect so that it can bump
the pointer.

Issues that concern the suite as a whole, such as `mcp-manager-ui` or
consistency across protocols, belong in
[IndustriConnect](https://github.com/IndustriAgents/IndustriConnect/issues).

## Repository layout

| Path | What it is |
|------|------------|
| `mqtt-python/` | The MCP server: a uv project, `src/mqtt_mcp/`, console script `mqtt-mcp` |
| `mqtt-mock-server/` | Simulated Sparkplug B edge node: a uv project, console script `mqtt-mock-server` |
| `.github/workflows/ci.yml` | The checks every pull request has to pass |

```text
MCP client ──stdio──► mqtt-mcp ──MQTT──► broker ◄──MQTT── mqtt-mock-server
```

The mock is an MQTT **client**, not a broker. Both the server and the mock
connect to a broker that you run.

## Prerequisites

- Python 3.10+ and [`uv`](https://docs.astral.sh/uv/)
- An MQTT 3.1.1 broker on `127.0.0.1:1883`, for example
  [Mosquitto](https://mosquitto.org/) (`brew install mosquitto` or
  `apt install mosquitto`, then `mosquitto -v`), or with Docker:
  `docker run --rm -p 127.0.0.1:1883:1883 eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf`

## Getting set up

```bash
git clone https://github.com/IndustriAgents/MQTT-MCP.git
cd MQTT-MCP
```

Each folder is its own uv project, so work inside the one you are changing.
Start the broker, then the mock, then the server:

```bash
# Terminal 1, from the repository root: the mock edge node
# (publishes to the broker on 127.0.0.1:1883)
cd mqtt-mock-server
uv sync
uv run mqtt-mock-server

# Terminal 2, from the repository root: the MCP server (speaks MCP on stdio)
cd mqtt-python
uv sync
MQTT_BROKER_URL=mqtt://127.0.0.1:1883 uv run mqtt-mcp
```

The `uv.lock` of each project is committed, and CI installs with
`uv sync --locked`. If you change dependencies in a `pyproject.toml`, run
`uv lock` in that folder and commit the updated lockfile with it.

## Checks CI runs

Run the same checks from the repository root before opening a pull request:

```bash
(cd mqtt-python && uv sync --locked && uv run --locked python -m compileall -q src)
(cd mqtt-mock-server && uv sync --locked && uv run --locked python -m compileall -q mqtt_mock_server.py)
```

CI also imports each console-script target and performs an MCP `initialize` and
`tools/list` handshake with the server over stdio. That handshake needs no
broker. See [`.github/workflows/ci.yml`](.github/workflows/ci.yml) for the exact
steps. There is no unit-test suite yet, and tests are welcome.

## Testing a change

Drive the server the way a user would, against the broker and the mock, through
the [MCP Inspector](https://modelcontextprotocol.io/docs/tools/inspector) or a
real MCP client:

```bash
npx @modelcontextprotocol/inspector uv --directory mqtt-python run mqtt-mcp
```

Call the tool you changed and check its output. To see what actually reached
the broker, watch it from another terminal, for example with
`mosquitto_sub -h 127.0.0.1 -t '#' -v`.

**Do not test against a production broker or equipment.** If a change
genuinely cannot be verified against the mock and a local broker, say so in the
pull request and describe the bench you used.

## House rules for the code

- MCP speaks over **stdio**, so `stdout` belongs to the protocol. All logging
  goes to `stderr`. A stray `print()` in the server corrupts the session. The
  mock is not an MCP server, so it may print.
- An error is a returned message, not an exception that kills the server. The
  model needs to be told what went wrong.
- Keep tool names and argument names aligned with the other servers in
  IndustriConnect.
- The suite's aim is a shared `{ success, data, error, meta }` response
  envelope. This server does not use it yet; its tools return plain text. A
  move to the envelope is welcome, but do it for every tool at once rather than
  for some of them.
- Anything that publishes can reach real devices. Call out in the pull request
  any new or changed tool that publishes, especially Sparkplug `NCMD`/`DCMD`
  or retained messages.
- If you add a tool, make sure the mock can exercise it, or nobody can test it
  without a plant.

## Pull requests

Say what changed, how you tested it (commands and tool calls a reviewer can
repeat against the mock), and whether the change affects what can be published
to the broker. Keep commits focused. If a change was AI-assisted, keep the
`Co-Authored-By:` trailer.

## Code of Conduct

By taking part you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).
