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
  [Mosquitto](https://mosquitto.org/):
  - macOS: `brew install mosquitto && mosquitto -v`
  - Debian/Ubuntu: `sudo apt install mosquitto mosquitto-clients`. The package
    starts the broker as a service on 127.0.0.1:1883, so skip `mosquitto -v`
    (it would fail with "Address already in use").
  - Docker:
    `docker run --rm --name mqtt-broker -p 127.0.0.1:1883:1883 eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf`.
    The image contains `mosquitto_sub`, so watch traffic with
    `docker exec mqtt-broker mosquitto_sub -h 127.0.0.1 -t '#' -v`.

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

# Terminal 2, from the repository root: the MCP server (speaks MCP on stdio,
# so it waits silently for a client; add MQTT_WRITES_ENABLED=true to publish)
cd mqtt-python
uv sync
MQTT_BROKER_URL=mqtt://127.0.0.1:1883 uv run mqtt-mcp
```

The `uv.lock` of each project is committed, and CI installs with
`uv sync --locked`. If you change dependencies in a `pyproject.toml`, run
`uv lock` in that folder and commit the updated lockfile with it.

## Checks CI runs

Run the same checks from the repository root before opening a pull request
(`uv sync` installs pytest and ruff as development dependencies):

```bash
(cd mqtt-python && uv sync --locked && uv run --locked ruff check . \
  && uv run --locked ruff format --check . && uv run --locked pytest)
(cd mqtt-mock-server && uv sync --locked && uv run --locked ruff check . \
  && uv run --locked ruff format --check .)
```

`pytest` runs the unit tests and, when it finds a broker, the integration
tests: they start the mock edge node and drive the real server over stdio with
the MCP client SDK. They use the broker in `MQTT_TEST_BROKER_URL` (for example
`MQTT_TEST_BROKER_URL=mqtt://127.0.0.1:1883 uv run pytest`) or, if `mosquitto`
is on `PATH`, start their own on a free port. Without a broker they are skipped
locally; CI sets `MQTT_TEST_REQUIRE_BROKER=1`, which turns that into a failure.
Each test uses its own topics and Sparkplug group, so a shared broker is fine.

CI also byte-compiles the sources, imports each console-script target, runs
`mqtt-mcp --help`, performs an MCP `initialize` and `tools/list` handshake
without a broker, and runs the mock against Mosquitto for a few seconds. A
`lowest-direct` job runs the suite with the lowest versions that
`pyproject.toml` allows for the direct dependencies; if you lower a floor, or
use a newer API of a dependency, check that job (the command is in
[mqtt-python/README.md](mqtt-python/README.md#development)). See
[`.github/workflows/ci.yml`](.github/workflows/ci.yml) for the exact steps.

A safety test has to be able to fail: when you add one (write gate, input
check, at-most-once publishing, clean stdout, ...), remove the protection it
guards once and see the test go red before you commit.

## Testing a change

Drive the server the way a user would, against the broker and the mock, through
the [MCP Inspector](https://modelcontextprotocol.io/docs/tools/inspector) or a
real MCP client:

```bash
npx @modelcontextprotocol/inspector uv --directory mqtt-python run mqtt-mcp
# publishing tools need the write gate open:
npx @modelcontextprotocol/inspector -e MQTT_WRITES_ENABLED=true uv --directory mqtt-python run mqtt-mcp
```

Call the tool you changed and check its output. To see what actually reached
the broker, use `subscribe_topic` and `read_messages`, or watch it from another
terminal, for example with `mosquitto_sub -h 127.0.0.1 -t '#' -v`.

**Do not test against a production broker or equipment.** If a change
genuinely cannot be verified against the mock and a local broker, say so in the
pull request and describe the bench you used.

## House rules for the code

- MCP speaks over **stdio**, so `stdout` belongs to the protocol. All logging
  goes to `stderr`. A stray `print()` in the server corrupts the session. The
  mock is not an MCP server, so it may print.
- Every tool returns the suite's `{ success, data, error, meta }` envelope. An
  error, including an invalid argument, is `success: false` with a message, not
  an exception. Never report success for something that was not confirmed.
- Keep tool names and argument names aligned with the other servers in
  IndustriConnect.
- Configuration is read when the server starts (after `.env` is loaded),
  never at import time.
- Anything that publishes can reach real devices, so every publishing tool
  checks `MQTT_WRITES_ENABLED` first and refuses while it is off (the
  default). A new tool that publishes must do the same, and its description
  must start with `WRITE:`. Call out such tools in the pull request,
  especially Sparkplug `NCMD`/`DCMD` or retained messages.
- The write gate is read only from the real environment or `--env-file`,
  never from an automatically found `.env`.
- A publish is sent at most once. Never retry one whose confirmation is late
  or lost; report `meta.outcome: "unknown"` with `meta.request_sent: true`.
  Only connecting may be retried.
- Arguments are strictly typed (`StrictInt`, `StrictBool`): never accept a
  boolean as a number.
- Tool descriptions are what an agent plans with: keep them exact about what
  the tool does and does not do.
- If you add a tool, make sure the mock can exercise it, or nobody can test it
  without a plant.

## Pull requests

Say what changed, how you tested it (commands and tool calls a reviewer can
repeat against the mock), and whether the change affects what can be published
to the broker. Keep commits focused. If a change was AI-assisted, keep the
`Co-Authored-By:` trailer.

## Code of Conduct

By taking part you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).
