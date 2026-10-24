# Architecture and migration (0.3.0)

This repository is an independent protocol MCP server. It follows OPCUA-MCP's
instance-owned composition, explicit MCP contract and application/adapter split.
It has no runtime dependency on another IndustriAgents protocol repository or
on IndustriConnect-MCPs. The umbrella repository can pin this repository as a
submodule and launch its released command.

## Execution path

```mermaid
flowchart LR
  Client[MCP client] -->|stdio| Contract[Protocol contract and dispatcher]
  Contract --> Execution[Instance policy, admission and audit]
  Execution --> Application[Protocol application operations]
  Application --> Adapter[Native protocol adapter]
  Adapter --> Device[Configured endpoint or bus]
```

`server.py` composes one server instance and its resources. `protocol/` owns
registration, schema dispatch, SDK request context access and wire errors.
`application/` owns reads, writes, discovery, diagnostics and metadata operations.
`domain/` holds protocol types, error/outcome vocabulary, structural ports and
policy values. `adapters/` contains the native driver and owns its connection,
retry eligibility, send tracking and cancellation fences. `infrastructure/`
contains audit and execution-context support. Compatibility modules at the old
Python import paths forward to the implementation rather than owning another
connection. Some established config/codec/metadata helpers keep their root
module paths to avoid unnecessary public import breaks.

The supported deployment is local stdio with one caller context and one
configured endpoint/bus per process. Instance ownership is not a claim of
multi-tenant authorization or remote HTTP transport support. Modbus and MQTT
provide `create_server(...)` with explicit per-instance configuration; the
other protocols keep their existing server constructors with injected clients
or masters. The CLI default instance exists only for compatibility.

## Tool contract

`contract/tools.json` is the reviewed contract, including JSON schemas, routing,
legacy names and the result envelope. Its identical packaged resource is
`mqtt-python/src/mqtt_mcp/_contract/tools.json`.
Run `python scripts/verify_contract.py` to reject drift. There is no second
TypeScript implementation: npm carries the same wheel as the PyPI release.

| Advertised tool | Modes | Effect |
|---|---|---|
| `publish_sparkplug` | `NBIRTH`, `NDEATH`, `DBIRTH`, `DDEATH`, `NDATA`, `DDATA`, `NCMD`, `DCMD` | Changes state or consumes events |
| `get_broker_info` | No mode selector | Read/local |
| `subscribe_topic` | No mode selector | Read/local |
| `unsubscribe_topic` | No mode selector | Read/local |
| `list_subscriptions` | No mode selector | Read/local |
| `read_messages` | No mode selector | Read/local |
| `list_sparkplug_nodes` | No mode selector | Read/local |
| `decode_sparkplug_payload` | No mode selector | Read/local |
| `publish_message` | No mode selector | Changes state or consumes events |

A mode selects one protocol operation. Parameters from different modes cannot
be mixed; unknown parameters fail before dispatch. Every advertised tool is
held to its contract `inputSchema` before dispatch; a mode is checked against
its own `oneOf` variant, so a refusal names the argument and what it must be
(`edge_node_id is required`, `qos must be 0, 1 or 2`) without echoing values.
The schemas state types, required arguments, enums, numeric bounds and list
sizes; topic and ID syntax and metric values stay with the native checks,
which word those failures precisely. Metric `type` stays case-insensitive.
Legacy names remain callable but are omitted from `tools/list`. Those legacy
names retain native argument validation and coercion.

| Legacy name | Advertised replacement |
|---|---|
| `publish_node_birth` | `publish_sparkplug` |
| `publish_node_death` | `publish_sparkplug` |
| `publish_device_birth` | `publish_sparkplug` |
| `publish_device_death` | `publish_sparkplug` |
| `publish_node_data` | `publish_sparkplug` |
| `publish_device_data` | `publish_sparkplug` |
| `publish_node_command` | `publish_sparkplug` |
| `publish_device_command` | `publish_sparkplug` |

Every tool returns `{success, data, error, meta}`. On the wire, `success:false`
sets MCP `isError:true`; structuredContent and the JSON text describe the same
result. Writes preserve `meta.outcome` and `meta.request_sent` from the native
adapter. A timeout/cancellation after possible dispatch is uncertain, never
converted to not_sent. There is no new generic retry loop. In particular,
retrying an unknown control outcome can apply a physical action twice.

## Bounded requests and failure classification

Every request is checked before route selection, argument forwarding, schema
validation or admission. The contract declares `maxNestingDepth: 32`,
`maxContainerItems: 10000` and `maxTotalValues: 50000`. The configured
`max_request_bytes` limit also applies to the exact JSON encoding. An iterative
walk rejects deep or cyclic objects, oversized containers, non-finite numbers,
non-string object keys and invalid Unicode without recursive traversal. The
MCP SDK has already decoded its frame; these bounds limit subsequent application
work rather than the transport decoder's initial allocation.

Canonical tools, including consolidated selector modes, validate against their
complete JSON schema. Legacy aliases retain their native validation and coercion
for compatibility. Unknown top-level arguments are
rejected for every route before an SDK model can discard a misspelled option.

Only explicit argument validation before invocation can establish `not_sent`.
An unexpected exception from a running handler, including a Pydantic validation
failure while processing a device reply, returns `meta.outcome: "unknown"` and
`meta.request_sent: null` for a control operation. Its exception payload stays
out of the MCP error response. Native adapters retain their more precise send
tracking for anticipated failures. No generic failure path resends a command.
Audit-result failures preserve the observed outcome, and JSON text and
structured content are regenerated together after any `meta.audit_error` flag.


Each tool's `outputSchema` embeds the shape of its `data` (`resultShapes`,
named per tool in `routes -> resultShape`) and the meta fields it reports.
Shapes list the fields a result always or may carry; clients should ignore
fields they do not know. A failed result always carries `meta.code`, one of
the contract's `errors`, each with what it means and what to do; clients
branch on the code, never on the wording of `error`. The test suite checks
every result it produces, failures included, against the advertised
`outputSchema`.

## Deployment policy and audit

Set `MQTT_POLICY_FILE` to an explicit JSON policy file. Unknown fields
and invalid types fail at server construction. Without a policy file the native
protocol write gates continue to apply; writes are disabled by default.

```json
{
  "read_only": true,
  "allowed_tools": null,
  "allowed_operations": null,
  "max_in_flight": 64,
  "max_request_bytes": 1048576,
  "audit_path": null
}
```

`allowed_tools` restricts canonical tool names; `allowed_operations` restricts
the concrete handler names in the contract's routes. Null means unrestricted;
an empty array denies everything. Legacy names map to the same canonical
policy, so they cannot bypass it. `read_only` also denies non-read-only
operations such as scans or event-consuming polls. This is tool/operation
policy, not resolved-address or engineering-value authorization. Existing
protocol maps, value limits and write/system gates remain authoritative for
those checks. No new OPC UA-style namespace or physical interlock semantics
are implied for these different protocols.

Admission rejects excess calls immediately, with one additional slot reserved
for a local read-only diagnostic. It does not create an unbounded wait queue.
The native adapter still owns ordering and pending native I/O. Cancellation
releases the MCP admission slot; a cancelled worker thread is not assumed to
have stopped, and native send fences remain responsible for that case.

When `audit_path` is set, a write intent is appended and fsynced before dispatch.
Failure to persist that intent refuses dispatch. The final observed outcome is
appended with the same call ID; a missing result after process failure means
unknown, not success. Failure to persist a result is logged and marked
`meta.audit_error`, without resending the operation or changing its outcome.
Audit records omit payloads and credentials. Manage file retention externally;
this append-only log is not a tamper-proof external audit service.

Policy documents are read only from regular files, with a 64 KiB read bound and
at most 16 JSON nesting levels. Duplicate fields are rejected. The size bound
still applies if a file grows during the read. Audit destinations must also be
regular files; devices and pipes are refused before any audit record is written.
A FIFO cannot block policy loading or audit admission while waiting for a peer.

## Protocol capability limits

MQTT uses paho-mqtt. Plain publish_message and typed Sparkplug publish_sparkplug remain distinct. QoS acknowledgements describe transport delivery, not physical command completion. Native session restart and cancellation logic controls queued publications; the MCP layer must not serialize away that behavior.

Subscription and unsubscription requests check cancellation, shutdown and the
call deadline immediately before handing a packet to Paho. Cancellation while
waiting for acknowledgement releases the pending future. An already dispatched
subscription request can still take effect at the broker; cancelling the waiter
does not undo it. CLI policy loading occurs after `--env-file` is merged, with
the process environment retaining precedence, so configured restrictions apply
to the same invocation as its broker settings. Invalid policy fails startup.

## Packages

```sh
uvx --from industriagents-mqtt-mcp==0.3.0 mqtt-mcp
npx -y industriagents-mqtt-mcp@0.3.0
```

The distribution is deliberately prefixed: several generic protocol names on
PyPI belong to other projects. Python import names and command names remain
`mqtt_mcp` and `mqtt-mcp`. npm requires Node 20+ and uv; uv provisions a
compatible Python on first run. npm bundles a version-checked wheel and passes
stdio, signals, environment and CLI arguments through to it. Network access is
needed on first use to resolve Python dependencies. Native drivers/permissions
are still required for live operation. The mock backend is not selected merely
because a native dependency or device is unavailable.

## Verification and release

The regression suite retains native-send counting, reply-loss, cancellation,
configuration-gate and stdio checks. New architecture tests exercise all
consolidated routes, unknown/mixed arguments, legacy-policy bypass, independent
admission state, durable intent failure and MCP error envelopes. npm tests
check the bundled version, argument boundaries, exit status and missing-uv
errors. See [release.md](release.md) for the registry workflow.
