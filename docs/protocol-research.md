# MQTT and Sparkplug protocol research and capability assessment

Reviewed 2026-10-25 against the configured **MQTT 3.1.1** implementation,
paho-mqtt **2.x**, Sparkplug **3.0.0** documentation, and the OPCUA-MCP execution
reference. The server implements a documented subset, not a certification profile.

## Primary sources and decisions

| Source | Sections used | Consequence |
|---|---|---|
| [OASIS MQTT 3.1.1, 2014-10-29](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) | CONNECT, PUBLISH, SUBSCRIBE/UNSUBSCRIBE, QoS, sessions and topic filters | Preserve broker delivery semantics separately from application action; subscription changes mutate broker/client state. |
| [Eclipse Paho Python API](https://eclipse.dev/paho/files/paho.mqtt.python/html/client.html) | Client lifecycle, callbacks, publish completion and queue bounds | Keep connection/callback/send ownership in the adapter; acknowledgements establish only the documented delivery stage. |
| [Paho known limitations](https://eclipse.dev/paho/clients/python/docs/) | Session persistence and automatic republishes | Preserve existing withdrawal/no-replay fences instead of adding a generic retry loop. |
| [Sparkplug 3.0.0, 2022-11-16](https://sparkplug.eclipse.org/specification/version/3.0/documents/sparkplug-specification-3.0.0.pdf) | Topic namespace, births/deaths, metric aliases and sequence/state management | Birth-derived metadata and received metrics form observed state, not authoritative device discovery or proof of physical completion. |

Accessed 2026-10-25. Native behavior was also checked against pinned installed
code and the existing Mosquitto/TLS/fault tests. The MQTT broker routes topics;
semantic device identity comes from configured conventions or Sparkplug births,
not an invented MQTT register model.

## Capability matrix

| Workflow / tools | Implemented behavior | Verification / remaining work |
|---|---|---|
| Broker capabilities / `get_broker_info` | Local connection status, backend support, publish gate and observed registry limits | Typed output and installed wheel/npm smoke; device_verified is always false |
| Monitoring / `subscribe_topic`, `unsubscribe_topic`, `list_subscriptions`, `read_messages` | Existing ACK-aware subscriptions, restoration, cursor/buffer loss metadata; now bounded by serialized bytes as well as count | Existing broker/lifecycle tests plus `test_receive_bounds.py` |
| General publishing / `publish_message` | Existing native gate, QoS/retain validation, one attempt, precise uncertain outcome | Existing at-most-once and TCP/broker fault suites |
| Sparkplug publishing / `publish_sparkplug` | Existing eight message modes, birth/death lifecycle and scalar metrics | Existing Sparkplug and integration suites; does not execute commands addressed to this server |
| Sparkplug inspection / `list_sparkplug_nodes`, `decode_sparkplug_payload` | Existing decoders plus bounded registry, oversized/decode rejection and connection-gap reporting | New LRU/alias cleanup, accumulated-device state and decode-limit tests |
| MQTT 5 properties, expiry, response topics, WebSockets, durable client session | Not exposed | Separate version/session contract and adapter work is needed; library support alone is insufficient |
| Sparkplug complex datasets/templates, complete host behavior, conformance TCK | Not claimed | Requires expanded schema/state-machine implementation and TCK evidence |
| Physical command confirmation | Application-specific, not supplied by MQTT | Requires the target application's correlated acknowledgement/status semantics and an authorized device test |

## Receive bounds and observation semantics

Raw retained payloads remain limited to 64 KiB per message. The inbox now also
limits its serialized content to 8 MiB; both raw payload and decoded metadata
count, and evictions contribute to `dropped_total` / `missed_messages`.

Sparkplug decode input is limited to 64 KiB. The observed registry retains at
most 256 nodes and 4 MiB of serialized state including device metrics and alias
tables. It evicts least-recently-updated nodes with their aliases. A node that
alone exceeds the budget is removed. `registry` diagnostics expose limits,
evictions, rejected payloads and connection gaps. `complete` is scoped to
`observed_messages_since_start`: it never proves complete broker discovery, and
becomes false after known observation loss. The budgets bound retained application
content, not Paho's initial packet allocation or total Python heap overhead.
Caller-created subscriptions separately share a 256-entry / 256 KiB encoded
state budget across confirmed, pending and uncertain topic filters. Publisher
devices have a 256-entry / 64 KiB encoded state budget. These charge JSON-encoded
identity plus retained QoS/sequence state (maximum sequence representation), not
Python heap overhead. Diagnostics expose limits, reservations and uncertain IDs.

Admission occurs under the native callback lock before SUBSCRIBE and under the
existing Sparkplug lock before DBIRTH. Reconnect restoration reuses already
admitted confirmed subscriptions; it cannot add capacity or replay uncertain
mutations. Duplicate operations on a pending filter are refused. Known failures
release reservations. Cancellation or a lost acknowledgement releases waiter
objects but preserves a bounded uncertainty record when dispatch may have occurred.
Late subscription ACKs cannot promote expired calls to confirmed state.
Explicit unsubscribe can clear an uncertain topic; explicit device death or node
rebirth can resolve an uncertain DBIRTH. New subscribe/birth attempts for those
uncertain identities are refused. Clean-session disconnect drops unconfirmed
subscriptions with an observable loss counter and clears publisher session state.
Failed restores also count as loss. No active state is silently evicted to make
room, and admission refusal uses `resource_limit` with `outcome:not_sent`.

`test_active_state_limits.py` covers concurrent admission, encoded Unicode sizes,
known failures, lost acknowledgements, cancellation, late callbacks, explicit
cleanup, restore admission, and generation-fenced birth commits.

## Policy and migration

Subscriptions and unsubscriptions now advertise `readOnlyHint:false`; explicit
policy `read_only:true` blocks them, including shared/exclusive broker filters.
For monitoring, allow the subscription operations with `read_only:false` and
leave `MQTT_WRITES_ENABLED=false` to keep publishing disabled. Special `$` broker
extensions still require the native write gate. Reading buffered messages and
local listings remain read-only.

Capabilities describe backend support; `enabled` means native configuration only,
not broker ACLs, policy permission or endpoint availability. New configuration
checks reject non-finite deadlines. Malformed broker URL errors no longer echo
credentials, and generic unexpected-failure logs omit exception payloads.

## Lossless JSON values

Following [RFC 8259 section 6](https://www.rfc-editor.org/rfc/rfc8259#section-6),
integer scalars outside ±9007199254740991 are decimal strings in decoded payloads
and cached metrics. Metrics include `value_encoding:"decimal"`; non-finite
floating-point values use `NaN`, `Infinity` or `-Infinity` with
`value_encoding:"nonfinite"`. Wide alias/timestamp/sequence fields are also
strings when needed. Native integer datatype information is retained.
Publishers may send plain decimal strings for int64, uint64 and datetime to avoid
JSON-client rounding. Other integer types keep their existing strict numeric
validation. Tests cover direct decoding and birth-derived alias updates.
