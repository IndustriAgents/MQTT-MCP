# 0.3.0 local release verification

Verified on 2026-10-24. Package: `industriagents-mqtt-mcp`.

These are local development builds from the current working tree, using the
existing 0.3.0 package manifests. Nothing was published to a registry.

## Checks

- Full Python regression run: 265 passed, using a local Mosquitto broker.
- Ruff lint and formatting checks passed for source and tests, including mock sources.
- `uv lock --check` and contract/package version parity passed.
- Three npm launcher tests passed.
- Installed wheel and installed npm tarball both initialized over stdio outside
  the checkout, reported the manifest version and listed 9 canonical tools.
  Their schemas matched the contract; invalid mode calls returned MCP errors.
- `twine check --strict` passed for the wheel and source distribution.
- Workspace artifact verification matched every wheel Python source to the
  current checkout and the packaged contract to its reviewed copy. The npm
  tarball contains exactly the same wheel bytes as the Python artifact.
- `git diff --check` passed.

No tests were skipped in the full regression run.

New regressions cover request depth/size/JSON bounds before dispatch, unknown
arguments on legacy/direct routes, cancellation cleanup, audit-result failures
for structured and text-only results, and runtime/validation exceptions after
handler dispatch. The latter preserve unknown control outcomes without retries.
Application/domain imports are checked against native driver and MCP dependencies.

The deadline-boundary test records the actual server deadline of all 20
concurrent publications using a test-only launcher. It releases the broker
connection after the latest deadline and still asserts that no publication
arrives and every outcome is `not_sent`. This replaces the earlier assumption
that client enqueue time equals server admission time; production send and
timeout behavior is unchanged.

The follow-up review added eight subscription lifecycle regressions covering
cancelled acknowledgement waiters and cancellation, shutdown and deadline gates
before native subscribe/unsubscribe. Three CLI regressions verify policy loading
after the explicit env-file merge, process-environment precedence, and refusal
of invalid policy before serving. The final suite also includes nine
regular-file audit and bounded-policy loading checks.

The final merge also preserves the upstream agent-facing argument descriptions,
canonical schema validation, structured result shapes and documented error codes.
Unexpected control failures explicitly allow `request_sent: null` in those output
schemas; post-dispatch regressions validate their complete advertised envelopes.

These are software and broker/mock checks, not physical-device certification.
See [protocol capability limits](architecture.md#protocol-capability-limits).

## Artifact SHA-256

Artifacts are local build outputs in `dist/` and `npm/`, excluded from Git.
Rebuilding can change archive hashes; these identify the verified files.

| Artifact | SHA-256 |
|---|---|
| `industriagents_mqtt_mcp-0.3.0-py3-none-any.whl` | `d38bdd35e58f492b8efbcd096abacd2dcc7fbe09c8b51bab59e0b3abb830e1c9` |
| `industriagents_mqtt_mcp-0.3.0.tar.gz` | `bf48c3aca186be0f08cd72e6a5eac3caed60704a0fc00dd1b3bb71da86cd0973` |
| `industriagents-mqtt-mcp-0.3.0.tgz` | `7e89b0d1373d82714182c46641cc18b80576244938138ff1ed4d6aaf014c7780` |

See [release instructions](release.md) for independent registry publication.
