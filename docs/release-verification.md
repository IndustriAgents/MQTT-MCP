# 0.3.0 local release verification

Verified on 2026-10-24. Package: `industriagents-mqtt-mcp`.

These are local development builds from the current working tree, using the
existing 0.3.0 package manifests. Nothing was published to a registry.

## Checks

- Full Python regression run: 240 passed, using a local Mosquitto broker.
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

These are software and broker/mock checks, not physical-device certification.
See [protocol capability limits](architecture.md#protocol-capability-limits).

## Artifact SHA-256

Artifacts are local build outputs in `dist/` and `npm/`, excluded from Git.
Rebuilding can change archive hashes; these identify the verified files.

| Artifact | SHA-256 |
|---|---|
| `industriagents_mqtt_mcp-0.3.0-py3-none-any.whl` | `f85a987b2e3817b6798728bae159ae88b6ba523e27fb1c141f6bb4c43dd7b934` |
| `industriagents_mqtt_mcp-0.3.0.tar.gz` | `da1d44c36c4e3a892e87bb9c49260ba653d39ad1009340473ef3ad06a5def03a` |
| `industriagents-mqtt-mcp-0.3.0.tgz` | `c947f5ff00d7ee499bdac8c5ed19fbc93d80604dfbacbdf662f7487de5efcb9b` |

See [release instructions](release.md) for independent registry publication.
