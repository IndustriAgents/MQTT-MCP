# 0.3.0 local release verification

Verified on 2026-10-15. Package: `industriagents-mqtt-mcp`.

This records local build verification, not registry publication. Check the
GitHub Actions runs and registry version pages for publication status. Registry
authentication/trusted publishers must be configured before release; GitHub
repository access alone does not authorize registry publication.

## Checks

- Full Python regression run: 204 passed.
- Final architecture/contract tests: 16 passed (a separate run, not additional unique tests).
- Ruff lint and formatting checks passed for source and tests.
- `uv lock --check` and contract/package version parity passed.
- Three npm launcher tests passed.
- Installed wheel and installed npm tarball both initialized over stdio outside
  the checkout, reported version 0.3.0 and listed 9 canonical tools.
  Their schemas matched the contract; invalid mode calls returned MCP errors.
- `twine check --strict` passed for the wheel and source distribution.
- The npm tarball contains exactly the same wheel bytes as the PyPI artifact.
- `git diff --check` passed.

No tests were skipped in the full regression run.

These are software and mock/loopback checks, not physical-device certification.
See [protocol capability limits](architecture.md#protocol-capability-limits).

## Artifact SHA-256

Artifacts are local build outputs in `dist/` and `npm/`, excluded from Git.
Rebuilding can change archive hashes; these identify the verified files.

| Artifact | SHA-256 |
|---|---|
| `industriagents_mqtt_mcp-0.3.0-py3-none-any.whl` | `6481ab76312f41f3282b0567652e5a50ae279c51b6bf54615ad63467eaf858f0` |
| `industriagents_mqtt_mcp-0.3.0.tar.gz` | `5a4ba56f7fd89435dc9e233133cc063eb754d452c391c506f41537d75b043581` |
| `industriagents-mqtt-mcp-0.3.0.tgz` | `4cf676215f4a8ffc421596e39616bbfcacb0e2cf5bea1e41565ca75f38c8e40c` |

See [release instructions](release.md) for independent registry publication.
