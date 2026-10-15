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
| `industriagents_mqtt_mcp-0.3.0-py3-none-any.whl` | `e5f52521e9b65ba8b27b1caf46eac5f8932b6112a26297c70155eed213d1aaf0` |
| `industriagents_mqtt_mcp-0.3.0.tar.gz` | `3a6f0d5b90cb30a1e794addd5e59e0362fe9a9a7346eb3625da204a3a2501aa4` |
| `industriagents-mqtt-mcp-0.3.0.tgz` | `8f175bd57ddf02602c31968d01814f07258e0f48c396876ea48d6024977a84c9` |

See [release instructions](release.md) for independent registry publication.
