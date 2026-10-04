## Summary

<!-- What does this PR do, and why? -->

## Part(s) affected

- [ ] MCP server (`mqtt-python`)
- [ ] Mock edge node (`mqtt-mock-server`)
- [ ] Docs / CI only

## Type of change

- [ ] Bug fix
- [ ] New tool
- [ ] Documentation
- [ ] Refactor / chore

## Does this change what can be published to the broker?

- [ ] No
- [ ] Yes. Describe the new capability (topics, Sparkplug `NCMD`/`DCMD`,
      retained messages) and how it is gated:

## Checklist

- [ ] The CI checks pass locally (see [CONTRIBUTING.md](https://github.com/IndustriAgents/MQTT-MCP/blob/main/CONTRIBUTING.md#checks-ci-runs)).
- [ ] The mock edge node can exercise the change, and I tested against it with a local broker.
- [ ] Nothing was tested against a production broker or equipment.
- [ ] `stdout` is still clean: the server logs only to `stderr`.
- [ ] If I changed dependencies, I updated and committed the project's `uv.lock`.
- [ ] I updated the README(s) where relevant.

## How to test

<!-- The exact commands and tool calls a reviewer can run against the mock. -->
