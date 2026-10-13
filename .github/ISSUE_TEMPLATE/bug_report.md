---
name: Bug report
about: Report a problem with the MQTT MCP server or the mock edge node
title: "[Bug] "
labels: bug
assignees: ""
---

**Which part?**
- [ ] MCP server (`mqtt-python`, `mqtt-mcp`)
- [ ] Mock edge node (`mqtt-mock-server`)

**Describe the bug**
What happens, and what you expected instead.

**To reproduce**
The MCP client you used (Claude Desktop / Claude Code / Cursor / Inspector /
IndustriConnect's mcp-manager-ui), the tool you called, and the arguments you
passed.

1.
2.
3.

**Tool output**
```
paste the tool output or the error
```

**What was on the other end?**
- Broker (Mosquitto / EMQX / HiveMQ / ...) and version:
- [ ] The mock edge node from this repo
- [ ] Real edge nodes or devices. Vendor and model:

**Environment**
- OS:
- Python version:
- Commit SHA:
- Relevant settings (`MQTT_BROKER_URL`, `MQTT_WRITES_ENABLED`,
  `SPARKPLUG_GROUP_ID`, `SPARKPLUG_EDGE_NODE_ID`, which `MQTT_TLS_*` are set).
  Do not paste passwords or keys:
- Output of `get_broker_info`, if the server starts:

**Additional context**
Topics, payloads (hex for Sparkplug B), broker logs and stderr from the server:
whatever would let someone else reproduce it against the mock.
