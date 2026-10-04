---
name: Feature request
about: Suggest a new tool or an improvement
title: "[Feature] "
labels: enhancement
assignees: ""
---

**What problem does this solve?**
The use case, from the point of view of someone asking an agent to do something.

**Which part?**
- [ ] MCP server (`mqtt-python`, `mqtt-mcp`)
- [ ] Mock edge node (`mqtt-mock-server`)

**Proposed solution**
If it is a new tool: its name, arguments, and what it returns. Tool names and
arguments are kept consistent across the servers in
[IndustriConnect](https://github.com/IndustriAgents/IndustriConnect), so say
how it lines up with the equivalent tool elsewhere in the suite.

**Does it publish to the broker?**
- [ ] No, it is read-only
- [ ] Yes, it publishes (say whether that includes Sparkplug `NCMD`/`DCMD` or
      retained messages, which can change physical equipment)

**Can the mock exercise it?**
What the mock edge node would need to simulate for this to be testable without
real plant.

**Additional context**
Links to the MQTT or Sparkplug B specification, example brokers or devices, or
related issues.
