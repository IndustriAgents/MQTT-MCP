# Security Policy

## What this server is, in security terms

The MQTT MCP server is a bridge between a language model and an MQTT broker. It
speaks MCP over stdio to its client and MQTT to the broker, which means the
trust boundary sits *inside* the server: whatever the model decides to publish,
the broker delivers to every subscriber on that topic. On a plant network those
subscribers include edge nodes and PLC gateways that act on Sparkplug B
`NCMD`/`DCMD` commands, so a published message can move physical equipment.

MQTT can be secured with TLS, authentication and topic ACLs on the broker.
Whether it *is* secured is a property of your deployment. Be aware of what this
server does and does not support today:

- **No TLS.** Connections are plain TCP. The `mqtts://` prefix in
  `MQTT_BROKER_URL` is accepted, but no TLS is set up; it is treated like
  `mqtt://`, and the default port is still 1883. Anything sent, including the
  username and password, crosses the network in clear text.
- **Username and password only.** `MQTT_USERNAME` and `MQTT_PASSWORD` are used
  only when *both* are set; a username on its own is silently ignored. Client
  certificates are not supported.
- **No read-only mode.** Every tool is always exposed, including
  `publish_message`, `publish_node_command` and `publish_device_command`. The
  only place to limit what the model can publish is the broker: give the
  server's MQTT account an ACL that allows exactly the topics you intend.

Treat the server as equipment on the control network, not as an application on
the office network.

## Running it safely

- **Start against the mock.** `mqtt-mock-server/` simulates a Sparkplug B edge
  node so that an agent can be exercised end to end without touching real
  plant. Run it against a local broker that you start yourself, which needs no
  credentials and is reachable only from localhost.
- **Use a dedicated broker account with ACLs.** It should have the narrowest
  publish rights that still work. Grant `NCMD`/`DCMD` and retained publishes
  only if you mean the agent to command devices. There is no undo, and a
  language model will call a tool it has been given.
- **Keep credentials out of files you commit.** Pass `MQTT_USERNAME` and
  `MQTT_PASSWORD` through the MCP client's `env` block or the environment,
  never through the repository.
- **Do not expose the server beyond the host running the client.** It is a
  stdio process meant to run beside the MCP client, not a network service.
- **Do not send credentials over an untrusted network.** Until TLS is
  supported, put the broker on the same host or a segmented control network, or
  reach it through a tunnel you trust.
- **Never point it at a safety system.** Safety-instrumented functions are out
  of scope for anything in this repository.

## Supported versions

There are no release branches. Fixes land on `main`, and the copy shipped in
the [IndustriConnect](https://github.com/IndustriAgents/IndustriConnect) suite
(the `MQTT-Project` submodule) follows `main`.

## Reporting a vulnerability

Please report privately, through
[GitHub private vulnerability reporting](https://github.com/IndustriAgents/MQTT-MCP/security/advisories/new),
or by email to hi@industriagents.com.

Please do not open a public issue for a vulnerability.

Include the version or commit, what an attacker would gain, and a
reproduction if you have one. We will acknowledge within a week and keep you
updated as we work on a fix.

## Scope

In scope: the MCP server (`mqtt-python/`) and the mock edge node
(`mqtt-mock-server/`) in this repository.

Out of scope: weaknesses of MQTT or Sparkplug B themselves, vulnerabilities in
the broker you run, and vulnerabilities in third-party libraries such as
`paho-mqtt` or the `mcp` SDK. Report those upstream.
