# Security Policy

## What this server is, in security terms

The MQTT MCP server is a bridge between a language model and an MQTT broker. It
speaks MCP over stdio to its client and MQTT to the broker, which means the
trust boundary sits *inside* the server: whatever the model decides to publish,
the broker delivers to every subscriber on that topic. On a plant network those
subscribers include edge nodes and PLC gateways that act on Sparkplug B
`NCMD`/`DCMD` commands, so a published message can move physical equipment.

MQTT can be secured with TLS, authentication and topic ACLs on the broker.
Whether it *is* secured is a property of your deployment. What this server
supports:

- **Read-only by default.** Every tool that publishes (`publish_message`, the
  Sparkplug birth/data/death tools, `publish_node_command` and
  `publish_device_command`) returns `success: false` and sends nothing unless
  the server runs with `MQTT_WRITES_ENABLED=true`. All publishes are gated, not
  only commands: on MQTT any topic can be acted on by a subscriber, retained
  messages persist, and Sparkplug births and data change what host
  applications show. Shared subscriptions (`$share/`, `$queue/`), which divert
  messages from other consumers, are refused while writes are off.
- **TLS for `mqtts://` URLs.** The broker certificate is verified against the
  system trust store or `MQTT_TLS_CA_FILE`, including the hostname. Mutual TLS
  works with `MQTT_TLS_CERT_FILE`/`MQTT_TLS_KEY_FILE`. `MQTT_TLS_INSECURE=true`
  turns verification off and is meant for testing against a self-signed broker
  only. `mqtt://` URLs are plain TCP, where everything, including the username
  and password, crosses the network in clear text; setting a TLS option with an
  `mqtt://` URL is a configuration error rather than a silent downgrade.
- **Username, with or without password.** `MQTT_USERNAME` is sent on its own if
  no `MQTT_PASSWORD` is set. Credentials in `MQTT_BROKER_URL` are refused, and
  `get_broker_info` never shows the password.
- **The broker is still the real boundary.** With writes enabled, the server
  can publish to any topic its MQTT account may publish to. Give that account
  an ACL that allows exactly the topics you intend.
- **Read-only still reads.** Whatever the account may subscribe to (including
  `#`) can end up in the model's context through `read_messages`. Restrict
  subscribe rights too if some topics are sensitive, and treat received
  payloads as untrusted input: anyone who can publish to a subscribed topic can
  put text in front of the model.

Treat the server as equipment on the control network, not as an application on
the office network.

## Running it safely

- **Start against the mock.** `mqtt-mock-server/` simulates a Sparkplug B edge
  node so that an agent can be exercised end to end without touching real
  plant. Run it against a local broker that you start yourself, which needs no
  credentials and is reachable only from localhost.
- **Leave writes off unless the agent is meant to change things.** Enable
  `MQTT_WRITES_ENABLED` per deployment, deliberately. There is no undo, and a
  language model will call a tool it has been given.
- **Use a dedicated broker account with ACLs.** It should have the narrowest
  publish rights that still work. Grant `NCMD`/`DCMD` and retained publishes
  only if you mean the agent to command devices.
- **Keep credentials out of files you commit.** Pass `MQTT_USERNAME` and
  `MQTT_PASSWORD` through the MCP client's `env` block or the environment,
  never through the repository.
- **Do not expose the server beyond the host running the client.** It is a
  stdio process meant to run beside the MCP client, not a network service.
- **Use `mqtts://` beyond localhost.** Do not send credentials over an
  untrusted network in plain TCP, and do not use `MQTT_TLS_INSECURE` outside a
  test bench.
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
