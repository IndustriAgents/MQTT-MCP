"""Backend support; enabled is the native configuration gate, not authorization."""


def capabilities(writes_enabled: bool) -> dict:
    names = (
        "publish_sparkplug",
        "get_broker_info",
        "subscribe_topic",
        "unsubscribe_topic",
        "list_subscriptions",
        "read_messages",
        "list_sparkplug_nodes",
        "decode_sparkplug_payload",
        "publish_message",
    )
    return {
        "protocol": "mqtt",
        "backend": "paho-mqtt",
        "simulated": False,
        "scope": "backend_support",
        "device_verified": False,
        "operations": {
            name: {
                "supported": True,
                "enabled": writes_enabled
                if name in ("publish_message", "publish_sparkplug")
                else True,
            }
            for name in names
        },
        "prerequisites": [
            "Configured MQTT 3.1.1 TCP/TLS broker",
            "Broker topic ACLs permit the operation",
        ],
        "limitations": [
            "No MQTT 5 properties, durable client session, or WebSocket transport",
            "Broker acknowledgement does not establish physical command completion",
            "Sparkplug scalar subset; not Sparkplug conformance certification",
            "enabled describes native configuration, not MCP policy or broker permission",
        ],
    }
