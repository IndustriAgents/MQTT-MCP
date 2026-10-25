from typing import Any

from mcp.server.fastmcp import Context
from pydantic import StrictBool, StrictInt

from ..application import diagnostics as diagnostics_operations
from ..application import read as read_operations
from ..application import write as write_operations
from ..protocol.hints import _LOCAL, _WRITE, _metrics_doc


def register_tools(mcp, context_for):

    @mcp.tool(annotations=_LOCAL)
    def get_broker_info(ctx: Context) -> dict[str, Any]:
        """Report the broker connection and this server's configuration and state.

        data: broker URL (credentials never shown), whether TLS is on, the client ID,
        whether the server is connected right now and the last connection error,
        whether writes are enabled (MQTT_WRITES_ENABLED), the subscriptions held, the
        number of buffered received messages, and the state of this server's own
        Sparkplug B edge node (born or not, bdSeq, devices born). Does not contact
        the broker.
        """
        return diagnostics_operations.get_broker_info(context_for(ctx))

    @mcp.tool(annotations=_WRITE)
    async def subscribe_topic(ctx: Context, topic: str, qos: StrictInt = 0) -> dict[str, Any]:
        """Subscribe this server to an MQTT topic filter so that matching messages are kept.

        Received messages go into a buffer that read_messages returns (the newest
        MQTT_MESSAGE_BUFFER_SIZE, default 1000); Sparkplug B messages also update
        list_sparkplug_nodes. A broker sends retained messages for the filter right
        away. Subscriptions are restored automatically after a reconnect.

        Parameters:
            topic: topic filter; '+' matches one level, '#' the rest (e.g. 'sensors/#',
                'spBv1.0/#'). Filters starting with '$' other than '$SYS/' are broker
                extensions such as shared ($share/, $queue/) or exclusive ($exclusive/)
                subscriptions that change what other clients receive; they are refused
                unless writes are enabled.
            qos: maximum QoS for delivery to this server: 0, 1 or 2 (default 0).

        data: the filter and the QoS the broker granted (success only after SUBACK).
        """
        return await read_operations.subscribe_topic(context_for(ctx), topic, qos)

    @mcp.tool(annotations=_WRITE)
    async def unsubscribe_topic(ctx: Context, topic: str) -> dict[str, Any]:
        """Remove one of this server's subscriptions (exactly as listed by list_subscriptions).

        Already buffered messages stay readable. Fails if this server holds no
        subscription with that exact filter.

        Parameters:
            topic: the subscribed topic filter, e.g. 'sensors/#'.
        """
        return await read_operations.unsubscribe_topic(context_for(ctx), topic)

    @mcp.tool(annotations=_LOCAL)
    def list_subscriptions(ctx: Context) -> dict[str, Any]:
        """List the subscriptions this server holds, with the QoS the broker granted.

        data: a list of {topic, qos}. Only this server's own subscriptions; the
        broker's other clients are not visible. Does not contact the broker.
        """
        return diagnostics_operations.list_subscriptions(context_for(ctx))

    @mcp.tool(annotations=_LOCAL)
    def read_messages(
        ctx: Context,
        topic_filter: str | None = None,
        limit: StrictInt = 50,
        since_id: StrictInt | None = None,
    ) -> dict[str, Any]:
        """Return messages this server has received on its subscriptions.

        Only messages that arrived after subscribe_topic are available, and only the
        newest MQTT_MESSAGE_BUFFER_SIZE (default 1000) are kept. Each message has an
        increasing id, topic, qos, retain flag, received_at (UTC), payload (UTF-8 text,
        or hex for binary and Sparkplug payloads) and, for Sparkplug B topics, the
        decoded payload under 'sparkplug'.

        Parameters:
            topic_filter: only messages whose topic matches this filter (+/# allowed).
            limit: at most this many messages, 1-1000 (default 50).
            since_id: only messages with an id greater than this, oldest first; pass
                meta.last_id from the previous call to poll for new messages. Without
                since_id the newest `limit` messages are returned (oldest first).

        meta: returned, has_more, buffered, dropped_total, last_id, and
        missed_messages=true if messages after since_id were already dropped. Ids
        start again at 1 when the server restarts.
        """
        return read_operations.read_messages(context_for(ctx), topic_filter, limit, since_id)

    @mcp.tool(annotations=_LOCAL)
    def list_sparkplug_nodes(ctx: Context) -> dict[str, Any]:
        """List Sparkplug B edge nodes and devices seen in messages this server received.

        Discovery is passive: a node appears only after one of its NBIRTH, NDATA,
        NDEATH, DBIRTH, DDATA or DDEATH messages arrives on a subscription this
        server holds (subscribe to 'spBv1.0/#', or 'spBv1.0/<group>/#', first). Births
        are not retained, so a node that was born before the subscription shows up
        with its next data message; metrics that its data messages send without a
        datatype then show their raw stored value until the node is reborn.

        data.nodes: per node group_id, edge_node_id, online (true/false, or null if
        unknown), last_message_type, last_seen, bd_seq, the latest metric values, and
        its devices with the same fields. data.this_server: the edge node this server
        itself publishes as (see publish_node_birth). Does not contact the broker.
        """
        return diagnostics_operations.list_sparkplug_nodes(context_for(ctx))

    @mcp.tool(annotations=_LOCAL)
    def decode_sparkplug_payload(payload_hex: str) -> dict[str, Any]:
        """Decode a hex-encoded Sparkplug B protobuf payload (as read_messages shows it).

        Parameters:
            payload_hex: the payload bytes as hex (spaces and a 0x prefix are ignored).

        data: timestamp, seq (null if absent), and metrics with name, alias,
        timestamp, datatype and value. Signed integer types are shown as signed
        numbers when the metric carries its datatype; a metric without datatype
        (allowed in DATA and CMD messages) shows its raw stored value. Fails with
        success:false if the hex is invalid or the bytes cannot be parsed as the
        Sparkplug B protobuf (protobuf cannot tell every other byte string apart, so
        success does not prove the bytes came from a Sparkplug node).
        """
        return diagnostics_operations.decode_sparkplug_payload(payload_hex)

    @mcp.tool(annotations=_WRITE)
    async def publish_message(
        ctx: Context, topic: str, payload: str, qos: StrictInt = 0, retain: StrictBool = False
    ) -> dict[str, Any]:
        """WRITE: publish a UTF-8 text message to an MQTT topic.

        Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
        nothing.

        Any subscriber gets the message, and on a plant network that can include
        devices that act on it, so this is gated like the Sparkplug commands. With
        retain=true the broker keeps the message and hands it to every future
        subscriber.

        Parameters:
            topic: topic name without wildcards, e.g. 'sensors/temp'.
            payload: message text (sent as UTF-8).
            qos: 0, 1 or 2 (default 0).
            retain: keep the message on the broker as the topic's retained message.

        Sent at most once: success:true (meta.outcome "written") means QoS 0: written
        to the network (the broker does not acknowledge QoS 0); QoS 1/2: the broker
        acknowledged it (an MQTT 3.1.1 broker acknowledges even a publish its ACL
        drops). meta.outcome "not_sent" means nothing left the server. "unknown"
        means it may have reached the broker: the server does not send it again, so
        check before retrying. If the connection drops while the call waits for the
        acknowledgement, the message is given up (never resent on reconnect).
        """
        return await write_operations.publish_message(context_for(ctx), topic, payload, qos, retain)

    @mcp.tool(annotations=_WRITE)
    @_metrics_doc
    async def publish_node_birth(
        ctx: Context, metrics: list[dict[str, Any]] | None = None
    ) -> dict[str, Any]:
        """WRITE: make this server a Sparkplug B edge node and publish its NBIRTH.

        Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
        nothing.

        The node is SPARKPLUG_GROUP_ID/SPARKPLUG_EDGE_NODE_ID (see get_broker_info);
        it must not be the ID of a real edge node. If no NDEATH will is registered
        on the current connection yet, the server first reconnects with an NDEATH
        Last Will carrying a new bdSeq, so the broker announces the node's death if
        the server disappears. The NBIRTH has seq 0 and automatically includes the
        'bdSeq' and 'Node Control/Rebirth' (false) metrics. Publishing NBIRTH again
        is a rebirth: devices must then be born again. The server does not act on
        commands (including rebirth requests) sent to its own node. After an
        unexpected disconnect the node is dead and needs a new NBIRTH.

        Parameters:
            metrics: the node's metrics (optional). {METRICS_HELP}

        data: topic, seq, bd_seq, metric count and delivery (QoS 0, as the
        specification requires for NBIRTH).
        """
        return await write_operations.publish_node_birth(context_for(ctx), metrics)

    @mcp.tool(annotations=_WRITE)
    async def publish_node_death(ctx: Context) -> dict[str, Any]:
        """WRITE: publish this server's NDEATH (edge node goes offline).

        Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
        nothing.

        Only possible after publish_node_birth. The payload carries the bdSeq of the
        NBIRTH and no seq, as the specification requires; it is sent with QoS 1 and
        confirmed by the broker. All of the node's devices count as dead afterwards.
        The server then reconnects without the NDEATH will, so that the broker does
        not announce the death a second time; the next NBIRTH uses a new bdSeq.
        """
        return await write_operations.publish_node_death(context_for(ctx))

    @mcp.tool(annotations=_WRITE)
    @_metrics_doc
    async def publish_device_birth(
        ctx: Context, device_id: str, metrics: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """WRITE: publish a DBIRTH for a device of this server's edge node.

        Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
        nothing.

        Needs a born node (publish_node_birth first). Uses the node's next seq.

        Parameters:
            device_id: device ID (no '+', '/' or '#').
            metrics: all metrics of the device (may be empty). {METRICS_HELP}
        """
        return await write_operations.publish_device_birth(context_for(ctx), device_id, metrics)

    @mcp.tool(annotations=_WRITE)
    async def publish_device_death(ctx: Context, device_id: str) -> dict[str, Any]:
        """WRITE: publish a DDEATH for a device born with publish_device_birth.

        Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
        nothing.

        Parameters:
            device_id: a device this server has published a DBIRTH for.
        """
        return await write_operations.publish_device_death(context_for(ctx), device_id)

    @mcp.tool(annotations=_WRITE)
    @_metrics_doc
    async def publish_node_data(ctx: Context, metrics: list[dict[str, Any]]) -> dict[str, Any]:
        """WRITE: publish NDATA (new values of this server's node metrics).

        Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
        nothing.

        Needs a born node (publish_node_birth first). Uses the node's next seq.

        Parameters:
            metrics: at least one metric. {METRICS_HELP}
        """
        return await write_operations.publish_node_data(context_for(ctx), metrics)

    @mcp.tool(annotations=_WRITE)
    @_metrics_doc
    async def publish_device_data(
        ctx: Context, device_id: str, metrics: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """WRITE: publish DDATA (new values of a device born by this server).

        Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
        nothing.

        Needs a DBIRTH for the device (publish_device_birth). Uses the node's next seq.

        Parameters:
            device_id: a device this server has published a DBIRTH for.
            metrics: at least one metric. {METRICS_HELP}
        """
        return await write_operations.publish_device_data(context_for(ctx), device_id, metrics)

    @mcp.tool(annotations=_WRITE)
    @_metrics_doc
    async def publish_node_command(
        ctx: Context, edge_node_id: str, metrics: list[dict[str, Any]], group_id: str | None = None
    ) -> dict[str, Any]:
        """WRITE: send a Sparkplug B NCMD to an edge node.

        Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
        nothing.

        A command can change real equipment: the edge node applies the metrics it
        receives. It is sent at most once. success:true (meta.outcome "written") only
        means the NCMD was written to the network (QoS 0, as the specification
        requires); it does not mean the node applied it. Watch its NDATA
        (read_messages) to confirm. meta.outcome "unknown" means it may have gone out:
        check before retrying. Example: {"name": "Node Control/Rebirth",
        "value": true, "type": "boolean"} asks a node to republish its births.

        Parameters:
            metrics: at least one metric to write. {METRICS_HELP}
            edge_node_id: target edge node (required; there is no default target).
            group_id: target group (default: SPARKPLUG_GROUP_ID).
        """
        return await write_operations.publish_node_command(
            context_for(ctx), edge_node_id, metrics, group_id
        )

    @mcp.tool(annotations=_WRITE)
    @_metrics_doc
    async def publish_device_command(
        ctx: Context,
        edge_node_id: str,
        device_id: str,
        metrics: list[dict[str, Any]],
        group_id: str | None = None,
    ) -> dict[str, Any]:
        """WRITE: send a Sparkplug B DCMD to a device of an edge node.

        Requires MQTT_WRITES_ENABLED=true; otherwise returns success:false and publishes
        nothing.

        A command can change real equipment: the edge node applies the metrics to
        the device. It is sent at most once. success:true (meta.outcome "written")
        only means the DCMD was written to the network (QoS 0, as the specification
        requires); it does not mean the device applied it. Watch its DDATA
        (read_messages) to confirm. meta.outcome "unknown" means it may have gone out:
        check before retrying.

        Parameters:
            device_id: target device ID.
            metrics: at least one metric to write. {METRICS_HELP}
            edge_node_id: edge node the device belongs to (required; no default target).
            group_id: target group (default: SPARKPLUG_GROUP_ID).
        """
        return await write_operations.publish_device_command(
            context_for(ctx), edge_node_id, device_id, metrics, group_id
        )

    from .contract import install_contract

    install_contract(mcp)
