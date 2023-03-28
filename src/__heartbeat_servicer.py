from src.protobuf import heartbeat_pb2_grpc, heartbeat_pb2
from src.__constants import HeartbeatEvent
from pprint import pprint
import src.utils as utils

from datetime import datetime

import grpc
import json
import asyncio


class HeartbeatServiceServicer(heartbeat_pb2_grpc.HeartbeatServiceServicer):
    def __init__(self, node, cluster_config, logger):
        # The current cluster node id that is using the servicer.
        self.node = node

        # Cluster configuration that has information about all the cluster nodes' parameters.
        self.cluster_config = cluster_config

        # Map of the last heartbeat timestamp from each cluster node.
        self.cluster_heartbeat_state = utils.compose_cluster_state(node=self.node, cluster_config=self.cluster_config)

        # Keep track if all the cluster nodes are alive (cluster is healthy).
        self.is_cluster_healthy = False

        self.__logger = logger

    def get_cluster_heartbeat_state(self):
        return self.cluster_heartbeat_state

    def update_cluster_health(self, is_healthy: bool):
        self.is_cluster_healthy = is_healthy

    def update_cluster_heartbeat_state(self, data):
        for peer in data.keys():
            if peer == self.node:
                continue
        pass

    # Implementation of the heartbeat RPC method
    def HeartbeatRPC(self, request, context):
        heartbeat_timestamp = datetime.now().timestamp()
        if not request.message:
            pass

        message_json = json.loads(request.message)
        event, sender, data = message_json.get("event"), message_json.get("node"), message_json.get("data")
        self.__logger.info(f"Message Received. Sender: [{sender}]. Event: [{event}].")
        # pprint(f"Data: {data}")

        self.cluster_heartbeat_state[sender]["state"]["alive"] = True
        self.cluster_heartbeat_state[sender]["state"]["hrbt-ts"] = heartbeat_timestamp

        if event == HeartbeatEvent.HEARTBEAT_PING.value:
            response_event = HeartbeatEvent.HEARTBEAT_ACK.value
        elif event == HeartbeatEvent.HEARTBEAT_CLUSTER_HEALTH.value:
            response_event = HeartbeatEvent.HEARTBEAT_ACK.value if self.is_cluster_healthy else HeartbeatEvent.HEARTBEAT_NACK.value
        else:
            response_event = HeartbeatEvent.HEARTBEAT_NACK.value

        response_message = utils.compose_heartbeat_message(event=str(response_event), node=self.node, data={})
        response = heartbeat_pb2.Heartbeat(message=response_message)
        return response
