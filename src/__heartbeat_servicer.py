from src.protobuf import heartbeat_pb2_grpc, heartbeat_pb2
from src.__constants import *
import src.utils as utils

import datetime
import json


class HeartbeatServiceServicer(heartbeat_pb2_grpc.HeartbeatServiceServicer):
    def __init__(self, node, cluster_config, logger):
        # The current cluster node id that is using the servicer.
        self.node = node

        # Cluster configuration that has information about all the cluster nodes' parameters.
        self.cluster_config = cluster_config

        # Map of the last heartbeat timestamp from each cluster node.
        self.gossip_map = utils.compose_cluster_state(node=self.node, cluster_config=self.cluster_config)

        # Keep track if all the cluster nodes are alive (cluster is healthy).
        self.is_cluster_healthy = False

        # Logger of the particular server. All the logs are saved in the log file.
        self.__logger = logger

        # The current state of the node and its latest information.
        self.state = {
            "state": "follower",
            "leader": None,
            "curr-term": 0,
        }

        # The last vote that a particular server has given. Used in leader election process.
        self.last_vote = {
            "node": None, 
            "term": 0
        }

    def update_cluster_heartbeat_state(self, data):
        pass

    def HeartbeatRPC(self, request, context):
        """
        Handler of the incoming to the Heartbeat Servicer remote procedure calls.

        :param request: RPC request sent to the Heartbeat Servicer.
        :param context: Context of the received RPC.
        :return: Response RPC.
        """
        # Preserve the heartbeat timestamp.
        heartbeat_timestamp = datetime.datetime.now().timestamp()
        
        # Do not acknowledge the RPC if it is empty.
        if not request.message:
            response_event = HeartbeatEvent.HEARTBEAT_NACK.value
            response_message = utils.compose_heartbeat_message(event=str(response_event), node=self.node, data={})
            response = heartbeat_pb2.Heartbeat(message=response_message)
            return response

        # Extract the RPC contents.
        message_json = json.loads(request.message)
        event, sender, data = message_json.get("event"), message_json.get("node"), message_json.get("data")
        self.__logger.info(f"Message Received. Sender: [{sender}]. Event: [{event}].")

        # Preserve the update information of the sender node in a gossip map.
        self.gossip_map[sender]["state"]["alive"] = True
        self.gossip_map[sender]["state"]["hrbt-ts"] = heartbeat_timestamp

        # Standard Heartbeat RPC response.
        if event == HeartbeatEvent.HEARTBEAT_PING.value:
            response_event = HeartbeatEvent.HEARTBEAT_ACK.value
            
        # Acknowledge if the cluster inter-server connections are preserved and all servers are up.
        elif event == HeartbeatEvent.HEARTBEAT_CLUSTER_HEALTH.value:
            response_event = HeartbeatEvent.HEARTBEAT_ACK.value \
                if self.is_cluster_healthy \
                else HeartbeatEvent.HEARTBEAT_NACK.value
        
        # Vote if the sender node is to become a leader as a response to RequestVote RPC.
        elif event == HeartbeatEvent.REQUEST_VOTE.value:
            response_event = self.vote(node=sender, term=data.get("state", {}).get("curr-term", 0))
            
        # Acknowledge a new established leader.
        elif event == HeartbeatEvent.LEADER_ESTABLISHMENT.value:
            self.state["leader"] = sender
            self.state["curr-term"] = data.get("state", {}).get("curr-term", self.state["curr-term"])
            self.state["state"] = STATE_FOLLOWER
            response_event = HeartbeatEvent.HEARTBEAT_ACK.value

        else:
            response_event = HeartbeatEvent.HEARTBEAT_NACK.value

        # Compose and send a response with a respective response event.
        response_message = utils.compose_heartbeat_message(event=str(response_event), node=self.node, data={})
        response = heartbeat_pb2.Heartbeat(message=response_message)
        return response

    def vote(self, node: str, term: int):
        """
        Give or reject the vote for particular server in the
        leader election process as a response to RequestVote RPC.

        :param node: (str) - Sender of the RequestVote RPC.
        :param term: (int) - The current term of a sender.
        :return: Acknowledge (give vote) or not (reject the vote) event.
        """
        # Check if the candidate has higher than the current server's term or the term in the last vote.
        if term > self.state.get("curr-term") and term > self.last_vote.get("term"):
            response_event = HeartbeatEvent.HEARTBEAT_ACK.value

            # Preserve the last vote as one server can vote only once in a given term.
            self.last_vote = {
                "node": node,
                "term": term
            }

        # If not -> reject the vote.
        else:
            response_event = HeartbeatEvent.HEARTBEAT_NACK.value
        return response_event
