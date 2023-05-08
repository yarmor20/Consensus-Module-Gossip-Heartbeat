from src.protobuf import inter_server_rpcs_pb2_grpc, inter_server_rpcs_pb2
from src.server_log import ServerLog
from src.server_state import ServerState
from src.__constants import *
import src.__utils as utils

from typing import Union
import datetime
import json


class InterServerRPCHandler(inter_server_rpcs_pb2_grpc.InterServerRPCHandlerServicer):
    """
    Handler for inter-server interaction RPCs.
    """
    def __init__(self, node: str, state: ServerState, log: ServerLog):
        # The current cluster node id that is using the servicer.
        self.node: str = node

        # A structure where the current server's state is preserved.
        self.__state: ServerState = state

        # A structure where all the Raft module log entries are preserved.
        self.__log: ServerLog = log

        # Map of the last updated state from each cluster node.
        self.gossip_map: dict = utils.compose_cluster_state(node=self.node, cluster_config=self.__state.cluster_config)

    def __get_rpc_message(self, request) -> Union[inter_server_rpcs_pb2.Heartbeat, tuple]:
        """
        Extract the RPC contents.
        In case the message is empty, return a NACK response.

        :param request: RPC request sent to the Heartbeat Servicer.
        :return: (Heartbeat or Tuple) - RPC contents, in case they were extracted.
        """
        # Do not acknowledge the RPC if it is empty.
        if not request.message:
            response_event = HeartbeatEvent.HEARTBEAT_NACK.value
            response_message = utils.compose_heartbeat_message(event=str(response_event), node=self.node, data={})
            response = inter_server_rpcs_pb2.Heartbeat(message=response_message)
            return response

        # Preserve the heartbeat timestamp.
        heartbeat_timestamp = datetime.datetime.now().timestamp()

        # Extract the RPC contents.
        message_json = json.loads(request.message)
        event, sender, data = message_json.get("event"), message_json.get("node"), message_json.get("data")
        self.__state.logger.info(f"Message Received. Sender: [{sender}]. Event: [{event}].")

        # Preserve the update information of the sender node in a gossip map.
        self.gossip_map[sender]["state"]["alive"] = True
        self.gossip_map[sender]["state"]["ts"] = heartbeat_timestamp

        return event, sender, data

    def __update_gossip_map(self):
        pass

    def HeartbeatRPC(self, request, context) -> inter_server_rpcs_pb2.Heartbeat:
        """
        Handler of the incoming to the Heartbeat Servicer remote procedure calls.

        :param request: RPC request sent to the Heartbeat Servicer.
        :param context: Context of the received RPC.
        :return: Response RPC.
        """
        # Do not acknowledge the RPC if it is empty.
        rpc_response = self.__get_rpc_message(request=request)
        if isinstance(rpc_response, inter_server_rpcs_pb2.Heartbeat):
            return rpc_response

        # Extract the RPC contents if succeeded to get the message contents.
        event, sender, data = rpc_response

        # Standard Heartbeat RPC response.
        if event == HeartbeatEvent.HEARTBEAT_PING.value:
            response_event = HeartbeatEvent.HEARTBEAT_ACK.value
        # Acknowledge if the cluster inter-server connections are preserved and all servers are up.
        elif event == HeartbeatEvent.HEARTBEAT_CLUSTER_HEALTH.value:
            response_event = HeartbeatEvent.HEARTBEAT_ACK.value \
                if self.__state.cluster_health == CL_HEALTH_GREEN \
                else HeartbeatEvent.HEARTBEAT_NACK.value
        # Acknowledge a new established leader.
        elif event == HeartbeatEvent.LEADER_ESTABLISHMENT.value:
            self.__state.leader = sender
            self.__state.current_term = data.get(RPC_MSG_PARAM_STATE, {}).get("curr-term", self.__state.current_term)
            self.__state.state = STATE_FOLLOWER
            response_event = HeartbeatEvent.HEARTBEAT_ACK.value
        else:
            response_event = HeartbeatEvent.HEARTBEAT_NACK.value

        # Compose and send a response with a respective response event.
        response_message = utils.compose_heartbeat_message(event=str(response_event), node=self.node, data={})
        response = inter_server_rpcs_pb2.Heartbeat(message=response_message)
        return response

    def RequestVoteRPC(self, request, context) -> inter_server_rpcs_pb2.Heartbeat:
        """
        Handler of the incoming to the Heartbeat Servicer remote procedure calls.

        :param request: RPC request sent to the Heartbeat Servicer.
        :param context: Context of the received RPC.
        :return: Response RPC.
        """
        # Do not acknowledge the RPC if it is empty.
        rpc_response = self.__get_rpc_message(request=request)
        if isinstance(rpc_response, inter_server_rpcs_pb2.Heartbeat):
            return rpc_response

        # Extract the RPC contents if succeeded to get the message contents.
        event, sender, data = rpc_response

        # Vote if the sender node is to become a leader as a response to RequestVote RPC.
        if event == HeartbeatEvent.REQUEST_VOTE.value:
            response_event = self.vote(node=sender, term=data.get("state", {}).get("curr-term", 0))
        else:
            response_event = HeartbeatEvent.HEARTBEAT_NACK.value

        # Compose and send a response with a respective response event.
        response_message = utils.compose_heartbeat_message(event=str(response_event), node=self.node, data={})
        response = inter_server_rpcs_pb2.Heartbeat(message=response_message)
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
        if term > self.__state.current_term and term > self.__state.last_vote_term:
            response_event = HeartbeatEvent.HEARTBEAT_ACK.value

            # Preserve the last vote as one server can vote only once in a given term.
            self.__state.last_vote_node = node
            self.__state.last_vote_term = term

        # If not -> reject the vote.
        else:
            response_event = HeartbeatEvent.HEARTBEAT_NACK.value
        return response_event
