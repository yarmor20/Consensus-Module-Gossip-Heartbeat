from src.protobuf import client_server_rpcs_pb2_grpc, client_server_rpcs_pb2
from src.__constants import HeartbeatEvent

from src.server_state import ServerState
from src.server_log import ServerLog

from typing import Optional
import json


class ClientServerRPCHandler(client_server_rpcs_pb2_grpc.RPCHandlerServicer):
    """
    Handler for client-server interaction RPCs.
    """
    def __init__(self, node: str, state: Optional[ServerState] = None, log: Optional[ServerLog] = None):
        # The node name (ID) in the cluster configuration.
        self.node = node

        # A structure where the current server's state is preserved.
        self.__state = state

        # A structure where all the Raft module log entries are preserved.
        self.__log = log

    def PutEntriesRPC(self, request, context) -> client_server_rpcs_pb2.ServerResponse:
        """
        Handler of the incoming to the Heartbeat Servicer remote procedure calls.

        :param request: RPC request sent to the Heartbeat Servicer.
        :param context: Context of the received RPC.
        :return: Response RPC.
        """
        # Do not acknowledge the RPC if it is empty.
        if not request.data:
            response_event = str(HeartbeatEvent.HEARTBEAT_NACK.value)
            response = client_server_rpcs_pb2.ServerResponse(message=response_event)
            return response

        # Check if the current node is a leader.
        if self.node != self.__state.leader:
            # If not, reject the new entries and return the leader channel params
            # for the client to retry his request with the leader node.
            self.__state.logger.info(
                f"PutEntries RPC Rejected: Returning request to leader: [{self.__state.leader}]"
            )

            # Get leader host and port.
            leader_host = self.__state.cluster_config.get("nodes").get(self.__state.leader).get("host")
            leader_port = self.__state.cluster_config.get("nodes").get(self.__state.leader).get("port")

            # Compose a response message.
            response_event = str(HeartbeatEvent.HEARTBEAT_NACK.value)
            response_msg = {"event": response_event, "leader-channel": f"{leader_host}:{leader_port}"}
            response = client_server_rpcs_pb2.ServerResponse(message=json.dumps(response_msg))
            return response

        # Retrieve data sent by client.
        data = json.loads(request.data)
        self.__state.logger.info(f"PutEntries RPC Received. Client: [client]. NumEntries: [{len(data)}].")

        # Append the retreived data to the log.
        if isinstance(data, list):
            self.__log.put_entries(data=data)
            response_event = str(HeartbeatEvent.HEARTBEAT_ACK.value)
            response_msg = {"event": response_event}
        else:
            response_event = str(HeartbeatEvent.HEARTBEAT_NACK.value)
            response_msg = {"event": response_event}

        # Compose and send a response with a respective response event.
        response = client_server_rpcs_pb2.ServerResponse(message=json.dumps(response_msg))
        return response

    def AckEntriesRPC(self, request, context) -> client_server_rpcs_pb2.ClientResponse:
        """
        Handles the AckEntries RPC sent from the leader to the client once the
        log entries have been committed.

        :param request: RPC request sent to the Heartbeat Servicer.
        :param context: Context of the received RPC.
        :return: (client_server_rpcs_pb2.ClientResponse) - Response RPC message.
        """
        # Do not acknowledge the RPC if it is empty.
        if not request.message:
            response_event = str(HeartbeatEvent.HEARTBEAT_NACK.value)
            response = client_server_rpcs_pb2.ClientResponse(message=response_event)
            return response

        message_json = json.loads(request.message)
        self.__state.logger.info(f"AckEntries RPC Received. Message: [{message_json}].")

        # TODO: Client receives notification of entries being committed.

        # Compose and send a response with a respective response event.
        response_event = str(HeartbeatEvent.HEARTBEAT_ACK.value)
        response = client_server_rpcs_pb2.ClientResponse(message=response_event)
        return response
