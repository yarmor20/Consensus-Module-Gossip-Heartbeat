from src.protobuf import inter_server_rpcs_pb2_grpc, inter_server_rpcs_pb2, client_server_rpcs_pb2_grpc
from src.grpc_server_servicer import InterServerRPCHandler
from src.grpc_client_servicer import ClientServerRPCHandler
from src.server_log import ServerLog
from src.server_state import ServerState
from src.__constants import *
import src.__utils as utils

from concurrent import futures
import asyncio
import random
import grpc
import time


class Server:
    """
    A cluster node of the Raft consensus module.
    """
    def __init__(self, node):
        # The node name (ID) in the cluster configuration.
        self.node = node

        # A structure where the current server's state is preserved.
        self.__state = ServerState(node=self.node)

        # A structure where all the Raft module log entries are preserved.
        self.__log = ServerLog(state=self.__state)

        # Servicers responsible for inter-server and client-server communication via RPC calls.
        self.interserver_servicer = InterServerRPCHandler(node=self.node, state=self.__state, log=self.__log)
        self.client_servicer = ClientServerRPCHandler(node=self.node, state=self.__state, log=self.__log)

        # Instance of a gRPC server.
        self.server = None

    def connect(self, peer: str) -> inter_server_rpcs_pb2_grpc.InterServerRPCHandlerStub:
        """
        Return a peer server connection stub.

        :param peer: (str) - Peer server (node) ID, e.g. "n0".
        :return: (heartbeat_pb2_grpc.HeartbeatServiceStub) - Connection stub.
        """
        # Get peer server connection params.
        peer_host = CLUSTER_HOST
        peer_heartbeat_port = self.interserver_servicer.gossip_map.get(peer, {}).get("port", None)

        # Create a peer server stub to establish connection.
        channel = grpc.aio.insecure_channel(f"{peer_host}:{peer_heartbeat_port}")
        stub = inter_server_rpcs_pb2_grpc.InterServerRPCHandlerStub(channel)
        return stub

    async def get_peer_acknowledgements(self, peers: list, event: HeartbeatEvent) -> bool:
        """
        Periodically send heartbeats to peer servers until
        all acknowledgents received or a timeout reached.

        :param peers: (List[str]) - List of peer server IDs, e.g. "n0".
        :param event: (config.HeartbeatEvent.value) - Heartbeat event value.
        :return: (bool) - Acknowledgement status.
        """
        # Preserve peer acknowledgements in a dictionary.
        acknowledgements = {peer: None for peer in peers}

        # Create a separate stub for each neighbor connection.
        stubs = {peer: self.connect(peer) for peer in peers}

        # Loop until all acknowledgements are received or task timeout is reached.
        while not all(acknowledgements.values()):
            for peer in peers:
                # Skip peer if it has already acknowledged a heartbeat.
                if acknowledgements[peer]:
                    continue

                # Get the peer stub to send a heartbeat RPC.
                stub = stubs.get(peer, None)

                # Compose a heartbeat request that consists of cluster state and proper heartbeat event.
                msg = utils.compose_message(
                    event=str(event.value),
                    node=self.node,
                    data={
                        RPC_MSG_PARAM_STATE: self.__state.to_dict(),
                        RPC_MSG_PARAM_GOSSIP_MAP: self.interserver_servicer.gossip_map
                    }
                )
                heartbeat_request = inter_server_rpcs_pb2.Heartbeat(message=msg)
                task = stub.HeartbeatRPC(heartbeat_request)

                try:
                    # Send a heartbeat request asynchronously and wait for response as a background task.
                    response = await asyncio.wait_for(task, timeout=HEARTBEAT_TIMEOUT)
                    response_event, response_node, response_data = utils.decompose_message(response.message)

                    # Check if the peer server acknowledged a heartbeat.
                    if response_event == HeartbeatEvent.HEARTBEAT_ACK.value:
                        acknowledgements[peer] = True

                except (asyncio.TimeoutError, grpc.aio.AioRpcError, ConnectionError) as e:
                    # Could not connect to peer server yet.
                    self.__state.logger.info(f"Awaiting connection to [{peer}]. Exception: [{type(e).__name__}]")

            # Wait until sending a heartbeat repeatedly.
            await asyncio.sleep(HEARTBEAT_TIMEOUT)
        return True

    async def check_cluster_readiness(self) -> bool:
        """
        Check if cluster node are all up and running. Send heartbeat continuously
        to all the nodes in the cluster until all nodes respond. After acknowledged,
        send another heartbeat to check if all the nodes have acknowledged each other
        and all connections are preserved.

        :return: (bool) - True if the cluster nodes are up and all connections are preserved.
        """
        # Get a list of all server-peers for the current node.
        peers = list(self.interserver_servicer.gossip_map.keys())

        # Send a heartbeat requests until all neighbors respond.
        event = HeartbeatEvent.HEARTBEAT_PING
        peer_aliveness_response = await self.get_peer_acknowledgements(peers=peers, event=event)
        if not peer_aliveness_response:
            self.__state.logger.info(
                f"Could not get connectivity acknowledgements from peer nodes. Current node [{self.node}]"
            )
            return False

        # Update cluster state to HEALTHY from the current node perspective as it has a connection to all the peers.
        self.__state.cluster_health = CL_HEALTH_GREEN

        # Send a heartbeat request every second until all neighbors respond.
        event = HeartbeatEvent.HEARTBEAT_CLUSTER_HEALTH
        peer_cluster_readiness_response = await self.get_peer_acknowledgements(peers=peers, event=event)
        if not peer_cluster_readiness_response:
            self.__state.logger.info(
                f"Could not get cluster health acknowledgements from peer nodes. Current node [{self.node}]"
            )
            return False
        return True

    async def start_leader_election(self) -> bool:
        """
        Start the leader election process as a cause of ELECTION_TIMEOUT or 
        adsence of cluster leader. Step up as a candidate and send the RequestVote RPC
        to other cluster nodes.
        
        :return: (bool) - True if the current node was elected as a leader.
        """
        # TODO: Do the consistency check.
        # Switch to the candidate state and increase the current term.
        self.__state.leader = None
        self.__state.state = STATE_CANDIDATE
        self.__state.current_term += 1

        # Vote for itself.
        votes = {self.node: HeartbeatEvent.HEARTBEAT_ACK.value}
        self.interserver_servicer.vote(node=self.node, term=self.__state.current_term)

        # Get a list of all server-peers for the current node.
        peers = list(self.interserver_servicer.gossip_map.keys())

        # Send the RequestVote RPC to all the peer nodes.
        for peer in peers:
            # Skip peer if it has already acknowledged a heartbeat.
            if peer in votes.keys():
                continue

            # Get the peer stub to send an RPC.
            stub = self.connect(peer)

            # Compose an RPC that consists of cluster state and proper heartbeat event.
            msg = utils.compose_message(
                event=str(HeartbeatEvent.REQUEST_VOTE.value),
                node=self.node,
                data={
                    RPC_MSG_PARAM_STATE: self.__state.to_dict(),
                    RPC_MSG_PARAM_GOSSIP_MAP: self.interserver_servicer.gossip_map
                }
            )
            heartbeat_request = inter_server_rpcs_pb2.Heartbeat(message=msg)
            task = stub.RequestVoteRPC(heartbeat_request)

            try:
                # Send a heartbeat request asynchronously and wait for response as a background task.
                response = await asyncio.wait_for(task, timeout=REQUEST_VOTE_TIMEOUT)
                response_event, response_node, response_data = utils.decompose_message(response.message)

                # Check if the peer server acknowledged a heartbeat.
                if response_event:
                    votes[peer] = response_event  # Preserve peer node's vote.

            except (asyncio.TimeoutError, grpc.aio.AioRpcError, ConnectionError) as e:
                # Could not receive the vote from the peer server.
                self.__state.logger.info(f"RequestVote RPC Timeout: [{peer}]. Exception: [{type(e).__name__}]")
                votes[peer] = HeartbeatEvent.HEARTBEAT_NACK.value

        # Calculate the number of positive votes ("ACK").
        votes_count = len(list(filter(
            lambda x: x == HeartbeatEvent.HEARTBEAT_ACK.value,
            [vote for _, vote in votes.items()]))
        )

        # If the majority votes is received, following formula c >= floor(n/2) + 1
        if votes_count >= utils.get_majority_threshold(num_nodes=self.__state.cluster_config.get("num_nodes")):
            # Acknowledge yourself as a leader.
            self.__state.leader = self.node

            # Send the Heartbeat RPC with LEADER_ESTABLISHMENT event to notify all other nodes
            # about the new chosen leader.
            acknowledged = await self.get_peer_acknowledgements(
                peers=peers,
                event=HeartbeatEvent.LEADER_ESTABLISHMENT
            )

            # Notify if not all nodes were able to acknowledge the new leader.
            # TODO: Think about how to preserve the cluster in running state in such case.
            if not acknowledged:
                self.__state.logger.info(
                    f"Couldn't get leader establishment acknowledgements from peer nodes. Current node [{self.node}]"
                )
                return False

            # Log the election results.
            term = self.__state.current_term
            self.__state.logger.info(f"Leader Election Closure: Node [{self.node}] elected for term: [{term}]")
            return True

        # If not enough votes received.
        else:
            # Log the election results.
            term = self.__state.current_term
            self.__state.logger.info(
                f"Leader Election Step Down: Node [{self.node}] did not receive enough votes for term: [{term}]"
            )

            # Transition back to the follower state.
            self.__state.state = STATE_FOLLOWER
            return False

    async def pull_entries(self) -> bool:
        """
        Pull new log entries from sync source by sending PullEntries RPC.
        Once the entries are replicated to the local log, send the UpdatePosition RPC to the sync source.

        :return: (bool) - True if managed to pull new entries and notify sync source of update position.
        """
        # TODO: Add sync source selection.
        # Pull entries from the sync source.
        leader = self.__state.leader
        if not leader:
            return False

        # Get the peer stub to send an RPC.
        stub = self.connect(leader)

        # Compose an RPC that consists of cluster state and proper heartbeat event.
        msg = utils.compose_message(
            event=str(HeartbeatEvent.PULL_ENTRIES.value),
            node=self.node,
            data={
                RPC_MSG_PARAM_STATE: self.__state.to_dict(),
                RPC_MSG_PARAM_GOSSIP_MAP: self.interserver_servicer.gossip_map
            }
        )
        heartbeat_request = inter_server_rpcs_pb2.Heartbeat(message=msg)
        task = stub.PullEntriesRPC(heartbeat_request)

        try:
            # Send a heartbeat request asynchronously and wait for response as a background task.
            response = await asyncio.wait_for(task, timeout=PULL_ENTRIES_TIMEOUT)
            response_event, response_node, response_data = utils.decompose_message(response.data)
        except (asyncio.TimeoutError, grpc.aio.AioRpcError, ConnectionError) as e:
            # Could not receive the vote from the peer server.
            self.__state.logger.info(f"RullEntries RPC Timeout: [{leader}]. Exception: [{type(e).__name__}]")
            return False

        # If did not manage to pull log entries.
        if not (response_data and isinstance(response_data, list)):
            self.__state.logger.info(f"RullEntries RPC: No new entries to pull from [{leader}].")
            return False

        # Replicate entries to the local server's log.
        entry_uuids = self.__log.put_entries(data=response_data)
        self.__state.logger.info(
            f"RullEntries RPC Succeeded: Pulled [{len(response_data)}] entries from [{leader}]."
        )

        # Send the UpdatePosition RPC to the sync source to acknowledge log entries replication.
        response = await self.update_position(sync_source=leader, entry_uuids=entry_uuids)
        return response

    async def update_position(self, sync_source: str, entry_uuids: list) -> bool:
        """
        Send UpdatePosition RPC to nofity the sync source that the pulled log entries
        were successfully replicated to the server's local log.

        :param sync_source: (str) - Sync source name.
        :param entry_uuids: (List[str]) - List of replicated entries' UUIDs.
        :return: True if managed to update position.
        """
        # Get the peer stub to send an RPC.
        stub = self.connect(sync_source)

        # Compose an RPC that consists of cluster state and proper heartbeat event.
        msg = utils.compose_message(
            event=str(HeartbeatEvent.UPDATE_POSITION.value),
            node=self.node,
            data={
                RPC_MSG_PARAM_STATE: self.__state.to_dict(),
                RPC_MSG_PARAM_GOSSIP_MAP: self.interserver_servicer.gossip_map,
                RPC_MSG_ENTRY_UUIDS: entry_uuids
            }
        )
        heartbeat_request = inter_server_rpcs_pb2.Heartbeat(message=msg)
        task = stub.UpdatePositionRPC(heartbeat_request)

        try:
            # Send a heartbeat request asynchronously and wait for response as a background task.
            response = await asyncio.wait_for(task, timeout=UPDATE_POSITION_TIMEOUT)
            response_event, response_node, response_data = utils.decompose_message(response.message)

            if response_event == HeartbeatEvent.HEARTBEAT_ACK.value:
                # Log the number of pulled and committed entries.
                updated_count, committed_count = response_data.get("updated", 0), response_data.get("committed", 0)
                self.__state.logger.info(
                    f"UpdatePosition RPC: Updated: [{updated_count}] Committed: [{committed_count}]."
                )
                return True
            else:
                self.__state.logger.info(
                    f"UpdatePosition RPC: Node [{response_node}] received no entries to update position."
                )
                return False

        except (asyncio.TimeoutError, grpc.aio.AioRpcError, ConnectionError) as e:
            # Could not receive the vote from the peer server.
            self.__state.logger.info(f"UpdatePosition RPC Timeout: [{sync_source}]. Exception: [{type(e).__name__}]")
            return False

    async def serve(self):
        """
        The main body of the server. Start the server, ensure it is connected to all
        other nodes of cluster. Start leader election to pick up a leader for the term.
        After, start the normal operation, e.g. pulling entries from the sync source and
        responding to other servers' RPCs.
        """
        host = self.__state.server_config.get("host", "")
        port = self.__state.server_config.get("port", None)

        # Start the heartbeat servicer.
        self.server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
        inter_server_rpcs_pb2_grpc.add_InterServerRPCHandlerServicer_to_server(self.interserver_servicer, self.server)
        client_server_rpcs_pb2_grpc.add_RPCHandlerServicer_to_server(self.client_servicer, self.server)
        self.server.add_insecure_port(f"{host}:{port}")

        # Start the node.
        await self.server.start()

        # Check the cluster connections and affirmate the cluster is up.
        await self.check_cluster_readiness()
        self.__state.logger.info(
            f"Cluster State: Cluster is up and running. Leader: [{self.__state.leader}]"
        )

        # Start first leader election process.
        while self.__state.leader is None:
            is_leader = await self.start_leader_election()
            if is_leader:
                break

            # Wait for a new election timeout before starting new leader election.
            # During that time a new leader can be chosen.
            time.sleep(random.randint(CANDIDATE_ELECTION_TIMEOUT, 2 * CANDIDATE_ELECTION_TIMEOUT))

        # Start the normal operation.
        try:

            while True:
                # Sleep some time before moking next requests.
                await asyncio.sleep(HEARTBEAT_TIMEOUT)

                # Pull entries from the sync sources. Leader does not have to do that.
                if self.node != self.__state.leader:
                    await self.pull_entries()

        except KeyboardInterrupt:
            await self.server.stop(0)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    server = Server(NODE)
    asyncio.run(server.serve())
