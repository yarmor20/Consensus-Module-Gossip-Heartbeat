from src.protobuf import heartbeat_pb2_grpc, heartbeat_pb2
from src.__heartbeat_servicer import HeartbeatServiceServicer
from src.__constants import *
import src.utils as utils

import numpy as np
import asyncio
import random
import grpc
import time


class Server:
    def __init__(self, node):
        # The node name (ID) in the cluster configuration.
        self.node = node

        # Logger of the particular server. All the logs are saved in the log file.
        self.__logger = utils.get_logger(name=f"Server-Logger-{self.node}")

        # Configuration of the particular server and the whole cluster. Used for inter-server communication.
        self.server_config, self.cluster_config = utils.read_config(self.node)

        # Servicer responsible for inter-server communication via RPC calls.
        self.heartbeat_servicer = HeartbeatServiceServicer(self.node, self.cluster_config, logger=self.__logger)

        # Instance of the inter-server communication servicer.
        self.heartbeat_server = None

    def connect(self, peer: str) -> heartbeat_pb2_grpc.HeartbeatServiceStub:
        """
        Return a peer server connection stub.

        :param peer: (str) - Peer server (node) ID, e.g. "n0".
        :return: (heartbeat_pb2_grpc.HeartbeatServiceStub) - Connection stub.
        """
        # Get peer server connection params.
        peer_host = CLUSTER_HOST
        peer_heartbeat_port = self.heartbeat_servicer.gossip_map.get(peer, {}).get("hrbt-port", None)

        # Create a peer server stub to establish connection.
        channel = grpc.aio.insecure_channel(f"{peer_host}:{peer_heartbeat_port}")
        stub = heartbeat_pb2_grpc.HeartbeatServiceStub(channel)
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
                msg = utils.compose_heartbeat_message(
                    event=str(event.value),
                    node=self.node,
                    data={
                        RPC_MSG_PARAM_STATE: self.heartbeat_servicer.state,
                        RPC_MSG_PARAM_GOSSIP_MAP: self.heartbeat_servicer.gossip_map
                    }
                )
                heartbeat_request = heartbeat_pb2.Heartbeat(message=msg)
                task = stub.HeartbeatRPC(heartbeat_request)

                try:
                    # Send a heartbeat request asynchronously and wait for response as a background task.
                    response = await asyncio.wait_for(task, timeout=HEARTBEAT_TIMEOUT)
                    response_event, response_node, response_data = utils.decompose_heartbeat_message(response.message)

                    # Check if the peer server acknowledged a heartbeat.
                    if response_event == HeartbeatEvent.HEARTBEAT_ACK.value:
                        acknowledgements[peer] = True

                except (asyncio.TimeoutError, grpc.aio.AioRpcError, ConnectionError) as e:
                    # Could not connect to peer server yet.
                    self.__logger.info(f"Awaiting connection to [{peer}]. Exception: [{type(e).__name__}]")

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
        peers = list(self.heartbeat_servicer.gossip_map.keys())

        # Send a heartbeat requests until all neighbors respond.
        event = HeartbeatEvent.HEARTBEAT_PING
        peer_aliveness_response = await self.get_peer_acknowledgements(peers=peers, event=event)
        if not peer_aliveness_response:
            self.__logger.info(
                f"Could not get connectivity acknowledgements from peer nodes. Current node [{self.node}]"
            )
            return False

        # Update cluster state to HEALTHY from the current node perspective as it has a connection to all the peers.
        self.heartbeat_servicer.is_cluster_healthy = True

        # Send a heartbeat request every second until all neighbors respond.
        event = HeartbeatEvent.HEARTBEAT_CLUSTER_HEALTH
        peer_cluster_readiness_response = await self.get_peer_acknowledgements(peers=peers, event=event)
        if not peer_cluster_readiness_response:
            self.__logger.info(
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
        self.heartbeat_servicer.state["leader"] = None
        self.heartbeat_servicer.state["state"] = STATE_CANDIDATE
        self.heartbeat_servicer.state["curr-term"] += 1

        # Vote for itself.
        votes = {self.node: HeartbeatEvent.HEARTBEAT_ACK.value}
        self.heartbeat_servicer.vote(node=self.node, term=self.heartbeat_servicer.state.get("curr-term"))

        # Get a list of all server-peers for the current node.
        peers = list(self.heartbeat_servicer.gossip_map.keys())

        # Send the RequestVote RPC to all the peer nodes.
        for peer in peers:
            # Skip peer if it has already acknowledged a heartbeat.
            if peer in votes.keys():
                continue

            # Get the peer stub to send an RPC.
            stub = self.connect(peer)

            # Compose an RPC that consists of cluster state and proper heartbeat event.
            msg = utils.compose_heartbeat_message(
                event=str(HeartbeatEvent.REQUEST_VOTE.value),
                node=self.node,
                data={
                    RPC_MSG_PARAM_STATE: self.heartbeat_servicer.state,
                    RPC_MSG_PARAM_GOSSIP_MAP: self.heartbeat_servicer.gossip_map
                }
            )
            heartbeat_request = heartbeat_pb2.Heartbeat(message=msg)
            task = stub.HeartbeatRPC(heartbeat_request)

            try:
                # Send a heartbeat request asynchronously and wait for response as a background task.
                response = await asyncio.wait_for(task, timeout=REQUEST_VOTE_TIMEOUT)
                response_event, response_node, response_data = utils.decompose_heartbeat_message(response.message)

                # Check if the peer server acknowledged a heartbeat.
                if response_event:
                    votes[peer] = response_event  # Preserve peer node's vote.

            except (asyncio.TimeoutError, grpc.aio.AioRpcError, ConnectionError) as e:
                # Could not receive the vote from the peer server.
                self.__logger.info(f"RequestVote RPC Timeout: [{peer}]. Exception: [{type(e).__name__}]")
                votes[peer] = HeartbeatEvent.HEARTBEAT_NACK.value

        # Calculate the number of positive votes ("ACK").
        votes_count = len(list(filter(
            lambda x: x == HeartbeatEvent.HEARTBEAT_ACK.value,
            [vote for _, vote in votes.items()]))
        )

        # If the majority votes is received, following formula c >= floor(n/2) + 1
        if votes_count >= np.floor(self.cluster_config.get("num_nodes") / 2) + 1:
            # Acknowledge yourself as a leader.
            self.heartbeat_servicer.state["leader"] = self.node

            # Send the Heartbeat RPC with LEADER_ESTABLISHMENT event to notify all other nodes
            # about the new chosen leader.
            acknowledged = await self.get_peer_acknowledgements(
                peers=peers,
                event=HeartbeatEvent.LEADER_ESTABLISHMENT
            )

            # Notify if not all nodes were able to acknowledge the new leader.
            # TODO: Think about how to preserve the cluster in running state in such case.
            if not acknowledged:
                self.__logger.info(
                    f"Couldn't get leader establishment acknowledgements from peer nodes. Current node [{self.node}]"
                )
                return False

            # Log the election results.
            term = self.heartbeat_servicer.state.get('curr-term')
            self.__logger.info(f"Leader Election Closure: Node [{self.node}] elected for term: [{term}]")
            return True

        # If not enough votes received.
        else:
            # Log the election results.
            term = self.heartbeat_servicer.state.get('curr-term')
            self.__logger.info(
                f"Leader Election Step Down: Node [{self.node}] did not receive enough votes for term: [{term}]"
            )

            # Transition back to the follower state.
            self.heartbeat_servicer.state["state"] = STATE_FOLLOWER
            return False

    async def serve(self):
        host, heartbeat_port = self.server_config.get("host", ""), self.server_config.get("heartbeat-port", None)

        # Start the heartbeat servicer.
        self.heartbeat_server = grpc.aio.server()
        heartbeat_pb2_grpc.add_HeartbeatServiceServicer_to_server(self.heartbeat_servicer, self.heartbeat_server)
        self.heartbeat_server.add_insecure_port(f"{host}:{heartbeat_port}")

        # Start the node.
        await self.heartbeat_server.start()

        # Check the cluster connections and affirmate the cluster is up.
        await self.check_cluster_readiness()
        self.__logger.info(
            f"Cluster State: Cluster is up and running. Leader: [{self.heartbeat_servicer.state.get('leader')}]"
        )

        # Start first leader election process.
        while self.heartbeat_servicer.state.get("leader") is None:
            is_leader = await self.start_leader_election()
            if is_leader:
                break

            # Wait for a new election timeout before starting new leader election.
            # During that time a new leader can be chosen.
            time.sleep(random.randint(CANDIDATE_ELECTION_TIMEOUT, 2 * CANDIDATE_ELECTION_TIMEOUT))

        await self.heartbeat_server.stop(0)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    server = Server(NODE)
    asyncio.run(server.serve())
