import src.__constants as config
from src.__heartbeat_servicer import HeartbeatServiceServicer
from src.utils import get_logger
import src.utils as utils
import random
import asyncio
import grpc
import json
from src.protobuf import heartbeat_pb2_grpc, heartbeat_pb2


class ClusterServer:
    def __init__(self, node):
        self.node = node
        self.__logger = get_logger(name=f"Server-Logger-{self.node}")
        self.server_config, self.cluster_config = utils.read_config(self.node)

        self.heartbeat_server = None
        self.heartbeat_servicer = HeartbeatServiceServicer(self.node, self.cluster_config, logger=self.__logger)
        self.gossip_cluster_state = self.heartbeat_servicer.get_cluster_heartbeat_state()

    def connect(self, peer: str) -> heartbeat_pb2_grpc.HeartbeatServiceStub:
        """
        Return a peer server connection stub.

        :param peer: (str) - Peer server (node) ID, e.g. "n0".
        :return: (heartbeat_pb2_grpc.HeartbeatServiceStub) - Connection stub.
        """
        # Get peer server connection params.
        peer_host = config.CLUSTER_HOST
        peer_heartbeat_port = self.gossip_cluster_state.get(peer, {}).get("hrbt-port", None)

        # Create a peer server stub to establish connection.
        channel = grpc.aio.insecure_channel(f"{peer_host}:{peer_heartbeat_port}")
        stub = heartbeat_pb2_grpc.HeartbeatServiceStub(channel)
        return stub

    async def get_peer_acknowledgements(self, stubs: dict, peers: list, event: config.HeartbeatEvent) -> bool:
        """
        Periodically send heartbeats to peer servers until
        all acknowledgents received or a timeout reached.

        :param stubs: (Dict[str, heartbeat_pb2_grpc.HeartbeatServiceStub]) - Peer servers' connection stubs.
        :param peers: (List[str]) - List of peer server IDs, e.g. "n0".
        :param event: (config.HeartbeatEvent.value) - Heartbeat event value.
        :return: (bool) - Acknowledgement status.
        """
        # Preserve peer acknowledgements in a dictionary.
        acknowledgements = {peer: None for peer in peers}

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
                    data=self.gossip_cluster_state
                )
                heartbeat_request = heartbeat_pb2.Heartbeat(message=msg)
                task = stub.HeartbeatRPC(heartbeat_request)

                try:
                    # Send a heartbeat request asynchronously and wait for response as a background task.
                    response = await asyncio.wait_for(task, timeout=config.HEARTBEAT_TIMEOUT)
                    response_event, response_node, response_data = utils.decompose_heartbeat_message(response.message)

                    # Check if the peer server acknowledged a heartbeat.
                    if response_event == config.HeartbeatEvent.HEARTBEAT_ACK.value:
                        acknowledgements[peer] = True

                except (asyncio.TimeoutError, grpc.aio.AioRpcError, ConnectionError) as e:
                    # Could not connect to peer server yet.
                    self.__logger.info(f"Awaiting connection to [{peer}]. Exception: [{type(e).__name__}/{e.code()}]")
                    # print(f"Waiting for connection to [{peer}]...\nException: [{type(e).__name__}/{e.code()}]")

            # Wait until sending a heartbeat repeatedly.
            await asyncio.sleep(config.HEARTBEAT_TIMEOUT)
        return True

    async def check_cluster_consistency(self):
        # Get a list of all server-peers for the current node.
        peers = list(self.gossip_cluster_state.keys())

        # Create a separate stub for each neighbor connection.
        stubs = {peer: self.connect(peer) for peer in peers}

        # Send a heartbeat requests until all neighbors respond.
        event = config.HeartbeatEvent.HEARTBEAT_PING
        peer_aliveness_response = await self.get_peer_acknowledgements(stubs=stubs, peers=peers, event=event)
        if not peer_aliveness_response:
            print(f"Could not get connectivity acknowledgements from peer nodes. Current node [{self.node}]")
            return False

        # Update cluster state to HEALTHY from the current node perspective as it has a connection to all the peers.
        self.heartbeat_servicer.update_cluster_health(is_healthy=True)

        # Send a heartbeat request every second until all neighbors respond.
        event = config.HeartbeatEvent.HEARTBEAT_CLUSTER_HEALTH
        peer_cluster_readiness_response = await self.get_peer_acknowledgements(stubs=stubs, peers=peers, event=event)
        if not peer_cluster_readiness_response:
            print(f"Could not get cluster health acknowledgements from peer nodes. Current node [{self.node}]")
            return False
        return True

    async def serve(self):
        host, heartbeat_port = self.server_config.get("host", ""), self.server_config.get("heartbeat-port", None)

        # Start the heartbeat service
        self.heartbeat_server = grpc.aio.server()
        heartbeat_pb2_grpc.add_HeartbeatServiceServicer_to_server(self.heartbeat_servicer, self.heartbeat_server)
        self.heartbeat_server.add_insecure_port(f"{host}:{heartbeat_port}")

        await self.heartbeat_server.start()
        await self.check_cluster_consistency()
        await self.heartbeat_server.stop(0)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    server = ClusterServer(config.NODE)
    asyncio.run(server.serve())
    print("Cluster is now up!")

    # try:
    #     heartbeat_task = loop.create_task(server.run_heartbeat_server())
    #     loop.run_until_complete(asyncio.gather(heartbeat_task))
    # except KeyboardInterrupt:
    #     loop.close()

