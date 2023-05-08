from src.protobuf import client_server_rpcs_pb2_grpc, client_server_rpcs_pb2
from src.__constants import *

from fastapi.responses import JSONResponse
from fastapi import FastAPI
from typing import List
import grpc
import json


# Start FastAPI server.
app = FastAPI(title=str.capitalize("CLIENT"))


def connect_server(channel: str) -> client_server_rpcs_pb2_grpc.RPCHandlerStub:
    """
    Connect to the Raft module server.

    :param channel: (str) - Server host and port, e.g. <host>:<port>
    :return: (client_server_rpcs_pb2_grpc.RPCHandlerStub) - gRPC server stub.
    """
    # Create a stub for the ClientServerRPCHandler servicer.
    channel = grpc.insecure_channel(channel)
    stub = client_server_rpcs_pb2_grpc.RPCHandlerStub(channel)
    return stub


def put_entries(data: List[dict], host: str, port: int) -> client_server_rpcs_pb2.ServerResponse:
    """
    Use PutEntries RPC to pass the data entries to the server.

    :param data: (List[dict]) - List of entries.
    :param host: (str) - Server host.
    :param port: (int) - Server port.
    :return: (client_server_rpcs_pb2.ServerResponse) - Server response.
    """
    # Get the server stub.
    stub = connect_server(channel=f"{host}:{port}")

    # Create a request message with the data to send to the server.
    request = client_server_rpcs_pb2.Entries(data=json.dumps(data))

    # Send the request to the server and wait for the response.
    response = stub.PutEntriesRPC(request)

    # Get the response message.
    msg = json.loads(response.message)

    # If the server the data was sent to is not a leader and is redirecting to a leader.
    if msg.get("event") == HeartbeatEvent.HEARTBEAT_NACK.value and msg.get("leader-channel"):
        # Send the request to the leader and wait for the response.
        leader_stub = connect_server(channel=msg.get("leader-channel"))
        response = leader_stub.PutEntriesRPC(request)
        msg = json.loads(response.message)

    if msg.get("event") == HeartbeatEvent.HEARTBEAT_ACK.value:
        response = {"msg": f"Data entries have been acknowledged by the server: [{msg.get('event')}]."}
    else:
        response = {"msg": f"Data entries were not acknowledged by the server: [{msg.get('event')}]."}
    return response


@app.post('/put_all', response_class=JSONResponse)
def put_all(entries: List[dict]):
    """
    Send multiple data entries to one of the cluster servers.
    :return: (JSONResponse) - Server repsonse.
    """
    response = put_entries(data=entries, host=RANDOM_SERVER_HOST, port=RANDOM_SERVER_PORT)
    return JSONResponse(content=response, status_code=200)


@app.post('/put_key', response_class=JSONResponse)
def put_key(entry: dict):
    """
    Send a data entry to one of the cluster servers.
    :return: (JSONResponse) - Server repsonse.
    """
    response = put_entries(data=[entry], host=RANDOM_SERVER_HOST, port=RANDOM_SERVER_PORT)
    return JSONResponse(content=response, status_code=200)
