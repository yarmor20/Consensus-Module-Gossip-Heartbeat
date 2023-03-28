import os
import json
import dotenv
from pprint import pprint
from typing import Tuple
from enum import Enum


dotenv.load_dotenv("cluster-server.env")


NODE = os.getenv("NODE")
CLUSTER_HOST = "localhost"
LOGS_PATH = "../logs/"


HEARTBEAT_TIMEOUT = 3.0


class HeartbeatEvent(Enum):
    """
    Denotes events used inside Kafka topics for message distinction.
    """
    HEARTBEAT_ACK = "ack"
    HEARTBEAT_NACK = "nack"
    HEARTBEAT_PING = "ping"
    HEARTBEAT_CLUSTER_HEALTH = "clhlth"
    HEARTBEAT_GOSSIP = "gsp"


if __name__ == '__main__':
    print(type(HeartbeatEvent.HEARTBEAT_GOSSIP.value))

