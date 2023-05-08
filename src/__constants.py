from enum import Enum
import dotenv
import os

# Load the .env file.
dotenv.load_dotenv("cluster-server.env")

# -------------- NODE CONFIGURATION --------------
NODE = os.getenv("NODE")  # Node name (ID) in the cluster.
CLUSTER_HOST = os.getenv("HOST")

# --------------------- LOGS ---------------------
LOGS_PATH = "logs/"

# ------------------- TIMEOUTS -------------------
HEARTBEAT_TIMEOUT = 2.0
ELECTION_TIMEOUT = 15.0
REQUEST_VOTE_TIMEOUT = 4.0
CANDIDATE_ELECTION_TIMEOUT = 7.0

# ----------------- NODE STATES ------------------
STATE_LEADER = "ldr"
STATE_FOLLOWER = "flwr"
STATE_CANDIDATE = "cndt"

# ----------------- CLUSTER HEALTH ------------------
CL_HEALTH_GREEN = "clhgrn"    # All connections are preserved.
CL_HEALTH_YELLOW = "clhylw"   # Not all connections are preserved but cluster can run.
CL_HEALTH_RED = "clhred"      # Cluster cannot run.

# ----------------- RPC MESSAGE ------------------
RPC_MSG_PARAM_STATE = "state"
RPC_MSG_PARAM_GOSSIP_MAP = "gossip"

# ------------ RANDOM SERVER CHANNEL -------------
# TODO: Add randomized port and host based on the cluster config.
RANDOM_SERVER_PORT = 50053
RANDOM_SERVER_HOST = "localhost"


class HeartbeatEvent(Enum):
    """
    Denotes events used inside the RPC Servicer for message distinction.
    """
    HEARTBEAT_ACK = "ack"
    HEARTBEAT_NACK = "nack"
    HEARTBEAT_PING = "ping"
    HEARTBEAT_CLUSTER_HEALTH = "clhlth"
    HEARTBEAT_GOSSIP = "gsp"
    REQUEST_VOTE = "rqstvt"
    LEADER_ESTABLISHMENT = "ldrestbl"
