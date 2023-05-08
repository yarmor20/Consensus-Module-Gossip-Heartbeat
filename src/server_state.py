from src.__constants import *
import src.__utils as utils

from typing import Optional
import logging


class ServerState:
    """
    A structure for preserving the current state (current-term, leader, last-committed-index, etc.)
    of the current server. That information is used to recover node's state from failures.
    """
    def __init__(self, node: str):
        # Common Raft module server attributes.
        self.node: str = node
        self.state: str = STATE_FOLLOWER
        self.leader: Optional[str] = None
        self.current_term: int = 0

        # Cluster configuration and health.
        self.cluster_health: str = CL_HEALTH_YELLOW
        self.server_config, self.cluster_config = utils.read_config(self.node)

        # Information about the given vote in the previous leader election.
        self.last_vote_node: Optional[str] = None
        self.last_vote_term: int = 0

        # Index of the last entry in the log and last committed entry.
        self.last_log_index: int = -1
        self.last_log_commited: int = -1

        # Server event logger. All the logs are preserved in the log file.
        self.logger: logging = utils.get_logger(name=f"Server-Logger-{self.node}")

    def to_dict(self) -> dict:
        """
        Return a serializable representation of a server state.

        :return: (dict) - Server state.
        """
        state = {
            "node":      self.node,
            "state":     self.state,
            "leader":    self.leader,
            "curr-term": self.current_term,
            "cl-health": self.cluster_health,
            "l-vt-term": self.last_vote_term,
            "l-log-idx": self.last_log_index,
            "l-log-cmt": self.last_log_commited
        }
        return state

    def __persist(self):
        """
        Persist current server's state on the disk to recover from failures.
        """
        pass
