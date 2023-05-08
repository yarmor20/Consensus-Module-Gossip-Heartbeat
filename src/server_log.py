from src.server_state import ServerState
from typing import Optional
import datetime


class ServerLog:
    """
    A structure to save the Raft module log entries.
    Logs are persisted on disk to recover node's state from failures.
    """
    def __init__(self, state: ServerState):
        # A structure to save the log entries in.
        self.__logs: list = []

        # A structure where the current server's state is preserved.
        self.__state: ServerState = state

    def put_entries(self, data: list) -> bool:
        """
        Append entry to the log. Method invoked by ClientServerRPCHandler servicer
        once the leader receives new entries from the client.

        :param data: (dict) - Data entry.
        :return: (bool) - True if the entry was added to the log.
        """
        ts = datetime.datetime.now().timestamp()
        # Compose a log entry that consists of the entry itself and the entry UID
        # that consists of the log index, current term and timestamp.
        entry = {
            "data": data,
            "index": {
                "logIdx": self.__state.last_log_index + 1,
                "term":   self.__state.current_term,
                "ts":     ts
            }
        }

        # Append log entry to the log.
        self.__logs.append(entry)

        # Increase the last index.
        self.__state.last_log_index += 1
        return True

    def get_entries(self, s_idx: int, e_idx: Optional[int] = None) -> list:
        """
        Return a list of log entries in the specified boundaries.

        :param s_idx: (int) - Start index.
        :param e_idx: (int) - End index.
        :return: (list) - Log entries.
        """
        e_idx = len(self.__logs) if not e_idx else e_idx
        entries = self.__logs[s_idx:e_idx]
        return entries

    def __persist(self):
        """
        Persist current server's log on the disk to recover from failures.
        """
        pass
