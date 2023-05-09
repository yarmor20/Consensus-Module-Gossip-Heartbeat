from src.server_state import ServerState
from typing import Optional
import datetime
import uuid


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

    def put_entries(self, data: list) -> list:
        """
        Append entry to the log. Method invoked by ClientServerRPCHandler servicer
        once the leader receives new entries from the client.

        :param data: (dict) - Data entry.
        :return: (list) - List of log entry UUIDs.
        """
        # Preserve the etries UUIDs to be used in UpdatePosition RPC later on.
        entry_uuids = []

        # Get the timestamp the entries were received.
        ts = datetime.datetime.now().timestamp()
        # Compose a log entry that consists of the entry itself and the entry UID
        # that consists of the log index, current term and timestamp.
        for entry in data:
            # If the received entry was pulled from the sync source, preserve it as it is.
            if entry.get("__data") and entry.get("__index"):
                # Get entry UUID.
                entry_uuid = entry.get("__index", {}).get("_uuid")

                # Do not modify the already composed entry.
                log_entry = entry

            # If the entry was received from the client by a leader, compose a log entry with UUID as a unique ID.
            else:
                # Concatenate the term value and timestamp.
                # Generate a UUID based on the concatenated string.
                uuid_string = f"{self.__state.current_term}-{ts}"
                entry_uuid = str(uuid.uuid5(uuid.NAMESPACE_OID, uuid_string))

                # Compose a log entry (only done by leader node).
                log_entry = {
                    "__data": entry,
                    "__index": {
                        "_uuid": entry_uuid,
                        "_term": self.__state.current_term,
                        "_ts":   ts
                    }
                }

                # Add that entry to a commit map as one that is acknowledged by leader.
                self.__state.commit_map[entry_uuid] = {self.__state.node}

            # Append log entry to the log.
            self.__logs.append(log_entry)
            entry_uuids.append(entry_uuid)

            # Increase the last index.
            self.__state.last_log_index += 1
        return entry_uuids

    def get_entries(self, s_idx: int, e_idx: Optional[int] = None) -> list:
        """
        Return a list of log entries in the specified boundaries.

        :param s_idx: (int) - Start index.
        :param e_idx: (int) - End index.
        :return: (list) - Log entries.
        """
        s_idx = 0 if s_idx == -1 else s_idx
        e_idx = len(self.__logs) if not e_idx else e_idx

        try:
            entries = self.__logs[s_idx:e_idx]
        except IndexError:
            return []
        else:
            return entries

    def __persist(self):
        """
        Persist current server's log on the disk to recover from failures.
        """
        pass
