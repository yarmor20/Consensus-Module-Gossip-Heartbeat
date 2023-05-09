from src.__logger import ServiceLogger
from src.__constants import *
from typing import Tuple, Union, List

import numpy as np
import logging
import datetime
import json
import os


def read_config(node: str) -> Tuple[dict, dict]:
    """
    Read cluster config.Get server config and the configuration of whole cluster.

    :param node: (str) - Node name (ID).
    :return: (dict, dict) - Server Config, Cluster Config.
    """
    with open(CONFIGURATION_PATH, "r") as node_config:
        cluster_config = node_config.read()
        cluster_config = json.loads(cluster_config)

    server_config = cluster_config.get("nodes", {}).get(node, {})
    return server_config, cluster_config


def compose_gossip_map(node: str, cluster_config: dict) -> dict:
    """
    Compose a gossip map to preserve the state of nodes in the cluster
    and to enable gossiping.

    :param node: (str) - Node name (ID).
    :param cluster_config: (dict) - Cluster config.
    :return: (dict) - Gossip map.
    """
    gossip_map = {}
    for cluster_node in list(cluster_config.get("nodes", {}).keys()):
        if cluster_node == node:
            continue

        gossip_map[cluster_node] = {
            "port": cluster_config.get("nodes", {}).get(cluster_node, {}).get("port", None),
            "state": {
                "node":      cluster_node,
                "state":     STATE_FOLLOWER,
                "leader":    None,
                "curr-term": 0,
                "cl-health": CL_HEALTH_RED,
                "l-vt-term": 0,
                "l-log-idx": -1,
                "l-log-cmt": -1,
                "ts":        0,
            }
        }
    return gossip_map


def compose_message(event: str, node: str, data: Union[dict, List[dict]]) -> str:
    """
    Compose the Heartbeat message used by the number of RPCs.

    :param event: (str) - Message event.
    :param node: (str) - Node name (ID).
    :param data: (Union[dict, List[dict]]) - Message contents (serializable).
    :return: (str) - Message.
    """
    message_json = {"event": event, "node": node, "data": data}
    message = json.dumps(message_json)
    return message


def decompose_message(message: str) -> (str, str, Union[dict, List[dict]]):
    """
    Get message contents from the Heartbeat type of message used by the number of RPCs.

    :param message: (str) - Message.
    :return: (tuple) - Deserialized message.
    """
    message_json = json.loads(message)
    event, node, data = message_json.get("event"), message_json.get("node"), message_json.get("data")
    return event, node, data


def get_majority_threshold(num_nodes: int) -> int:
    """
    Get the threshold that identifies the majority of nodes.

    :param num_nodes: (int) - Number of active nodes in the cluster.
    :return: (int) - Majority threshold.
    """
    threshold = np.floor(num_nodes / 2) + 1
    return threshold


def get_logger(name) -> logging.Logger:
    """
    Return INFO logger that is writing logs to a file.
    :return: (logging.Logger) - info logger.
    """
    # Create a directory to preserve logs.
    os.makedirs(LOGS_PATH, exist_ok=True)
    os.makedirs(f"{LOGS_PATH}{name}", exist_ok=True)

    filepath = f"{LOGS_PATH}{name}/logs_{datetime.datetime.utcnow().timestamp()}.log"

    # Introduce a bare logger.
    logging.basicConfig(level=logging.INFO)
    # Create Info Logger.
    ilogger = logging.getLogger(name)
    info_logger = ServiceLogger(name, filepath)
    ilogger.addHandler(info_logger)
    return ilogger
