from src.__logger import ServiceLogger
from src.__constants import LOGS_PATH
from typing import Tuple

import logging
import datetime
import json
import os


def read_config(node: str) -> Tuple[dict, dict]:
    with open("cluster-configuration.json", "r") as node_config:
        cluster_config = node_config.read()
        cluster_config = json.loads(cluster_config)

    server_config = cluster_config.get("nodes", {}).get(node, {})
    return server_config, cluster_config


def compose_cluster_state(node: str, cluster_config: dict) -> dict:
    cluster_state = {}
    for cluster_node in list(cluster_config.get("nodes", {}).keys()):
        if cluster_node == node:
            continue

        cluster_state[cluster_node] = {
            "hrbt-port": cluster_config.get("nodes", {}).get(cluster_node, {}).get("heartbeat-port", None),
            "srvr-port": cluster_config.get("nodes", {}).get(cluster_node, {}).get("server-interaction-port", None),
            "state": {
                "leader": None,
                "curr-term": 0,
                "alive": False,
                "hrbt-ts": None
            }
        }
    return cluster_state


def compose_heartbeat_message(event: str, node: str, data: dict):
    message_json = {"event": event, "node": node, "data": data}
    message = json.dumps(message_json)
    return message


def decompose_heartbeat_message(message: str):
    message_json = json.loads(message)
    event, node, data = message_json.get("event"), message_json.get("node"), message_json.get("data")
    return event, node, data


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

