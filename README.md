![](https://img.shields.io/github/license/yarmor20/Gossip-Raft-Consensus-Algorithm)
![](https://img.shields.io/github/commit-activity/w/yarmor20/Gossip-Raft-Consensus-Algorithm)
![](https://img.shields.io/github/last-commit/yarmor20/Gossip-Raft-Consensus-Algorithm)
![](https://img.shields.io/github/languages/code-size/yarmor20/Gossip-Raft-Consensus-Algorithm)

# Gossip-Raft-Consensus-Algorithm

This repository introduces a basic implementation of the Raft Consensus Algorithm that uses Gossip Heartbeat to solve the problems occurring with leader elections in the base version of Raft. The implementation uses a pull-based approach presented by MongoDB, modifying it for the requirements and benefits that Gossip Heartbeat introduces. More information about the whole approach can be found in the thesis *"Gossip Raft Consensus Algorithm"*.

The implementation is based on Python **gRPC**, an open-source high-performance remote procedure call (RPC) framework developed by Google, which allows to write distributed applications by defining remote procedure calls (RPCs) in a structured way.

## Functionality 
- A distributed system that benefits from multiple servers running simultaneously.
- Cluster *consistency checks*.
- Leader elections.
- Inter-server communication via the number of RPCs.
- Client-server interaction.
- Consistent data logging across the whole cluster.
- Data is committed once it has been replicated to the majority of servers.

## Remote Procedure Calls
We implemented several RPCs to provide an easy and consistent way to inter-server and client-server communication:
- `HeartbeatRPC`: Used to exchange gossip heartbeats between any couple of servers.
- `RequestVoteRPC`: Used by a candidate server during the leader election process to ask other servers to vote for it to get elected in the new term.
- `PullEntriesRPC`: Used by *syncing servers* to pull new log entries from their *sync sources*.
- `UpdatePositionRPC`: Used by *syncing servers* notify their *sync sources* that the pulled log entries have been replicated the their local log.
- `PutEntriesRPC`: Used by client server to pass new data entries to the leader server.
- `AckEntriesRPC`: Used by leader to notify the client that the data entries were safely committed.

## Cluster Normal Operation

Once the servers start running, they:
1. Establish connections with their peer servers.
2. Check if all the peer servers have acknowledged all the connections.
3. Leader election process starts to pick a leader server for a new term.
4. Client sends a request with data entries to any of the cluster servers. These requests are returned by follower servers to the leader.
5. Leader logs new log entries to its local log.
6. Followers continuously send PullEntries RPC to their sync source (leader) to pull new entries and log them in their local log.
7. After the log entries are replicated by the follower to its local log, the follower issues UpdatePosition RPC to notify its sync source that entries were replicated.
8. Once the log entries are replicated to the majority of servers, the log entry is committed by the leader.
9. Finally, the leader notifies the client that an entry is committed via AckEntries RPC.

> With each RPC call, each server sends a *gossip map* that contains the current server's information about the cluster's current state. Our gossiping approach uses this information to preserve a consistent cluster state.

## Installation
1. Clone the repository.
```bash
$ git clone https://github.com/yarmor20/Gossip-Raft-Consensus-Algorithm.git
$ cd Gossip-Raft-Consensus-Algorithm
```
2. Install requirements.
```bash
$ pip3 install -r requirements.txt
```

## Run
1. Change the configuration file `cluster-configuration.json` to the amount of cluster nodes needed.
2. Run severs in different Terminal windows.
```bash
$ sh start-server.sh n0
$ sh start-server.sh n1
$ sh start-server.sh n2
```
3. Run the client server on the desired port.
```bash
$ sh start-client.sh
```
4. Send a POST request to the client server `http://127.0.0.1:8000/put_all` with sample data in the request body, e.g:
```json
[
  {
    "data": {
      "sample": "key"
    },
  },
  {
    "mydata": {
      "my": "command"
    },
  }
]
```
