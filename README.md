# Simple Amazon Dynamo

This application is a very simplified version of Dynamo. There are three main pieces that have been implemented:

```
1. Partitioning
2. Replication
3. Failure handling
```
The main goal is to provide both availability and linearizability at the same time. In other words, the implementation will always perform read and write operations successfully even under failures. At the same time, a read operation always returns the most recent value. Partitioning and replication are done exactly the way Dynamo does.

Based on the design of Amazon Dynamo, the following features have been implemented:

## Membership
Just as the original Dynamo, every node can know every other node. This means that each node knows all other nodes in the system and also knows exactly which partition belongs to which node; any node can forward a request to the correct node without using a ring-based routing.

## Request routing
Unlike Chord, each Dynamo node knows all other nodes in the system and also knows exactly which partition belongs to which node. Under no failures, all requests are directly forwarded to the coordinator, and the coordinator should be in charge of serving read/write operations.

## Chain replication
- The replication degree N should be 3. This means that given a key, the key’s coordinator as well as the 2 successor nodes in the Dynamo ring should store the key.

- Both the reader quorum size R and the writer quorum size W should be 2.

- The coordinator for a get/put request should always contact other two nodes and get a vote from each (i.e., an acknowledgement for a write, or a value for a read).

- For write operations, all objects can be versioned in order to distinguish stale copies from the most recent copy.

- For read operations, if the readers in the reader quorum have different versions of the same object, the coordinator should pick the most recent version and return it.


## Failure handling
Handling failures should be done very carefully because there can be many corner cases to consider and cover. Just as the original Dynamo, each request is used to detect a node failure. For this purpose, a timeout for a socket read is used; and if a node does not respond within the timeout, it is considered as a failed node. When a coordinator for a request fails and it does not respond to the request, its successor is contacted next for the request.
