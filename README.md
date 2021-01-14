# juraft

juraft is a benchmark platform for exploring the potential benefits of using
raft to replicate juju transactions (`[]mgo.v2/txn.Op`) and coordinate their
application to an in-memory store which can support watchers and can be queried 
with tunable consistency options:
 - Quorum: read from the leader's store.
 - Local: read from any node and use a `txn-revno` assertion to avoid stale writes.

## The benchmark CLI 

### Getting started
First build the CLI (`make`), then bring up a juju controller and deploy a workload:

```
juju bootstrap lxd test
juju deploy percona-cluster
juju deploy keystone
juju relate percona-cluster keystone
```

Once everything is up and running:

```
CONTROLLER=$(juju show-controller | grep instance-id | cut -d' ' -f8)
lxc file push juraft $CONTROLLER/root/juraft
```

Then open three shells on the controller (e.g. `lxc exec -t $CONTROLLER bash`)
and run the following commands:

```
# terminal 0
./juraft --raft-port 1337 --raft-node-id n0 serve

# terminal 1
./juraft --raft-port 2337 --raft-node-id n1 --cluster-api-endpoints 127.0.0.1:1338 serve

# terminal 2: import juju db from controller and join the cluster
./juraft --raft-port 3337 --raft-node-id n2 --cluster-api-endpoints 127.0.0.1:2338 import-juju-db
./juraft --raft-port 3337 --raft-node-id n2 --cluster-api-endpoints 127.0.0.1:2338 serve
```

At this point the controller's DB will be imported and replicated across all 
three nodes. You can send a SIGHUP signal to any one of the three nodes to 
cause it to capture a snapshot, unpack it and dump its contents to STDOUT.

### Running benchmarks

TODO

## Design
### The in-memory store
The in-memory store maintains a map per collection where the index is the `_id`
of each document. For simplicity, a mutex is used to enforce single writer
semantics. This access pattern aligns nicely with single raft group semantics
(which is what the HA store provides) as raft log entries get applied sequentially.

The store supports most common assertions used by juju code:
- DocExists and DocMissing
- Field value assertions (e.g. `{"life": "Alive"}`)
- The `$or` operator for satisfy-any-of semantics.
- The `$eq` or `$ne` operators.

Updates expect payloads that include one or more mongo update operators. The
PoC only currently supports `$set` but support for other update operators that
juju uses (`$push`, `$pop`, `$pull`) can easily be added. Update arguments
may include either field names or field path expressions (e.g. `foo.bar.baz`).

Snapshots of the store contents are generated by calling `bson.Marshal` on the
root collection map. This makes it easy to dump them, unpack them and import
the contents into a mongo instance for debugging purposes.

### The HA store
The HA store decorates any `store.Store` value and provides fault-tolerance by
using raft to replicate apply transaction commands and apply them to each node's
local store instance. The store uses two kinds of services:
- The raft subsystem (implemented using hashicorp/raft) handles the raft internals
and drives the FSM that applies transactions to the local store instance.
- The cluster API service (exposed on `raft-port+1`) is a grpc endpoint that
implements raft leader discovery and allows the relaying of transactions from 
followers to the leader.

#### Leader discovery
Leader discovery is implemented via a simple gossip protocol. To join a
cluster, a node needs to be simply pointed to a list of cluster API endpoints.
The cluster join logic sequentially (in a loop) connects to each provided
cluster API endpoint and asks the remote node for the leader's raft address. 

If the remote node is not yet aware of the leader's address (i.e. it is also
trying to join the cluster), another endpoint is tried next. If the list of 
endpoints is exhausted, the local node sleeps and tries again.

If the remote node replies with the leader's address, the local node connects
to its cluster API endpoint (calculated from the raft address) and makes
another `GetRaftLeader` request providing its own raft address. The leader
checks whether the node is part of the cluster and adds it as a voting member
if it's not. This allows the raft subsystem to establish a connection from 
the leader to the local node and for FSM replication to commence.

Note that if the leader is already known from the raft configuration persisted
in the local log, it will be used and the above steps are effectively bypassed.

The cluster service observes the state of the raft cluster and automatically
connects to the cluster API endpoint of the new leader if a new leader gets
elected.

#### Transaction relaying
The HA store also implements the `store.Store` interface. When applying a
transaction, the store first checks if it is the raft leader. 

If it is, it serializes the transaction data and appends an entry to the raft
log so it can be applied to the FSMs of all nodes in the cluster.

Otherwise, it uses the cluster API to relay the serialized transaction data
to the leader, get back the result and return it to the caller.

#### Future work/ideas
The hashicorp raft implementation (as most OSS raft implementations) used by
this PoC works with a single raft group. This limits the transaction throughput
and allows "chatty" models that emit a large volume of transactions to starve
smaller models. 

To improve on this design, a multi-raft implementation can be used instead. In
this case, the UUID space will be dynamically partitioned into groups and each
node can be assigned to one or more groups (each group will be assigned to 3
nodes). 

For each group, one of the nodes will serve as the **group leader** and handle
reads and writes. Followers will relay transactions (and reads) to the group
leader using the `model-uuid` as a key.

This allows us to effectively namespace the log entries and distribute log
writes to different machines based on the `model-uuid` so that transactions
can be applied in parallel for different `model-uuid` values. Also, this model
allows us to implement more interesting features such as:
- follow the data: check the `model-uuid` at login time and redirect incoming clients 
  to the leader for the group responsible for that `model-uuid`.
- dynamic rebalancing to support cases as geo-replicated controllers where 
  the majority (n/2+1) of nodes (and the group leader) managing the group for a
  `model-uuid` are located in the same DC as the workload.

A much better overview of the follow-the-workload pattern can be found in the
CockroachDB [blog](https://www.cockroachlabs.com/docs/v20.2/topology-follow-the-workload).
