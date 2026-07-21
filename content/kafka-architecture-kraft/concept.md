# Kafka Architecture: Brokers, Controllers & KRaft

## What it is

A Kafka cluster is a set of **brokers** — processes that own partitions of
data, serve produce/fetch requests, and replicate data to each other. Every
cluster also needs a **control plane**: something that tracks cluster
metadata (which topics/partitions exist, which broker leads each partition,
which brokers are alive) and decides things like leader election when a
broker dies. **KRaft** (Kafka Raft) is Kafka's own built-in implementation of
that control plane, using the Raft consensus protocol — every broker in this
cluster runs in combined `broker,controller` mode, so the same three JVM
processes that serve reads and writes also *are* the metadata quorum. There
is no separate ZooKeeper ensemble anywhere in this stack.

## Why it matters

Every Kafka deployment older than a few years — and most tutorials, blog
posts, and interview questions you'll encounter — assumes **ZooKeeper**
coordinated the cluster: it held the metadata (topic configs, partition
assignments, ACLs), ran its own leader-election protocol among *its own*
nodes, and brokers watched it for changes. That was a second distributed
system a Kafka deployment depended on, with its own quorum, its own failure
modes, and its own operational burden (a separate process to run, monitor,
and upgrade in lockstep with Kafka itself).

KRaft removes that second system entirely. The metadata that used to live in
ZooKeeper's tree now lives in Kafka's own internal `__cluster_metadata` log,
replicated via Raft directly among the brokers that already exist. One of
those brokers is the **active controller** (KRaft's Raft leader for the
metadata log) at any given time; if it fails, the remaining voters elect a
new one the same way Kafka partitions elect a new leader — no ZooKeeper
watch, no second failover path to reason about. This is why every modern
Kafka deployment (and this app's own cluster) ships KRaft-only: fewer moving
parts, one consensus mechanism instead of two, and one less thing to
operate. A learner who only ever sees the current-generation setup should
still recognize the ZooKeeper-era vocabulary (ensemble, znode, controller
election via ZK) the moment they meet it in an older codebase or a senior
engineer's war story.

Two other terms this topic anchors, reused by nearly every later Kafka
topic in this curriculum:

- **Replication factor (RF)** — how many brokers hold a copy of each
  partition. RF=3 (this topic's default) means each partition survives up
  to 2 broker failures without data loss.
- **In-sync replica (ISR) set** — the subset of a partition's replicas that
  are fully caught up with the leader right now. A partition's ISR can
  shrink (a replica falls behind or its broker dies) and grow back
  (the replica catches up) without ever losing the partition's leader —
  `kafka-replication-fault-tolerance` (later in this curriculum) watches
  that shrink/grow cycle happen live.

## What to look for in this exercise

This topic runs no Spark job — it's Kafka's own control-plane and metadata
mechanics, observed directly. The notebook uses `kafka-python` (in-cluster,
reachable at `kafka-1:9092`/`kafka-2:9092`/`kafka-3:9092`) for the one
Python-side step (creating a topic by producing to it), and walks you
through a handful of CLI commands you run yourself in a **host terminal**
(not inside the notebook) — the driver container that runs this Jupyter
kernel has no Docker CLI/socket access, so anything that needs `docker exec`
has to run from your own machine, exactly like the broker-kill exercise in
`kafka-replication-fault-tolerance` later on.

- **`docker ps --filter "name=spark-kafka"`** — confirm 3 `spark-kafka-N`
  containers are running, and that no ZooKeeper container exists anywhere in
  `docker ps`'s full output.
- **`kafka-metadata-quorum.sh describe --status`** (via `docker exec`) —
  confirm `CurrentVoters` lists all 3 broker node IDs (the KRaft quorum) and
  note which one `LeaderId` names as the active controller.
- **`kafka-topics.sh --describe`** (via `docker exec`) — after the notebook
  creates a topic, read `PartitionCount`, `ReplicationFactor`, and each
  partition's `Leader` and `Isr` columns; confirm the ISR set has all 3
  broker IDs (nothing has failed yet).

Form a hypothesis about which broker will be the active controller and
which brokers will show up in each partition's ISR *before* you run these
commands — same habit this whole tool is built around.
