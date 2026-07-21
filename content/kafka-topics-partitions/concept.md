# Topics & Partitions: Ordering and Distribution

## What it is

A Kafka **topic** is a named stream of messages, but the unit Kafka actually
stores, replicates, and parallelizes on is the **partition** — a topic is
just an ordered set of one or more partitions, each an append-only log with
its own offsets starting at 0. Every message produced to a topic is written
to exactly one partition, chosen by the producer's **partitioner** using two
signals: the message's optional **key** and the number of partitions the
topic has. Same key, same partition count → same partition, deterministically,
every time — Kafka hashes the key bytes (murmur2, the same algorithm the
Java client uses) and maps the hash onto one of the topic's partitions. A
message with **no key** has no such hash input, so the producer falls back
to some other selection strategy to spread load — which one varies by client
implementation and version (see "What to look for" below for what this
repo's pinned client actually does, confirmed by running it, not assumed).

Kafka's ordering guarantee is scoped to exactly this unit: messages **within
one partition** are delivered to consumers in the exact order they were
produced. There is **no ordering guarantee across partitions** of the same
topic — two messages sent to different partitions, even microseconds apart,
can be consumed in either order or interleaved arbitrarily depending on
consumer timing. This is the single most common thing a learner new to
Kafka gets wrong: assuming "the topic" is ordered, when the actual unit of
ordering is "the partition."

## Why it matters

Partition count is also the topic's **unit of parallelism**: a consumer
group can have at most one consumer per partition actively reading it at a
time (more consumers than partitions just sit idle), so partition count sets
the ceiling on how many consumers can process a topic's traffic in parallel.
Key choice is therefore a real design decision with two competing pulls:

- **Pick a key that groups what must stay ordered together** — e.g. all
  events for one `user_id`, one `order_id`, one `device_id` — so a single
  consumer processing that partition sees that entity's history in order.
- **Don't pick a key so skewed that it collapses parallelism** — if one key
  value dominates the traffic (a single huge customer, a single hot device),
  every message for it lands on the same partition regardless of how many
  partitions the topic has, and that one partition (and the one consumer
  reading it) becomes the bottleneck no amount of extra partitions fixes.

Getting this wrong is a recurring real-world failure mode: teams that
assume topic-wide ordering build downstream logic that silently
misorders events the moment traffic crosses more than one partition, and
teams that pick a low-cardinality or skewed key build a "parallel" system
that never actually parallelizes.

## What to look for in this exercise

This topic runs no Spark job — it's Kafka's own producer/partitioner and
consumer-ordering mechanics, observed directly via `kafka-python` against
the live 3-broker cluster (in-cluster at `kafka-1:9092` / `kafka-2:9092` /
`kafka-3:9092`), the same client this app's driver image ships for
`kafka-architecture-kraft`. The notebook auto-creates a 3-partition topic
(`KAFKA_NUM_PARTITIONS=3` on this cluster) and walks through:

- **Keyed sends land on the same partition, every time.** Sending 4 messages
  each under 3 distinct keys shows every message under one key reporting the
  identical `RecordMetadata.partition`, while different keys spread across
  the topic's partitions.
- **Unkeyed sends, actually measured.** This repo pins `kafka-python==2.0.2`.
  Reading that version's `DefaultPartitioner` source
  (`kafka/partitioner/default.py`) and confirming it live against the
  running cluster: when `key is None`, it calls `random.choice(available)`
  on **every single send** — an independent uniformly-random partition pick
  per message. This is neither classic round-robin (a fixed repeating cycle)
  nor the newer sticky-partitioner batching behavior (long runs on one
  partition before switching) — it's per-message random selection. The
  notebook prints the raw partition sequence from 30 unkeyed sends so you
  can see the lack of cycle or run structure directly, not just a claim
  about it.
- **Per-partition ordering, and its limit.** Reading back one key's messages
  from the single partition they landed on (`TopicPartition` + `seek_to_beginning`)
  reproduces the exact produce order. The notebook's closing section makes
  explicit what that step does *not* prove: nothing guarantees order across
  the topic's other partitions, and the unkeyed sends from the partitioner
  step are the notebook's own evidence of messages landing on different
  partitions in an order a naive per-partition read would not reconstruct.

Form a hypothesis before running the no-key cell: will 30 unkeyed sends look
like round-robin, sticky batching, or something else? Same habit this whole
tool is built around.
