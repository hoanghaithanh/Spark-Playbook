# Kafka Curriculum (learn Kafka itself) — Requirements

Status: Draft for architect handoff — 2 open questions resolved by the human 2026-07-20 (see
"Resolved decisions" below and the updated Open Questions section)
Owner: requirements-analyst
Date: 2026-07-20
Traceability: Formalizes a human-approved plan from an interactive planning session (2026-07-20,
not yet captured as a saved plan file) into testable requirements. No GitHub milestone exists yet
for this body of work — per CLAUDE.md, milestones are owned solely by `project-manager`; the
release-vs-fold-into-an-existing-milestone call is explicitly left to that agent's next pass (see
`docs/backlog.md` row #41 and its "New body of work" section). No GitHub issues are filed by this
doc either, matching how v1.1 (`live-market-data-streaming.md`) and v1.2
(`multi-broker-kafka-cluster.md`) both left issue-filing until after their architect pass.

## Resolved decisions (human, 2026-07-20)

Two of this doc's original open questions were answered directly by the human before an architect
pass began, and both **expand this body of work's scope beyond what the original draft assumed**:

- **Topics-index grouping: resolved YES.** The topics-index page will visually group the 12 Kafka
  topics together and the 15 existing Spark topics together (two sections/tracks), not one flat
  `order`-sorted list. This reverses the original Non-goals bullet ("No new manifest schema field for
  topic grouping/categorization") — grouping is now in scope. Concretely this needs a new manifest
  field (e.g. `track: kafka` / `track: spark`) plus an `app/topics/loader.py` and topics-index
  template change to render two grouped sections instead of one flat list — **this is no longer a
  content-only body of work**; it has a real, if small, app-code component. Exact field name,
  whether the 15 existing Spark topics need their manifests touched or default to `track: spark` when
  the field is absent, and the rendered section order/labels are left to the architect pass, not
  decided here.
- **Schema registry + Avro/Protobuf dependency: resolved YES, in scope.** This reverses the original
  Non-goals bullet ruling both out and un-blocks US-KC8 (`kafka-serialization-schema-evolution`) —
  see that story's updated status below. This is a materially bigger addition than a manifest tweak:
  it means a new service in the compose stack (a schema registry — e.g. Confluent Schema Registry,
  Apicurio, or another lightweight alternative — the specific choice is an architect call) plus an
  Avro or Protobuf client library baked into the driver/Jupyter image, comparable in kind to how
  Kafka itself was introduced as new infra in #50's ADR (`docs/architecture/kafka-streaming-infra.md`).
  It likely warrants its own architecture doc/ADR the same way the Kafka broker addition got one,
  rather than being folded as a footnote into whatever ADR covers the rest of this curriculum. Exact
  registry product, resource-budget impact (the compose stack already tracks a 32GB ceiling that
  Kafka brokers contribute to), and client library choice are architect-level decisions, not made
  here.

The remaining open questions (US-KC7 standalone-vs-fold, exact `order` values, US-KC11's spike
timing, per-topic `kafka_broker_count` defaults) are **explicitly left open** at the human's request,
to be revisited after project-manager and architect have looked at this doc — see the updated Open
Questions section.

## Relationship to existing docs (not a supersession or amendment)

This is a **new, additive** curriculum area, not a change to previously-shipped scope — no existing
requirements doc or ADR decision is being reversed, so there is no "Supersedes"/"Amends" section per
CLAUDE.md's convention. It does, however, sit next to several existing bodies of work and the
overlap needs to be named explicitly so it isn't mistaken for silent duplication:

- **v1.1 — Live Market Data Streaming** (`docs/requirements/live-market-data-streaming.md`, backlog
  row #18, milestone #13, unstarted). That work teaches Structured Streaming *against a real
  application* (a live price dashboard) using Kafka as plumbing underneath. US-KC7
  (`kafka-spark-structured-streaming`) originally taught the Structured-Streaming-reads-from-Kafka
  *mechanic itself* as a standalone topic — **resolved 2026-07-20: it folds into v1.1's
  `structured-streaming` topic (issue #53) instead**, per the human's answer to what was Open
  Question 1. It is no longer part of this doc's 12-topic schedulable set; see US-KC7's entry below
  for what carries forward as reference material for #53.
- **v1.2 — Multi-Broker Kafka Cluster & Monitor** (`docs/requirements/multi-broker-kafka-cluster.md`,
  backlog row #40, milestone #15). Sub-stories (a)/#56 and (b)/#57 are done (multi-broker topology,
  observability data layer); (c)/#58 (JMX exporter), (d)/#59 (Monitor UI tab), (e)/#60 (broker-kill
  demo panel) remain open and unstarted. US-KC10 below (`kafka-monitoring-observability`) and US-KC5
  (`kafka-replication-fault-tolerance`) both lean on that infrastructure — US-KC10 is explicitly
  gated on #58/#59 for its JVM-level metrics (see that story), and US-KC5 exercises the same
  broker-kill mechanic as #60's in-app demo panel but as a **manual notebook exercise**, independent
  of whether #60 ever ships (#60 is an in-app UI convenience over the same underlying `docker stop`
  mechanic; US-KC5 doesn't need the panel to exist).
- **Kafka infra ADRs** (`docs/architecture/kafka-streaming-infra.md`, `docs/architecture/
  multi-broker-kafka-cluster.md`). Both already establish the CLI-shellout-over-`KafkaAdminClient`
  discipline and the baked-at-build-time discipline for connector jars — this doc's notebooks
  inherit both without restating the ADR decisions themselves.

## Problem statement

Every one of the 15 existing curriculum topics teaches PySpark; Kafka appears only as
infrastructure plumbing under one still-unstarted streaming topic. The human has a standing,
explicitly stated interest in going deeper on Kafka mechanics for their own sake — partitioning
strategy, delivery guarantees, consumer-group rebalancing, replication, exactly-once semantics —
not just "Kafka as a pipe Spark reads from." A learner using this app today has no way to build that
understanding: there is no topic anywhere that teaches Kafka *as a distributed system in its own
right*. This work adds a second, parallel curriculum track — Kafka topics teaching Kafka — using the
exact same topic-page shell, manifest schema, and notebook format the 15 PySpark topics already use,
so no application code changes are required to ship it.

## Goals / Non-goals

### Goals

- **G-KC1 — 12 new topics, same content-topic pattern as every existing topic.** Each new topic is
  a `content/<id>/` folder with `concept.md` + `notebook.ipynb` + `manifest.yaml`, following the
  exact conventions `app/topics/loader.py` already parses (verbatim-scraped "What it is" blurb,
  numbered `## N. Title` notebook cells, `cluster_defaults`/`requires_kafka` manifest fields) — no
  loader, routing, or template changes are needed for these topics to appear on the topics-index page
  and render through the existing shell.
- **G-KC2 — Basic → advanced ladder, mirroring the existing curriculum's structure.** 4 basic
  (architecture/KRaft, topics/partitions, producers/delivery, consumers/groups), 4 intermediate
  (replication/fault-tolerance, log compaction/retention, Kafka-as-a-Structured-Streaming-source,
  serialization/schema evolution), 4 advanced (performance tuning, monitoring/observability,
  exactly-once/transactions, multi-broker cluster ops).
- **G-KC3 — Every topic runs against the app's already-spawnable Kafka cluster.** All 12 topics set
  `requires_kafka: true` and lean on `kafka_broker_count` (1-5, per-topic default) exactly as the
  manifest schema already supports — this is the schema's first real consumer.
- **G-KC4 — Honest status per topic, not silent optimism.** Topics with a real gap between "fully
  demonstrable today" and "the real mechanic" are documented as such, not quietly descoped to a
  weaker demo without saying so (US-KC10: JVM metrics need #58/#59; US-KC11: `kafka-python==2.0.2`'s
  transactional producer support needs a feasibility spike).
- **G-KC5 — Grouped topics-index (resolved 2026-07-20).** The topics-index page groups the 12 Kafka
  topics and the 15 existing Spark topics into two visually distinct sections/tracks, not one flat
  `order`-sorted list. Requires a new manifest field and an `app/topics/loader.py` + template change
  — mechanism left to the architect pass (see "Resolved decisions" above).
- **G-KC6 — Schema registry + Avro/Protobuf in scope (resolved 2026-07-20).** US-KC8 gets a real
  schema-registry demo, not a JSON-only stand-in. Requires new compose-stack infra (a schema registry
  service) and a new driver-image dependency (Avro or Protobuf client library) — product/library
  choice and resource-budget impact left to the architect pass (see "Resolved decisions" above).

### Non-goals

- **No self-check `annotation:` blocks on any of the 12 new topics.** See "Self-check annotation
  decision" below — this is an explicit decision, not an oversight.
- ~~No new manifest schema field for topic grouping/categorization.~~ **Resolved into scope
  2026-07-20 — see "Resolved decisions" above.** A Kafka-vs-Spark grouped topics-index is now a goal
  (G-KC5 below), not a non-goal.
- ~~No schema registry, no Avro/Protobuf library.~~ **Resolved into scope 2026-07-20 — see "Resolved
  decisions" above.** US-KC8 is no longer written against this constraint; it's gated on an
  architect-level infra decision instead (G-KC6 below).
- **No new Kafka client capability beyond what's already baked into the driver/Jupyter image**
  (`kafka-python==2.0.2`, `spark-sql-kafka-0-10_2.13:4.0.3`). If US-KC11's spike concludes
  `kafka-python`'s transactional producer is unusable, the fallback is the CLI
  (`kafka-console-producer.sh --transactional-id`), not a new pinned dependency.
- **No app-code, loader, routing, or template changes.** This doc is `content/` only — confirmed via
  a light check of `app/topics/loader.py` (already reads `manifest.yaml`'s `requires_kafka` and
  `cluster_defaults.kafka_broker_count`, both currently unused by any shipped topic; `list_topics()`
  already auto-discovers any `content/*/manifest.yaml`, sorted by `order`) that no schema or
  discovery-logic gap needs filling for these topics to appear and render.
- **No new GitHub milestone or issues filed by this doc.** Per CLAUDE.md, that's `project-manager`'s
  call (see Traceability above).

## Self-check annotation decision

**Decision: none of the 12 new Kafka topics ship a self-check `annotation:` block in v1.** The
existing self-check engine (`app/annotation/`, the `plan_nodes`/`stage_metrics`/`executor_metrics`
manifest rules) matches against Spark's own `explain()` physical-plan text and Spark's REST
job/stage/executor metrics. There is no equivalent schema for Kafka-native state — ISR membership,
consumer-group lag, partition-leader assignment, in-flight transaction state — and inventing one is
a real engine-extension project (a new manifest-rule type, a new evidence-fetch path against
`kafka_stats.py`'s `KafkaSnapshot`, comparable in scope to the architect-level "Decision A" calls
already made for Executor Tuning/Memory Management/Fault Tolerance & Lineage), not something this
requirements doc can wave into existence. Each of the 12 topics instead teaches its self-check
entirely through the notebook's own printed output and live CLI commands (`kafka-topics.sh
--describe`, `kafka-consumer-groups.sh --describe`, etc.) — consistent with how every acceptance
criterion below is phrased as "the notebook/CLI output shows X," not "Reveal confirms X." Whether a
Kafka-native self-check schema is worth building is **future scope**, explicitly not decided here —
if the human wants it, that's a dedicated architect-led design effort on its own, sequenced after
these 12 topics exist and after seeing which ones would most benefit.

## User stories and acceptance criteria

Each story below is independently shippable — same "one topic, one story" grain the existing
curriculum backlog rows already use (e.g. row #14 Caching, row #27 Executor Tuning). Sizing follows
the same S/M/L convention as the rest of `docs/backlog.md`.

**US-KC1 — `kafka-architecture-kraft` (Basic, S).**
As a learner, I want to see brokers, controllers, partitions, and replication in a KRaft (no
ZooKeeper) cluster, so I understand Kafka's own control-plane architecture before touching any
producer/consumer code.

- *Given* a spawned cluster with `kafka_broker_count: 3`, *when* the notebook runs
  `kafka-metadata-quorum.sh describe --status` via `docker exec`, *then* it shows all 3 brokers as
  quorum voters and identifies the active controller — demonstrating combined `broker,controller`
  mode with no separate ZooKeeper process anywhere in the stack.
- *Given* the same cluster, *when* the notebook creates a topic (auto-create via a first produce, or
  `kafka-topics.sh --create`) and runs `kafka-topics.sh --describe`, *then* the output shows
  partition count, replication factor, leader, and ISR set per partition, and `concept.md` explains
  what each column means.
- *Given* the notebook's walkthrough, *when* a learner reads `concept.md`'s "Why it matters" section,
  *then* it explicitly contrasts KRaft against the legacy ZooKeeper-coordinated architecture (what
  ZooKeeper used to do, why KRaft removes it) — this is a "why it matters" a Kafka-only learner
  needs and a PySpark-only topic never surfaces.

**US-KC2 — `kafka-topics-partitions` (Basic, S).**
As a learner, I want to see how partition count and key choice determine message ordering and
distribution, so I understand the unit Kafka actually parallelizes and orders on.

- *Given* a topic created with 3+ partitions, *when* the notebook produces keyed messages via
  `kafka-python` (same key repeated multiple times, several distinct keys), *then* it shows — via
  each message's returned `RecordMetadata.partition` — that all messages sharing a key land on the
  same partition every time, and different keys are distributed across partitions.
- *Given* messages produced with no key, *when* observed across many sends, *then* the notebook
  shows the resulting partition distribution (round-robin or the sticky-partitioner batching
  behavior actually shipped in this `kafka-python` version — whichever it verifiably is, not
  assumed) and `concept.md` names which one it is.
- *Given* a single partition read back via a consumer, *when* messages are consumed in order,
  *then* the notebook demonstrates per-partition ordering holds (messages appear in produce order)
  while explicitly noting no cross-partition ordering guarantee exists — the concept a learner most
  often gets wrong.

**US-KC3 — `kafka-producers-delivery` (Basic, M) — CORRECTED 2026-07-20 (idempotence bullet).**
As a learner, I want to see how `acks`, idempotence, and retries actually change delivery behavior
under failure, so I understand at-least-once vs. at-most-once as observed behavior, not just
vocabulary.

**Correction, 2026-07-20:** the architect's US-KC11 feasibility spike (`docs/architecture/
kafka-curriculum.md` D-KC3) found `kafka-python==2.0.2` — the client baked into the driver image —
has no idempotent-producer support at all: no `enable_idempotence`/`transactional_id` in
`KafkaProducer.DEFAULT_CONFIG`, confirmed directly against the library source, not just documented as
"limited." The second bullet below originally assumed `enable.idempotence=true` was settable via the
`kafka-python` client; it wasn't, and this story was incorrectly marked "fully buildable today, no
blockers" in `docs/backlog.md` row #43 as a result. Fixed below using the same CLI-fallback pattern
already established for US-KC11 (`kafka-console-producer.sh`) rather than the Python client — still
fully buildable today, no new dependency, just a different mechanism for one bullet. The `acks`-only
bullets (unaffected — `acks` itself is a real, supported `kafka-python` config) are unchanged.

- *Given* a producer configured with `acks=0`, *when* the notebook induces a failure (e.g. a
  transient broker restart or an artificially short timeout), *then* it shows message loss is
  possible and unacknowledged — no delivery confirmation exists to detect it.
- *Given* the same scenario with `acks=all` and idempotence enabled **via
  `kafka-console-producer.sh --producer-property acks=all --producer-property
  enable.idempotence=true` (subprocess-driven from the notebook, not `kafka-python` — that client has
  no idempotence support in this repo's pinned version)**, *when* the same failure is induced, *then*
  the notebook shows retries occur automatically and no duplicate is written (confirmed via a
  consumer re-read count matching the produced count) — demonstrating the idempotent producer's dedup
  guarantee, not just asserting it in prose.
- *Given* `acks=1` (leader-only ack, the middle ground, demonstrable via the `kafka-python` client
  same as the `acks=0` bullet above), *when* contrasted against the two runs above, *then*
  `concept.md` explains the specific failure window `acks=1` leaves open (leader acks before
  followers replicate, then the leader dies) that `acks=all` closes.
- *Given* the three `acks` configurations run back to back, *when* the notebook is complete, *then*
  it prints a summary table of at-least-once vs. at-most-once vs. (practically) exactly-once
  producer-side behavior observed, tying back to the delivery-semantics vocabulary a learner will
  meet again in US-KC11.
- *Given* this story now mixes `kafka-python` (first/third/fourth bullets) and CLI subprocess calls
  (second bullet) in one notebook, *when* `concept.md` is read, *then* it explains why — the same
  "client library doesn't support this, CLI does" constraint already established for US-KC11 — so a
  learner isn't confused by the inconsistency.

**US-KC4 — `kafka-consumers-groups` (Basic, M).**
As a learner, I want to see offset commits, consumer-group rebalancing, and partition-bounded
parallelism firsthand, so I understand why a consumer group can never have more active consumers
than partitions.

- *Given* a topic with N partitions and a consumer group with fewer than N members, *when* the
  notebook runs `kafka-consumer-groups.sh --describe --group <g>`, *then* it shows each active
  consumer owning one or more partitions and the group's total lag.
- *Given* the same group scaled up to exactly N members, *when* re-described, *then* each consumer
  owns exactly one partition — demonstrating the partition-count ceiling on group parallelism.
- *Given* the group scaled beyond N members, *when* re-described, *then* the notebook shows the
  excess consumer(s) sitting idle (zero partitions assigned) and `concept.md` explains why adding
  more consumers than partitions buys nothing.
- *Given* a consumer with `enable.auto.commit=False` performing manual `commit()` after processing,
  *when* the consumer is killed mid-batch and restarted, *then* the notebook shows it resumes from
  the last committed offset (possible reprocessing of the in-flight batch, never silent data loss)
  — contrasting manual commit against auto-commit's weaker at-most-once-on-crash behavior.

**US-KC5 — `kafka-replication-fault-tolerance` (Intermediate, M).**
As a learner, I want to manually kill a broker and watch leader election and ISR shrink happen live,
so I understand replication as an observable guarantee, not a diagram.

- *Given* a live `kafka_broker_count: 3` spawn with a topic at RF=3/`min.insync.replicas=2` (the
  app's existing default policy, per `multi-broker-kafka-cluster.md`), *when* the notebook runs
  `kafka-topics.sh --describe`, *then* it shows the initial leader and a full 3-member ISR set.
- *Given* that state, *when* a learner runs `docker stop spark-kafka-<leader's broker id>` (a
  documented manual step in `concept.md`, mirroring the established manual-kill convention from
  Fault Tolerance & Lineage/`multi-broker-kafka-cluster.md` US-MBK5), *then* re-describing the topic
  shows a new leader elected among the surviving 2 brokers and the ISR set shrunk to 2 members.
- *Given* a producer with `acks=all` actively writing during the kill, *when* observed, *then*
  writes continue succeeding throughout (min-isr=2 is still satisfiable with 2 surviving brokers) —
  the concrete "the cluster keeps serving" payoff of RF=3/min-isr=2.
- *Given* the killed broker is restarted, *when* it rejoins, *then* the notebook shows the ISR set
  grow back to 3 as it catches up — completing the shrink/grow cycle.
- *Given* `min.insync.replicas` is deliberately set to 3 (equal to RF) on a second, contrast topic,
  *when* one broker is killed, *then* the notebook shows `acks=all` writes now fail
  (`NotEnoughReplicasException` or equivalent) — the concrete cost of an over-strict min-isr,
  contrasted against the first topic's min-isr=2 configuration.
- **Independent of issue #60** (the in-app broker-kill demo panel, still open/unstarted): this
  story's kill mechanic is a manual `docker stop`/CLI-observed exercise entirely inside the
  notebook, with no dependency on #60's UI ever shipping.

**US-KC6 — `kafka-log-compaction-retention` (Intermediate, M).**
As a learner, I want to see retention-based deletion contrasted against log-compaction, so I
understand when a topic behaves like a queue and when it behaves like a changelog/KTable.

- *Given* a standalone topic created with `cleanup.policy=compact` (self-contained — does not
  depend on v1.1's still-unstarted `price-subscriptions` topic), *when* the notebook produces
  several updates to the same small set of keys, *then* it shows (via `kafka-log-dirs.sh` segment
  info or a full re-consume) that only the latest value per key survives after compaction runs.
- *Given* a key produced with a `null` value (a tombstone), *when* compaction runs and enough time
  passes (or `delete.retention.ms` is configured short enough for the demo), *then* the notebook
  shows that key disappears entirely from the compacted log — the mechanism the control-topic
  pattern in v1.1 relies on, taught here standalone.
- *Given* a second, contrast topic with `cleanup.policy=delete` and a short `retention.ms`, *when*
  the retention window elapses, *then* the notebook shows old segments age out regardless of key
  repetition — the queue-like behavior contrasted directly against the changelog-like behavior
  above.
- *Given* both behaviors demonstrated, *when* a learner reads `concept.md`, *then* it names the
  concrete use case each policy fits (event queue vs. changelog/KTable-style "latest state per key")
  and forward-references v1.1's control-topic as a real-world example of the compacted pattern
  (without depending on v1.1 being built).

**US-KC7 — `kafka-spark-structured-streaming` (Intermediate, M) — FOLDED INTO v1.1, 2026-07-20. NOT
a standalone Kafka-curriculum topic.**
**Resolved (human, 2026-07-20):** Open Question 1 is answered — this story folds into v1.1's
`structured-streaming` topic (GitHub issue [#53](https://github.com/hoanghaithanh/Spark-Playbook/issues/53),
"Live Market Data Streaming (b/d): Spark Structured Streaming job/notebook", milestone #13) rather
than shipping as its own `content/kafka-spark-structured-streaming/` folder. It does **not** get a
GitHub issue of its own and is not part of the 12-topic curriculum's schedulable set going forward
(11 topics remain independently schedulable: US-KC1-6, US-KC8-12). The acceptance criteria below are
kept as **reference material for whoever implements #53** — the minimal-mechanic lesson (schema,
watermarking/bounded-state, checkpoint recovery) they describe should be incorporated into that
topic's notebook alongside its real-data application, not dropped.

As a learner, I want to see Kafka used as a Structured Streaming source with watermarking and
checkpointing, so I understand the Kafka-to-Spark integration mechanic itself, independent of any
specific application built on top of it.

- *Given* a small throwaway topic and the already-baked-in `spark-sql-kafka-0-10_2.13:4.0.3`
  connector jar, *when* the notebook opens a `readStream` against it, *then* it shows the resulting
  DataFrame's schema (`key`, `value`, `topic`, `partition`, `offset`, `timestamp`) and that no Maven
  fetch occurs at runtime (jar is baked, per the existing ADR discipline).
- *Given* a synthetic producer script writing timestamped test events into the throwaway topic,
  *when* a windowed aggregation with `withWatermark` runs, *then* the notebook demonstrates state
  plateauing (bounded state) vs. an equivalent run with no watermark showing unbounded state growth
  — same core lesson as v1.1's US-LMD2, taught here with synthetic data since this topic has no
  real-feed dependency.
- *Given* a checkpoint directory, *when* the streaming query is killed and restarted against it,
  *then* it resumes from the correct offset with no data loss or duplication beyond what watermark
  semantics predict — the minimal checkpoint-recovery demo, independent of v1.1's real-data version.
- ~~*Given* this topic's narrower scope, *when* `concept.md` is read...~~ superseded by the fold-in
  decision above — no separate `concept.md` cross-reference is needed since there's only one topic
  now, not two to cross-reference.

**US-KC8 — `kafka-serialization-schema-evolution` (Intermediate, size TBD — likely M/L) — GATED ON
ARCHITECT INFRA DECISION, no longer JSON-only-by-necessity.**
As a learner, I want to see a schema registry actually reject an incompatible schema change, so I
understand forward/backward compatibility as an enforced mechanic, not just vocabulary.

- *Given* a schema registry service and an Avro or Protobuf client library added to the stack
  (**resolved into scope 2026-07-20** — see "Resolved decisions" above; product/library choice is an
  architect decision, not made here), *when* the notebook registers a schema and produces
  Avro/Protobuf-encoded messages against it, *then* a consumer using the registry successfully
  deserializes them — the baseline registry-aware produce/consume loop.
- *Given* a backward-compatible schema change (e.g. a new field with a default), *when* registered and
  produced against, *then* the notebook shows an old consumer still deserializes new messages
  successfully — real compatibility-checking behavior, not asserted in prose.
- *Given* a backward-*incompatible* schema change (e.g. removing a required field with no default),
  *when* registration is attempted against a compatibility-checked subject, *then* the notebook shows
  the registry rejecting it — the concrete payoff a schema registry exists for, and the mechanic a
  JSON-only demo structurally cannot show (JSON has no enforcement to demonstrate).
- *Given* the registry-based demo above, *when* `concept.md` is read, *then* it contrasts this against
  plain JSON's lack of any such enforcement (a field can be added or removed silently, old consumers
  never notified) — keeping the "why this matters vs. JSON" framing from the original draft, now
  backed by a real enforced example instead of an assumed one.
- **Still gated, not yet buildable:** this story cannot be implemented until the architect pass picks
  a registry product, resource-budget impact, and client library (per G-KC6) — tracked in
  `docs/backlog.md` row #41 as gated-on-architecture, not as blocked-forever.

**US-KC9 — `kafka-performance-tuning` (Advanced, M).**
As a learner, I want to measure how `linger.ms`, `batch.size`, and compression actually change
throughput and latency, so tuning decisions are grounded in numbers I produced, not received wisdom.

- *Given* a fixed message volume and size, *when* the notebook sweeps `linger.ms`/`batch.size`
  across a small set of values (e.g. 0ms/no-batching vs. a batched configuration) via `kafka-python`,
  *then* it measures and prints real throughput (messages/sec, bytes/sec) for each configuration —
  never a hardcoded or assumed number.
- *Given* the same sweep repeated with `compression.type` set to `none` vs. `gzip`/`snappy`/`lz4`
  (whichever codecs `kafka-python` 2.0.2 actually supports — confirmed live during implementation,
  not assumed here), *when* measured, *then* the notebook prints real produced-bytes and CPU-time
  deltas per codec, not an assumed ranking.
- *Given* a topic re-created with a different partition count (e.g. 3 vs. 12) against the same
  consumer-group size, *when* consumed, *then* the notebook shows the relationship between partition
  count and achievable consumer parallelism, tying back to US-KC4's partition-count-bounds-
  parallelism lesson.
- *Given* all sweeps run, *when* `concept.md` is read, *then* it frames the results as a genuine
  throughput-vs-latency trade-off (larger `linger.ms`/`batch.size` raises throughput, raises
  per-message latency) rather than a one-size-fits-all "always batch more" prescription.

**US-KC10 — `kafka-monitoring-observability` (Advanced, M) — PARTIALLY BLOCKED.**
As a learner, I want to see consumer lag and (once available) broker-level JVM health as operational
signals, so I understand what a real Kafka operator watches.

- *Given* a consumer group falling behind (a slow/paused consumer against an actively-produced
  topic), *when* the notebook runs `kafka-consumer-groups.sh --describe --group <g>`, *then* it
  shows real, nonzero per-partition lag — **this part is demonstrable today**, no blocker.
- *Given* the lag demo above, *when* `concept.md` is read, *then* it explains lag as the single most
  operationally important Kafka signal (the thing that actually pages someone), grounding the topic
  in something fully working even before the blocked sections below.
- **BLOCKED pending issue [#58](https://github.com/hoanghaithanh/Spark-Playbook/issues/58) (JMX
  exporter, open/unstarted):** broker heap %, GC time, and produce/fetch request latency have no
  data source today — `docker stats` (already piggybacked by `kafka_stats.py` per
  `multi-broker-kafka-cluster.md` US-MBK2) gives OS-level CPU/RAM/disk/net only, not JVM internals.
  This topic's notebook cannot demonstrate these until #58 ships a scrapable `/metrics` endpoint.
  Written up front as blocked, not silently narrowed to "just show `docker stats`" without saying
  why the JVM-level story is missing.
  **Further gated on [#59](https://github.com/hoanghaithanh/Spark-Playbook/issues/59)** (Kafka
  Cluster Monitor UI tab) only in the sense that #59's tab is *one place* this data could also be
  viewed live in-app — the notebook itself doesn't require #59, since it reads the same
  `kafka_stats.py`/JMX data directly via CLI, not through the UI. Listed as a soft dependency for
  completeness, not a hard blocker on the notebook content itself.
- **Once #58 lands:** per-broker idle-ratio (request-handler-idle %) and heap-usage % should be
  added as a follow-on acceptance criterion to this story — not written here as a hard requirement,
  since #58 hasn't shipped and this doc shouldn't assert MBean names/metric shapes it can't verify
  (same "flagged for the architect/developer to verify live" discipline `multi-broker-kafka-
  cluster.md`'s US-MBK3 already established).

**US-KC11 — `kafka-exactly-once-transactions` (Advanced, M) — NEEDS A PRE-ARCHITECTURE SPIKE.**
As a learner, I want to see the idempotent-producer + transactional-API combination that gives
Kafka's exactly-once guarantee, so I understand what "exactly-once" actually means mechanically
(and how it relates to Spark's own exactly-once sink guarantees).

- *Given* `kafka-python==2.0.2`'s transactional producer API (`init_transactions()`,
  `begin_transaction()`, `commit_transaction()`/`abort_transaction()`), *when* a feasibility spike
  runs against this repo's actual broker topology (multi-broker, RF=3/min-isr=2), *then* it
  determines whether that API works reliably enough to build a notebook demo on — **this doc does
  not assume the answer.** `kafka-python` 2.0.2's transactional support has a documented history of
  being limited/flaky; this needs to be verified live, not guessed.
- *Given* the spike succeeds, *when* the notebook is built, *then* it demonstrates a multi-message
  transaction that either fully commits (all messages visible to a `read_committed` consumer) or
  fully aborts (no messages visible), and a competing non-transactional/`read_uncommitted` consumer
  seeing uncommitted messages regardless — the concrete transactional-isolation contrast.
- *Given* the spike instead finds `kafka-python`'s transactional producer unusable, *when* the
  fallback is chosen, *then* the same demo runs via CLI (`kafka-console-producer.sh
  --transactional-id ...`) instead, with the notebook driving the CLI via subprocess calls rather
  than the Python client — same acceptance bar (commit/abort visibility contrast), different
  mechanism.
- *Given* either mechanism, *when* `concept.md` is written, *then* it explicitly ties Kafka's
  transactional/exactly-once guarantee back to Spark Structured Streaming's own exactly-once sink
  semantics (idempotent sinks, checkpoint-coordinated offsets) that `kafka-spark-structured-
  streaming` (US-KC7) and v1.1's streaming job already rely on implicitly — closing the loop between
  the two curricula.
- **Flagged for the architect step:** the spike itself (which API path works) should run before or
  during the architect pass for this story, not be left to the developer to discover mid-
  implementation — same "resolve empirically before committing to a design" discipline
  `multi-broker-kafka-cluster.md` already applied to its own open MBean-name question.

**US-KC12 — `kafka-multi-broker-cluster-ops` (Advanced, M).**
As a learner, I want to directly exercise the app's own multi-broker knob — scaling broker count,
watching partition placement, and surviving a rolling restart — so this topic closes the loop between
"Kafka theory" and "the actual infrastructure feature this app ships."

- *Given* the cluster-config drawer's broker-count field (1-5, per `multi-broker-kafka-cluster.md`
  US-MBK1, already shipped), *when* a learner respawns at different broker counts (e.g. 1 → 3 → 5)
  against the same topic, *then* the notebook/`concept.md` walkthrough shows how partition placement
  across brokers changes as broker count grows — this topic is the deliberate "go use the drawer
  knob yourself" exercise, not just a read-only CLI demo like US-KC1/US-KC5.
  A learner cannot fully exercise partition placement across brokers with `kafka_broker_count: 1`,
  so this topic's *default* is 3-5, and its walkthrough explicitly instructs the learner to try
  re-spawning at a different count.
- *Given* a live spawn at `kafka_broker_count >= 3`, *when* the notebook performs a rolling restart
  (`docker stop`/`docker start` one broker at a time, waiting for ISR to recover between each),
  *then* it shows continuous produce/consume availability throughout — no window where the whole
  cluster is unavailable, since only one broker is down at a time.
- *Given* the same rolling-restart exercise, *when* a broker is deliberately left down longer than
  the others (simulating a slow node), *then* the notebook shows under-replicated-partition count
  (`kafka-topics.sh --describe --under-replicated-partitions`) rise above zero and fall back to zero
  once it rejoins — under-replication as a concrete operational signal, not just a term.
- *Given* this topic directly exercises #56's shipped feature, *when* `concept.md` is read, *then*
  it explicitly names the cluster-config drawer as "the same broker-count knob you just used" —
  connecting the curriculum content back to the platform feature, not treating them as unrelated.

## Open questions

1. ~~Does US-KC7 stay a standalone topic once v1.1 ships, or fold into `content/structured-streaming/`?~~
   **RESOLVED by the human, 2026-07-20: folds into v1.1's `structured-streaming` topic (issue #53).**
   US-KC7 is no longer part of the 12-topic curriculum's independently-schedulable set — see its
   updated story entry above for what carries forward as reference material for #53.
2. **Exact `order` values for all 12 new topics — STILL OPEN, narrowed by the grouping decision.**
   Now that topics-index grouping is resolved (Open Question 3 below), the two tracks won't
   literally interleave in one flat list — each track (Kafka, Spark) gets its own internal
   basic→advanced ordering. What's still unresolved: the exact numeric `order` values within the
   Kafka track (this doc's basic→advanced sequence above is a reasonable default) and how the
   grouping mechanism actually orders two tracks against a single `order` field (e.g. a per-track
   `order` vs. one global field plus the new `track` field as the grouping key) — an architect-level
   schema-design detail, not decided here.
3. ~~Should the topics-index page eventually get a Kafka-vs-Spark visual grouping?~~ **RESOLVED YES
   by the human, 2026-07-20 — see "Resolved decisions" above.** No longer open; mechanism (manifest
   field name, template change) is now an architect-pass task.
4. ~~Is adding a schema registry + Avro/Protobuf library in scope for this project?~~ **RESOLVED YES
   by the human, 2026-07-20 — see "Resolved decisions" above.** No longer open; US-KC8 has been
   rewritten around a real registry-enforced demo. Product/library choice is now an architect-pass
   task.
5. **US-KC11's spike timing.** Written up as "the architect step should run/commission the spike,"
   but whether that spike happens *during* the architect pass (delaying the ADR) or *before* it (a
   throwaway script run ad hoc first) is left to the architect's own judgment, not prescribed here.
6. **Per-topic `kafka_broker_count` cluster defaults** are named qualitatively above (e.g. "3" for
   most, "3-5" for US-KC12, ">= 2" for US-KC5) but not pinned to exact numbers for every topic —
   left as an implementation-level default choice for the developer, same disposition this repo's
   other requirements docs already give to comparable sizing calls (e.g. Executor Tuning's
   dataset-size choice, US-LMD2's watermark-window size).

## Constraints

- **Mostly content-only, with two resolved exceptions.** 11 of the 12 topics (all but US-KC8) need no
  changes to `app/`, `compose/`, or any loader/routing/template code — confirmed via
  `app/topics/loader.py` that the manifest schema and auto-discovery already support everything they
  need (`requires_kafka`, `cluster_defaults.kafka_broker_count`, the
  `concept.md`/`notebook.ipynb`/`manifest.yaml` triad). Two things now touch beyond content, per the
  "Resolved decisions" above: (1) the grouped topics-index (G-KC5) needs a manifest field +
  `app/topics/loader.py` + template change, applying to all 27 topics' rendering, not just the 12 new
  ones; (2) US-KC8 needs new compose-stack infra (a schema registry service) + a new driver-image
  dependency (Avro/Protobuf library).
- **No self-check `annotation:` blocks** on any of the 12 topics (see "Self-check annotation
  decision" above) — each topic's manifest either omits `annotation:` entirely or leaves it empty,
  consistent with `app/topics/loader.py`'s existing `data.get("annotation") or {}` default.
- **`kafka-python==2.0.2` and the baked `spark-sql-kafka-0-10_2.13:4.0.3` connector are the Kafka
  client capability available to the 11 topics other than US-KC8** — no `KafkaAdminClient`
  (documented dead end, per #50's ADR). US-KC8 is the one topic that now gets a new dependency (a
  schema registry client library, per G-KC6) rather than being written against this constraint.
- **Topic creation relies on `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` or CLI shellouts** (`kafka-
  topics.sh --create`), never `KafkaAdminClient`, per the same established dead end.
- **Notebook cleanliness**: per CLAUDE.md, every one of these 12 `notebook.ipynb` files must ship
  with `execution_count: null` and empty `outputs` on every cell, and any live-execution done during
  development/verification must be reset (`git checkout -- <path>`) before the work is considered
  done — same discipline as every existing topic.
- **Sizing**: all 12 stories are S/M as scoped above (no story here is L) — this is a wide body of
  work (12 independently-shippable topics) rather than a deep one; sub-story splitting across
  sprints is a project-manager sequencing call, not decided in this doc.
