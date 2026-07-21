# Producers & Delivery: acks, Idempotence, and Retries Under Failure

## What it is

Every Kafka producer decides how much confirmation it wants before it
considers a message "sent," via the **`acks`** setting:

- **`acks=0`** — fire-and-forget. The producer doesn't wait for any response
  from the broker at all; the client-side call returns as soon as the
  record is handed off, regardless of whether the broker ever durably wrote
  it. Fastest, but there is no way for the producer to know a message was
  lost — that's the point of this level, not a bug in it.
- **`acks=1`** — the leader's local write is enough. The producer waits for
  the partition leader to append the record to its own log, then returns.
  Faster than waiting on replication, but leaves a real gap: if the leader
  dies *after* acking but *before* the followers have replicated that
  record, the new leader (elected from the surviving replicas) never has
  it — an acked message that's still gone.
- **`acks=all`** (a.k.a. `acks=-1`) — the leader waits for every in-sync
  replica (the topic's `min.insync.replicas` count) to have the record
  before acking. Slowest, but a message the producer was told "yes" for
  cannot vanish on a single broker's failure.

None of these three levels, by themselves, stop **retries** from producing
**duplicates**: if a producer sends a record, the broker writes it but the
acknowledgment is lost in transit (or times out), the producer's automatic
retry resends the *same logical message*, and the broker — with no way to
tell "this is a retry" from "this is a new message" — writes it again. That
combination (`acks=all` + retries, no dedup) is Kafka's baseline
**at-least-once** delivery: nothing is silently lost, but duplicates are
possible. Enabling the **idempotent producer** (`enable.idempotence=true`)
closes that gap: the producer tags each batch with a producer ID and a
per-partition sequence number, and the broker recognizes and drops an exact
retry of a sequence number it has already written — turning "at-least-once
with possible duplicates" into what's usually called **effectively
exactly-once** on the producer side (not the end-to-end pipeline sense of
exactly-once, which also needs the consumer/sink to cooperate — see
`kafka-exactly-once-transactions` later in this curriculum for the full
transactional picture).

## Why it matters

"At-least-once," "at-most-once," and "exactly-once" show up in almost every
Kafka discussion and interview, but they are properties of a *specific
producer configuration under a specific failure*, not fixed facts about
Kafka itself:

- **`acks=0`** is **at-most-once**: a message is delivered zero or one
  times, never more, because nothing is ever retried — the producer doesn't
  even know it needs to.
- **`acks=all` with retries and no idempotence** is **at-least-once**: a
  message is delivered one or more times, never zero, because the producer
  keeps retrying until it gets a clean ack, but a lost-in-transit ack causes
  a harmless-looking duplicate write.
- **`acks=all` + idempotence** is (practically) **exactly-once on the
  producer side**: the retry-driven duplicate-write case above is
  specifically what the producer ID + sequence number dedup mechanism
  exists to close.

Choosing `acks=0` for anything you can't afford to lose (payments, orders,
audit events) is a real, recurring production mistake — usually made by a
team that read "acks controls speed" and optimized for throughput without
reading the other half of the trade-off. Just as commonly, teams assume
`acks=all` alone gives them exactly-once and are surprised the first time a
downstream consumer double-processes a record after a network blip.

## What to look for in this exercise

This topic runs no Spark job — it's Kafka's own producer/broker delivery
mechanics, exercised against the live 3-broker cluster. The notebook
deliberately mixes two mechanisms, and this is worth naming up front so the
inconsistency doesn't read as an accident: **`kafka-python==2.0.2`** (the
client this app's driver image ships) drives every `acks=0`/`acks=1` step,
but it has **no `enable_idempotence` or `transactional_id` config at all** —
confirmed directly against the installed library's `KafkaProducer.
DEFAULT_CONFIG` (see `docs/architecture/kafka-curriculum.md` D-KC3), not
assumed. The idempotent-producer step below instead runs
`kafka-console-producer.sh --producer-property acks=all --producer-property
enable.idempotence=true` — the broker's own bundled CLI, which *does*
support it. It runs from a **host terminal**, not from a notebook `subprocess`
call: the driver container this Jupyter kernel runs in has no Docker
CLI/socket access, the exact same constraint `kafka-architecture-kraft`
already documents for its `docker exec` steps. Same "client library doesn't
support this, CLI does" pattern this curriculum will use again in
`kafka-exactly-once-transactions`.

- **`acks=0`, induced failure → observed loss.** The notebook sends a batch
  of messages with `acks=0` while you restart the topic's leader broker
  (`docker restart spark-kafka-<id>`, a host-terminal step) partway through.
  It then re-reads the topic and prints produced-count vs. actually-landed
  count — any gap is silent loss the producer itself never detected, because
  `acks=0` never asked for confirmation.
- **`acks=all` + idempotence, the same kind of induced failure → no
  duplicates.** The console-producer command restarts the same broker mid-run
  in the same way. The notebook re-reads the topic afterward and confirms
  every message sent shows up **exactly once** — the automatic retries this
  configuration performs under the hood are invisible from the outside
  except for the dedup guarantee holding.
- **`acks=1`, the middle ground.** Run without a live-restart repeat of the
  exercise above (a leader dying in the exact sub-millisecond window between
  its own local write and follower replication is too narrow a race to
  reliably reproduce on demand) — the notebook still measures a normal
  `acks=1` run's produced-vs-consumed count for the summary table, and this
  file's "What it is" section above spells out the specific gap `acks=1`
  leaves open that `acks=all` closes.
- **Summary table.** The notebook's last section prints the three runs'
  actual measured counts side by side, tying each back to the at-most-once /
  at-least-once / (effectively) exactly-once vocabulary above.

Before running the `acks=0` cell, form a hypothesis: will *every* message
sent during the broker restart be lost, or only some of them? (Hint: think
about which messages were already handed off to the socket before the
restart began, versus which were still queued.) Same habit this whole tool
is built around.
