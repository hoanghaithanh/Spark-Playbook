# Fault Tolerance & Lineage

## What it is

Spark's resilience model is **recomputation from lineage, not replication**.
Every RDD/DataFrame carries a record of exactly how it was built — the
sequence of narrow/wide transformations applied to its parent(s), all the way
back to a source read. When a worker (and the executor(s) it was running)
dies mid-job, the driver does not restart the whole job and does not need a
second copy of the lost data sitting somewhere else. It looks up which
partitions were being computed on the lost executor, reschedules **only
those tasks** on the surviving executors, and each rescheduled task
recomputes its partition by walking back through the recorded lineage —
re-reading/re-deriving exactly as much upstream data as that one partition
needs, nothing more.

This is a deliberate tradeoff against replication-based fault tolerance (the
model something like HDFS block replication or a replicated in-memory store
uses): Spark keeps no redundant copy of intermediate data by default. It pays
for that with a recomputation cost when something is lost, in exchange for
not paying a storage/network cost to keep replicas of every intermediate
result all the time.

## Why it matters

- **Only the lost tasks are retried — the driver does not restart the job.**
  A multi-stage job (e.g. `filter` → `join` → `groupBy`) that loses a worker
  partway through does not throw away work from stages that already
  completed successfully, and does not re-run every task in the stage that
  was active — only the specific partitions whose tasks were running on (or
  whose shuffle output was written to) the lost executor. The Stages tab /
  REST task-list data shows this directly: most of a stage's tasks keep their
  original `SUCCESS` result, while a strictly smaller subset shows a second
  `attempt` (a retry) tied to the lost executor.
- **Recomputation cost scales with lineage length.** Recomputing a lost
  partition means re-deriving it by walking back through its recorded
  lineage — a partition that's one `filter` away from its source read is
  cheap to recompute (re-read the source rows for that partition, re-apply
  one filter). A partition sitting at the end of a long chain of joins and
  aggregations is expensive to recompute: Spark has to re-derive every
  upstream step that partition depends on, not just redo the last operation.
  A job that "just hangs" for a while after a worker dies is very often not
  actually stuck — it's silently recomputing a long lineage chain for the
  lost partitions, and the wait is proportional to how much upstream work
  that chain represents.
- **This is exactly why Checkpointing and Caching matter for resilience, not
  just speed.** [Checkpointing](../checkpointing/concept.md) truncates
  lineage entirely — a checkpointed DataFrame's plan is a single flat scan of
  already-materialized data, so if a partition built from it is lost later,
  recomputation means re-reading the checkpoint, not re-running everything
  that produced it. [Caching](../caching-persistence/concept.md) doesn't
  truncate lineage, but a cached partition that's still resident when a
  *different* partition is lost shortens the *recompute path* for anything
  downstream of it, because Spark can resume recomputation from the cached
  step instead of walking all the way back to the original source. Both
  techniques are usually introduced as performance optimizations (skip
  re-deriving data you already have), but they are just as much resilience
  optimizations: they are what keeps a worker failure's recomputation cost
  cheap instead of expensive, by shortening exactly the lineage chain this
  topic's recovery mechanism has to walk.
- **Recovering the final result stays correct.** Recomputation from lineage
  is deterministic — the same transformations applied to the same source
  data produce the same output, whether a partition was computed the first
  time or recomputed after a worker loss. A job that survives a mid-run
  worker kill should finish with an identical result to a clean run with no
  failure; if it didn't, "resilience" would just mean "produces some answer
  after a failure," not the correctness guarantee it's actually meant to be.

## What to look for

Spark UI **Stages tab** (task list for the active/affected stage), or the
REST equivalent `/api/v1/applications/<id>/stages/<id>/<attempt>/taskList`:

- Before a worker is killed, every task in the running stage shows `attempt
  0` and (once finished) `SUCCESS`.
- After a worker is killed mid-stage, a **strictly smaller-than-total**
  subset of that stage's task indices shows a second record with `attempt
  ≥ 1` — those are the partitions whose tasks were lost with the killed
  worker and had to be rescheduled and recomputed elsewhere. The rest of the
  stage's tasks keep their original, single `attempt 0` result untouched.
- The job still reaches `SUCCESS` overall (assuming enough surviving
  capacity to pick up the lost tasks), and its final collected/written result
  matches a clean run of the same job with no worker killed — the retry
  activity changes *how* the result was computed, not *what* it is.
