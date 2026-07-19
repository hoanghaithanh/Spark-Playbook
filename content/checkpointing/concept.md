# Checkpointing

## What it is

`.checkpoint()` writes a DataFrame's current data to storage **and** severs
its lineage — Spark forgets the transformation chain that built it (the
joins, filters, aggregations) and treats the checkpointed data as a fresh
source, exactly as if it had been read from a file. This is different from
`.cache()`/`.persist()`: caching stores the data for reuse but *keeps* the
lineage, so a lost cached partition is recomputed from the original chain;
checkpointing throws the chain away entirely once the write completes.

Spark offers two checkpoint modes:

- **Reliable checkpointing** — `sc.setCheckpointDir(path)` (a durable path,
  typically HDFS or another fault-tolerant filesystem) followed by
  `df.checkpoint()`. The data is written to that durable storage, so it
  survives an executor failure: if a partition is lost afterward, Spark reads
  it back from the checkpoint files instead of recomputing anything upstream
  (there is nothing upstream left to recompute from).
- **Local checkpointing** — `df.localCheckpoint()`. Writes to executor local
  disk instead of durable storage, which is faster (no network/durable-FS
  write), but **not fault-tolerant** — if the executor holding that local
  data is lost, the checkpoint itself is gone. Local checkpointing still
  truncates lineage the same way, so it's useful for capping plan-compile
  time on a long chain, but it does not protect against executor failure the
  way reliable checkpointing does.

## Why it matters

- **Long transformation chains grow plans that are expensive to plan and
  risky to recompute.** A loop that joins against a lookup table N times (a
  common pattern for iterative feature-building or graph-style traversal)
  produces a plan with N nested join nodes. Catalyst has to analyze and
  optimize that entire tree on every `.explain()`/action, and a single lost
  partition anywhere downstream forces Spark to recompute the *whole* chain
  from the original source, not just the lost partition's most recent step.
- **Checkpointing collapses that chain to one flat read.** After
  `df.checkpoint()`, the DataFrame's plan is a single scan of the
  checkpointed data — the N join nodes, and everything they depended on, are
  gone from the lineage. A later failure only ever needs to re-read from the
  checkpoint, not re-run N joins.
- **Reliable vs. local is a durability-vs-speed tradeoff, not a formality.**
  Reliable checkpointing's durable write means the lineage truncation is
  *safe* — the data it replaces the old chain with is guaranteed to survive
  an executor loss. Local checkpointing's local-disk write is faster to
  produce but reintroduces exactly the failure mode checkpointing exists to
  remove: lose that executor, lose the checkpoint, lose the ability to
  recompute (the lineage that would have let Spark rebuild it is already
  gone). Local checkpointing is the right choice only when the goal is
  capping plan complexity/compile time within a single run, not surviving a
  failure.
- **Structured Streaming's checkpoint mechanism is the same idea, applied
  continuously.** A streaming query's checkpoint directory persists the
  source offsets already processed and the stateful operators' state store
  (e.g. running aggregates, join buffers) after every micro-batch. On
  restart, Spark reads that checkpoint to resume exactly where it left off —
  it does not reprocess data already accounted for, and it does not lose
  accumulated state. It is the same "replace a chain of dependent history
  with a durable, self-contained snapshot" idea as `df.checkpoint()`, just
  applied on every batch instead of once, which is why it's mandatory (not
  optional) for stateful streaming to resume correctly after a restart.

## What to look for

Spark UI **SQL / Executors tab**, or directly via `.explain()`:

- Before checkpointing, `.explain()` on a DataFrame built from a long chain
  of joins shows a deeply nested plan — one join node per iteration of the
  chain.
- After `sc.setCheckpointDir(path)` + `df.checkpoint()`, `.explain()` on the
  *returned* (checkpointed) DataFrame shows a single flat scan node — the
  entire prior chain is gone from the plan, not just hidden or collapsed
  visually.
- The checkpoint write itself is a real job (visible in the Jobs tab) — the
  data is materialized once, up front, which is the cost checkpointing pays
  to buy the lineage truncation.
