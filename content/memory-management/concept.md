# Memory Management

## What it is

Every executor JVM splits its heap into a handful of pools; the two that
matter for this topic share **one region** governed by `spark.memory.fraction`
(default 0.6 of usable heap): **storage memory** (cached RDD/DataFrame
partitions, broadcast variables) and **execution memory** (shuffles, joins,
sorts, aggregations — anything that needs scratch space to build hash tables,
sort buffers, or shuffle spill files). This is Spark's **unified memory
manager**: rather than two hard-partitioned pools, storage and execution
dynamically borrow from each other inside that one shared region, with one
asymmetric rule — **execution memory can evict storage memory** (cached
blocks get dropped, or spilled to disk if the DataFrame is
`MEMORY_AND_DISK`), but storage memory can never forcibly reclaim execution
memory back while a task genuinely needs it. When both are under pressure at
the same time, execution wins.

**This is a different region from what the Caching & Persistence topic
covers.** That topic (US-C5) is entirely about storage memory — how much of
a `.cache()`'d DataFrame fits, `MEMORY_ONLY` vs `MEMORY_AND_DISK` behavior
under storage pressure alone. This topic is about what happens to that
*already-cached* data when a **separate, competing job** needs execution
memory from the same shared pool — a large shuffle, sort, or aggregation
running concurrently or afterward. Storage memory answers "how much of my
DataFrame is cached"; execution memory answers "what did caching just lose
to make room for this shuffle."

## Why it matters

- **`OutOfMemoryError` and spill are almost always a memory-tuning problem,
  not a "need a bigger cluster" problem.** A cached DataFrame competing with
  a memory-hungry shuffle on the same executor doesn't need more nodes — it
  needs either less concurrent memory pressure (don't cache and shuffle-heavy
  at once), a `spark.memory.fraction` tuned for the actual workload mix, or a
  storage level that tolerates eviction (`MEMORY_AND_DISK` spills instead of
  losing the block outright).
- **Eviction is silent and partial, not an error.** Spark doesn't fail the
  competing shuffle to protect the cache, and it doesn't fail the cache to
  protect the shuffle — it evicts just enough storage blocks to satisfy the
  execution-memory request, and the *next* read of the cached DataFrame pays
  a partial, not total, recompute cost: some partitions are still resident,
  others were evicted and silently recompute from lineage. A learner
  expecting "either it's cached or it isn't" will misread the resulting mixed
  latency (some partitions instant, others slow) as a bug.
- **This connects directly to the original spill/OOM-diagnosis skill
  (US-4.4).** A deliberately under-provisioned executor triggering
  `OutOfMemoryError`, and spill metrics (`memoryBytesSpilled`,
  `diskBytesSpilled`) surfacing on a memory-constrained sort/aggregation, are
  both the *same* unified-memory-pool contention this topic demonstrates —
  just pushed further, to the point where even eviction isn't enough to make
  room. Understanding storage-vs-execution contention under a competing
  shuffle is the same skill as diagnosing spill/OOM, applied one notch before
  the failure point.

## What to look for

Spark UI **Storage tab** (`/storage/`, or the REST equivalent
`/api/v1/applications/<id>/storage/rdd`) and **Executors tab**
(`/executors/`, or `/api/v1/applications/<id>/executors`):

- After `.cache()` + `.count()` on the ~3GB feature table, confirm the
  Storage tab shows it **fully cached** — fraction cached at or near 100%,
  matching US-C5's existing materialization-confirmation pattern.
- Run a memory-hungry competing shuffle (a large `sort` or `groupBy`) that
  needs execution memory from the same pool. Compare the Executors tab's
  per-executor storage memory usage (`memoryUsed`/`maxMemory`) **before and
  after** — a measured drop in `memoryUsed` relative to `maxMemory` is direct
  evidence that execution memory reclaimed storage blocks, not something to
  take on faith.
- Re-run the original cached query against the now-partially-evicted
  DataFrame: expect a **partial-recompute signal**, not a uniform one — some
  partitions return instantly (task duration near zero, still resident),
  others measurably recompute (task duration matching the original,
  uncached run). The mockup's target shape is "3 of 8 partitions evicted";
  this notebook measures its own real fraction rather than assuming that
  number.
- Connect back to US-4.4: on a deliberately under-provisioned executor, the
  same contention pushed further shows up as `memoryBytesSpilled`/
  `diskBytesSpilled` on the sort/aggregation stage, or an outright
  `OutOfMemoryError` when even spilling isn't enough — eviction is the
  earlier, survivable version of the same pressure that spill/OOM is the
  later, more severe version of.
