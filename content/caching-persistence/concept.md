# Caching & Persistence

## What it is

`.cache()` is shorthand for `.persist(StorageLevel.MEMORY_AND_DISK)`: the
first time an **action** runs against the DataFrame, Spark keeps every
computed partition resident (in memory if it fits, spilled to disk if it
doesn't) instead of discarding it once the action finishes. Every later
action against that same DataFrame object reads the stored partitions back
instead of recomputing the whole lineage — the join/filter/aggregation chain
that built it only actually runs again for whichever partitions were evicted.

Caching is **lazy**, exactly like a transformation: calling `.cache()` only
flags the DataFrame for storage. Nothing is actually stored until the next
action forces materialization. `.persist()` takes an explicit
`StorageLevel` — `MEMORY_ONLY` (fastest reads, but partitions that don't fit
are silently dropped and recomputed on next use, never spilled), versus
`MEMORY_AND_DISK` (partitions that don't fit spill to disk instead of being
dropped, trading some disk I/O for never falling back to full recompute).
`.unpersist()` releases the storage explicitly; forgetting it on a large,
short-lived DataFrame is a common source of memory pressure that quietly
starves later jobs on the same cluster.

## Why it matters

- **Caching only pays off when a DataFrame is reused.** A DataFrame touched
  by exactly one action gains nothing from `.cache()` — the "first action
  materializes it" cost is paid either way, and there is no second read to
  benefit from the stored result. Caching a single-use DataFrame is pure
  memory pressure with no corresponding payoff.
- **Storage level is a real memory-vs-recompute tradeoff, not a formality.**
  `MEMORY_ONLY` on a DataFrame that doesn't fit means some fraction of it is
  silently recomputed from source on every later access — correct, but
  possibly no faster than not caching at all for the evicted partitions.
  `MEMORY_AND_DISK` avoids that recompute at the cost of disk I/O for the
  overflow, which is usually still far cheaper than re-running the full
  upstream shuffle/join.
- **The Storage tab is the ground truth for "is this DataFrame actually
  cached, and how much of it,"** independent of what the code says — a
  `.cache()` call that never gets touched by an action shows nothing there,
  and a DataFrame too large for available memory shows a fraction cached
  under 100% even though the code path treats it as "the cached one."

## What to look for

Spark UI **Storage tab** (`http://localhost:4040/storage/`, or the REST
equivalent `/api/v1/applications/<id>/storage/rdd`):

- After `.cache()` + the first forcing action (`.count()`), confirm an entry
  appears for the DataFrame showing its storage level, **fraction cached**
  (`numCachedPartitions` / `numPartitions`), and size **in memory vs. on
  disk** — not just "cached: yes/no".
- Compare a 2nd and 3rd action against the cached DataFrame's timing to a
  1st/only action against a comparable-cost, never-cached DataFrame — the
  cached repeats should be dramatically faster; the single-use DataFrame
  gains nothing from ever calling `.cache()` on it.
- With a DataFrame sized to exceed available cache memory, compare
  `MEMORY_ONLY` against `MEMORY_AND_DISK`: `MEMORY_ONLY`'s Storage-tab entry
  shows fraction cached under 100% with zero disk usage (the overflow is
  dropped, not spilled); `MEMORY_AND_DISK`'s entry shows a fraction cached at
  least as high (typically higher, often at or near 100%) with nonzero disk
  usage for the same data — the overflow that `MEMORY_ONLY` drops instead
  gets spilled here.
