# Window Functions

## What it is

A window function computes a value **per row**, using a window of *other*
rows related to it, instead of collapsing rows into groups the way
`groupBy().agg()` does. `Window.partitionBy("user_id").orderBy("ts")` defines
that window: partition the rows by `user_id` (each user's rows form one
independent window), then order each partition's rows by `ts`.
`row_number().over(that_window)` assigns 1, 2, 3, ... within each partition,
in timestamp order — every input row still comes out as one output row,
just with a new column attached.

A **running total** is the same partition/order window with an explicit
**frame**: `.rowsBetween(Window.unboundedPreceding, 0)` says "for this row,
aggregate over every row from the start of its partition up to and including
itself" — `F.sum("amount").over(window.rowsBetween(Window.unboundedPreceding, 0))`
gives each row the cumulative sum of its partition so far, not the whole
partition's total.

Both queries physically need the same two things before the actual `Window`
node can run: rows for the same `partitionBy` key must land on the same task
(a shuffle — `Exchange`, hash-partitioned on the partition column), and each
task's rows must be sorted by the `orderBy` column (`Sort`) before the window
function can walk them in order.

## Why it matters

- **`partitionBy` is what keeps a window function's shuffle scoped and
  parallel.** Dropping it doesn't make the query fail — it silently changes
  what "the window" means. With no partition key, Spark has no per-key
  boundary to shuffle on, so it falls back to guaranteeing one **global**
  order across the *entire* dataset: every row is funneled onto a single
  partition/task, sorted once, and the window function walks the whole
  dataset serially on that one task. Spark logs a `WARN` for exactly this
  case ("No Partition Defined for Window operation! Moving all data to a
  single partition, this can cause serious performance degradation.") — this
  is a real, commonly-hit failure mode, not a contrived edge case, and it is
  easy to trigger by accident (e.g. forgetting `partitionBy` when copying a
  window spec from a different query).
- **A missing `partitionBy` is a correctness *and* a scale problem at once.**
  The result may even look "correct" on a small dataset (there's still only
  one meaningful ordering), which is exactly what makes it easy to ship
  unnoticed until the dataset — and the single task carrying all of it —
  grows large enough to become the job's only bottleneck.
- **The running-total frame (`rowsBetween`) is what distinguishes "cumulative
  so far" from "whole partition."** Leaving off `.rowsBetween(...)` on an
  aggregate window function defaults to a frame that already includes the
  entire partition for ordered windows in some cases — being explicit about
  the frame is what makes "running total up to this row" an intentional
  statement rather than an assumption about Spark's default.

## What to look for

- **`.explain(mode="formatted")` on the correct `partitionBy("user_id").orderBy("ts")`
  query** — look for `Window`, preceded by `Sort` (per-partition ordering),
  preceded by `Exchange` (the hash-partitioned shuffle on `user_id`). This is
  the same node shape the topic's self-check plan-node panel labels once you
  `checkpoint()` it.
- **The same query with `partitionBy` dropped** — the plan still shows
  `Window`/`Sort`/`Exchange` (same node *shapes*, since Spark still needs to
  shuffle-and-sort to guarantee a global order), so the plan alone doesn't
  distinguish "correct" from "accidentally global." Check the **Stages
  tab** (`http://localhost:4040/stages/`, or `/api/v1/applications/<id>/stages`)
  instead: the stage feeding the window collapses to `numTasks: 1` — the
  entire dataset funneled onto a single task — versus dozens/hundreds of
  tasks for the properly-partitioned version. Also check the driver log for
  the `WARN ... Moving all data to a single partition` message Spark emits
  for exactly this case.
