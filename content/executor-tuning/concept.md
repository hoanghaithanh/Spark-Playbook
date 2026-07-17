# Executor Tuning

## What it is

An **executor** is a JVM process a worker launches to run tasks for one
application — it owns a slice of the worker's advertised cores and memory
for the application's whole lifetime (unlike a task, which is scheduled and
released per-partition). Sizing a cluster is really three interacting knobs,
not one:

- **`spark.executor.cores`** — how many tasks this executor runs
  concurrently (one core per concurrently-running task).
- **`spark.executor.memory`** — the JVM heap this executor gets, shared by
  every one of its concurrent tasks plus whatever fraction of it Spark's
  unified memory manager reserves for shuffle/cache.
- **Executors per node** — a worker's total advertised cores/memory divided
  among however many executors you configure it to host. A worker with 16
  cores can run **one** 16-core executor, **two** 8-core executors, or
  **several** smaller ones — the same physical hardware, sliced differently.

These three aren't independent: for a fixed worker, more cores per executor
means fewer executors per node; a bigger heap per executor means the same.
Tuning executor size is choosing a point on that tradeoff, not picking a
single "correct" number.

## Why it matters

- **The "5 cores per executor" heuristic exists for two concrete reasons,
  not superstition.** First, HDFS/remote-storage clients have a per-JVM-process
  throughput ceiling — past roughly 5 concurrent readers *from the same
  client*, contention on that client's I/O path starts costing more than the
  extra parallelism gains. Second, GC pause overhead does not scale linearly
  with heap size: a much larger heap means much longer full-GC pauses when
  they happen, and an executor running many concurrent tasks on a huge heap
  pays for that pause on every task in flight at once, not just one. A
  handful of cores per executor keeps both costs bounded; doubling the heap
  to fit twice the cores does not double the safe pause budget.
- **"1 fat executor per node" is a real, recognizable anti-pattern, not a
  strawman.** Configuring exactly one executor to consume an entire worker's
  cores and memory looks efficient (no wasted capacity, minimal JVM
  overhead) but concentrates all of that node's parallelism into a single
  process: one long GC pause stalls every task the node was running at
  once, and a single slow/failing task on that executor blocks the rest of
  that node's work queue behind it in a way several smaller executors would
  not.
- **"Low utilization despite a full cluster" is the diagnostic symptom this
  produces.** The cluster monitor shows every node busy (CPU high, all
  workers "in use") and yet the job is slow — because there are too few,
  too-fat executors *serializing* the work through a small number of
  concurrent-task slots, not because the cluster lacks capacity. The fix is
  almost never "add more nodes"; it's re-slicing the same nodes into more,
  smaller executors so more tasks actually run in parallel.
- **GC time is the concrete, measurable signal, not a guess.** Spark's own
  executor REST metrics (`totalGCTime` against `totalDuration`, per
  executor) turn "this executor's heap is too big/too contended" from a
  hunch into a fraction you can compare run-to-run — a fat executor spending
  a large share of its wall-clock time in GC pauses versus a right-sized one
  spending very little is the same underlying cause showing up as a number.

## What to look for

Spark UI **Executors tab** (`http://localhost:4040/executors/`, or the REST
equivalent `/api/v1/applications/<id>/executors`):

- **Executor count** — how many executor rows appear, and how each one's
  advertised cores/memory compares to a full worker's budget. One row per
  worker with the worker's full budget is the "1 fat executor per node"
  shape; several smaller rows per worker's total budget is "right-sized".
- **`totalGCTime` as a fraction of `totalDuration`**, per executor — this is
  the GC-time-fraction evidence this topic's self-check spotlights. A higher
  fraction on the fat-executor run than the right-sized run is the concrete,
  measured version of "bigger heaps pay more in GC pause overhead", not
  something you have to take on faith.
- **Wall-clock job duration**, compared between the two runs against the
  *same* job on the *same* total cluster resources — the fat run finishing
  slower despite using an identical core/memory budget is the "low
  utilization despite a full cluster" symptom made concrete: the cluster
  wasn't short on capacity, it was short on *executors to spread the work
  across*.

**Deviation note (see `manifest.yaml`'s and `notebook.ipynb`'s own comments
for the full reasoning):** the source mockup's exact numbers
(`executor-cores=8, executor-memory=28g` fat vs. `executor-cores=5,
executor-memory=12g` right-sized) exceed this platform's own hard
per-worker ceiling (4 cores / 8GB, `app/config.py`). The notebook reproduces
the same *concept* — few fat executors packing more concurrent tasks into
one shared heap vs. more right-sized executors giving each task more
headroom — at `executor-cores=4, executor-memory=2g` (fat: ~500MB/task) vs.
`executor-cores=2, executor-memory=2g` (right-sized: ~1GB/task, same total
cluster cores and memory either way) instead.

**Honest result from actually running both configs (not assumed) on this
dev-scale cluster:** the GC-time-fraction signal this topic's self-check
spotlights held up on most runs — the fat config's GC-time fraction usually
measured higher than the right-sized one's (~0.035 vs. ~0.026 on one measured
run, a ~30% relative gap) — but **not on every run**: at least one real run
at this scale saw the fraction flip (fat lower than right-sized), so the
notebook reports the measured direction rather than asserting it. **Wall-clock
did not reliably favor the right-sized run at this small scale either** — with total
cluster cores held equal, doubling executor *count* (3 → 6) adds real
shuffle fan-out/coordination overhead (more reduce-side JVMs, more network
endpoints) that can outweigh the GC savings on a toy dataset. That crossover
is itself a genuine, worth-knowing nuance for interview depth: the "5 cores
per executor" heuristic's payoff is about *reducing GC-pause and I/O-client-
contention risk*, not a blanket "more executors always finishes faster"
claim — at production scale (multi-GB heaps, real HDFS/object-store client
contention, minutes-long jobs) the GC and I/O savings dominate the fixed
per-JVM coordination cost; at this project's small dev-cluster scale, the
fixed cost is large enough, relative to the job, to sometimes show up in the
opposite direction on wall-clock even while GC-time fraction still moves the
"right" way. The notebook captures and prints both numbers rather than
asserting away this nuance.
