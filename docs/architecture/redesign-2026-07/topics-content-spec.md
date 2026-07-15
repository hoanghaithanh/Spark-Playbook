# Topic content extracted from the design mockups

Source: Claude Design project "Spark Playbook Topic Redesign"
(`https://claude.ai/design/p/08d7dfe3-637e-435e-a18e-6fa9792e1f34`), imported 2026-07-15.

Each of the 11 non-shared mockups in the project is the same shell component
(`shell-topic-page.dc.html`) with different Concept/Notebook/Self-check content baked into its
script block. Rather than keeping 11 near-duplicate ~20KB HTML files around, this doc extracts
that per-topic content so requirements-analyst / developer can turn it into
`content/<topic-id>/concept.md` + `manifest.yaml` entries per the existing pattern (PLAN.md
section 3's `manifest.yaml` schema, section 4's `content/` layout).

The mockup's numbering (01–13, skipping 03) does not match backlog ordering — treat the `TOPIC N`
labels below as provenance only, not final ordering. Two of these topics are already built under
different backlog entries (marked below); the rest are net-new backlog stories per the human's
2026-07-15 decision to adopt the full topic set.

---

## 01 — DAG & Lazy Evaluation *(new topic, not currently in backlog)*

- **What it is:** every `.select()`/`.filter()`/`.join()` is a transformation recorded into a
  logical plan; nothing runs until an action (`.count()`, `.collect()`, `.write()`). Catalyst then
  turns the logical plan into a physical plan, then a DAG of stages split at shuffle boundaries.
- **Why it matters:** laziness lets Spark reorder filters, prune columns, and fuse narrow ops
  before running anything — impossible if each line executed eagerly. Also explains why a
  `.collect()` stack trace can point far from the actual bug (that's just where evaluation
  triggered). Reading the DAG is the reliable way to know what Spark will actually do.
- **What to look for:** Spark UI Jobs tab (DAG visualization per stage).
- **Notebook walkthrough:** (1) chain filter→select→groupBy, confirm no job runs yet; (2)
  `.explain(True)` to see parsed/analyzed/optimized/physical plans pre-execution; (3) trigger
  `.count()`, watch the job+DAG appear; (4) confirm stage boundary lines up with the shuffle.
- **Self-check hypothesis:** will a job appear after `.explain(True)`, or only after `.count()`?
  (Answer: only after `.count()` — `.explain()` never runs anything.)

## 02 — Spark SQL Catalyst *(already built — backlog #4, "Done (Sprint 1)")*

Existing topic; mockup content included for shell-migration reference only, not new scope.
Covers: parse→analyze→optimize→physical-plan phases; DataFrame vs. SQL compile to the same plan;
why UDFs block predicate pushdown (Catalyst can't see inside Python bytecode). Self-check:
compare a DataFrame filter-after-join vs. the same as raw SQL vs. the same as a UDF — the UDF
version is the one whose filter does NOT get pushed below the join.

## 04 — Partitioning & Shuffle Mechanics *(already built — backlog #2/#3, Phase 1)*

Existing topic; mockup content matches the already-implemented `content/partitioning-shuffle/`.
Included for shell-migration reference only, not new scope.

## 05 — Caching & Persistence *(new topic — matches backlog #14)*

- **What it is:** `.cache()` = `.persist(MEMORY_AND_DISK)`; first action materializes and keeps
  data resident across executors. Storage levels (`MEMORY_ONLY`, `MEMORY_AND_DISK`,
  serialized/replicated variants) trade memory for CPU.
- **Why it matters:** caching the wrong thing wastes memory and evicts data other jobs need;
  caching a DataFrame reused across several actions can cut runtime dramatically. Caching is lazy
  — nothing stored until the first action after `.cache()`. Forgetting `.unpersist()` is the most
  common cause of silent memory pressure.
- **What to look for:** Spark UI Storage tab (cached RDD/DataFrame sizes).
- **Notebook walkthrough:** (1) build an expensive DataFrame (join of two large parquet tables);
  (2) `.cache()` + `.count()` to force materialization; (3) check Storage tab for fraction
  cached, size in memory vs. disk; (4) reuse vs. `.unpersist()`, compare timings.
- **Self-check hypothesis:** will the 2nd/3rd action on a cached DataFrame be faster, same, or
  slower than the 1st? (Answer: ~17x faster after caching; a single-use DataFrame gains nothing.)

## 06 — Skew & Salting *(new topic — related to existing AQE topic but distinct)*

- **What it is:** data skew = one/few keys hold vastly more rows, so hash partitioning piles them
  onto one task. Salting appends a random suffix (`key_0`…`key_9`) to hot keys before a shuffle,
  splitting one overloaded partition into several, then explodes the smaller join/agg side to
  match before recombining.
- **Why it matters:** a single skewed task can dominate a stage's wall-clock even when every other
  task finishes in seconds — more executors doesn't help. `spark.sql.adaptive.skewJoin.enabled`
  (AQE) auto-detects/splits at runtime, but manual salting is still needed when AQE can't help
  (e.g. a skewed `groupBy` with no join to rebalance against).
- **What to look for:** Spark UI Stages tab (per-task duration + shuffle read spread).
- **Notebook walkthrough:** (1) `groupBy(key).count()` where one key covers 60% of rows; (2)
  inspect Stages tab task-level spread (one straggler task); (3) salt the hot key with a random
  0–9 suffix, strip after aggregating; (4) re-check task spread is now even.
- **Self-check hypothesis:** after salting into 10 sub-keys, does the slowest task's duration
  roughly match the rest, or does it just move the imbalance? (Answer: flattens to 2–4s across
  all 200 tasks — no single straggler.)

## 07 — Memory Management *(new topic — matches backlog #17)*

- **What it is:** each executor JVM heap is split by the unified memory manager into reserved,
  user memory, and a shared execution+storage region (`spark.memory.fraction`). Execution
  (shuffles/joins/sorts/aggs) and storage (cached DataFrames, broadcast vars) borrow from the
  same pool and can evict each other; execution wins contention.
- **Why it matters:** `OutOfMemoryError` and disk spills are almost always a memory-tuning
  problem, not a "need a bigger cluster" problem. A cached DataFrame competing with a large
  shuffle causes evictions/recomputation.
- **What to look for:** Spark UI Executors tab (storage vs. peak execution memory per executor).
- **Notebook walkthrough:** (1) cache a ~3GB feature table; (2) run a memory-hungry shuffle
  (large sort/groupBy) needing execution memory from the same pool; (3) check Executors tab —
  cached blocks evicted to make room; (4) re-run the cached query, confirm partial recompute.
- **Self-check hypothesis:** after a large sort competes for memory, does the cached DataFrame
  still read instantly, or does some of it recompute? (Answer: partial recompute — 3 of 8
  partitions evicted; execution always wins over storage.)

## 08 — Executor Tuning *(new topic, not currently in backlog)*

- **What it is:** sizing executors trades off `executor-cores`, `executor-memory`, and executors
  per node. Few large executors = better memory sharing but slower/riskier GC and one bad task
  blocking many cores; many small executors = more parallelism/fault isolation but more JVM/OS
  overhead. Commonly-cited sweet spot: 5 cores per executor.
- **Why it matters:** one giant executor per node hurts HDFS I/O throughput and GC pause cost.
  Getting this wrong shows up as low CPU utilization despite a "full" cluster, or executors
  repeatedly dying/restarting under memory pressure.
- **What to look for:** Spark UI Executors tab (task count, GC time per executor).
- **Notebook walkthrough:** (1) run with 1 fat executor/node (`executor-cores=8`,
  `executor-memory=28g`); (2) check GC time fraction in Executors tab; (3) re-run with
  right-sized executors (`executor-cores=5`, `executor-memory=12g`); (4) compare wall-clock + GC
  time.
- **Self-check hypothesis:** switching to 5-cores-each executors — does total job duration go
  up, down, or stay the same? (Answer: down — 118s→76s, ~36% faster, despite same total cores;
  each JVM manages a smaller heap shared by fewer concurrent tasks.)

## 09 — Checkpointing *(new topic, not currently in backlog)*

- **What it is:** `.checkpoint()` writes a DataFrame to reliable storage AND truncates lineage —
  Spark forgets the transformation chain and treats the checkpoint as a fresh source (unlike
  caching). Reliable checkpointing survives executor failure; local checkpointing is faster but
  not durable. Streaming checkpoints also persist offsets + state store data for exact resume.
- **Why it matters:** long transformation chains (recursive joins, loop iterations) grow a plan
  large enough that Catalyst planning is noticeably slow, and a lost executor triggers a huge
  recompute cascade. Mandatory for stateful streaming resume.
- **What to look for:** Spark UI SQL tab (plan depth before/after checkpoint).
- **Notebook walkthrough:** (1) loop 40x joining against a lookup table, growing lineage; (2)
  `.explain()`, note ~40 nested plan nodes; (3) `setCheckpointDir()` + `df.checkpoint()`; (4)
  `.explain()` again, confirm collapse to a single scan.
- **Self-check hypothesis:** after checkpointing the 40-join DataFrame, does `.explain()` still
  show all 40 nested joins, or a single flat scan? (Answer: single flat scan — checkpoint reads
  back as a new source, all 40 join nodes gone.)

## 10 — Window Functions *(new topic — matches backlog #15)*

- **What it is:** a `Window` spec (`partitionBy` + `orderBy`) lets `row_number()`/`lag()`/running
  `sum()` see other rows in the same group without collapsing them (unlike `groupBy`). Internally:
  shuffle rows so each partition key's rows land together, sort within partition, compute over a
  sliding frame.
- **Why it matters:** replaces slower, error-prone self-joins. Same skew risk as any
  `partitionBy`-based shuffle — a window with NO `partitionBy` sorts the entire dataset on a
  single partition, a common accidental OOM cause on an otherwise ordinary-looking query.
- **What to look for:** Spark UI SQL tab (Window/Sort plan operators).
- **Notebook walkthrough:** (1) `row_number()` over `partitionBy("user_id").orderBy("ts")`; (2)
  running total via `rowsBetween(unboundedPreceding, 0)`; (3) drop `partitionBy` by mistake,
  inspect plan; (4) compare shuffle partition count — collapses to a single task.
- **Self-check hypothesis:** dropping `partitionBy` — does Spark run across the cluster as usual,
  or funnel onto one task? (Answer: funneled onto one task — WARN logged, entire dataset moved to
  a single partition to guarantee global order.)

## 11 — Structured Streaming *(new topic — matches backlog #18, Phase 3)*

- **What it is:** models a stream as an unbounded table that new data appends to; re-runs the
  same DataFrame query incrementally. A trigger decides how often to check for new data; a
  watermark tells Spark how long to wait for late events before finalizing a windowed
  aggregation and dropping old state.
- **Why it matters:** the same API covers batch and streaming, so it's easy to write a stateful
  aggregation with no watermark — Spark then keeps every group's state forever, growing memory
  until the job dies. Watermark + output mode (`append`/`update`/`complete`) choice is the
  difference between a job that runs for months vs. OOMs after a day.
- **What to look for:** Spark UI Structured Streaming tab (batch duration, state row counts).
- **Notebook walkthrough:** (1) windowed streaming agg with NO watermark; (2) watch state rows
  grow every batch, never drop; (3) add `withWatermark("event_time", "10 minutes")`, restart;
  (4) confirm state plateaus instead of growing.
- **Self-check hypothesis:** with a 10-minute watermark added, does state row count keep growing
  every batch, or level off? (Answer: levels off around 210 rows after batch 15 — closed windows'
  state gets dropped instead of kept forever.)
- **Note:** this is Phase 3 territory (requires Kafka/conditional compose service per PLAN.md
  section 5's Phase 3 — backlog #19). Sequence accordingly; don't build ahead of the Kafka
  harness.

## 12 — Serialization Formats *(new topic, not currently in backlog)*

- **What it is:** file format determines how much data Spark reads off disk. Parquet/ORC are
  columnar with predicate/column pushdown — Spark skips whole row groups and unread columns
  before they hit memory. CSV/JSON are row-oriented, schema-less, requiring a full read +
  inference pass.
- **Why it matters:** reading a 50-column CSV to use 3 columns pulls all 50 across the network;
  the Parquet equivalent touches only the 3 requested column chunks. Compounds with partitioning
  — a well-partitioned Parquet table lets Spark prune whole files before opening them.
- **What to look for:** Spark UI SQL tab (bytes read per scan).
- **Notebook walkthrough:** (1) read a 50-column, 2GB CSV, select 3 columns, check bytes read;
  (2) write out as Parquet; (3) read the Parquet version, select the same 3 columns, compare
  bytes read; (4) add a filter on a partitioned column, confirm whole-file skipping.
- **Self-check hypothesis:** selecting 3 of 50 columns from Parquet instead of CSV — does bytes
  read drop roughly proportionally, or barely change? (Answer: drops from 2.1GB to 118MB, close
  to 3/50 of the data.)

## 13 — Fault Tolerance & Lineage *(new topic, not currently in backlog)*

- **What it is:** Spark doesn't replicate data by default — it recovers a lost partition by
  recomputing it from lineage (the recorded transformation chain). A dead worker mid-shuffle
  means the driver reschedules only the lost tasks elsewhere, replaying upstream stages as
  needed, not restarting the whole job. This is what makes RDDs "resilient" — durability through
  recomputation, not replication.
- **Why it matters:** recomputation is cheap for a short lineage, expensive for a long one — the
  reason caching/checkpointing/short chains matter for resilience, not just speed. A job that
  "just hangs" after a worker dies is often recomputing a huge lineage, not actually failing.
- **What to look for:** Spark UI Stages tab (retried/recomputed tasks after a worker is killed).
- **Notebook walkthrough:** (1) start a long multi-stage job (filter→join→groupBy); (2) kill a
  worker mid-job (`kill -9` on the worker process); (3) watch Stages tab — only lost partitions'
  tasks retried, not the whole job; (4) confirm final result correctness matches a clean run.
- **Self-check hypothesis:** after killing a worker mid-job, does Spark restart the whole 3-stage
  job, or only recompute lost-worker partitions? (Answer: only the lost partitions — 48/50 tasks
  kept their results, 2 retried on a healthy executor using recorded lineage.)

---

## Cross-cutting notes for requirements-analyst

- Every topic's "Self-check" tab in the mockup shows **hardcoded fake "actual annotation output"
  text** — this is a design-tool placeholder. Real behavior must come from the Phase 2 annotation
  engine reading the learner's own `plan.explain()` dump via `playbook.checkpoint()`, per G3/G7 in
  PLAN.md. New topics need their own `manifest.yaml` `plan_nodes`/`stage_metrics` mappings (see
  PLAN.md section 3's schema) — several of these topics (Checkpointing, Fault Tolerance, Executor
  Tuning) involve plan shapes or failure scenarios the existing annotation engine has never been
  asked to label before (e.g. "plan collapsed after checkpoint," "task retried after worker
  kill," "GC time per executor") and may need engine changes, not just new manifests — flag this
  to the architect.
- Structured Streaming (11) and Fault Tolerance (13) both involve killing/restarting processes —
  these are meaningfully harder to make safe/repeatable in a self-serve local tool than the
  read-only topics, and may need their own UX/safety design pass (e.g. how does a learner safely
  kill a worker from the UI without needing a separate shell).
- Two mockup topics (Catalyst, Partitioning & Shuffle) already exist in the codebase — pull their
  real `content/<topic>/concept.md` in as ground truth rather than the mockup's abbreviated
  design-preview copy, which is not the authoritative content.
