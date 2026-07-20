# UDF vs pandas UDF: Serialization Cost

## What it is

A Python `udf()` and a `pandas_udf()` both let you run arbitrary Python code
inside a Spark job, but they cross the JVM/Python process boundary in
fundamentally different ways:

- **`udf()` (row-at-a-time).** For every single row, the executor's JVM
  process serializes that row, sends it to a companion Python worker
  process over a pipe, the worker deserializes it, calls your Python
  function once, serializes the result, and sends it back. This happens
  **once per row** — a 20-million-row column means 20 million individual
  Python function calls, each preceded and followed by a serialize/
  deserialize round trip.
- **`pandas_udf()` (vectorized, Arrow-backed).** The executor batches many
  rows together (Spark's internal Arrow batch size, thousands of rows by
  default), hands the *whole batch* to the Python worker as a single Apache
  Arrow buffer — a zero-copy, columnar wire format both the JVM and Python
  sides understand natively — and the worker materializes it as a single
  `pandas.Series`/`DataFrame`. Your function runs **once per batch**, and if
  it's written as a vectorized NumPy/pandas expression (not a Python loop
  over the Series), the actual per-row work happens in C, not interpreted
  Python bytecode.

Same capability (arbitrary Python logic per row), radically different cost
per row: one Python function call and one serialize/deserialize round trip
per row, versus one Python function call and one Arrow buffer transfer per
*thousands* of rows.

## Why it matters

- **This directly connects to [Catalyst plans'](../catalyst-plans/concept.md)
  `BatchEvalPython`/`ArrowEvalPython` framing.** That topic teaches these two
  operators from the *optimizer's* angle: both are opaque to Catalyst (it
  cannot see inside Python bytecode to reorder a UDF-wrapped filter past a
  join), which is a predicate-pushdown-blocking side effect. This topic
  teaches the *execution-cost* distinction the same two operators represent
  — same operators, a complementary angle. Confusingly, `BatchEvalPython`'s
  name doesn't mean "vectorized/batched execution" — the "batch" refers to
  batching rows for the *inter-process pipe I/O* (fewer, larger pipe writes
  than one syscall per row), not to running your function once per batch.
  Each row still gets its own individual Python function call inside that
  I/O batch. `ArrowEvalPython` is the operator that's actually vectorized,
  in both the I/O sense and the per-call sense.
- **The gap is real, measurable, and large enough to matter in an
  interview.** Running the identical per-row computation on the identical,
  identically-partitioned 20-million-row dataset (this topic's notebook)
  measured the row-at-a-time `udf()` run at **~6.9-7.0s wall-clock / ~36,100-
  36,800ms summed `executorRunTime`**, against the vectorized `pandas_udf()`
  run at **~2.2-2.5s wall-clock / ~11,700-12,700ms summed `executorRunTime`**
  — a consistent **~2.8-3.2x** speedup across independent runs on this
  project's default 3-worker/2-core/4GB dev cluster. The exact ratio is not
  the claim to memorize (it will vary by machine, dataset, and the
  computation's own cost) — the claim to internalize is *why* it's faster:
  fewer, larger cross-process calls beat many small ones, and vectorized
  NumPy beats an interpreted Python loop for the per-row math itself.
- **This is not "pandas UDFs are always free."** A `pandas_udf()` still pays
  the Arrow serialization cost per batch, still runs Python (not JVM native
  code), and still can't be reasoned through by Catalyst any more than a
  plain `udf()` can (see the Catalyst-plans tie-in above). For anything
  expressible as a built-in Spark SQL expression instead of a UDF at all,
  the built-in will beat *both* — this topic's comparison is about "if you
  must write a UDF, which flavor," not "UDFs vs. no UDF."

## What to look for

- **The `.explain(mode="formatted")` plan** shows a `BatchEvalPython` node
  for the `udf()` implementation and an `ArrowEvalPython` node for the
  `pandas_udf()` implementation — live-confirmed against a real Spark 4.0.3
  cluster capture for this topic (both single-word tokens, compatible with
  `plan_parser.py`'s first-word-only tokenizer), the same live-verification
  discipline this project's other plan-node rules follow rather than
  shipping an assumed operator name.
- **The stage's `executorRunTime`** (this topic's self-check spotlight, from
  `/api/v1/applications/<id>/stages` — the same REST field the Spark UI's
  Stages tab surfaces) is the ground truth for "how much executor CPU time
  did this run actually cost," alongside a plain wall-clock measurement —
  compare both directly between the two runs, the same "report the real
  numbers, don't assert a fixed multiplier" discipline
  [Executor Tuning](../executor-tuning/concept.md) (issue #37) and
  [Fault Tolerance & Lineage](../fault-tolerance-lineage/concept.md)
  (US-C9's illustrative retry count) already established for this project.
