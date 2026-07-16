# Spark SQL Catalyst: How a Query Becomes a Plan

## What it is

Every Spark SQL query — whether written as a DataFrame chain
(`df.join(...).filter(...)`) or as a raw SQL string
(`spark.sql("SELECT ... WHERE ...")`) — goes through the **same** Catalyst
pipeline before a single task runs:

1. **Parse.** The DataFrame API builds an *unresolved logical plan* directly
   (no text to parse); a SQL string first goes through Spark's SQL parser,
   which produces the identical shape of unresolved logical plan. This is
   the point where the two entry points converge — from here on there is no
   "DataFrame plan" or "SQL plan", just *the* plan.
2. **Analyze.** The Analyzer resolves every unresolved reference against the
   catalog — column names, table names, function names, types — turning the
   unresolved logical plan into a *resolved (analyzed) logical plan*. This is
   the stage that fails loudly (`AnalysisException`) for a typo'd column or
   missing table, for both DataFrames and SQL identically.
3. **Optimize.** The rule-based Catalyst optimizer rewrites the analyzed
   logical plan into an *optimized logical plan* — constant folding, filter
   pushdown, projection pruning, and (relevant to this topic)
   `PushPredicateThroughJoin`: pushing a `WHERE`/`.filter()` condition below a
   join and onto whichever single side it actually references, so that side
   is filtered *before* the shuffle instead of after.
4. **Plan (physical).** The query planner turns the optimized logical plan
   into one or more candidate *physical plans* (concrete algorithms — which
   join strategy, which shuffle, etc.), picks one by cost, and that's what
   `.explain(mode="formatted")` prints.

**DataFrame and SQL queries compile to the same plan** — by step 2 there is
no way to tell, from the analyzed logical plan onward, which surface syntax
produced it. Any performance difference people attribute to "DataFrames are
faster than SQL" (or vice versa) is a myth for structured queries: same
Catalyst, same optimizer, same physical plan.

## Why it matters

- **"Does Spark compile SQL to something slower than DataFrames?" is a
  common interview question**, and the honest, testable answer is no — this
  notebook proves it by diffing the actual physical plan of the same query
  written both ways.
- **Predicate pushdown through a join is one of Catalyst's highest-leverage
  optimizations**, and it has a real, teachable blind spot: a Python UDF is
  opaque bytecode to the JVM-side optimizer. Catalyst cannot inspect what a
  UDF does, so it cannot safely prove a UDF-wrapped condition is safe to
  reorder past a join — the conservative, real Spark behavior is to leave it
  exactly where it was written, evaluated with a dedicated
  `BatchEvalPython`/`ArrowEvalPython` operator after the join has already
  happened. Recognizing this from a plan (not just reciting "UDFs are slow")
  is the actual interview-relevant skill.
- **This directly explains a common performance regression**: swapping a
  built-in expression for an equivalent-looking Python UDF can silently move
  a filter from before a join to after it, multiplying the number of rows
  the join itself has to process.

## What to look for in this exercise

The notebook runs the *same* logical query three ways against the same two
tables — a filter applied after a join on a single-side numeric column:

1. **DataFrame API** — `.join(...).filter(F.col("amount") > threshold)`.
2. **Raw SQL** — the same condition as a `WHERE` clause over temp views of
   the same two DataFrames.
3. **UDF-wrapped** — the identical condition wrapped in a Python UDF marked
   `.asNondeterministic()` (so Catalyst cannot assume it's safe to reorder,
   the same conservative treatment any genuinely opaque UDF gets).

For each cell:

- Form a hypothesis **before** running: will the `Filter` in this plan sit
  above or below the `SortMergeJoin`?
- Read `.explain(mode="formatted")` yourself.
- Call `playbook.checkpoint(df, topic="catalyst-plans")` and click **Reveal
  self-check** to compare against the manifest-driven annotation
  (`content/catalyst-plans/manifest.yaml`, US-SH8) — it distinguishes a
  `Filter` positioned below the join (pushed down) from one still pending
  above it (blocked), purely by plan-node position, and separately flags the
  `BatchEvalPython` node that explains *why* the UDF case is blocked.
- Confirm cells 1 and 2 produce the **same** physical plan shape (modulo
  node IDs) — that's the "DataFrame and SQL compile to the same plan" claim,
  made concrete rather than asserted.
- Expect **two** `filter-pushed-below-join`-labeled nodes in the DataFrame and
  SQL reveals, not one: the join key (`customer_id`) is nullable, so the
  analyzer inserts its own `isnotnull(customer_id)` null-check filter on each
  side, which also gets pushed down and picked up by the same rule. That's a
  second, real pushed-down filter — not a duplicate and not a bug — distinct
  from the `amount > THRESHOLD` filter the exercise is actually testing.
