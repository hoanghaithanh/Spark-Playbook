# DAG & Lazy Evaluation

## What it is

Every DataFrame transformation — `.select()`, `.filter()`, `.join()`,
`.groupBy()` — is **lazy**: calling it only records a step into a logical
plan. Nothing runs until an **action** (`.count()`, `.collect()`, `.write()`,
`.show()`, …) is called. At that point Spark runs the transformation chain
through the same Catalyst pipeline every query goes through (parse → analyze
→ optimize → physical plan — see the Catalyst topic for the full pipeline),
then turns the chosen physical plan into a **DAG (directed acyclic graph) of
stages**, split wherever a shuffle is required. Each stage is further split
into tasks, one per partition, which is what actually executes on the
cluster.

`.explain(True)` (or `.explain(mode="formatted")`) prints this whole
pipeline's output — parsed, analyzed, optimized, and physical plans — and is
itself **not** an action: it only asks Catalyst to compile the plan, it never
runs a task. Only a real action submits a job.

## Why it matters

- **Laziness is what makes Catalyst's optimizations possible at all.** If
  every line executed the instant you wrote it, Spark could never reorder a
  filter below a join, prune unread columns, or fuse narrow operations into
  one pass — it would only ever see one operation at a time, with no whole
  plan to optimize. Laziness is a prerequisite for optimization, not
  incidental to it.
- **It explains why a `.collect()`/`.count()` stack trace can point far from
  the actual bug.** The exception surfaces at whichever action first forced
  execution, not at the line that logically caused the problem — reading the
  transformation chain (and its plan) is the reliable way to find the real
  cause, not just the line number in the traceback.
- **The DAG and its stage boundaries are the ground truth for "what will
  Spark actually do,"** independent of how the code happens to be laid out
  across cells or functions. A chain spanning ten lines and three functions
  still compiles to one DAG, split at shuffle boundaries wherever the data
  actually needs to move — that's what the Spark UI's Jobs tab shows you
  after the fact.

## What to look for

Spark UI **Jobs tab** (`http://localhost:4040`) — specifically, whether a
job appears at all, and its **DAG visualization** once one does:

- After building the transformation chain, confirm the Jobs tab (and
  `/api/v1/applications/<id>/jobs`) shows **no job** yet — laziness made
  concrete, not just asserted.
- After `.explain(True)`, confirm the Jobs tab **still** shows no job —
  compiling a plan is not running one.
- After the triggering action, confirm a job **does** appear, then open its
  DAG visualization and check that the stage boundary lines up with the
  shuffle (`Exchange`) the physical plan showed — the DAG's stage split is
  the same shuffle boundary you already saw in `.explain()`, just drawn as a
  graph instead of a plan tree.
