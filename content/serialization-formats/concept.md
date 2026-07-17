# Serialization Formats

## What it is

A **row-oriented** format (CSV, JSON) stores every column of a row
contiguously, one row after another — reading any single column means
reading and parsing the *entire* row it lives in, because there's no way to
locate "just column 3" without walking through columns 0, 1, and 2 first. A
**columnar** format (Parquet, ORC) stores each column's values contiguously
instead, in separate chunks with a footer that records exactly where each
column chunk starts and how big it is — reading 3 columns out of 20 means
seeking straight to those 3 chunks and skipping the other 17 entirely.

This is **column pruning** (a.k.a. projection pushdown): `select("c0", "c1",
"c2")` on a Parquet-backed DataFrame lets Spark's `Scan parquet` node read
only the requested columns' bytes off disk. The same `select()` on a
CSV-backed DataFrame produces a `Scan csv` node that still reads every
byte of every row — CSV has no per-column addressing at all, so pruning
happens only *after* the full row is already read and parsed.

**Partitioning** a Parquet table by a column (e.g. `partitionBy("region")`)
takes this further: each partition value gets its own subdirectory
(`region=0/`, `region=1/`, ...), so a filter on the partition column
(`.filter(col("region") == "0")`) lets Spark's file-listing step (Catalyst's
partition pruning) skip the *other* partitions' files entirely, before a
single byte of their contents is read — not row-level filtering, whole-file
skipping.

## Why it matters

- **Column pruning turns "select 3 of 20 columns" into "read roughly 3/20 of
  the data" for Parquet, but into "read the same 100% every time" for CSV.**
  This notebook measures it directly, from the stage's real `inputBytes`
  metric (the same field this topic's self-check spotlights): reading a
  ~235MB, 20-column CSV file and selecting 3 columns reads **246.2 MB**
  (`inputBytes`) — no drop at all, since CSV must parse every row's full
  width regardless of which columns the query keeps. The identical data
  written as Parquet and read with the same `select()` reads only **24.1 MB**
  — a **~10x drop**, even better than proportional (columnar compression
  also does more work on 3 all-numeric columns than the CSV encoding does),
  and well past the order-of-magnitude the 3/20 column ratio predicts.
- **Partition pruning is a second, independent win on top of column
  pruning — it skips whole files, not just unneeded columns within a file.**
  Reading a Parquet table partitioned into 8 `region` values with no filter
  reads **15.0 MB** (already column-pruned, `select` only touches `c0`);
  adding `.filter(col("region") == "0")` drops that to **1.9 MB** — almost
  exactly **1/8**, since Spark's file-listing step lists only the one
  matching partition directory and never even opens the other seven.
- **This is why "just use Parquet" is close to correct default advice for
  analytical workloads**, but the two prunings are genuinely different
  mechanisms with different preconditions: column pruning needs a columnar
  file format; partition pruning needs the table to actually be partitioned
  on the column the query filters on. A `select()` alone gets you the first;
  neither gets you the second unless the write path partitioned by that
  column.

## What to look for

- **The stage's `inputBytes` metric** (this topic's self-check spotlight,
  from `/api/v1/applications/<id>/stages` — the same REST field the Spark
  UI's Stages tab and SQL tab both surface) is the ground truth for "how much
  did this read actually cost", not the column count or file size alone —
  compare it directly across the CSV-select-3, Parquet-select-3, and
  partitioned-Parquet-filtered scenarios below.
- **`.explain(mode="formatted")` on the CSV read** shows a `Scan csv`
  node; the same query against the Parquet-backed table shows `Scan
  parquet` in the raw explain text — but the self-check reveal panel's plan
  labels both as the same generic `Scan` node (the annotation engine's
  tokenizer only ever reads a node's first word, so it can't distinguish the
  two — see `manifest.yaml`'s note). The plan *shape* was never going to show
  the bytes-read difference either way; that's on the stage metric below,
  not the node text, which is exactly what this topic's self-check spotlights.
- **The partitioned-Parquet filtered read's plan** shows `PartitionFilters:
  [isnotnull(region#...), (region#... = 0)]` in the `Scan parquet` node's
  detail — confirmation the filter was pushed into partition pruning, not
  applied as a row-level `Filter` after a full scan.

**Deviation from the original mockup target.** `docs/requirements/curriculum-topics-2026-07.md`'s
US-C8 suggests a 50-column, ~2GB CSV. This notebook uses a 20-column, ~235MB
CSV instead — large enough to produce a clear, real, order-of-magnitude
proportional bytes-read drop (demonstrated above: ~10x, in the same
direction and magnitude the mockup's own "2.1GB → 118MB" example shows) without
making the notebook slow to run or review on a 3-worker/2-core/4GB-per-worker
dev cluster. The mechanism and the measured proportionality are identical;
only the absolute scale is smaller.
