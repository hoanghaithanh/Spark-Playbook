# Bucketing (Co-partitioned Joins)

## What it is

**Bucketing** pre-partitions a table's rows into a fixed number of files
("buckets") based on a hash of one or more columns, at *write* time —
`df.write.bucketBy(n, "col").sortBy("col").saveAsTable("t")`. The bucketing
metadata (bucket column(s), bucket count) is recorded in the table's catalog
entry (`BucketSpec`), not just baked into the files.

If two tables are bucketed on the **same column, with the same bucket
count**, Spark knows — from the catalog metadata alone, without looking at
the data — that rows sharing a join key value are *already* guaranteed to
live in corresponding bucket files across both tables. Joining them on that
key needs no shuffle at all: each task can read the matching bucket file pair
directly and merge-join them (`SortMergeJoin` with **no** `Exchange`
underneath it). This is bucketing's entire point: **pay the shuffle cost
once, at write time, instead of every time the tables are joined.**

Bucketing only pays off when the *exact same* bucketing spec is reused across
many joins on many tables (a common data-warehouse pattern: a large fact
table bucketed once, joined repeatedly against several dimension tables
bucketed the same way) — it is a distinct optimization from broadcast join
(which avoids a shuffle by replicating a *small* side) and from sort-merge
join (which always pays the shuffle cost fresh every time).

## Why it matters

- **It's a distinct, frequently-asked interview topic in its own right** —
  "how would you avoid repeatedly shuffling the same large tables on every
  join?" is a real senior-level question, and the answer is bucketing, not
  caching or broadcasting.
- **The failure modes are exactly as important as the success case.**
  Bucketing only eliminates the shuffle when the bucket column *and* bucket
  count match exactly on both sides — a mismatched bucket count, a different
  join key, or (in Spark 3.1+) the `autoBucketedScan` heuristic deciding
  against it will silently fall back to a normal shuffling sort-merge join.
  Recognizing "I bucketed this and it *still* shuffled" from the plan is the
  actual skill being tested.
- **It requires managed tables, not bare file writes.** `bucketBy()` is only
  honored via `saveAsTable()` — a plain `.parquet("path")` write silently
  drops the bucketing metadata, because there is no catalog entry to record
  it in. This trips people up in practice and is worth knowing cold.
- **Connects back to partitioning/shuffle and forward to joins.** Bucketing
  is best understood as "partitioning/shuffle mechanics (topic 1) applied at
  write time, to eliminate a join-time shuffle Spark would otherwise plan the
  way topic 3 (join strategies) describes."

## What to look for in this exercise

The notebook writes two tables, `bucketed_a` and `bucketed_b`, both bucketed
on `id` into 8 buckets, and joins them — this should produce **no**
`Exchange` node. It then writes `bucketed_c_mismatched`, bucketed on the same
`id` column but into only 4 buckets, and joins it against `bucketed_a` — same
key, same optimization *intent*, but the bucket counts don't match, so this
join shuffles anyway.

- Form a hypothesis for each join **before** running it: will this one show
  an `Exchange`, or not, and why?
- Read `.explain(mode="formatted")` yourself for both joins.
- Call `playbook.checkpoint(df, topic="bucketing")` and click **Reveal
  self-check** for each. The manifest
  (`content/bucketing/manifest.yaml`) deliberately distinguishes a
  co-partitioned `SortMergeJoin` (no nearby `Exchange`) from a standard
  shuffling `SortMergeJoin` as two separate concepts (US-2.4) — check that
  the label you get for the no-shuffle case really does read differently
  from the mismatched-bucket-count case, not just "SortMergeJoin" both times.
- Cross-check against the stage-metrics panel (US-2.2): the co-partitioned
  join's stage(s) should show zero/near-zero `shuffleReadBytes` compared to
  the mismatched case.
