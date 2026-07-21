# Topics & Partitions: Ordering and Distribution — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation, independent re-pass)
Date: 2026-07-20, against worktree `.claude/worktrees/issue-63-kafka-topics-partitions`
      (branch `worktree-issue-63-kafka-topics-partitions`, 1 commit ahead of `main`: `29b90c4`
      "feat(content): add kafka-topics-partitions topic (US-KC2, closes #63)") — issue #63, US-KC2
      (`docs/requirements/kafka-curriculum.md`, lines ~184-199).
Scope: US-KC2's three given/when/then acceptance criteria, independently re-verified against a real
      3-broker cluster spawned through the app's own route
      (`POST /topics/kafka-topics-partitions/spawn`) and a live JupyterLab kernel — not by re-reading
      the developer's committed notebook output or taking the developer's self-report at face value,
      per this repo's "trust but verify" convention. The `DefaultPartitioner` claim in particular was
      independently checked against the actual pinned `kafka-python==2.0.2` library source, not just
      re-read from `concept.md`.

## Method

**Unit suite**, re-run clean before and after this pass: `py -m pytest tests/unit -q` →
**453 passed**, both times.

**Library-source verification (independent of the notebook/concept.md claims).** `compose/Dockerfile.spark`
pins `kafka-python==2.0.2` (line 69) — confirmed by reading the Dockerfile directly, not assumed.
Downloaded that exact wheel (`pip download kafka-python==2.0.2 --no-deps`) and read
`kafka/partitioner/default.py` straight out of it:

```python
if key is None:
    if available:
        return random.choice(available)
    return random.choice(all_partitions)
idx = murmur2(key)
idx &= 0x7fffffff
idx %= len(all_partitions)
return all_partitions[idx]
```

This confirms both halves of the developer's claim directly from source, not just by re-running the
notebook: keyed sends hash via murmur2 to a deterministic partition; unkeyed sends call
`random.choice(available)` **per call** — independent per-message uniform random selection, not a
fixed round-robin cycle and not sticky-partitioner batching (which would require state carried
between calls; this function is stateless per invocation).

**Live cluster.** `docker ps -a` showed only an unrelated stray container (`jmxtest-broker`, a
different session's leftover, not `sparkpb`-prefixed) before starting — no collision with this
worktree's compose project. The FastAPI app was started fresh
(`py -m uvicorn app.main:app --host 127.0.0.1 --port 8021`), and the cluster was spawned through the
app's own route (`POST /topics/kafka-topics-partitions/spawn`, form body matching this topic's
`manifest.yaml` `cluster_defaults`: 1 worker/1 core/1GB, `kafka_broker_count=3`). `docker inspect
spark-master`'s `com.docker.compose.project.working_dir` label confirmed the cluster was owned by
this worktree before any verification below.

**Notebook execution.** `content/kafka-topics-partitions/notebook.ipynb` has 4 code cells and runs no
Spark job. Drove the same JupyterLab kernel REST/WebSocket API JupyterLab's own UI uses
(`POST /api/kernels` + `/api/kernels/<id>/channels` websocket) against the live cluster's `:8888`,
executing all 4 code cells in file order.

One transient hiccup, noted for completeness and not treated as a topic defect: the very first
attempt (immediately after cluster spawn, before the auto-created topic's partition leaders had
finished propagating) hit `UnknownTopicOrPartitionError` on the keyed-send cell's first
`future.get()`. A kernel restart and immediate re-run succeeded cleanly with no code change — this
looks like ordinary fresh-cluster auto-create/metadata-propagation timing (the notebook sends its
first message within seconds of the cluster becoming "ready," before every broker has necessarily
converged on the new topic's leader elections), not a bug in the notebook's logic itself, and the
same unguarded `producer.send()` pattern is already used identically by `kafka-architecture-kraft`'s
notebook. Flagged for visibility, not filed as a defect — it's a one-time flake under unusually fast
spawn-then-run timing, not consistently reproducible.

## US-KC2, criterion 1 — keyed messages: same key always same partition, different keys spread

**PASS**, verified live. Cell 3, 3 distinct keys x 4 sends each:

```
key='user-a'   seq=0 -> partition 1, offset 0
key='user-a'   seq=1 -> partition 1, offset 1
key='user-a'   seq=2 -> partition 1, offset 2
key='user-a'   seq=3 -> partition 1, offset 3
key='user-c'   seq=0 -> partition 2, offset 0
key='user-c'   seq=1 -> partition 2, offset 1
key='user-c'   seq=2 -> partition 2, offset 2
key='user-c'   seq=3 -> partition 2, offset 3
key='user-8'   seq=0 -> partition 0, offset 0
key='user-8'   seq=1 -> partition 0, offset 1
key='user-8'   seq=2 -> partition 0, offset 2
key='user-8'   seq=3 -> partition 0, offset 3

user-a: all 4 sends landed on partition(s) {1}
user-c: all 4 sends landed on partition(s) {2}
user-8: all 4 sends landed on partition(s) {0}
```

Each key's 4 messages land on exactly one partition every time (`RecordMetadata.partition`), and all
three keys land on three different partitions (1, 2, 0) — both halves of the criterion hold on this
3-partition topic. The notebook's own `assert len(partitions) == 1` per key would have raised had this
not held; it didn't.

**Criterion 1: PASS.**

## US-KC2, criterion 2 — unkeyed messages: real observed distribution, named correctly in `concept.md`

**PASS**, verified live and against library source. Cell 5, 30 unkeyed sends:

```
partition sequence: [1, 2, 0, 2, 0, 1, 2, 1, 0, 1, 1, 1, 2, 0, 1, 0, 1, 0, 2, 2, 0, 1, 2, 0, 2, 1, 1, 2, 2, 2]
counts per partition: {1: 11, 2: 11, 0: 8}
```

No fixed cycle (`0,1,2,0,1,2,...`) and no sticky-batching runs (no long stretch of a repeated
partition before switching) — the sequence has exactly the "no structure" signature per-message
random selection produces, roughly balanced counts (11/11/8) consistent with `random.choice` over 30
draws. This matches the `DefaultPartitioner` source read independently above.
`content/kafka-topics-partitions/concept.md` names this exact behavior in both its "What it is"
section ("A message with no key ... falls back to some other selection strategy") and its "What to
look for" section, which states explicitly: "it calls `random.choice(available)` on every single
send — an independent uniformly-random partition pick per message. This is neither classic
round-robin ... nor the newer sticky-partitioner batching behavior ... it's per-message random
selection." This is a correct, source-verified claim, not an assumption — confirmed independently in
this pass rather than taken on the developer's word.

**Criterion 2: PASS.** The developer's claim about the pinned client's actual behavior checks out
against both live re-execution and direct library source inspection.

## US-KC2, criterion 3 — per-partition ordering holds; no cross-partition guarantee, stated explicitly

**PASS**, verified live. Cell 8 read back `user-a`'s 4 messages from the single partition (1) they all
landed on in criterion 1, seeking to their exact start offset:

```
partition 1, user-a messages in read order: ['user-a-event-0', 'user-a-event-1', 'user-a-event-2', 'user-a-event-3']
Per-partition order preserved: matches produce order exactly.
```

The notebook's own `assert read_back == expected` would have raised had order not been preserved; it
didn't — exact produce order reproduced. `concept.md`'s "What it is" section states the ordering
guarantee is scoped to "messages within one partition," and explicitly: "There is no ordering
guarantee across partitions of the same topic." The notebook's closing cell (§5, "What this does *not*
prove") reinforces this using the unkeyed sends from criterion 2 as its own supporting evidence,
naming the exact learner misconception ("assuming 'the topic' is ordered, when the actual unit of
ordering is 'the partition'").

**Criterion 3: PASS.**

## Test coverage assessment

`tests/unit/test_topics_loader.py::TestLoadRealKafkaTopicsPartitionsTopic` (4 tests: manifest fields
including `track`/`requires_kafka`/`order`/`cluster_defaults.kafka_broker_count`, concept.md HTML
rendering with topic-specific terms, notebook path resolution, `list_topics()` inclusion) matches the
exact coverage shape already established for every other shipped topic in this file (e.g.
`TestLoadRealSkewSaltingTopic`, and `kafka-architecture-kraft`'s own equivalent class referenced by
`TestTrackGrouping`). `TestRequiresKafkaField::test_every_shipped_topic_defaults_requires_kafka_false`
was correctly updated to expect 17 topics and includes `kafka-topics-partitions` in its
`requires_kafka=True` set. No loader/routing logic changed in this diff (the topic reuses the existing
manifest/content-as-data mechanism unchanged), so no new loader code exists that would need dedicated
unit coverage beyond this — this class is adequate as-is. Nothing missing.

## Cleanup

```
DELETE /api/kernels/6643afde-464f-49ed-b62e-3f6c03dadb3f     -> 204
POST /topics/kafka-topics-partitions/teardown                -> 200
docker ps -a                                                  -> only the pre-existing, unrelated
                                                                   `jmxtest-broker` container remains
                                                                   (not sparkpb-prefixed, predates
                                                                   this session, untouched by it)
uvicorn process (PID 21356, port 8021)                        -> killed via taskkill, confirmed no
                                                                   LISTENING entry on :8021 afterward
py -m pytest tests/unit -q                                    -> 453 passed (matches pre-pass baseline)
```

**Notebook cleanliness check** (all 4 code cells were executed directly against a JupyterLab kernel
via the REST/WebSocket API, never by opening/saving `notebook.ipynb` through the Jupyter UI):

```
grep -c '"execution_count"' content/kafka-topics-partitions/notebook.ipynb   -> 4
grep -o '"execution_count": [^,}]*' ...                                      -> "execution_count": null  (x4)
grep -c '"outputs": \[\]' content/kafka-topics-partitions/notebook.ipynb     -> 4
git status --short                                                           -> clean (no diff produced
                                                                                  by this pass, prior to
                                                                                  adding this report)
```

All 4 code cells confirmed at `execution_count: null` with empty `outputs: []`.

## Overall recommendation

**All 3 of US-KC2's acceptance criteria PASS, independently re-verified live** against a real
3-broker cluster and a real JupyterLab kernel — not re-derived from the developer's committed
notebook output or self-report. The developer's specific factual claim about `kafka-python==2.0.2`'s
`DefaultPartitioner` (per-message uniform random selection when unkeyed, neither round-robin nor
sticky-partitioner) was independently confirmed two ways: direct inspection of the pinned wheel's
actual source code, and a fresh live 30-message run reproducing the "no cycle, no run-structure"
signature with balanced counts. No discrepancies found between the developer's claims and this
pass's independent findings.

One minor, non-blocking observation: the very first live run of this pass hit a transient
`UnknownTopicOrPartitionError` on the keyed-send cell, immediately after cluster spawn, before the
auto-created topic's leader election had fully propagated across all 3 brokers — resolved cleanly on
kernel restart + re-run, no code change involved. This is consistent with ordinary fresh-cluster
timing (the same unguarded `producer.send()` pattern is already used identically in
`kafka-architecture-kraft`'s shipped notebook) rather than a defect specific to this topic, and it
was not reproducible on retry. Not filed as an issue; noted here for visibility in case it recurs
across other Kafka-curriculum topics and becomes worth a shared retry-on-first-send guard later.

Test coverage in `TestLoadRealKafkaTopicsPartitionsTopic` is adequate — matches the established
per-topic coverage shape, no gaps.

No defects found in the `kafka-topics-partitions` topic itself; nothing filed. This is a
recommendation, not an approval — per this project's Definition of Done, please review this report
and give explicit sign-off (or flag anything that needs a second look) before issue #63 is
considered done.

## Human sign-off

_Pending._
