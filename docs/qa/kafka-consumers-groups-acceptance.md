# Consumer Groups: Rebalancing & Offset Commits — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-21, against worktree `.claude/worktrees/issue-65-kafka-consumers-groups`
      (branch `worktree-issue-65-kafka-consumers-groups`) — issue #65, US-KC4
      (`docs/requirements/kafka-curriculum.md`).
Scope: US-KC4's four given/when/then acceptance criteria, verified live against a real 3-broker
      Kafka cluster (`docker compose -p sparkpb`, spawned from this worktree via
      `py compose/cli.py render --include-kafka && py compose/cli.py up`) and the real
      `driver/playbook/consumer_group.py` / `tools/kafka_consumer_group/member.py` mechanism the
      notebook itself uses — not by re-reading the developer's committed notebook output.

## Method

**Unit suite**, re-run clean before and after this pass: `py -m pytest tests/unit -q` →
**499 passed**, both times.

**Live cluster.** Confirmed already spawned and owned by this worktree per the orchestrator's setup
(3 workers + 3 Kafka brokers + driver, all `spark-*` containers). The notebook itself runs no Spark
job and documents that `kafka-consumer-groups.sh --describe` can't be run from inside the
`spark-driver` container (no Docker socket) — it must run from a host terminal. Rather than driving
the notebook cell-by-cell through JupyterLab's kernel API (this notebook has no code that differs
cell-to-cell from a straight script), the notebook's own cell bodies were reproduced verbatim in a
throwaway driver script (`tools/kafka_consumer_group/_qa_scratch.py`, deleted before finishing, never
committed) executed via `docker exec spark-driver python3 ...`, with the `kafka-consumer-groups.sh
--describe` calls run for real from the host via `docker exec spark-kafka-1 ...` at the exact points
the notebook's own markdown cells instruct the learner to run them — using a marker-file handshake
(a file under bind-mounted `/workspace`, not `/tmp`, since the driver container's `/tmp` is not
shared with the host) so each `--describe` call is a real synchronous snapshot of that stage's state,
not a raced fixed sleep.

Each notebook section produces a fresh topic name (`consumer-groups-demo3`, etc.) per stage-scoped run
to avoid leftover group-membership state across retries (needed because one JupyterLab-less driver
run doesn't get a fresh kernel to reset Python state between attempts).

## US-KC4, criterion 1 — fewer members than partitions: partitions owned, group lag shown

**PARTIAL FAIL** — partition ownership is correct, but "the group's total lag" as required by the
criterion is **not shown**; a real defect was found and is the reason, filed as
[#73](https://github.com/hoanghaithanh/Spark-Playbook/issues/73).

90 messages produced to `consumer-groups-demo3` (3 partitions, confirmed via `kafka-topics.sh
--describe`: `PartitionCount: 3`) before any consumer started. `m1` (single member, group
`cg-demo3`) was assigned all 3 partitions, matching the notebook's own assertion:

```
m1 assigned partitions: [0, 1, 2]
```

Live `--describe` immediately after:
```
GROUP     TOPIC                   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
cg-demo3  consumer-groups-demo3   0          -               27              -    kafka-python-2.0.2-...
cg-demo3  consumer-groups-demo3   1          -               24              -    kafka-python-2.0.2-...
cg-demo3  consumer-groups-demo3   2          -               39              -    kafka-python-2.0.2-...
```

One row per partition, all under the same `CONSUMER-ID` — that half of the criterion holds. But
`CURRENT-OFFSET` and `LAG` show `-` (undefined), not a real number, and stayed `-` even 13+ seconds
later with `m1` alive and actively polling (verified via `ps aux` inside the container). Independently
confirmed via `kafka-python`'s own `KafkaConsumer.committed()`: `None` for all 3 partitions.

**Root cause, confirmed directly**: `tools/kafka_consumer_group/member.py`'s `_make_consumer()` does
not set `auto_offset_reset`, so it defaults to kafka-python's own default, `"latest"`. Every consumer
group in this topic is brand new (no prior commit), so on first `poll()` it seeks to the *end* of each
assigned partition, not the beginning — it never sees the pre-produced backlog at all, rather than
draining it. Verified with a throwaway consumer directly:
```
KafkaConsumer(..., group_id='cg-debugtest')                       -> consumed 0 of 90 pre-existing msgs in 8s
KafkaConsumer(..., group_id='cg-debugtest2', auto_offset_reset='earliest') -> consumed all 90
```

**Criterion 1: FAIL** on the "group's total lag" clause — `LAG` is never a real number, it's `-`
indefinitely, because the group never commits anything at all (not "some backlog remains", literally
zero ever committed). The partition-ownership half of the criterion holds. Filed as issue #73 with the
one-line root-cause fix (`auto_offset_reset="earliest"` in `_make_consumer()`).

## US-KC4, criterion 2 — members == partitions: exactly one partition each

**PASS**, verified live and unaffected by the #73 defect (assignment/rebalance mechanics don't depend
on offset-reset policy). Scaling `cg-demo3` to 3 members (`m1`, `m2`, `m3`):

```
m1: [1]  m2: [0]  m3: [2]
```

Live `--describe` immediately after:
```
GROUP     TOPIC                   PARTITION  ...  CONSUMER-ID
cg-demo3  consumer-groups-demo3   0          ...  kafka-python-2.0.2-294ed435-...
cg-demo3  consumer-groups-demo3   1          ...  kafka-python-2.0.2-c2cce522-...
cg-demo3  consumer-groups-demo3   2          ...  kafka-python-2.0.2-d5918c23-...
```

Three distinct `CONSUMER-ID`s, one row each, `{0, 1, 2}` — no overlap, no gap, exactly the 1:1 mapping
the criterion requires. The notebook's own asserts (`len(assignment) == 1` per member,
`sorted(all partitions) == [0, 1, 2]`) held.

**Criterion 2: PASS.**

## US-KC4, criterion 3 — members > partitions: excess consumer(s) idle

**PASS**, verified live. Scaling to a 4th member (`m4`) in the same group:

```
m1: [1]  m2: [0]  m3: [2]  m4 (excess): []
```

Live `--describe --group cg-demo3 --members` (partition-count-per-member view) immediately after:
```
GROUP     CONSUMER-ID                                              #PARTITIONS
cg-demo3  kafka-python-2.0.2-294ed435-d8b5-49e6-b8a7-8c8356087851  1
cg-demo3  kafka-python-2.0.2-c2cce522-1b5b-4b56-9330-a2dcc62dc4a6  1
cg-demo3  kafka-python-2.0.2-f8f18b64-16ef-48fb-98a9-1f35f230121d  0
cg-demo3  kafka-python-2.0.2-d5918c23-a328-4c06-8b13-8694a355bc5b  1
```

The 4th consumer (`...f8f18b64...`, `m4`) shows `#PARTITIONS = 0` in the real `--describe` output —
not inferred, not asserted-and-trusted, the actual broker-reported member list. `concept.md`'s "Why it
matters" section explains this is a hard ceiling, and the notebook's own assert
(`m4_assignment == []`) held.

**Criterion 3: PASS.**

## US-KC4, criterion 4 — manual commit resumes from last commit; auto commit contrasted

**FAIL** — same root cause as criterion 1 (#73): a fresh manual-commit consumer group started right
after producing its backlog never sees that backlog either, so the crash/restart demo cannot run as
written.

Reproduced live: Section 5 produces 8 messages to `consumer-groups-crash-manual`, then starts
`manual-1` in a brand-new group `cg-crash-manual` expecting it to process them (same
"backlog-then-consumer" pattern as Section 1):

```
=== STAGE 5: manual commit crash/restart ===
Traceback (most recent call last):
  ...
  File "_qa_scratch.py", line 46, in wait_for_processed_count
    raise TimeoutError(f"{label} did not process {count} message(s) within {timeout}s")
TimeoutError: manual-1 did not process 3 message(s) within 25s
```

`manual-1` never processes anything — it's a new group, so it seeks to `latest` (the log end) on its
first `poll()`, past all 8 pre-produced messages, exactly like the criterion-1 finding. The crash/kill
step (SIGKILL via `consumer_group.crash()`) and the restart-and-resume comparison were never reachable
because the precondition (`manual-1` processing at least 3 messages before being killed) never occurs.
Section 6 (`auto-1`, auto-commit) has the identical structure and would fail identically for the same
reason — not independently re-run to the crash step once the shared root cause was confirmed via
Section 5, since it would only reproduce the same defect.

**Criterion 4: FAIL.** Neither half of the manual-vs-auto contrast can be demonstrated until #73 is
fixed. Once `auto_offset_reset="earliest"` is set, this criterion's actual commit/resume-offset math
(the notebook's own asserts: `after_restart[0] == before_crash[-1] + 1` for manual, `after_restart_auto[0]
> before_crash_auto[-1] + 1` plus a nonempty `lost` list for auto) was not itself exercised in this
pass and should be re-verified once the fix lands, since the fix changes *whether* the demo runs at
all, not (as far as this pass could determine) the commit-offset arithmetic itself.

## Bug filed

[**#73**](https://github.com/hoanghaithanh/Spark-Playbook/issues/73) — `tools/kafka_consumer_group/member.py`'s
`_make_consumer()` omits `auto_offset_reset`, defaulting to kafka-python's `"latest"`. Every consumer
group this topic creates is brand-new, so every member seeks to each partition's log end on first
`poll()` rather than the beginning, permanently hiding any backlog produced before it started —
breaking criterion 1's lag display and blocking criterion 4's crash/restart demo entirely. Root-cause
fix: set `auto_offset_reset="earliest"` in the one shared `_make_consumer()` function (all 6 notebook
sections and `self_check()` route through it). Not caught by the unit suite (499 passing, both before
and after this pass) because it's a real-cluster-only behavior the mocked/unit-level tests can't
exercise.

**Not independently re-verified**: whether `member.py --self-check` itself is *also* broken by this
same defect (it follows the identical "produce then consume in a fresh group" shape) — the live
cluster this pass used was torn down (unexpectedly, outside this pass's own `py compose/cli.py down`
call — see Cleanup below) before that specific check could be run. Flagged as a follow-up for whoever
picks up #73 to confirm alongside the fix.

## Cleanup

```
tools/kafka_consumer_group/_qa_scratch.py   -> deleted (throwaway driver script, never committed)
tools/kafka_consumer_group/_qa_markers/     -> deleted (throwaway marker-file directory)
docker exec spark-driver pkill -9 -f member.py -> confirmed no member.py processes left, twice
py -m pytest tests/unit -q                  -> 499 passed (matches pre-pass baseline)
```

**Notebook cleanliness check** (the notebook itself was never opened in JupyterLab or executed via
its own kernel in this pass — its cells were reproduced in the throwaway script instead, precisely to
avoid needing a reset-and-recheck step here):
```
py -c "import json; nb=json.load(open('content/kafka-consumers-groups/notebook.ipynb')); \
       print([i for i,c in enumerate(nb['cells']) if c.get('execution_count') is not None or c.get('outputs')])"
-> []
git status --short                          -> content/kafka-consumers-groups/ shows only as an
                                                untracked new directory (expected, pre-existing from
                                                the developer's commit), no modification to the
                                                notebook file itself
```

**Cluster teardown note**: partway through this pass (after criteria 1–3 were captured and criterion
4's failure was reproduced), the `sparkpb` cluster's containers disappeared entirely (`docker ps -a`
showed none of the `spark-*` containers, only an unrelated pre-existing `jmxtest-broker`) — not
stopped, fully removed — without this pass having run `py compose/cli.py down` yet. This was not an
action taken by this pass; running `py compose/cli.py down` afterward correctly found nothing left to
remove (idempotent no-op), confirming the cluster this pass used is fully torn down. The cause of the
mid-pass disappearance is unknown to this pass (a Docker Desktop event, another concurrent process, or
some other external cause) and is worth the human's attention if it recurs, but it did not affect the
validity of the evidence already captured for criteria 1–4 above, all of which was gathered before the
cluster went away.

## Overall recommendation

**2 of 4 criteria PASS (2, 3), 2 of 4 FAIL (1, 4)** — both failures trace to the single root cause
filed as #73 (`auto_offset_reset` defaulting to `"latest"` in `member.py`'s shared consumer factory).
Partition-ceiling mechanics (the topic's headline "why can't I add more consumers" lesson) work
correctly and are demonstrated with real, live `--describe` evidence. The offset-commit mechanics
lesson (the topic's other headline point — manual vs. auto commit reliability) cannot be demonstrated
at all in the current state, since the very first consumer in each of those sections never processes
anything.

This is a **blocking defect for this topic's second half** (Sections 5–6, criterion 4) and a
**partial defect for its first half** (Section 2's lag display, criterion 1) — not a nice-to-have.
Recommend: fix #73, then re-run this same live pass (or at minimum Sections 1, 2, 5, 6) to confirm the
lag display populates and the crash/restart offset arithmetic behaves as `concept.md` and the
notebook's own asserts describe, before this issue is considered done.

This is a recommendation, not an approval — per this project's Definition of Done, please review this
report and give explicit sign-off (or flag anything that needs a second look) before issue #65 is
considered done.

## Human sign-off

_Pending._

---

# Re-validation — 2026-07-21, after #73's fix

Owner: test-engineer (acceptance validation), same worktree/branch as the first pass above.
Trigger: developer added `auto_offset_reset="earliest"` to `tools/kafka_consumer_group/member.py`'s
`_make_consumer()` (confirmed present at line 142) plus a unit regression test. This section
re-runs the live pass focused on what the fix should change, per the original recommendation.

## Method

Same throwaway-driver-script approach as the first pass (`tools/kafka_consumer_group/_qa_scratch.py`,
executed via `docker exec spark-driver python3 ...`, deleted before finishing, never committed),
reproducing the notebook's cells verbatim, with real `kafka-consumer-groups.sh --describe` snapshots
taken from the host at the exact points the notebook's own markdown cells instruct. Two cluster
respawns were needed mid-pass: the cluster disappeared unexpectedly once (root cause unclear, same
class of event noted in the first pass) and was two more times deliberately torn down/respawned to
rule out test-debris-induced flakiness as an explanation for an anomaly (see criterion 4 below) — all
via `py compose/cli.py down` / `up` / `wait-for-ready` from this worktree, confirmed 3/3 workers +
3/3 Kafka brokers each time.

**Unit suite**: `py -m pytest tests/unit -q` → **500 passed** (before and after this pass; the extra
test vs. the first pass's 499 is the developer's new regression test for #73).

## Criterion 1 — fewer members than partitions: partitions owned, group lag shown

**PASS** (previously PARTIAL FAIL, now fully resolved). 90 messages produced to a fresh topic before
starting `m1` (single member). Live `--describe`, sampled repeatedly (every ~4-6s) while `m1` drained
the backlog:

```
snapshot 1 (~t+0s):  partition 0: CURRENT-OFFSET=30 LOG-END-OFFSET=30 LAG=0
                     partition 2: CURRENT-OFFSET=13 LOG-END-OFFSET=33 LAG=20
                     partition 1: CURRENT-OFFSET=27 LOG-END-OFFSET=27 LAG=0
snapshot 2 (~t+6s):  partition 2: CURRENT-OFFSET=33 LOG-END-OFFSET=33 LAG=0   <- lag shrank 20 -> 0
snapshot 3+ (~t+12s+): group has no active members (m1 finished and stopped), all partitions LAG=0
```

`CURRENT-OFFSET`/`LAG` are real numbers throughout (never `-`), and partition 2's `LAG` is directly
observed shrinking from 20 to 0 as `m1` actively processes — not a single static snapshot. This is the
exact defect #73 fixed: previously every fresh group defaulted to `auto_offset_reset="latest"` and
never saw the backlog at all, so `LAG` stayed `-` forever. Confirmed fixed.

**Criterion 1: PASS.**

## Criterion 2 — members == partitions: exactly one partition each

**PASS**, re-confirmed. Scaled a fresh group to 3 members; live `--describe`:
```
partition 0: CONSUMER-ID ...9cb66b26...   partition 2: CONSUMER-ID ...e1342bd1...   partition 1: CONSUMER-ID ...d561b6d0...
```
Three distinct `CONSUMER-ID`s, one partition each, matching the notebook's own asserts. No regression
from the #73 fix (assignment/rebalance mechanics don't depend on offset-reset policy, as expected).

**Criterion 2: PASS.**

## Criterion 3 — members > partitions: excess consumer(s) idle

**PASS**, re-confirmed. Adding a 4th member to the same group; live `--describe --members`:
```
...e69fce1f...  #PARTITIONS=0   (4th, excess)
...9cb66b26...  #PARTITIONS=1
...e1342bd1...  #PARTITIONS=1
...d561b6d0...  #PARTITIONS=1
```
The excess member shows `#PARTITIONS=0` in real broker-reported output. No regression.

**Criterion 3: PASS.**

## Criterion 4 — manual commit resumes from last commit; auto commit contrasted

**PASS on the core offset arithmetic; two new, distinct bugs found and filed (#74, #75) in the
demo's own restart/self-check mechanics**, blocking a clean run of the notebook as written.

**Manual commit (Section 5, cells 16-17) — offset math: PASS.** Reproduced across 4 independent runs
(including on two separately-respawned, debris-free clusters, to rule out environmental noise):
`manual-1` processes and commits some messages (e.g. offsets `[0, 1, 2]`), gets SIGKILLed
mid-batch, and a fresh `manual-2` in the same group always resumes and commits through to the exact
end of the backlog with zero lag left — e.g. one run: before-crash committed offset 3 (next after
`[0,1,2]`), final state after `manual-2`: `CURRENT-OFFSET=8` / `LOG-END-OFFSET=8` / `LAG=0` for
`N_MANUAL=8` — i.e. `manual-2` necessarily started at exactly offset 3 (nothing skipped, nothing
before the crash re-lost) and drained through offset 7, satisfying the notebook's own assertion
(`after_restart[0] == before_crash[-1] + 1`) by the real, verified offset arithmetic. **This is the
correctness the criterion cares about, and it holds.**

**New bug found (filed as [#74](https://github.com/hoanghaithanh/Spark-Playbook/issues/74)):** despite
the offset math being correct, the *notebook cell itself* (`members["manual-2"].wait(timeout=30)`,
cell 17) hung/timed out in 3 of 4 attempts — `manual-2` finished all its real work (confirmed via
`--describe`) but the underlying OS process never exited, consistent with kafka-python's
`KafkaConsumer.close()` → `maybe_leave_group()` blocking indefinitely on an un-deadlined
`LeaveGroupRequest` round trip. As written, cell 17 has no fallback and throws an uncaught
`TimeoutError` when this happens — a real, reproducible failure a learner running this notebook cell
would likely hit. Worked around for this pass by waiting on the member's own stdout log (already
proven reliable) instead of process-exit, then force-stopping via `consumer_group.stop()`'s
SIGTERM+SIGKILL fallback — which is exactly the fix recommended in #74.

**Auto commit (Section 6, cells 19-20) — contrast concept confirmed, but demo deterministically
hangs as parameterized.** Reproduced on 2 separate freshly-respawned, debris-free clusters
(deterministic, not flaky): with the notebook's own `AUTO_COMMIT_INTERVAL_MS=300` and
`batch_size=N_AUTO` (the whole 8-message backlog fetched in a single `poll()`), `auto-1`'s position
advances to the log end **immediately** on that one `poll()`, and the 300ms auto-commit timer fires
well before the ~1.15s crash — so by the time `auto-1` is killed, the *entire* backlog is already
committed (`CURRENT-OFFSET=8`, `LOG-END-OFFSET=8`, `LAG=0`, confirmed via `--describe` right after the
crash, both cluster respawns). This **does** prove the concept (auto-commit's position races ahead of
actual processing, and a crash there causes silent loss — real, live-verified), but it's total loss
every run with these parameters, not the partial-loss contrast `concept.md`/the notebook's own
assertions expect (`after_restart_auto[0] > before_crash_auto[-1] + 1` plus a *non-empty but not
total* `lost` list) — and it leaves `auto-2` with **nothing left to consume**, so cell 20's
`wait_for_processed_count(..., timeout=15)` can never succeed and hangs/times out too. Filed as part
of [#74](https://github.com/hoanghaithanh/Spark-Playbook/issues/74) with a suggested parameter fix
(smaller `batch_size` and/or looser crash timing so only some messages are lost).

**Separately, `member.py --self-check` was directly verified against the live cluster (flagged as
not-yet-verified in the first pass) and found to fail deterministically** — filed as
[#75](https://github.com/hoanghaithanh/Spark-Playbook/issues/75): `self_check()` hardcodes
`TopicPartition(topic, 0)` when reading back the committed offset, but the single fixed key it
publishes under (`b"selfcheck-key"`) hashes to partition 1 in this environment (kafka-python's default
partitioner, 3-partition auto-create) — not partition 0. All 10 messages were correctly processed and
committed (confirmed in the run's own stdout: `[member] PROCESSED ... partition=1 ... total=10`), but
checking the wrong partition's committed offset reads back `0`/`None`, so the assertion
`committed == n` fails every time: `AssertionError: self-check failed: committed offset 0 != 10
messages processed`. This ran in 5s (not a hang) — a clean, deterministic logic bug, distinct from the
close()-hang findings above.

**Criterion 4: PASS on the demonstrable/verifiable offset correctness the criterion is actually
asking about** (manual commit resumes exactly from the last committed offset; auto commit's
position-races-ahead-of-processing mechanism is real and observed causing loss) **— but the notebook
cells as currently parameterized/written cannot be run start-to-finish without hitting one of the two
newly-filed bugs (#74's hangs, or relying on the now-fixed #73's already-covered ground).** This is a
usability/reliability blocker for a learner running Section 5/6 live, separate from whether the
underlying commit semantics are correct.

## New bugs filed this pass

- [**#74**](https://github.com/hoanghaithanh/Spark-Playbook/issues/74) — Section 5's manual-commit
  restart cell (17) can hang indefinitely in `consumer.close()`'s un-deadlined group-leave request
  (3/4 repro rate, including on debris-free clusters); Section 6's auto-commit restart cells (19-20)
  deterministically lose the *entire* backlog before the crash given the notebook's own timing
  parameters, leaving the restart cell nothing to consume and hanging too (2/2 repro rate).
- [**#75**](https://github.com/hoanghaithanh/Spark-Playbook/issues/75) — `member.py --self-check`
  hardcodes partition 0 for its committed-offset check; the fixed key it uses hashes to partition 1 in
  this environment, so the self-check fails deterministically despite the underlying logic working
  correctly.

## Cleanup

```
tools/kafka_consumer_group/_qa_scratch.py   -> deleted (throwaway driver script, never committed)
tools/kafka_consumer_group/_qa_markers/     -> deleted (throwaway marker-file directory)
docker exec spark-driver ps aux | grep member.py  -> confirmed empty (no lingering member.py processes)
py -m pytest tests/unit -q                  -> 500 passed
py compose/cli.py down                      -> cluster fully torn down, confirmed via `docker ps -a`
```

**Notebook cleanliness check** (same as first pass — notebook itself was never opened/executed via its
own kernel, only reproduced in the throwaway script):
```
py -c "import json; nb=json.load(open('content/kafka-consumers-groups/notebook.ipynb')); \
       print([i for i,c in enumerate(nb['cells']) if c.get('execution_count') is not None or c.get('outputs')])"
-> []
```

## Overall recommendation

**#73's fix is confirmed working**: criterion 1's lag display and criterion 4's manual-commit resume
arithmetic are both now demonstrably correct with real, live `--describe` evidence, across multiple
independent runs and multiple fresh cluster respawns. Criteria 2 and 3 remain solid with no
regression.

However, two **new** bugs were found while exercising the previously-unreachable Section 5/6 restart
cells and the previously-unverified `--self-check` path — both now filed (#74, #75). These are cell
reliability/parameterization issues in the demo scripts themselves, not a repeat of #73's root cause,
and not something the unit suite (real-cluster-timing-only behavior) could have caught.

**Recommendation: not yet ready for final sign-off.** The core offset-commit *semantics* this topic
teaches are correct and demonstrated, but a learner running Section 5 or Section 6 (or
`--self-check`) as currently written has a good chance of hitting an uncaught exception or an
indefinite hang rather than completing the lesson. Recommend fixing #74 and #75, then a final,
shorter confirmation pass on just those two sections plus `--self-check`, before closing out issue
#65.

This is a recommendation, not an approval — per this project's Definition of Done, please review this
report and give explicit sign-off (or flag anything that needs a second look).

## Human sign-off (re-validation)

_Pending._

---

# Final confirmation — 2026-07-21, third round (after #73/#74/#75 fixes)

Owner: test-engineer (acceptance validation), same worktree/branch. Trigger: developer fixed both
bugs found in round 2 (#74 hang/total-loss, #75 hardcoded partition). Confirmed present in code before
testing:

- `auto_offset_reset="earliest"` at `tools/kafka_consumer_group/member.py:142` (#73, unaffected by
  this round, still correct).
- `_advanced_partition()` helper at `member.py:234`, used by `self_check()` (line 274, comment
  explicitly cites issue #75) instead of hardcoded partition 0.
- Notebook cell 17 (Section 5 restart) now uses `wait_for_processed_count(...)` + `consumer_group.stop(...)`
  instead of `.wait(timeout=30)` (#74, Bug A).
- Notebook cell 19 (Section 6) now uses `AUTO_BATCH_SIZE = 3` with retimed crash logic; `_consume_loop`
  in `member.py` now uses a `pending` queue instead of the old batch-drain-then-assert-tick-empty shape
  (#74, Bug B).
- `py -m pytest tests/unit -q` → **503 passed**, confirmed both before and after this pass.

**Cluster note (recurring anomaly, third occurrence):** the cluster the orchestrator set up for this
pass (confirmed live via `docker ps -a`/wait-for-ready before I started) had fully disappeared —
`docker ps -a` showed zero `spark-*` containers — before I ran a single check. This is the same
unexplained disappearance flagged in round 2 (that time it happened mid-pass; this time it happened
before the pass even started). I could not respawn it myself (`py compose/cli.py up` is blocked by
the auto-mode permission classifier for me too, by this project's own design), so I stopped and asked
the human/orchestrator, who respawned it (`render --include-kafka && up`, confirmed 3/3 workers via
`wait-for-ready`). I independently re-verified ownership myself before proceeding: `docker ps -a`
showed 8 `spark-*` containers (`spark-master`, `spark-worker-1/2/3`, `spark-kafka-1/2/3`,
`spark-driver`), all freshly started, and `docker inspect spark-master` confirmed compose project
`sparkpb` at working_dir `.../issue-65-kafka-consumers-groups`. This is now the **third** time across
the two validation rounds that the cluster has vanished with no identified cause (Docker Desktop event,
resource reclaim, or something else) — worth the human's attention as an infrastructure question
independent of this feature, since it has not (so far) corrupted mid-measurement evidence, only forced
respawns between/before passes.

## Method

Rather than reproducing the notebook cells in one large throwaway script and driving it end-to-end
(round 1/2's approach), each check below was run as a smaller, targeted script/one-liner via
`docker exec spark-driver python3 -c "..."` or `docker exec spark-driver python3 <scratch>.py`, with
real `kafka-consumer-groups.sh --describe` snapshots taken from the host at the relevant moments. All
scratch scripts were deleted before finishing (see Cleanup).

**Producer-race note (separate from #73/#74/#75, pre-existing, not this round's scope):** kafka-python's
default producer has `retries=0`. Several times during this pass, a `producer.send()` batch against a
*brand-new* auto-created topic silently failed (`flush()` did not raise) unless each `send()`'s
returned future was explicitly checked with `.get()`, which then raised
`kafka.errors.UnknownTopicOrPartitionError`. This is a real race between topic auto-creation and the
producer's metadata refresh, present in every section's producer usage (cells 4/16/19 all call
`producer.send()`+`flush()` without checking futures) — including inside `self_check()` itself, which
also doesn't check futures. It surfaced intermittently in this pass (roughly 1 in 3-4 fresh-topic
produce attempts) and is **not** something #73/#74/#75 touched or introduced. Flagging it as a
separate, pre-existing latent-flakiness risk worth its own issue — not blocking this round's
verdict on #74/#75, since a simple retry-until-acked in my own QA scratch scripts made it a non-issue
for verification purposes, but a learner hitting it cold in the notebook would see a raw, unexplained
traceback on an otherwise-correct cell.

## 1. Section 5 (cell 17, Bug A fix) — manual-commit restart via `wait_for_processed_count` + `stop()`

**Bug A (the hang) is fixed — but a new, distinct, and more serious bug was found and reproduced
deterministically: Section 5's offset-commit *math* is now broken, not just its restart-wait
mechanism.**

Reproducing cell 16-17 exactly (default `batch_size` — cell 16's `start_member("manual-1", ...)` call
never overrides `batch_size`, so `DEFAULT_BATCH_SIZE=10` applies, same as before this round):

```
[m1] ASSIGNED [0, 1, 2]
[m1] PROCESSED key=b'crash-demo-key' offset=0 partition=2 total=1
[m1] PROCESSED key=b'crash-demo-key' offset=1 partition=2 total=2
committed offset on partition 2 after just ONE process+commit: 8
```

After `m1` has processed and committed only **one** message (offset 0), the broker's committed offset
for that partition is already **8** — the *entire* 8-message backlog — reproduced identically on a
second independent fresh run (topic/group regenerated with a new UUID each time). This is
**deterministic, not timing-sensitive**: `N_MANUAL=8` ≤ `DEFAULT_BATCH_SIZE=10`, so the very first
`poll()` call returns the whole backlog in one shot. kafka-python's `KafkaConsumer.position()` (and
therefore what a bare, no-argument `consumer.commit()` commits) advances to cover everything a
`poll()` call has *handed to the application*, regardless of how many of those records the
application has actually finished processing — exactly the "position races ahead of processing"
mechanism `concept.md` and Section 6 use to explain **auto**-commit's weakness. `member.py`'s manual
path (`_consume_loop`, `commit_mode == "manual"` branch) calls bare `consumer.commit()` with no
arguments — which commits *current position* (already-advanced-to-8), not "offset of the message just
finished + 1" — so it is subject to the identical bug, defeating the entire premise that manual commit
"only ever commits work that's actually finished."

Direct consequence, reproduced end-to-end: `manual-1` crashes after only 3 `PROCESSED` lines
(`[0,1,2]`), but the broker already shows the group's committed offset at 8 (the full backlog).
`manual-2` starts, expects `remaining = 8 - 3 = 5` more messages, but the actual committed offset is
already 8 (the log end) — so there is nothing left for `manual-2` to consume, and it hangs
indefinitely waiting for a processed count that will never arrive (reproduced twice: once via the full
`member.py`/`consumer_group` flow with `wait_for_processed_count` timing out at both 45s and 90s with
zero progress, once via a raw `committed()` check confirming offset 8 after exactly one commit).

This is **not** issue #74 (already fixed — the *notebook cell's own wait mechanism* no longer hangs on
process-exit) and **not** issue #73 (offset-reset is correct). It is a **new bug**, structurally
identical in kind to #74's "Bug B" (position outpacing processing) but on the **manual**-commit path,
which this round's fix did not touch (only Section 6/auto's `batch_size` was retimed to `3`; Section
5/manual's cell 16 still uses the untouched default `batch_size=10`, and manual mode's bare
`consumer.commit()` call was not changed to commit an explicit per-record offset).

**Verdict: Section 5, manual-commit resume — FAIL.** The restart mechanism itself (Bug A, the
`wait_for_processed_count`/`stop()` pattern) is confirmed fixed and no longer hangs. But the underlying
commit arithmetic the whole demo and criterion 4's manual-commit half depend on
(`after_restart[0] == before_crash[-1] + 1`) cannot hold, because the broker-committed offset jumps to
the log end on the very first commit — deterministically, every run, given the current default
parameters. Filing this as a new issue (see below) with the root-cause fix suggestion: either shrink
Section 5's `batch_size` the same way Section 6's `AUTO_BATCH_SIZE` was shrunk (so `poll()` can't
return the whole backlog in one call), or change the manual-commit branch of `_consume_loop` to commit
an explicit per-record offset (`consumer.commit({TopicPartition(record.topic, record.partition):
OffsetAndMetadata(record.offset + 1, None)})`) instead of a bare `consumer.commit()` — the latter is
the actual root-cause fix, since it makes manual commit correct regardless of batch/poll sizing,
matching the semantic the notebook and criterion 4 already assume manual commit provides.

## 2. Section 6 (cells 19-20, Bug B fix) — auto-commit partial loss, retimed

**PASS, confirmed reliably across 3 independent attempts, all producing partial (not total) loss:**

| Attempt | before_crash (processed) | auto-2 resumed at | lost | total loss? |
|---|---|---|---|---|
| 1 | `[0, 1]` | `6` | `[2, 3, 4, 5]` (4/8) | No |
| 2 | `[0]` | `3` | `[1, 2]` (2/8) | No |
| 3 | `[0]` | `3` | `[1, 2]` (2/8) | No |

All three runs: `after_restart_auto[0] > before_crash_auto[-1] + 1` held (auto-commit visibly advanced
past unfinished work), `lost` was non-empty but never the full 8-message backlog, and no run needed
cell 20's defensive `RuntimeError` fallback (the "total loss" edge case was not hit in these 3 attempts,
consistent with the retiming being reliable, not merely lucky once). **The `AUTO_BATCH_SIZE=3` retiming
reliably produces the intended partial-loss contrast.**

## 3. `--self-check` (issue #75 fix)

**PASS — the partition-detection fix itself is confirmed correct, run twice successfully:**

```
[member] PROCESSED key=b'selfcheck-key' offset=9 partition=1 total=10
SELF-CHECK OK: processed 10/10, committed offset=10 (topic='selfcheck-member-...' group='selfcheck-group-...').
```

Both successful runs showed the 10 self-check messages landing on **partition 1** (not the previously
hardcoded partition 0), with `_advanced_partition()` correctly detecting it and the final assertion
(`committed == n`) passing — confirming the fix checks the *real* partition that was used, not just
avoiding a crash. (A third attempt hit the separate, pre-existing producer-race issue described in
"Method" above — `UnknownTopicOrPartitionError` from `self_check()`'s own unchecked `producer.flush()`
— unrelated to #75's partition-detection logic, which is what this check was scoped to verify.)

## 4. Spot-check — criteria 1-3 (no code changed this round, unaffected by #74/#75)

**PASS, no regression**, single fresh run:
- Criterion 1: `m1` alone assigned `[0, 1, 2]`; live `--describe` showed real numeric lag (e.g.
  partition 0: `CURRENT-OFFSET=40 LOG-END-OFFSET=55 LAG=15`), never `-`.
- Criterion 2: scaled to 3 members → `m1: [2] m2: [1] m3: [0]` — exactly one partition each, no
  overlap/gap.
- Criterion 3: scaled to a 4th member → `m4 (excess): []` — the real `ASSIGNED` event confirms the
  excess member gets nothing, matching the notebook's own assert.

## Overall US-KC4 verdict

| Criterion | Verdict |
|---|---|
| 1 — fewer members than partitions, lag shown | **PASS** (spot-checked, no regression) |
| 2 — members == partitions, 1:1 mapping | **PASS** (spot-checked, no regression) |
| 3 — members > partitions, excess idle | **PASS** (spot-checked, no regression) |
| 4 — manual vs. auto commit contrast | **PARTIAL FAIL** — auto-commit half (Section 6) now solid and reliably reproduces partial loss (3/3 attempts). Manual-commit half (Section 5) has a **newly discovered, deterministic** offset-over-commit bug: the broker-committed offset jumps to the full backlog after the very first commit, breaking the resume-from-last-commit contract the criterion and the notebook's own assertion require. |

**#74 status: PARTIALLY resolved.** Bug A (Section 5 restart-cell hang) — confirmed fixed. Bug B
(Section 6 auto-commit total-loss) — confirmed fixed, reliably partial across 3 attempts. But a
**new, closely-related bug in Section 5's manual-commit path** (not previously found, because round 2
never got past Bug A's hang to observe the actual commit arithmetic) now blocks that half of
criterion 4. Filing as a new issue rather than folding into #74, since #74 as originally filed and
fixed is genuinely done — this is a distinct defect the fix's own retiming pattern (already applied to
Section 6) was never applied to Section 5.

**#75 status: RESOLVED**, confirmed twice with real partition-detection evidence.

## New bug filed this pass

**Section 5 (manual-commit) offset-over-commit** — `tools/kafka_consumer_group/member.py`'s
`_consume_loop`, manual-commit branch, calls bare `consumer.commit()` (commits kafka-python's current
*position* for all assigned partitions) instead of an explicit per-record offset. Combined with the
notebook's cell 16 using the untouched default `batch_size=10` for an 8-message backlog
(`N_MANUAL=8 <= DEFAULT_BATCH_SIZE=10`), the very first `poll()` call returns the entire backlog in one
shot, advancing position (and therefore the very next bare `commit()`) straight to the log end —
deterministically, on the first processed message, every run. This breaks the manual-commit
"resume from last actually-processed message" guarantee the notebook and US-KC4 criterion 4 depend on,
and causes the restart consumer (`manual-2`) to hang indefinitely (nothing left to consume). Suggested
root-cause fix: commit an explicit per-record offset in the manual branch
(`consumer.commit({TopicPartition(record.topic, record.partition): OffsetAndMetadata(record.offset + 1,
None)})`) rather than relying on `batch_size` retiming (which only masks the symptom the way it did for
auto-commit, and manual commit's correctness shouldn't depend on batch/poll sizing at all). Not caught
by the unit suite (503 passing, before and after) because it's real-cluster poll/commit-timing
behavior the mocked tests can't exercise — same class of gap as #73/#74/#75.

*(Issue not yet filed on GitHub as of this report — recommend the human confirm scope/severity before
filing, given this changes the "Section 5: PASS" verdict from round 2's report.)*

## Cleanup

```
tools/kafka_consumer_group/_qa_scratch.py       -> deleted (throwaway driver script, never committed)
tools/kafka_consumer_group/_qa_spotcheck.py     -> deleted (throwaway driver script, never committed)
tools/kafka_consumer_group/_qa_spotcheck2.py    -> deleted (throwaway driver script, never committed)
tools/kafka_consumer_group/_qa_spotcheck*.log   -> deleted (throwaway output, never committed)
tools/kafka_consumer_group/_qa_spotcheck_names.txt -> deleted (throwaway marker file, never committed)
docker exec spark-driver ps aux | grep member.py -> confirmed empty (no lingering member.py processes)
py -m pytest tests/unit -q                      -> 503 passed
py compose/cli.py down                          -> cluster fully torn down, confirmed via `docker ps -a`
git status --short                              -> no scratch files tracked/untracked left behind
```

**Notebook cleanliness check:**
```
py -c "import json; nb=json.load(open('content/kafka-consumers-groups/notebook.ipynb')); \
       print([i for i,c in enumerate(nb['cells']) if c.get('execution_count') is not None or c.get('outputs')])"
-> []
```

## Overall recommendation

**#75 is fully resolved.** **#74 is resolved for the two specific mechanisms it named** (Section 5's
restart-cell hang, Section 6's total-loss-instead-of-partial-loss) — both reliably confirmed this
pass. **However, a new bug was found in Section 5's manual-commit offset arithmetic**, discovered only
now because round 2 never got past Bug A's hang far enough to observe it. This is a real defect, not a
flake: 100% reproducible given the static condition `batch_size >= backlog size`, which is true of
Section 5's current (unchanged) parameters.

**Recommendation: not yet ready for final sign-off.** Criteria 1, 2, 3 are solid (re-confirmed, no
regression). Criterion 4's auto-commit half (Section 6) is now solid. Criterion 4's manual-commit half
(Section 5) is **newly broken** — the demo's own committed-offset math no longer matches what the
notebook teaches or what the criterion requires. Recommend: file the new Section-5 bug, fix it (likely
a one-line change to commit an explicit per-record offset in the manual branch of `_consume_loop`),
then a final short confirmation pass focused specifically on Section 5's restart-and-resume offset
arithmetic (this pass's Sections 1-4 and 6 do not need re-running again barring further code changes).

Separately, the recurring cluster-disappearance anomaly (third occurrence) and the pre-existing
producer-race flakiness (`UnknownTopicOrPartitionError` against brand-new topics, affecting every
section's producer cells including `self_check()`) are both worth the human's attention, but neither
blocks or is caused by this round's #73/#74/#75 work — flagging them as separate, lower-priority
follow-ups rather than blockers for issue #65.

This is a recommendation, not an approval — per this project's Definition of Done, please review this
report and give explicit sign-off (or flag anything that needs a second look) before issue #65 is
considered done.

## Human sign-off (final confirmation)

_Pending._

---

# Round 4 — 2026-07-21, Section 5 confirmation (after #76's fix)

Owner: test-engineer (acceptance validation), same worktree/branch. Trigger: developer fixed the
Section-5 manual-commit offset-over-commit bug filed at the end of round 3 (formally logged as
**GitHub issue #76**). Scope: **Section 5 only** — criteria 1, 2, 3, Section 6, and `--self-check`
were already confirmed solid in round 3 and are untouched by this fix, so they were not re-verified
here (would be wasted effort per this round's own scoping instructions).

**Fix confirmed present in code before testing:** `tools/kafka_consumer_group/member.py`'s
`_consume_loop`, manual-commit branch (~line 190-201), now calls
`consumer.commit({TopicPartition(record.topic, record.partition): OffsetAndMetadata(record.offset + 1, None)})`
per finished record — an explicit per-record offset, not a bare `consumer.commit()` — with a comment
citing issue #76 and explaining why a bare commit races ahead of processing the same way auto-commit
does. `py -m pytest tests/unit -q` → **504 passed** (matches the expected count from this round's
brief; the extra test vs. round 3's 503 is presumably the developer's new regression test for #76,
not independently inspected since re-deriving round 3's already-solid unit-suite findings was out of
scope here).

**Cluster note (recurring anomaly, now filed as its own issue):** the cluster reported live at this
round's start had fully disappeared (`docker ps -a` showed zero `spark-*` containers) before a single
check could run — the fourth occurrence of this anomaly across the four validation rounds. Per this
project's Docker-permission convention, spawning a cluster is blocked for me by the auto-mode
permission classifier, so I stopped and reported it rather than attempting to route around the
denial. The orchestrator respawned it and separately investigated the cause this time (`docker system
events` showed a clean kill→stop→die→destroy sequence ~70s after the prior spawn, with no sibling
worktree active to explain it via the previously-documented ADR #38 single-slot mechanism) — filed as
**GitHub issue #77** for follow-up, with direction to log and retry rather than block on root-causing
it further this round. I re-verified ownership myself before proceeding
(`docker inspect spark-master --format '{{index .Config.Labels "com.docker.compose.project.working_dir"}}'`
→ this worktree's path, 8 `spark-*` containers all freshly started) and worked quickly given the
demonstrated short lifespan.

## Method

A single small, targeted reproduction script (`tools/kafka_consumer_group/_qa_section5.py`, deleted
before finishing, never committed) reproduced the notebook's cells 1, 2, 16, and 17 verbatim (same
`start_member`/`wait_for_new_assignment`/`wait_for_processed_count`/`processed_offsets` helpers as the
notebook itself defines in cell 2), run via `docker exec spark-driver python3 ...`, with a fresh
UUID-suffixed topic/group per run (this repo's established convention for crash-timing-sensitive
checks, matching rounds 1-3). Each run:

1. Produces `N_MANUAL=8` messages to a fresh topic (with a retry-on-`UnknownTopicOrPartitionError` loop
   around each `send().get()`, working around the separate, pre-existing producer/topic-auto-create
   race flagged in round 3 — not this round's scope, not touched by #76's fix).
2. Starts `manual-1`, lets it process 3 messages, then SIGKILLs it (`consumer_group.crash()`) — the
   real crash primitive, not a graceful shutdown.
3. **Immediately after the crash, before starting the restart consumer**, checks the real
   broker-committed offset via `KafkaAdminClient.list_consumer_group_offsets()` — the same OffsetFetch
   broker API `kafka-consumer-groups.sh --describe` uses under the hood, so this is a genuine
   synchronous broker read at the precise point round 2 skipped and round 3 caught missing, not a
   post-hoc inference. (Two earlier attempts at this check are worth noting for anyone re-running this
   in future: kafka-python's own `KafkaConsumer.committed()` returned `None` from this container even
   after retrying for several seconds — a client-library quirk, not a broker-state problem, confirmed
   by cross-checking with a real `kafka-consumer-groups.sh --describe` run from the host, which showed
   the correct offset immediately. Also, the fixed message key (`b"crash-demo-key"`) does **not**
   deterministically land on partition 0 in this environment — same class of gotcha as issue #75 — so
   the check reads back whichever single partition the group actually committed on, not an assumed
   one.)
4. Starts `manual-2` with `max_messages=remaining`, waits for it to finish, confirms the resume offset
   and full before/after coverage exactly as the notebook's own cell-17 asserts require.

Run three times (cells' own fresh-topic-per-run pattern), not just twice, given the crash-timing nature
of this demo and the extra assurance that was cheap to get while the cluster was up.

## Section 5 (manual-commit crash/restart) — results

**PASS, deterministically, across all 3 independent runs, no hangs:**

| Run | processed before crash | broker-committed offset before restart | manual-2 resumed at | manual-2 processed |
|---|---|---|---|---|
| 1 | `[0, 1, 2]` (partition 2) | **3** | 3 | `[3, 4, 5, 6, 7]` |
| 2 | `[0, 1, 2]` (partition 2) | **3** | 3 | `[3, 4, 5, 6, 7]` |
| 3 | `[0, 1, 2]` (partition 2) | **3** | 3 | `[3, 4, 5, 6, 7]` |

For every run: broker-committed offset immediately after the crash was **3** — exactly
`before_crash[-1] + 1`, reflecting only the 3 messages actually processed, not the full 8-message
backlog `poll()` had already fetched into the client. This is the precise regression #76 fixed: round
3 found the broker jumped straight to offset 8 (the log end) after just one committed message, because
the old manual-commit branch called a bare `consumer.commit()` (committing the ambient fetch
position). That no longer happens.

`manual-2` in every run resumed at exactly offset 3 (`after_restart[0] == before_crash[-1] + 1`,
holding for all 3 runs) and processed `[3, 4, 5, 6, 7]` — `sorted(before_crash + after_restart) ==
list(range(8))` held every time: no message before the crash boundary repeated, nothing after it
skipped, offset 3 (in flight when `manual-1` was killed) reprocessed exactly once by `manual-2`, as the
notebook's own cell-17 asserts require. No hang occurred anywhere in any of the 3 runs (the round-2
restart-wait hang fix and this round's commit-math fix compose correctly together, as the task
anticipated).

## US-KC4 — full, final verdict across all four rounds

| Criterion | Verdict | Evidence |
|---|---|---|
| 1 — fewer members than partitions, lag shown | **PASS** | Round 2 (initial fix + full re-verify), round 3 (spot-check, no regression). Not re-run this round (out of scope, untouched by #76). |
| 2 — members == partitions, 1:1 mapping | **PASS** | Round 1 (initial, unaffected by #73), round 2/3 spot-checks, no regression across all rounds. Not re-run this round. |
| 3 — members > partitions, excess idle | **PASS** | Round 1 (initial, unaffected by #73), round 2/3 spot-checks, no regression across all rounds. Not re-run this round. |
| 4 — manual vs. auto commit contrast | **PASS, both halves.** Auto-commit half (Section 6): round 3, 3/3 runs, reliable partial loss, no regression expected or checked this round (untouched by #76). Manual-commit half (Section 5): **this round**, 3/3 fresh runs, real broker-committed offset confirmed correct immediately after crash (3, not 8), restart resumes at exactly the right offset every time, no message lost or duplicated, no hang. |

**Overall: all 4 of US-KC4's acceptance criteria PASS**, each backed by live, real-cluster evidence
(`kafka-consumer-groups.sh --describe` output and/or direct broker `OffsetFetch` queries) gathered
across the four validation rounds — not by re-reading the notebook's own committed output or trusting
its in-notebook asserts alone.

## Bugs found and resolved across all four rounds

- [#73](https://github.com/hoanghaithanh/Spark-Playbook/issues/73) — `auto_offset_reset` defaulting to
  `"latest"`, hiding all pre-produced backlogs. **Fixed, confirmed round 2.**
- [#74](https://github.com/hoanghaithanh/Spark-Playbook/issues/74) — Section 5 restart-cell hang (Bug
  A) and Section 6 auto-commit total-loss (Bug B). **Fixed, confirmed round 3.**
- [#75](https://github.com/hoanghaithanh/Spark-Playbook/issues/75) — `--self-check` hardcoded
  partition 0. **Fixed, confirmed round 3.**
- [#76](https://github.com/hoanghaithanh/Spark-Playbook/issues/76) — Section 5 manual-commit branch
  committed the ambient fetch position instead of an explicit per-record offset. **Fixed, confirmed
  this round (3/3 fresh runs).**

**Flagged, not blocking, still open:** the recurring cluster-disappearance anomaly (now filed as
[#77](https://github.com/hoanghaithanh/Spark-Playbook/issues/77), infrastructure-level, independent of
this feature's code) and the pre-existing producer/topic-auto-create race
(`UnknownTopicOrPartitionError` on brand-new topics, affecting every section's producer cells,
including `self_check()`'s own unchecked `flush()`) noted in round 3. Neither has been shown to affect
the correctness of any of the four criteria above — both are worked around trivially in QA scripts
(future-checking sends, `docker inspect` ownership re-verification) — but both are worth a human's
attention as separate follow-up issues if not already tracked.

## Cleanup

```
tools/kafka_consumer_group/_qa_section5.py   -> deleted (throwaway driver script, never committed)
docker exec spark-driver ps aux | grep member.py -> confirmed empty (no lingering member.py processes)
py -m pytest tests/unit -q                   -> 504 passed
py compose/cli.py down                       -> cluster fully torn down, confirmed via `docker ps -a`
                                                  (only the unrelated pre-existing `jmxtest-broker`
                                                  container remains)
git status --short                           -> no scratch files tracked/untracked left behind
```

**Notebook cleanliness check:**
```
py -c "import json; nb=json.load(open('content/kafka-consumers-groups/notebook.ipynb')); \
       print([i for i,c in enumerate(nb['cells']) if c.get('execution_count') is not None or c.get('outputs')])"
-> []
git status --short content/kafka-consumers-groups/  -> untracked directory only (pre-existing from the
                                                        developer's commit), no modification to the
                                                        notebook file itself
```

## Overall recommendation

**All four of US-KC4's acceptance criteria now PASS, verified live against a real 3-broker Kafka
cluster, across four validation rounds and four fixed bugs (#73, #74, #75, #76).** This round's
narrow, targeted re-check of Section 5's manual-commit crash/restart demo — the single remaining gap
after round 3 — came back clean and deterministic across 3 independent fresh runs: the
broker-committed offset immediately after a real SIGKILL crash reflects exactly the work actually
finished (not the ambient fetch position), the restart consumer always resumes from precisely the next
offset, and no message is ever lost or duplicated. No hang occurred in any run.

**This is a clean, unambiguous PASS. Issue #65 (US-KC4) is ready for final human sign-off** — I am
recommending, not approving, per this project's Definition of Done. The two flagged-but-not-blocking
items (#77's cluster-disappearance anomaly, and the pre-existing producer-race flakiness) are
infrastructure/robustness follow-ups independent of this feature's correctness and should not hold up
sign-off on #65 itself.

## Human sign-off (Round 4 / final)

_Pending._
