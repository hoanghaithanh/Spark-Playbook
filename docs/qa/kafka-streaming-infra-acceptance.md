# Conditional Kafka (KRaft) infra + synthetic producer — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-19, against `main` at commit `af8fb9e` (design `8afc625`, implementation
      `9188254`, review fixes `af8fb9e`) — issue #50, backlog row #19, Sprint 10.
Scope: `docs/architecture/kafka-streaming-infra.md` (ADR), US-3.1 / US-3.2 / US-3.3
      (`docs/requirements/spark-playbook-mvp.md` lines 167-186), verified against a real
      3-worker cluster spawned through the app's own routes and a real KRaft Kafka broker —
      not a code read-through.

## Scoping note (read first)

US-3.1/3.2/3.3 as written assume the Structured Streaming topic (issue #18) already exists.
It doesn't — #18 is explicitly out of scope for #50, per the ADR's own header ("Unblocks:
backlog row #18 ... out of scope here") and the implementation commit. This report therefore
splits each given/then into **in #50's scope** (tested live below) vs. **N/A, deferred to
#18** (the streaming query, live progress chart, watermark demo, and checkpoint-recovery
demo don't exist yet and were not fabricated to force a pass). No given/then is marked FAIL
for being out of scope — that would misrepresent #50 as broken when the missing piece is a
different, not-yet-started story.

## Method

**Unit suite**, re-run clean before this pass: `py -3.9 -m pytest tests/unit -q` →
**393 passed, 0 skipped**. (The task brief cited "393 passed, 2 skipped" from the developer's
own run; this pass found 0 skips in `tests/unit` — no `skipif`/`importorskip` markers exist
in that directory, and `kafka-python` is now installed on this host, which may be why an
earlier run elsewhere skipped a host-dependent case. Not investigated further: the count that
matters, 393 passed with no failures, matches and the discrepancy doesn't indicate a
regression.)

**Local image staleness caught and fixed before live testing.** The pre-existing
`sparkpb/spark:4.0.3` image on this host was built 2026-07-14 — before the Kafka Dockerfile
changes landed. The first in-cluster `produce.py --self-check` run failed with `kafka-python`
missing, even though `compose/Dockerfile.spark` correctly pins `kafka-python==2.0.2`. This is
**not a code defect** — `git log` confirms the Dockerfile line is committed and correct; the
locally cached image simply predated it. Fixed by running `bash compose/build.sh` (rebuild
from the current `Dockerfile.spark`) before continuing; the rebuilt image confirmed
`kafka-python` present and the self-check then passed. Flagging this because it's a real trap
for anyone else validating on a host with an older cached image — `compose/build.sh` is not
run automatically by the spawn path, only `docker build`'s own layer cache decides what's
current.

**Live cluster.** `docker ps -a` was empty before starting and after finishing. The FastAPI
app was started fresh (`py -3.9 -m uvicorn app.main:app --host 127.0.0.1 --port 8001`) and
every spawn/teardown went through the app's own routes (`POST /topics/{id}/spawn` and
`/teardown`) — never `compose/cli.py` or `docker compose` directly, so `include_kafka` was
threaded exactly as `spawn_cluster()` (`app/web/routes/topics.py`) does it in production:
`include_kafka=topic.requires_kafka`.

**Scratch manifest.** No real streaming topic exists yet (#18), so a temporary,
not-committed-to-`content/` scratch topic (`content/_qa-scratch-kafka/`, `requires_kafka:
true`, minimal `manifest.yaml` + `concept.md` + empty `notebook.ipynb`) was created solely to
drive `include_kafka` through the real spawn route — same spirit as backlog row #24's "second
app instance pointed at a scratch content dir" precedent for isolated verification without
real topic content. Deleted at the end of this pass (see Cleanup).

## US-3.1 — Kafka available conditionally for streaming topics

### Given/then #1 — non-streaming topic → no Kafka container (in scope, PASS)

Spawned `partitioning-shuffle` (an existing, real 14-topic manifest, `requires_kafka: false`)
via `POST /topics/partitioning-shuffle/spawn`. `docker ps` showed exactly the 5 expected
services (`spark-master`, `spark-worker-1/2/3`, `spark-driver`) and **no `spark-kafka`**:

```
NAMES            IMAGE                 STATUS
spark-driver     sparkpb/spark:4.0.3   Up 17 seconds
spark-worker-2   sparkpb/spark:4.0.3   Up 17 seconds
spark-worker-3   sparkpb/spark:4.0.3   Up 17 seconds
spark-worker-1   sparkpb/spark:4.0.3   Up 17 seconds
spark-master     sparkpb/spark:4.0.3   Up 17 seconds
```

**PASS.** Torn down cleanly afterward (`docker ps -a` empty).

### Given/then #2 — streaming topic → KRaft broker present, reachable from driver, within budget (in scope via scratch manifest, PASS)

Spawned the scratch `_qa-scratch-kafka` topic (`requires_kafka: true`) via
`POST /topics/_qa-scratch-kafka/spawn`. `docker ps` showed exactly **one** `spark-kafka`
container, image `apache/kafka:3.9.0`, and **no ZooKeeper container** — confirming KRaft mode:

```
NAMES            IMAGE                 STATUS          PORTS
spark-driver     sparkpb/spark:4.0.3   Up 21 seconds   127.0.0.1:4040-4042->4040-4042/tcp, 127.0.0.1:8888->8888/tcp
spark-kafka      apache/kafka:3.9.0    Up 22 seconds   127.0.0.1:9092->29092/tcp
spark-worker-2   sparkpb/spark:4.0.3   Up 22 seconds
spark-worker-3   sparkpb/spark:4.0.3   Up 22 seconds
spark-worker-1   sparkpb/spark:4.0.3   Up 22 seconds
spark-master     sparkpb/spark:4.0.3   Up 22 seconds
```

**Reachable from the driver** — `docker exec spark-driver python3 /workspace/tools/kafka_producer/produce.py --self-check --bootstrap kafka:9092`:

```
SELF-CHECK OK: 20/20 events accepted on kafka:9092 (topic='selfcheck-ace760b9').
```

**Within the resource budget, and the +2GB Kafka reservation is real, not just documented.**
Two spawn attempts through the real `validate()`/`spawn_cluster` path, at the same worker
config (`worker_count=4, worker_cores=2, worker_memory_gb=7` → `1(master) + 4×7(workers) +
2(driver) = 31GB`):
- Against the streaming (Kafka) topic: **rejected**, no containers spawned
  (`docker ps -a` stayed empty), with the exact ceiling error rendered in the drawer:
  `"requested config totals ~33GB, exceeding the 32GB sanity ceiling (PLAN.md §2
  resource-ceiling check)"` — `31 + KAFKA_MEMORY_GB(2) = 33`.
- Against a non-streaming topic (`partitioning-shuffle`) with the identical worker config:
  **accepted**, 4 workers spawned, no `spark-kafka` container, confirming the same 31GB total
  passes when Kafka's +2GB isn't added.

This directly and live-confirms both halves of the ceiling accounting (`app/lifecycle/renderer.py::validate()`'s `total_gb += config.KAFKA_MEMORY_GB if params.include_kafka`), not just
the arithmetic read off the ADR.

**PASS**, all three sub-claims (broker present, KRaft-only, reachable, in-budget accounting)
verified live.

## US-3.2 — Synthetic streaming producer

### Given/then #1 — publishes at ~requested rate until stopped (in scope, PASS)

Ran a real, timed in-cluster produce run: `docker exec spark-driver python3
/workspace/tools/kafka_producer/produce.py --bootstrap kafka:9092 --topic rate-timing-test3
--rate 50 --count 600`, wall-clock timed from the host:

```
Sent 600 events.
Elapsed: 13.19s -> observed rate: 45.5 ev/s (requested 50 ev/s)
```

~91% of the requested rate over a real ~13s run — matches US-3.2's "approximately the
requested rate" (the CLI's own docstring/D5 describes this as a simple sleep-per-batch loop,
approximate by design, not hard-real-time).

**Independently confirmed the count landed in Kafka**, not just the CLI's own stdout claim —
read topic offsets from the host via `kafka-python` against the loopback listener
(`127.0.0.1:9092`):

```
beginning: {p0: 0, p1: 0, p2: 0}
end:       {p0: 154, p1: 165, p2: 281}
total messages in topic: 600
```

Exactly 600, matching "Sent 600 events." and confirming both the rate mechanism and that
messages actually reach the broker (not silently dropped on send).

**Host-shell path (OQ-1) also verified end-to-end**, per the human's explicit ask — installed
nothing new (this host already had `kafka-python` 3.0.8), ran `produce.py` directly from a
host shell against the loopback publish:

```
py -3.9 tools/kafka_producer/produce.py --bootstrap 127.0.0.1:9092 --topic host-shell-test --rate 40 --count 500
Publishing to 127.0.0.1:9092 topic='host-shell-test' rate=40.0/s partitions=3 late_frac=0.05 (500 events)...
Sent 500 events.
```

and `--self-check` from the host shell:

```
py -3.9 tools/kafka_producer/produce.py --bootstrap 127.0.0.1:9092 --self-check
SELF-CHECK OK: 20/20 events accepted on 127.0.0.1:9092 (topic='selfcheck-3ffbe4fd').
```

Both the in-cluster (`kafka:9092`, container DNS) and host-shell (`127.0.0.1:9092`, loopback
publish) bootstrap paths work live, confirming D3's dual-listener design and D5's
"`127.0.0.1`, not `localhost`" deviation note hold in practice, not just in the ADR's reasoning.
(Host client emitted `kafka-python` `DeprecationWarning`s about the serializer interface —
cosmetic, from the newer `kafka-python` 3.0.8 on this host vs. the image's pinned 2.0.2; did
not affect correctness, not a #50 defect.)

**PASS.**

### Given/then #2 — checkpoint recovery across a stopped/restarted streaming job (N/A, deferred to #18)

Requires an actual Structured Streaming query reading from Kafka and writing to a checkpoint
directory. No such query or notebook exists yet (#18). Not fabricated to force a pass. **N/A
to #50** — the producer infra this criterion depends on (a live broker, a working `produce.py`)
is in place and verified above; the streaming query itself is #18's scope.

## US-3.3 — Structured Streaming topic (all three given/thens: N/A, deferred to #18)

All three given/thens require a Structured Streaming query, a notebook, and a live
query-progress UI element that don't exist yet:
- Live query-progress chart sourced from `query.lastProgress`/`recentProgress` — no such UI
  exists; #18's scope.
- Watermark/late-data demonstration — `produce.py`'s `--late-frac`/`--late-seconds` knobs
  exist and are ready for #18 to use (not exercised here since there's no aggregation query
  to demonstrate dropping against), but the demonstration itself is #18's scope.
- Checkpoint recovery per Kafka + Structured Streaming semantics — same as US-3.2's second
  given/then above; #18's scope.

**N/A to #50, deferred to #18** for all three — consistent with the ADR's explicit scope
boundary and this repo's "flag, don't guess/force" convention.

## Coverage review

No unit-test gaps found requiring new tests for #50 itself. `tests/unit` covers the
`include_kafka` threading (`ClusterParams`, `validate()` ceiling math, template rendering) and
`produce.py`'s validation/self-check logic per the developer's own report; this pass's job was
live behavior, not code-path coverage, and no gap was found there that a unit test could catch
better than the live evidence above.

## Defects found

**None.** The one issue hit during this pass (stale local Docker image missing
`kafka-python`) was **not a code defect** — confirmed via `git log`/`git show` that
`compose/Dockerfile.spark` correctly bakes `kafka-python==2.0.2`; the local image cache
predated that commit. Rebuilding via `compose/build.sh` resolved it, and the rebuilt image is
what all subsequent evidence above was gathered against. Flagged in Method for anyone else's
awareness, not filed as a GitHub issue since there is no code to fix.

## Cleanup confirmation

- `docker ps -a` returned empty after every teardown in this pass, and empty at the end —
  confirmed after each of the three spawn/teardown cycles (`partitioning-shuffle`,
  `_qa-scratch-kafka` twice: once before the image rebuild, once after) and once more at the
  very end.
- The scratch topic folder `content/_qa-scratch-kafka/` (created solely for this pass) was
  deleted (`rm -rf`).
- The `uvicorn` process started for this pass (PID confirmed via `netstat`) was killed; port
  8001 confirmed free afterward.
- `git status` shows a clean working tree (`nothing to commit, working tree clean`) before
  adding this report — no stray scratch files, no modified tracked files.
- No notebook was executed during this pass (no streaming notebook exists yet), so the
  notebook-cleanliness convention (CLAUDE.md) doesn't apply here.

## Recommendation

This is a **recommendation, not final sign-off** — the human should review and give final
sign-off before marking issue #50 done.

- US-3.1: **PASS**, both given/thens, live evidence against a real cluster.
- US-3.2: given/then #1 **PASS** (live, both in-cluster and host-shell paths, message count
  independently verified); given/then #2 **N/A, deferred to #18** (no regression — the
  dependency itself works, the consumer of it doesn't exist yet).
- US-3.3: all three given/thens **N/A, deferred to #18** by design (ADR's explicit scope
  boundary).
- No defects filed. No blockers found in #50's actual scope.
