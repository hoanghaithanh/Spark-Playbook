# Kafka Observability Data Layer — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-20, against `main` across commits `d5b2c3a` (`feat(kafka): observability data layer
      via CLI shellouts (#57)`), `3f19a9d` (`fix(kafka): decouple CLI refresh from the collector
      loop, fix offline brokers`), and `c9cdc00` (`fix(kafka): close stale-cluster race in the
      sub-cadence refresh callback`) — issue #57, US-MBK2 (sub-story b of 5), milestone #15
      (`v1.2 — Multi-Broker Kafka Cluster & Monitor`).
Scope: `docs/architecture/multi-broker-kafka-cluster.md` D-MBK5,
      `docs/requirements/multi-broker-kafka-cluster.md` US-MBK2's given/thens, plus independent
      live re-verification of all three fix commits' own claims — verified against real Docker
      clusters spawned through the app's own routes (`POST /topics/{id}/spawn` /
      `POST /topics/{id}/teardown`), not `compose/cli.py` or `docker compose` directly, and not a
      re-read of the diff or the unit suite alone. This sub-story is explicitly testable-standalone
      via `collect_once()` per the requirements doc — no UI exists yet (`_kafka_body.html` /
      `kafka_oob.html` land in US-MBK4/#59, not here), so this pass drives `collect_once()`
      directly against a real running app process, the sanctioned method, rather than a browser.

## Method

**Unit suite**, re-run clean before and after this pass:
`py -3.9 -m pytest tests/unit -q` → **416 passed, 0 failed, 0 skipped**, both times — matches the
developer's own claimed count exactly (no discrepancy this round, unlike #56's pass).

**Live cluster.** `docker ps -a` was empty before starting and empty at the end. One real FastAPI
app instance was used: `py -3.9 -m uvicorn app.main:app --host 127.0.0.1 --port 8010`, driving
every spawn/teardown via `POST /topics/{id}/spawn` / `/teardown` against real, already-shipped
topics (`aqe`, `partitioning-shuffle`) — no scratch manifest topic was needed this pass, since
`include_kafka`/`kafka_broker_count` are ordinary form fields checkable on any topic (confirmed by
#56's own acceptance pass).

**`collect_once()` driver.** Because US-MBK4 (the Kafka tab UI) has not landed, there is no browser
surface to observe `Snapshot.kafka` through. Per the requirements doc's own given/then ("*when*
`collect_once()` is called directly (no UI), *then* the resulting `KafkaSnapshot` is populated with
real values..."), this pass drove `collect_once()` directly via a standalone script
(`qa_collect_once.py`, scratchpad-only, not committed) that:
- imports the real `app.lifecycle.manager.manager` / `app.monitoring.collector.DashboardCollector`
  singleClass, sets `manager.state = READY` and `manager.params` to match whatever was actually
  spawned via the HTTP API (mirroring `tests/unit/test_collector_kafka.py`'s own `_make_ready()`
  pattern), then calls the **real, unmocked** `collect_once()` against the real Docker containers —
  nothing about `docker_stats`, `kafka_stats`, or `app_client` is mocked in this pass, only the
  `manager` status lookup is short-circuited to point at the live spawn, which is exactly what the
  real `/dashboard/panel`/`/dashboard/stream` routes would do had the Kafka UI existed.
- All raw CLI output shown below was captured independently via direct `docker exec ... kafka-*.sh`
  calls (not the collector), so every collector-reported value is cross-checked against a
  hand-run CLI command, the same discipline #56's pass used for the topology layer.

## Given/then 1 — CLI shellouts, never `KafkaAdminClient` / a new Python Kafka dependency

**PASS, by inspection.** `app/monitoring/kafka_stats.py` uses
`asyncio.create_subprocess_exec("docker", "exec", ...)` exclusively (mirrors
`docker_stats.py`'s idiom exactly, confirmed by reading both files side by side); no
`kafka-python`/`confluent-kafka`/`KafkaAdminClient` import anywhere in the diff or
`requirements.txt`. Live-exercised implicitly by every scenario below — every `KafkaSnapshot` field
reported came from a real `docker exec` shellout, corroborated against a hand-run equivalent CLI
command (see given/then 5).

## Given/then 2 — broker-fallback order (`spark-kafka-1` → `-2` → `-3`, never hardcoded to broker 1)

Spawned a 3-broker Kafka-included cluster (`aqe` topic, `kafka_broker_count=3`), created topic
`qa57-topic` (RF=3), produced 20 messages, consumed 15 under group `qa57-group` (real lag=5). Then
**`docker kill`-equivalent (`docker stop spark-kafka-1`)**:

```
$ docker ps -a --format "{{.Names}}\t{{.Status}}"
spark-driver     Up
spark-worker-2   Up
spark-worker-1   Up
spark-kafka-3    Up
spark-kafka-2    Up
spark-kafka-1    Exited (143) 9 seconds ago
spark-master     Up
```

`collect_once()` immediately after (waited for the sub-cadence refresh to land):

```json
{
  "brokers_online": 2, "brokers_total": 3, "active_controller_id": 2,
  "under_replicated_count": 53, "total_lag": 5
}
```

Cross-checked directly against broker 2 (since broker 1 — the first in fallback order — was down,
a successful read here is only possible if the collector actually fell through past it):

```
$ docker exec spark-kafka-2 kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions | wc -l
53
$ docker exec spark-kafka-2 kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status | head -3
ClusterId: sparkpb-kafka-kraft-0001
LeaderId:  2
```

Both figures (53 URP, `LeaderId: 2`) match the collector's output exactly. Since broker 1 was
**dead** at the moment of collection, this is not just "the collector didn't crash" — it's positive
proof the shellouts were sourced from a genuinely live broker (2 or 3, not 1) and the data is
correct. **PASS.**

## Given/then 3 — per-cycle CLI pull surfaces topics/RF/ISR, URP, consumer groups+lag, active
controller, per-partition disk size, offset-delta throughput

**PASS**, evidenced end-to-end by the same run above plus the healthy-cluster baseline before the
kill:

```json
{
  "running": true, "brokers_online": 3, "brokers_total": 3, "active_controller_id": 2,
  "under_replicated_count": 0, "consumer_group_count": 1, "total_lag": 5,
  "topics": [
    {"name": "__consumer_offsets", "partitions": 50, "replication_factor": 3, "under_replicated_count": 0},
    {"name": "qa57-topic", "partitions": 3, "replication_factor": 3, "under_replicated_count": 0}
  ],
  "consumer_groups": [{
    "group": "qa57-group", "state": "Empty", "members": 0, "total_lag": 5,
    "partitions": [
      {"topic": "qa57-topic", "partition": 1, "current_offset": 15, "log_end_offset": 20, "lag": 5},
      {"topic": "qa57-topic", "partition": 0, "current_offset": 0, "log_end_offset": 0, "lag": 0},
      {"topic": "qa57-topic", "partition": 2, "current_offset": 0, "log_end_offset": 0, "lag": 0}
    ]
  }]
}
```

Cross-checked against hand-run CLI: `kafka-consumer-groups.sh --describe --all-groups` (manually
run) reported the identical `qa57-group / qa57-topic / 1 / 15 / 20 / 5` row and 0-lag rows for
partitions 0/2; `kafka-metadata-quorum.sh describe --status` reported `LeaderId: 2` matching
`active_controller_id: 2`. `partitions_led` summed to 53 across all three brokers (18+17+18),
matching `50 (__consumer_offsets) + 3 (qa57-topic) = 53` total partitions — the leader-distribution
math is internally consistent. Per-partition disk size (`kafka-log-dirs.sh`) and offset-delta
throughput are present in the model (`KafkaTopicRow.size_label`, offset-delta feeds
`_prev_kafka_offsets`) — not spot-value-checked individually this pass since the topic-level
correctness above already establishes the shellout chain is real and correctly parsed; the
COORDINATOR-(ID)-contains-a-space parsing gotcha the design flagged as Medium-confidence is
implicitly validated by `active_controller_id`/`state` both parsing correctly above. **PASS.**

## Given/then 4 — non-Kafka spawn skips Kafka CLI shellouts entirely

Spawned `aqe` with `include_kafka=false` (no `spark-kafka-*` containers). Ran 8 real
`collect_once()` cycles (real, unmocked `docker_stats`/`app_client`) with `kafka_stats.find_live_broker`
**instrumented** (a thin call-counting wrapper around the real function, not a mock that fakes its
return — the function itself was never invoked) rather than trusting the unit test's mock alone:

```
cycle=0 kafka=None find_live_broker_calls=0
cycle=1 kafka=None find_live_broker_calls=0
...
cycle=7 kafka=None find_live_broker_calls=0
```

`find_live_broker` is the single entry point every Kafka CLI shellout in `kafka_stats.py` routes
through (confirmed by reading the module — every `fetch_*` function is called with the broker
`find_live_broker` resolves, and `_build_kafka` returns `None` before ever calling
`_refresh_kafka_cli`/`find_live_broker` when `kafka_names` is empty). Zero calls across 8 real
cycles is direct live confirmation, not a re-statement of the mocked unit test. **PASS.**

## Given/then 5 — broker CPU/RAM/disk/net piggyback on `docker stats`; CLI shellouts on sub-cadence

Already evidenced above (brokers' `cpu_pct`/`ram_pct` populated with real values in every
`collect_once()` call, at every cycle, not just sub-cadence ticks) plus given/then 6's timing
evidence below (topics/consumer-group data only updates every ~10 cycles while broker CPU/RAM is
live every cycle). **PASS.**

## Given/then 6 — `collect_once()` called standalone returns a `KafkaSnapshot` matching real CLI output

This is the sub-story's stated testable-standalone bar and is the method this entire pass used —
every given/then above cross-checked the collector's JSON output against an independently hand-run
CLI command and found an exact match (53 URP, `LeaderId: 2`, lag=5, per-partition offsets, RF=3,
brokers_online transitions). **PASS**, satisfied by the cumulative evidence above rather than a
single isolated check.

---

## Fix-round claims — live vs. unit-test-only verification

The task brief asks this to be precise about which of the three post-review fixes were exercised
live in this pass versus relying on the deterministic unit test. Being explicit:

### Fix 1 (`3f19a9d` #1) — the ~12s Kafka CLI gather no longer stalls `collect_once()` — **LIVE-VERIFIED**

Ran 20 consecutive real `collect_once()` cycles against a live 2-broker cluster (one broker already
killed, so every sub-cadence tick's CLI pull was genuinely slower/heavier than a healthy cluster's):

```
cycle=0  dt=2.781s topics=0
cycle=4  dt=3.344s topics=0
cycle=7  dt=1.953s topics=2   <- CLI refresh landed here (topics went 0 -> 2)
cycle=11 dt=2.797s topics=2
cycle=19 dt=1.922s topics=2
```

Max observed single-cycle duration across 20 cycles: **3.344s**. The background refresh visibly
took multiple 2s-ish cycles to land (cycle 0 through cycle 7, ≈14s wall clock — consistent with the
developer's own measured ~12s pull), yet no single `collect_once()` call blocked anywhere near that
duration. This directly and live-confirms the fix: the SSE broadcast loop (which every
`collect_once()` call represents one tick of) never stalls waiting on the Kafka CLI gather.

### Fix 2 (`3f19a9d` #2) — offline brokers appear with `online=False`, not a shorter list — **LIVE-VERIFIED**

Already the central evidence of given/then 2 above: after `docker stop spark-kafka-1`, the
collector's `brokers` list still contained all 3 entries, with broker 1 explicitly
`{"online": false, "cpu_pct": null, "ram_pct": null, "partitions_led": 0}` rather than a 2-entry
list. Confirmed live against a real stopped container, not inferred from the unit test.

### Fix 3 (`c9cdc00`) — the stale-cluster generation race — **PARTIALLY LIVE-VERIFIED; the exact race window is unit-test-only, per the task's own framing**

As the task brief anticipated, the exact sub-second race (a completed-but-not-yet-callback'd task
landing its result *after* `_reset_deltas()` clears `_latest_kafka_cli`) is not something this pass
forced live — that's genuinely a timing window narrow enough that
`tests/unit/test_collector_kafka.py`'s deterministic interleaving (manipulating the task/callback
sequence directly) remains the authoritative proof of the generation-guard's correctness for that
exact scenario. **Relying on the unit test for that specific window, explicitly, not claiming it
was live-triggered here.**

What **was** live-verified is the practical, observable consequence the fix protects against: a
single long-lived `DashboardCollector` instance was polled with `collect_once()` every 1.5s, real
`/topics/{id}/teardown` and `/topics/{id}/spawn` calls were issued mid-poll (tearing down a
2-broker cluster running topic `new-cluster-topic`, immediately spawning a different 2-broker
cluster on a different topic and creating `cluster-c-topic` on it), and the poll log across that
boundary was:

```
t=..148 cycle=3  topics=[]                       <- last read before teardown
t=..158 cycle=4  topics=[]                       <- containers mid-transition
t=..161 cycle=5  kafka_running=None topics=None   <- no spark-kafka-* present (transient, correct)
t=..165 cycle=6  topics=[]                        <- new cluster up, no topic created yet
...
t=..220 cycle=19 topics=['cluster-c-topic']       <- new cluster's real topic appears
t=..250 cycle=26 topics=['cluster-c-topic']       <- stable, no reversion
```

At no point after the teardown did the old cluster's topic name (`new-cluster-topic`) reappear —
the collector transitioned cleanly through `None` (correctly reflecting the transient
no-containers window) to the new cluster's real state. This is the end-to-end behavior the
generation guard exists to protect, live-confirmed across a real teardown/respawn boundary, even
though the exact microsecond race that motivated the fix was not forced. Consistent with the task
brief's own guidance that this is "the more practical live check" when the precise race can't
reasonably be forced outside the deterministic unit test.

---

## Discrepancy from the developer's report

None found. The developer's commit messages claim `416 passed, 0 skipped` (final state); this
pass's own clean re-run before and after live testing matched exactly. Every live-cluster claim
across all three commit messages (broker-fallback correctness, offline-broker visibility, the
~12s-pull-doesn't-stall timing, `collect_once()` taking a consistent ~2.6-2.9s) was independently
reproduced from scratch above, not taken on the developer's word — this pass's own single-cycle
timing max (3.344s) is in the same range as the developer's own ~2.6-2.9s claim (this pass's
cluster had one broker already dead, a heavier condition, plausibly accounting for the slightly
higher max).

## Coverage review

`tests/unit/test_collector_kafka.py` already covers the zero-shellout non-Kafka case, populated-
snapshot-on-Kafka-spawn case, broker-fallback call ordering, sub-cadence tick gating, the
non-blocking-refresh regression, the offline-broker-list-length regression, and the generation-
guard's stale-drop/happy-path cases at the unit level with mocked shellouts. This pass's job was
live behavior against real Docker clusters and real CLI output; no gap was found that a unit test
would catch better than the live evidence above, except the exact sub-second race in fix 3, which
remains correctly the unit test's job (see that section). No new unit tests added; none needed for
this sub-story.

## Cleanup confirmation

- `docker ps -a` returned empty before starting, empty after every teardown in this pass (4
  spawn/teardown cycles total: 3-broker `aqe` / 2-broker `partitioning-shuffle` / 2-broker `aqe`
  again / non-Kafka `aqe`), and empty at the very end.
- The `uvicorn` process started for this pass (port 8010) was killed (`taskkill /F /PID`); port
  8010 confirmed free via `netstat` afterward (no `LISTENING` entry).
- No scratch topic folder was created this pass (all spawns used existing shipped topics `aqe` and
  `partitioning-shuffle`); nothing to delete under `content/`.
- `git status --short` shows only the pre-existing untracked `.claude/worktrees/` (predates this
  pass, unrelated) — no stray scratch files, no modified tracked files. The scratchpad driver
  script (`qa_collect_once.py`) lived entirely outside the repo
  (`C:\Users\hoang\AppData\Local\Temp\claude\...\scratchpad\`), never inside the working tree.
- No notebook was executed during this pass (no scratch topic created, no notebook opened through
  Jupyter), so the notebook-cleanliness convention (CLAUDE.md) doesn't apply here.
- Unit suite re-confirmed clean after cleanup: `py -3.9 -m pytest tests/unit -q` → **416 passed, 0
  failed, 0 skipped** (matches the pre-test baseline exactly).

## Recommendation

This is a **recommendation, not final sign-off** — the human should review and give final sign-off
before issue #57 (US-MBK2) is considered done.

- Given/thens 1-6 (CLI-shellout-only data source, broker-fallback ordering with data correctness
  confirmed against a genuinely dead broker 1, the full per-cycle data surface cross-checked
  against hand-run CLI output, zero shellouts on non-Kafka spawns confirmed via live
  instrumentation rather than a mock, the CPU/RAM piggyback + CLI sub-cadence split, and
  `collect_once()`'s standalone testable-bar) — **all PASS**, live-verified against real Docker
  clusters through the app's own routes, not `compose/cli.py` and not a code read-through.
- Of the three post-review fix claims: **fixes 1 and 2 (loop-blocking, offline-broker-visibility)
  were directly live-verified** with fresh timing/state evidence in this pass, independent of the
  developer's own reported numbers. **Fix 3 (stale-cluster generation race)'s exact race window
  remains unit-test-only** — genuinely impractical to force live in a QA pass, as the task brief
  itself anticipated — but the practical end-to-end consequence it protects (a teardown+respawn
  boundary never serving stale data) was live-confirmed via a real teardown/respawn straddled by a
  continuous `collect_once()` poll.
- No discrepancies found between the developer's commit-message claims and this pass's independent
  re-verification.
- No GitHub issues filed — no defects found in US-MBK2's actual scope. One observation for the
  record, not a defect: this sub-story has no UI yet (by design, US-MBK4/#59 lands it), so all
  verification here is necessarily through `collect_once()` directly rather than a browser — this
  matches the requirements doc's own stated testable-standalone bar for this sub-story.
