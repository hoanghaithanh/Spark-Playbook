# JMX Exporter — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-20, against the working tree at
      `.claude/worktrees/issue-58-jmx-exporter` (branch `worktree-issue-58-jmx-exporter`,
      uncommitted feature diff) — issue #58, US-MBK3 (sub-story c of 5), milestone #15
      (`v1.2 — Multi-Broker Kafka Cluster & Monitor`).
Scope: `docs/architecture/multi-broker-kafka-cluster.md` D-MBK6,
      `docs/requirements/multi-broker-kafka-cluster.md` US-MBK3's given/thens — verified against a
      real Docker cluster built and spawned through the app's own routes
      (`compose/build.sh` then `POST /topics/{id}/spawn`), not `compose/cli.py` or `docker compose`
      directly, and not a re-read of the diff or the unit suite alone. This sub-story is additive to
      US-MBK2's already-accepted data layer (`docs/qa/kafka-observability-layer-acceptance.md`) — this
      pass focuses on what's new here: the baked exporter image, the `KAFKA_OPTS` wiring (and its
      `command:`-vs-`environment:` fix), the scrape, and the resulting real JVM-level numbers.

## Method

**Unit suite**, re-run clean before and after this pass: `py -3.9 -m pytest tests/unit -q` →
**443 passed, 0 failed, 0 skipped**, both times — matches the task brief's stated baseline (443
passed, 2 skipped claimed elsewhere did not reproduce here; no skip marker found in the repo, the
same "skip count doesn't reproduce, not a defect" pattern the two sibling acceptance reports already
flagged for #56/#57).

**Image build.** `bash compose/build.sh` from a clean state built both `sparkpb/spark:4.0.3` (cached,
no Dockerfile.spark changes) and `sparkpb/kafka:3.9.0` (new, from `compose/Dockerfile.kafka`) —
confirmed via the build log showing `Built sparkpb/kafka:3.9.0` and the jar `ADD` step running against
Maven Central.

**Live cluster.** `docker ps -a` was empty before starting and empty at the end. One real FastAPI app
instance: `py -3.9 -m uvicorn app.main:app --host 127.0.0.1 --port 8020`, driving spawn/teardown via
`POST /topics/aqe/spawn` / `/teardown` (the shipped `aqe` topic, `include_kafka=true&
kafka_broker_count=3`) — not `compose/cli.py`, not raw `docker compose`.

**`collect_once()` driver.** Same technique the sibling #57 acceptance pass used and the requirements
doc's own testable-standalone bar: a scratch script (`qa_collect_once_jmx.py`, scratchpad-only, not
committed) imports the real `app.lifecycle.manager.manager` / `app.monitoring.collector.
DashboardCollector`, sets `manager.state = READY` / `manager.params` to match the live spawn, then
calls the real, unmocked `collect_once()` repeatedly (8 cycles, ≈2.2s apart, to cross the
`KAFKA_COLLECTOR_SUBCADENCE_CYCLES=5` sub-cadence gate) against the real Docker containers — nothing
mocked. All raw JMX/CLI output shown below was independently captured via direct `docker exec`
(`wget`/`kafka-*.sh`), cross-checked against the collector's own output.

## Given/then 1 — broker image bakes the exporter at build time, `-javaagent` via `KAFKA_OPTS`, loopback-only, no new host port

**PASS, live.** `docker build -f compose/Dockerfile.kafka` succeeded from a clean cache
(`FROM apache/kafka:3.9.0`, `ADD .../jmx_prometheus_javaagent-1.0.1.jar`, no runtime fetch). On the
live 3-broker spawn:

```
$ docker ps -a --format "table {{.Names}}\t{{.Image}}\t{{.Ports}}"
spark-kafka-1    sparkpb/kafka:3.9.0   127.0.0.1:9092->29092/tcp
spark-kafka-2    sparkpb/kafka:3.9.0   127.0.0.1:9192->29092/tcp
spark-kafka-3    sparkpb/kafka:3.9.0   127.0.0.1:9292->29092/tcp

$ docker port spark-kafka-1
29092/tcp -> 127.0.0.1:9092
```

No `7071` entry anywhere in `docker port`/`docker ps` — confirmed no new host-published port.
`docker exec spark-kafka-1 sh -c "ps aux | grep java"` showed the actual running JVM command line
includes `-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=127.0.0.1:7071:/opt/jmx/kafka-metrics.yml`
(loopback-bound, per the agent-arg's `host:port:configFile` form) alongside the normal
`kafka.Kafka /opt/kafka/config/server.properties` broker startup — the agent is genuinely attached to
the running broker process, not a decorative env var. The scrape itself succeeds in-container:

```
$ docker exec spark-kafka-1 wget -qO- http://127.0.0.1:7071/metrics | head -4
# HELP java_lang_memory_heapmemoryusage_committed ...
java_lang_memory_heapmemoryusage_committed 5.18979584E8
```

**PASS.**

## Given/then 2 — `kafka_stats.py` scrapes via `docker exec ... wget -s localhost:<port>/metrics`, feeds the same `KafkaSnapshot`, same CLI-shellout idiom

**PASS, live via real `collect_once()`.** `app/monitoring/kafka_stats.py::fetch_jmx_metrics` shells
out via `asyncio.create_subprocess_exec("docker", "exec", container, "wget", "-qO-", ...)` — the same
idiom as every other function in the module (confirmed by reading), scraping `wget` per
`compose/Dockerfile.kafka`'s own verified-live finding that the base image has no `curl`.

Live-spawned 3 brokers, created a real topic, drove real traffic (see given/then 3), then polled
`collect_once()` 8 times (real Docker, real subprocess execs, nothing mocked):

```
cycle=0 running=True brokers_online=3 jmx_available=False p99=—
cycle=1 running=True brokers_online=3 jmx_available=False p99=—
cycle=2 running=True brokers_online=3 jmx_available=False p99=—
cycle=3 running=True brokers_online=3 jmx_available=True  p99=506.0ms   <- sub-cadence tick landed
cycle=4..7                             jmx_available=True  p99=506.0ms   <- reused between refreshes
```

`jmx_available` is honestly `False`/`—` for the first 3 cycles (before the sub-cadence tick fires),
then flips to real data and stays populated — exactly D-MBK7's "never fabricated, honest `—` until
populated" posture, confirmed live rather than assumed from the code read. Every broker's
`heap_pct`/`heap_label`/`produce_p99_label`/`fetch_p99_label`/`rh_idle_label` in the final snapshot
(see given/then 3) is non-`None`/non-dash — **PASS**, real values from a live spawn, not a mocked unit
test.

## Given/then 3 — real numbers from real traffic, cross-checked against `docker stats`/JVM reality

Created `jmx-qa-topic` (RF=3, 3 partitions) on the live 3-broker cluster, produced 500 messages with
`acks=all` via `kafka-console-producer.sh`, consumed all 500 back via `kafka-console-consumer.sh` (both
succeeded, `Processed a total of 500 messages`) — genuine, non-trivial produce/fetch traffic, not an
idle cluster.

**Collector's final `KafkaSnapshot` (real `collect_once()` output, not asserted/hand-written):**

```json
{
  "p99_latency_label": "506.0ms",
  "jmx_available": true,
  "brokers": [
    {"node_id": 1, "heap_pct": 52, "heap_label": "52%",
     "produce_p99_label": "19.0ms", "fetch_p99_label": "4.0ms", "rh_idle_label": "100%"},
    {"node_id": 2, "heap_pct": 52, "heap_label": "52%",
     "produce_p99_label": "0.0ms", "fetch_p99_label": "506.0ms", "rh_idle_label": "100%"},
    {"node_id": 3, "heap_pct": 52, "heap_label": "52%",
     "produce_p99_label": "0.0ms", "fetch_p99_label": "506.0ms", "rh_idle_label": "100%"}
  ]
}
```

**Cross-checked against independently hand-run `docker exec ... wget .../metrics` on all three
brokers** (captured separately from the collector, after the same traffic): broker 1's raw
`java_lang_memory_heapmemoryusage_used`/`_max` = `2.57330456E8 / 5.18979584E8` = 49.6% (collector's
`52%` is from a slightly later poll a few seconds on — heap naturally drifted, same order of
magnitude, not a mismatch); broker 1's raw `99thpercentile{request="Produce"}` = `19.0`,
`{request="FetchConsumer"}` = `4.0` — **exact match** to the collector's `produce_p99_label`/
`fetch_p99_label` for broker 1. Brokers 2/3 raw `FetchConsumer` 99th percentile = `506.0` — exact
match to the collector's `fetch_p99_label`/cluster-wide `p99_latency_label`. The non-trivial 506ms
figure (vs. broker 1's fast 4ms) is real and traffic-driven — the console-consumer's `--max-messages
500` pull against `jmx-qa-topic`'s partitions led by brokers 2/3 registered as slower fetches than
broker 1's, plausible for a `docker exec`-driven CLI consumer against a lightly-resourced 1-core
broker, not fabricated.

**Sanity check against `docker stats`** (independently captured): `spark-kafka-1` container memory
399.5MiB/2GiB (≈20% of the *container* limit — matches the collector's own `ram_pct: 20` field, a
different metric than JVM heap%). The JVM heap% (52%) is against the JVM's own `-Xmx512m` bound
(`java_lang_memory_heapmemoryusage_max` = 518979584 bytes ≈ 495MiB, matching `KAFKA_HEAP_OPTS:
-Xmx512m -Xms512m` from the template) — a *different, smaller* ceiling than the 2GB container limit,
so a higher heap% than container-RAM% is expected and correct, not a bug. Both figures are plausible:
neither negative nor exceeding 100%, and internally consistent with the configured heap bound.

`rh_idle_label: "100%"` on all three brokers is the documented clamp
(`kafka_stats.py::parse_jmx_metrics`'s own comment): the raw `OneMinuteRate` meter sums idle time
across all `num.io.threads` (8 threads), so it reads well above `1.0` even near-idle (raw values
observed: `1.97`, `1.97`, `1.97` on the three brokers, `*100` clamped to `100%` rather than a
misleading `197%`) — confirmed live to match the code comment's own prior finding exactly, not a
one-off. **PASS** — real numbers, not fabricated, cross-checked against independently hand-run CLI
output and plausible against `docker stats`.

## Given/then 4 — sub-story (b)'s CLI-shellout observability layer still works alongside the JMX scrape (the `command:` vs `environment:` fix)

This is the load-bearing regression the template's own DEVIATION comment calls out: if `KAFKA_OPTS`
had been wired as a plain `environment:` entry (the ADR's literal draft) rather than scoped into the
`command:` override, every `docker exec ... kafka-*.sh` shellout would inherit it, launch its own JVM
trying to rebind the same loopback agent port, and crash with `BindException` before running the
actual command — silently breaking US-MBK2's entire CLI layer the moment JMX landed.

**Live-verified this does NOT happen:**

```
$ docker exec spark-kafka-1 sh -c 'echo "KAFKA_OPTS=[$KAFKA_OPTS]"'
KAFKA_OPTS=[]                                          # confirms docker exec's env is clean

$ docker exec spark-kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
kafka-2:9092 (id: 2 rack: null) -> (
	Produce(0): 0 to 11 [usable: 11], ...                # succeeds, no BindException stack trace

$ docker exec spark-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic jmx-qa-topic --partitions 3 --replication-factor 3
Created topic jmx-qa-topic.

$ docker exec spark-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic jmx-qa-topic
Topic: jmx-qa-topic  PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,...
        Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1  ...
```

And end-to-end through the actual collector (not raw CLI): the same `collect_once()` run in given/then
2/3 populated `topics` (`jmx-qa-topic`, PartitionCount 3, ReplicationFactor 3, `__consumer_offsets`)
correctly from the CLI layer *simultaneously* with the JMX fields being populated — proving both
sub-cadence pulls (CLI admin-plane + per-broker JMX scrape, run via a shared `asyncio.gather`) coexist
without one breaking the other on the same live cluster. **PASS** — the fix holds end-to-end, not just
via the unit test's YAML-parsing check.

## Given/then 5 — no host port 7071 published

Already evidenced in given/then 1: `docker ps -a`/`docker port spark-kafka-{1,2,3}` show only
`9092`/`9192`/`9292`→`29092` per broker; no `7071` entry on any broker, on any host interface.
**PASS.**

## Coverage review

`tests/unit/test_kafka_stats_jmx.py` (150 lines, new) covers `parse_prometheus_text`/
`parse_jmx_metrics` at the unit level: heap%, produce/fetch p99 extraction, the `OneMinuteRate`→
`MeanRate` fallback, the >100%-idle clamp, and the honest-`None`-on-missing-series case — each backed
by a captured real sample string per the module's own `demo_parse_jmx_metrics()` self-check (also
exercised by `demo()`/`py -3.9 -m app.monitoring.kafka_stats` convention). `tests/unit/
test_collector_kafka.py` gained 121 lines covering `_kafka_jmx` state threading, the per-broker
population logic, and `jmx_available`/`p99_latency_label` health-strip aggregation with mocked
shellouts. This pass's job was live behavior against a real Docker cluster and real JVM metrics; no
gap was found that a unit test would catch better than the live evidence above — the two
Medium-confidence MBean details the ADR flagged (`FetchConsumer` tag, `OneMinuteRate` vs `MeanRate`)
are both independently confirmed live in given/then 3 to match the code's own comments exactly. No new
unit tests added by this pass; none needed for this sub-story.

## Live-only findings

None. Both risks the ADR flagged as "verify live" (OQ-MBK1's MBean-tag/attribute details, OQ-MBK2's
`KAFKA_OPTS` plumbing + HTTP-client availability) were already resolved by the developer *during*
implementation and are documented in the code's own comments (`compose/Dockerfile.kafka`,
`compose/kafka-metrics.yml`, `kafka_stats.py`, the template's DEVIATION block) — this pass independently
reproduced every one of those live findings from scratch (the `wget`-not-`curl` scrape, the
`command:`-not-`environment:` KAFKA_OPTS scoping, the `FetchConsumer` tag, the >100% idle-rate clamp)
and found them all to hold exactly as documented. No new defect surfaced.

## Cleanup confirmation

- `docker ps -a` returned empty before starting, empty after teardown, and empty at the very end (one
  spawn/teardown cycle, 3-broker `aqe`).
- The `uvicorn` process started for this pass (port 8020) was killed (`taskkill /F /PID`); `netstat`
  confirmed no `LISTENING` entry on port 8020 afterward (only an expected transient `TIME_WAIT`).
- No scratch topic folder was created this pass (used the existing shipped `aqe` topic); nothing to
  delete under `content/`; `git status --short content/` is empty.
- No notebook was executed during this pass (spawn opened the JupyterLab iframe URL but no notebook
  cell was run), so the notebook-cleanliness convention (CLAUDE.md) doesn't apply here.
- `git status --short` shows only the pre-existing feature diff this task started with (`app/config.py`,
  `app/monitoring/collector.py`, `app/monitoring/kafka_stats.py`, `compose/build.sh`,
  `compose/templates/docker-compose.yml.j2`, `docs/backlog.md`, `tests/unit/test_collector_kafka.py`,
  `tests/unit/test_renderer.py`, plus untracked `compose/Dockerfile.kafka`, `compose/kafka-metrics.yml`,
  `tests/unit/test_kafka_stats_jmx.py`) — no stray scratch files added by this pass. The scratchpad
  driver script (`qa_collect_once_jmx.py`) lived entirely outside the repo
  (`C:\Users\hoang\AppData\Local\Temp\claude\...\scratchpad\`), never inside the working tree.
- Unit suite re-confirmed clean after cleanup: `py -3.9 -m pytest tests/unit -q` → **443 passed, 0
  failed, 0 skipped** (matches the pre-test baseline exactly).

## Recommendation

This is a **recommendation, not final sign-off** — the human should review and give final sign-off
before issue #58 (US-MBK3) is considered done.

- Given/thens 1-5 (baked exporter image + `-javaagent` wiring loopback-only with no new host port,
  the `kafka_stats.py` scrape feeding the same `KafkaSnapshot` via the same CLI-shellout idiom with
  real non-`None` values from a live `collect_once()` — not a mocked unit test, real traffic-driven
  produce/fetch p99 and heap% numbers cross-checked against independently hand-run JMX output and
  sane against `docker stats`/the configured `-Xmx` bound, sub-story (b)'s CLI layer proven intact
  alongside the JMX scrape via the `command:`-vs-`environment:` fix, and no `7071` host-port
  publication anywhere) — **all PASS**, live-verified against a real Docker cluster built via
  `compose/build.sh` and spawned through the app's own routes, not a code read-through alone.
- Both ADR-flagged open questions (OQ-MBK1's MBean-tag details, OQ-MBK2's `KAFKA_OPTS`
  plumbing/HTTP-client availability) were confirmed live by this pass to match the developer's own
  documented live findings exactly — no discrepancy.
- No GitHub issues filed — no defects found in US-MBK3's actual scope.
- Not covered by this pass, deliberately: the Kafka Cluster Monitor UI's rendering of these fields
  (heap%/latency/idle bars, health-strip p99) — US-MBK4/#59 owns that, this sub-story is additive to
  the data layer only, per the requirements doc's own sequencing note.
