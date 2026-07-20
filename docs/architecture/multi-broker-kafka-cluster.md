# ADR: Multi-Broker Kafka (KRaft) Cluster + Kafka Cluster Monitor panel (v1.2)

Status: Draft for human review · Date: 2026-07-19
Drives: GitHub milestone [`v1.2 — Multi-Broker Kafka Cluster & Monitor`](https://github.com/hoanghaithanh/Spark-Playbook/milestone/15) (#15), backlog row #40
Requirements: `docs/requirements/multi-broker-kafka-cluster.md` (US-MBK1–US-MBK5, Open Questions 1–4)
Formalizes: `C:\Users\hoang\.claude\plans\for-18-i-want-lazy-candle.md` (human-approved plan, 2026-07-19)

**Amends `docs/architecture/kafka-streaming-infra.md` (#50): supersedes D1's "not a user-facing
toggle" framing, extends D3 (one loopback host port per broker instead of one fixed port).** D2
(same compose project), D4 (bake-at-build discipline), D5 (producer), D6 (ephemeral/bounded
retention) are unchanged and still hold. Same "state the supersession explicitly" pattern
`docs/architecture/live-market-data-streaming.md` used for US-C7.

Builds on / reuses (no modification): `docs/architecture/realtime-monitoring-dashboard.md`
(D-A signal-not-conclusions, D-B collector+SSE, D-C docker-stats, D-E ring-buffer);
`docs/architecture/topic-shell-redesign.md` (Decision B — shared collector, one SSE per open panel,
client-switched views); `docs/architecture/worktree-cluster-isolation.md` (#38 project-label guard).
Forward dependency (not implemented here): `docs/architecture/live-market-data-streaming.md`
D-LMD4's `kafka-init` `--replication-factor` flags must be authored against this ADR's RF policy.

---

## Context

The single-node broker #50 shipped proves the compose-lifecycle plumbing but is structurally
incapable of teaching replication, ISR, leader election, or broker-failure survival. This release
replaces it with a user-configurable multi-broker KRaft cluster (1–5 brokers, default 3, RF=3 /
`min.insync.replicas=2`) and a Kafka tab in the existing Cluster Monitor panel, laying the Kafka
infrastructure foundation future Kafka curriculum topics build on. The requirements doc handed the
architect four open questions to close (exact JMX MBean names, exact CLI output-parsing shapes, the
collector sub-cadence figure, mockup deviations) plus full implementation detail across five
sub-stories.

The single load-bearing correctness decision is **D-MBK2 — per-broker loopback host port with a
per-broker advertised address.** Get it wrong and a host client that bootstraps against broker 1
hangs the moment a partition's leader is broker 2 or 3 — R-K6's mismatch class, now *guaranteed*
under normal RF=3 operation instead of hypothetical. Everything else (drawer config, observability
layer, JMX, panel, broker-kill demo) is additive on top of an existing seam.

---

## Decision

### D-MBK1 — Kafka becomes a user-facing drawer config section; `requires_kafka` demotes from sole gate to default checked-state (supersedes #50 D1)

#50 D1 stated `include_kafka` "is **not** a user choice in the cluster drawer... set server-side
[from] `topic.requires_kafka`." That was correct for a single always-off-unless-streaming broker.
This release reverses it: the cluster-config drawer gains a **"Kafka" section** — an "Include Kafka"
checkbox and a broker-count number input (1–5, default 3), mirroring the existing `worker_count`
field's min/max/default pattern exactly. `topic.requires_kafka` now only **pre-checks** the box and
suggests a default broker count; the learner can enable, disable, or resize Kafka on any topic,
honored over the manifest. Folded into the single existing Spawn/Teardown action — one
`docker compose up` brings up the whole `sparkpb` project including N brokers; no second lifecycle
control (the single-slot state machine, `manager.py`, and the #38 guard are unchanged — D2 holds).

`ClusterParams` (`renderer.py`) gains `kafka_broker_count: int = 3` alongside the existing
`include_kafka: bool`. `spawn_cluster` (`topics.py`) reads `include_kafka` and `kafka_broker_count`
from **form fields**, not from `topic.requires_kafka`. The form's default checked-state comes from
`topic.requires_kafka` passed through the panel context.

### D-MBK2 — N combined broker+controller nodes; one loopback host port per broker, per-broker advertised (extends #50 D3, load-bearing)

The one `{% if include_kafka %}` `kafka` service becomes a `{% for i in range(1, kafka_broker_count + 1) %}`
loop over `spark-kafka-{{i}}` (hostname `kafka-{{i}}`), all in the same `sparkpb` project on
`sparkpb-net` (D2 unchanged). Every broker runs combined `broker,controller` mode (no separate
controller quorum — a non-goal), `CLUSTER_ID` is the single fixed value on all N, and
`KAFKA_CONTROLLER_QUORUM_VOTERS` lists all N identically:
`1@kafka-1:9093,2@kafka-2:9093,…,N@kafka-N:9093`.

**Per-broker host port (the correctness requirement).** Broker `i` publishes
`127.0.0.1:{{ 9092 + (i-1)*100 }}:29092` (broker 1 → 9092, broker 2 → 9192, broker 3 → 9292, …) and
advertises **its own** host port on `PLAINTEXT_HOST`. In-cluster `PLAINTEXT` advertises
`kafka-{{i}}:9092`. Because Kafka's client protocol is two-hop (bootstrap, then reconnect to the
advertised leader address), a host client bootstrapping against 127.0.0.1:9092 receives every
broker's distinct `127.0.0.1:<port>` from metadata and can reach whichever broker leads a partition
— the generalization of #50 D3's R-K6 fix from one broker to N. Broker 1 keeps `9092`, so existing
bootstrap defaults (`config.CLUSTER_HOST`, `produce.py --bootstrap 127.0.0.1:9092`) need no change.
Controller port `9093` stays in-cluster-only on every broker, never published.

The `0.0.0.0`→empty-host bind-syntax deviation and the `127.0.0.1`-not-`localhost` advertised-host
fix that #50's live testing already baked into the template both carry forward unchanged (see the
template's existing DEVIATION comments — they are equally load-bearing per broker).

### D-MBK3 — RF = `min(3, kafka_broker_count)`, `min.insync.replicas` = 2 when `kafka_broker_count >= 2`

Broker-env replication settings, rendered from `kafka_broker_count` so an RF never exceeds the
broker count:

| Env | Value | Note |
|---|---|---|
| `KAFKA_DEFAULT_REPLICATION_FACTOR` | `{{ [3, kafka_broker_count] \| min }}` | auto-created data topics |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `{{ [3, kafka_broker_count] \| min }}` | was `"1"` |
| `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` | `{{ [3, kafka_broker_count] \| min }}` | was `"1"` |
| `KAFKA_MIN_INSYNC_REPLICAS` | `{{ 2 if kafka_broker_count >= 2 else 1 }}` | data-topic ISR floor |
| `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR` | `{{ 2 if kafka_broker_count >= 2 else 1 }}` | was `"1"` |

At RF/ISR=1 (single broker) the topic must not request an unsatisfiable floor — hence the `>= 2`
conditional. This makes "kill one broker, `acks=all` writes still succeed against
`min.insync.replicas=2`" a real, demonstrable property (US-MBK5), not an assumption. v1.1's future
`kafka-init` must author its explicit `--replication-factor {{ [3, kafka_broker_count] \| min }}`
against this (forward dependency, restated for traceability — not implemented here).

### D-MBK4 — Resource ceiling: Kafka contributes `KAFKA_MEMORY_GB * kafka_broker_count`

`renderer.validate()`'s Kafka contribution changes from a flat `+KAFKA_MEMORY_GB` to
`config.KAFKA_MEMORY_GB * params.kafka_broker_count` (at the default 3 brokers, +6GB; the streaming
topic's 3×4GB-worker default totals `1+12+2+6 = 21GB`, under the 32GB ceiling). Mirrored in
`compose/cli.py::_validate_ranges`, which also gains a `--kafka-broker-count` flag and a
`kafka_broker_count` DEFAULTS entry. `validate()` range-checks `kafka_broker_count` against
`config.KAFKA_BROKER_COUNT_RANGE = (1, 5)` when `include_kafka`.

> **Flagged pre-existing drift (developer must reconcile):** `compose/cli.py::_validate_ranges`
> still hardcodes the **48GB** ceiling (line 145), while `app/config.py::RESOURCE_CEILING_GB` was
> lowered to **32GB** (issue #6). This drift pre-dates this release; since the developer is editing
> `_validate_ranges` for the Kafka formula anyway, bring the CLI ceiling to 32GB in the same pass so
> the two paths agree (the CLI-mirror obligation #50's ADR flagged). Not a v1.2-scope design
> decision, just a drift to close while in the file.

### D-MBK5 — Observability data layer: `docker exec` CLI shellouts with broker fallback, on a slower sub-cadence

A new `app/monitoring/kafka_stats.py` mirrors `docker_stats.py`'s `asyncio.create_subprocess_exec`
idiom exactly — **no `KafkaAdminClient`** (#50 proved `NodeNotReadyError`) and no new Python Kafka
dependency. All admin-plane reads are `docker exec` into a live broker; any one broker's CLI
describes the whole cluster via `--bootstrap-server localhost:9092` (its own listener). The
collector picks a live broker in order (`spark-kafka-1`, `-2`, `-3`, …), falling to the next on a
non-zero exec, so the broker-kill demo never blinds the monitor when broker 1 is the one that's
down.

**Two cadences, one collector cycle** (reuses the existing `DashboardCollector`, no second collector):

- **Broker CPU/RAM/disk/net piggyback on the existing `docker stats` batch at zero added cost** —
  `docker_stats.sample()` already covers `spark-kafka-*` via the `com.docker.compose.project=sparkpb`
  label filter (confirmed by reading `list_container_ids`; it never enumerates container names). The
  only wiring needed is adding `spark-kafka-{i}: 1.0` to `_cpu_limits()` so broker CPU% normalizes
  against the 1-core limit. **Brokers are deliberately *not* added to `_expected_containers()`** —
  that feeds the Spark Overview/Node grids; brokers live in the Kafka tab via a separate
  `_expected_kafka_containers()`.
- **The heavier CLI shellouts run on a sub-cadence** — a per-collector tick counter
  (`config.KAFKA_COLLECTOR_SUBCADENCE_CYCLES = 5`, ≈10s at the 2s base cadence). Between refreshes
  the last `KafkaSnapshot` is reused. **Resolves Open Question 3:** every 5th cycle (≈10s) is the
  right figure **provided the shellouts run concurrently** (`asyncio.gather`), which makes a full
  Kafka pull ≈ the slowest single call (~2–3s wall clock) rather than the ~7–14s sum of ~7
  sequential JVM-startup CLI invocations. 10s comfortably absorbs that. `ponytail:` if broker CPU
  spikes are observed from ~7 concurrent JVM starts inside a 1-core broker, widen to every 8th–10th
  cycle or serialize a subset — the figure is a tunable config knob, not a hard-coded constant. The
  base 2s cycle is skipped for Kafka entirely when no `spark-kafka-*` containers are in that cycle's
  stats (non-Kafka spawns never shell out to Kafka tooling).

**Resolves Open Question 2 — exact CLI output-parsing shapes** (Kafka 3.9 `/opt/kafka/bin/*.sh`;
each parser gets a `demo()`/`__main__` assert-based self-check against a captured sample string,
per ponytail — the smallest thing that fails if the parse breaks):

| Tool | Shape | Parse approach | Confidence |
|---|---|---|---|
| `kafka-topics.sh --describe` | Tab-delimited. Topic header line `Topic: X\tTopicId: …\tPartitionCount: N\tReplicationFactor: R\tConfigs: …`; then per-partition indented lines `Topic: X\tPartition: P\tLeader: L\tReplicas: r,r,r\tIsr: i,i,i` (3.7+ may append `Elr:`/`LastKnownElr:` — ignored keys). | Split each line on `\t`, then each field on first `:`; a line is a partition iff it has a `Partition:` field. Leader/Replicas/Isr → topic rows + ISR sets. | **High** |
| `kafka-topics.sh --describe --under-replicated-partitions` | Same partition-line shape, **URP lines only**, no topic headers; empty output = none. | Same per-line splitter; count = number of lines. | **High** |
| `kafka-consumer-groups.sh --describe --all-groups` | Fixed-width header `GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID`; `-` for absent values; groups separated by blank lines. | Skip header/blank lines, `re.split(r"\s+")`; take indices 0–5 (GROUP, TOPIC, PARTITION, CURRENT-OFFSET, LOG-END-OFFSET, LAG). GROUP/TOPIC never contain spaces, so index-based is safe; guard `-`→None. | **High** |
| `kafka-consumer-groups.sh --describe --all-groups --state` | Header `GROUP COORDINATOR (ID) ASSIGNMENT-STRATEGY STATE #MEMBERS`; **`COORDINATOR (ID)` contains a space**. | Do **not** index from the left. GROUP = first token, `#MEMBERS` = last token, STATE = second-to-last token. (Second call, run alongside the offsets call.) | **Medium** (coordinator-space gotcha — verify live) |
| `kafka-metadata-quorum.sh describe --status` | `key:<pad>value` per line: `ClusterId`, `LeaderId`, `LeaderEpoch`, `HighWatermark`, `CurrentVoters`, `CurrentObservers`, … | Split each line on first `:`, strip. `LeaderId` = **active controller node id**. (`CurrentVoters` format `[1,2,3]` w/o `--json` — only parsed if the quorum panel needs it.) | **High** for `LeaderId` |
| `kafka-log-dirs.sh --describe` | Two human lines (`Querying…`, `Received…`) then one JSON line: `{"version":1,"brokers":[{"broker":1,"logDirs":[{"logDir":…,"partitions":[{"partition":"prices-0","size":1024,…}]}]}]}`. | Find the line starting `{`, `json.loads`; walk brokers→logDirs→partitions→size for per-partition/per-topic disk. | **High** |
| `kafka-run-class.sh kafka.tools.GetOffsetShell --topic X` | `topic:partition:latestOffset` per line (e.g. `prices:0:120`). | `line.split(":")` → (topic, part, offset). Sum per topic; delta across two sub-cadence polls / elapsed → msg/s (the same delta idiom `collector.py` uses for disk/net). | **High** |

### D-MBK6 — JMX via a baked Prometheus exporter agent, scraped in-cluster; broker image gains a Dockerfile (US-MBK3)

To attach the agent and to have an HTTP client for scraping, US-MBK3 introduces a **custom broker
image** `compose/Dockerfile.kafka` (`FROM apache/kafka:3.9.0`) that bakes in the Prometheus JMX
exporter Java agent + a minimal metrics config at build time (same "bake, don't fetch at runtime"
discipline as #50 D4's connector jars). The template's kafka `image:` swaps from stock
`apache/kafka:3.9.0` to the built `sparkpb/kafka:3.9.0`, and `compose/build.sh` builds it.
**US-MBK1/MBK2 keep the stock image** (topology + CLI observability need no JMX); the image swap and
`KAFKA_OPTS` wiring land only in US-MBK3, keeping the sub-stories cleanly separable.

- **Maven coordinate:** `io.prometheus.jmx:jmx_prometheus_javaagent:1.0.1` (Maven Central
  `io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar`). Pin
  exactly and re-check for the latest 1.x stable at build time — same version-pin discipline as the
  Spark jars (R-K3). *(Confidence: Medium on the exact patch version; High that a `jmx_prometheus_javaagent`
  1.x jar is the right artifact.)*
- **Wiring:** `KAFKA_OPTS="-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=127.0.0.1:{{ config.KAFKA_JMX_EXPORTER_PORT }}:/opt/jmx/kafka-metrics.yml"`.
  The `[host:]port:configFile` agent-arg form binds the HTTP endpoint to the container's **loopback
  only** — no host-published port (non-goal). **Verify live** that the apache/kafka 3.9 native-image
  launcher (`KafkaDockerWrapper`) honors `KAFKA_OPTS` as JVM opts; if it uses a different pass-through
  env, use that instead (Medium confidence — flag).
- **Scrape:** `docker exec spark-kafka-{i} <httpclient> -s localhost:{{ port }}/metrics`, parsed as
  Prometheus text into the same `KafkaSnapshot`. **Verify live** that the base image ships a usable
  HTTP client; the apache/kafka image is slim and may lack `curl`. Since US-MBK3 owns a Dockerfile
  anyway, install a minimal client (`curl` or busybox `wget`) there rather than assuming one exists.

**Resolves Open Question 1 — exact MBean names.** Verified from strong knowledge of the Kafka 3.9 /
JVM MBean tree, **not** re-confirmed against a live broker at design time; each carries a stated
confidence and is flagged for live re-verification during US-MBK3 (per this repo's "verify by
running it" discipline — the exact metric families are stable, the fetch/tag details are what drift):

| Metric | MBean ObjectName | Attribute(s) | Confidence |
|---|---|---|---|
| **Heap usage %** | `java.lang:type=Memory` (JVM-standard, **not** Kafka-specific) | `HeapMemoryUsage` composite → `used` / `max`; heap% = used/max·100 | **High** — stable across JVMs; only risk is the exporter config whitelisting `java.lang` (a config we control) |
| **Produce latency p50/p95/p99** | `kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce` | `50thPercentile` / `95thPercentile` / `99thPercentile` (also `Mean`, `Count`) | **High** on family + percentile attrs |
| **Fetch latency p50/p95/p99** | `kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer` (consumer-facing; `FetchFollower` is replica traffic) | same percentile attrs | **Medium-High** — confirm `FetchConsumer` vs a plain `Fetch` tag on 3.9 |
| **Request-handler-idle %** | `kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent` | `OneMinuteRate` (fraction 0–1; idle% = ·100); `MeanRate` as fallback | **High** on MBean; **Medium** on which rate attribute |

Supporting MBeans available for the health strip once JMX lands (optional, not required — the CLI
layer already sources the equivalents): `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions`
(gauge), `kafka.controller:type=KafkaController,name=ActiveControllerCount` (1 on the controller),
`kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec/BytesInPerSec/BytesOutPerSec`
(`OneMinuteRate` — a cleaner throughput source than GetOffsetShell, but JMX is sequenced *after*
US-MBK2, so GetOffsetShell remains the day-one throughput source per the plan).

### D-MBK7 — Kafka tab is a 4th client-switched view in the existing panel; ISR-shrink events in a bounded ring buffer (US-MBK4/MBK5)

The Kafka tab is a 4th `.dash-view` in `_dashboard_body.html`, fed by the same shared collector and
single SSE connection (Decision B) — **not** a new panel, **not** manifest-gated. Always present
(like the other three views); renders a clear "Kafka not running" empty state when
`snapshot.kafka is None` (Kafka excluded or no cluster). This is the panel's READY-gated posture,
explicitly *not* v1.1's separate manifest-flag-gated price panel.

- `model.py` gains `KafkaBrokerStat`, `KafkaTopicRow`, `ConsumerGroupRow` (+ `PartitionLagRow` for
  drill-down), `IsrShrinkEvent`, `KafkaSnapshot`; `Snapshot` gains `kafka: Optional[KafkaSnapshot] = None`.
  Pre-formatted display strings + colors like every existing model, and — enforcing D-A / G3 by
  construction — **no `suggestion`/`fix` field anywhere** (see D-MBK8).
- `collector.py`: `_cpu_limits()` includes brokers; new `_expected_kafka_containers()`,
  `_build_kafka()` (sub-cadence), instance state `_cycle_count`, `_prev_kafka_offsets`,
  `_prev_isr: Dict[(topic,partition), set[int]]`, `_isr_events: deque(maxlen=KAFKA_ISR_EVENT_HISTORY_LENGTH)`,
  `_latest_kafka`. `_reset_deltas()` also clears these.
- New templates `_kafka_body.html` (initial render) + `kafka_oob.html` (SSE push), mirroring
  `_overview_body.html`/`overview_oob.html`. `dashboard.py::_render_oob_payload()` appends the Kafka
  fragment as the 4th OOB swap over the same SSE connection.
- `_dashboard_body.html`: `#kafka-content` view + `'kafka'` in `state.view`/`applyView()` + a small
  view-switch nav (Overview | Kafka) in the panel header — the existing panel reaches Job/Node Detail
  by in-content drill-down, but Kafka needs a top-level entry, so a minimal two-button nav is added
  (the smallest change that surfaces the tab).

**ISR-diff tracking (US-MBK5):** each Kafka snapshot builds `{(topic,partition): set(isr_ids)}`.
For each partition where a previous ISR set exists and `dropped = prev - current` is non-empty,
append one `IsrShrinkEvent(topic, partition, dropped_replica_id, now)` per dropped id to the
`_isr_events` ring buffer (mirroring the `DASHBOARD_HISTORY_LENGTH` idiom). Rejoin does **not**
retroactively remove entries — the buffer records history; current health is read from the live
snapshot, never inferred from event history. This is what turns the ISR-shrink feed and
under-replicated diagnostics from "structurally present, always empty" into genuinely exercisable
when a broker is manually `docker stop`ped (US-MBK5's documented-manual-kill demo — no in-app kill
control, matching the Fault Tolerance & Lineage / checkpoint-recovery precedent). A `concept.md`/docs
callout ("try `docker stop spark-kafka-2` while watching the monitor") documents the step.

Data-readiness by section (all real, never fabricated): broker grid (CPU/disk/net/online-count),
topics table, consumer-groups table + per-partition lag drill-down, leader distribution, and
under-replicated count are **real from day one** (US-MBK2 CLI layer + reused container stats), flat/
zero absent a fault. Heap %, produce/fetch latency percentiles, request-handler-idle % show an honest
`—` until US-MBK3's JMX lands. ISR-shrink feed and incident cards render an honest empty state until
US-MBK5 is exercised.

### D-MBK8 — Signal, not conclusions: Kafka diagnostics/incident cards carry no remedy text (G3 / D-A precedent); resolves Open Question 4

**I could not fetch `Kafka Cluster Monitor.dc.html` at design time** — the DesignSync MCP tool is
not in this architect agent's toolset, and the four local `images/*.png` screenshots are the
*existing* Spark app (topics index, topic shell, current Overview + Job-Detail monitor views), not
the Kafka mockup. The mockup's structure is known from the plan + requirements (health strip;
diagnostics cards for active incidents; throughput/latency charts; broker card grid with drill-down;
leader distribution; ISR-shrink feed; topics table; consumer-groups table with lag drill-down; demo
data of 5 brokers + a simulated incident).

The plan's "diagnostics cards for active incidents" are the direct analog of the realtime
dashboard's bottleneck cards, which `realtime-monitoring-dashboard.md` D-A stripped of their
`Suggestion:` lines for G3 ("signal, not conclusions"). **Apply the same rule by construction here:**
a Kafka incident card states the *fact* ("Broker 2 offline · 4 partitions under-replicated",
"prices-0 ISR shrank to {1,3}") with a deep link into the relevant section or the real broker CLI —
never a remedy ("increase replication factor", "restart broker 2"). The `KafkaSnapshot`/incident
models have **no suggestion field**, so this stays true structurally, not by convention (the same
R-Dash-6 mechanism). If the actual mockup carries a prescriptive element, dropping it is a
**deliberate, documented deviation**, not a missed element (the D-A precedent).

**Flagged for the developer/test-engineer:** re-inspect the real `Kafka Cluster Monitor.dc.html` via
the DesignSync tool (projectId `911c0961-ad6e-4cb2-bee2-e117ad1e3f2e`) at build/acceptance time,
confirm any prescriptive element and record its removal as intentional; the mockup's demo values
(5 brokers, simulated incident) are illustrative, not a literal value spec.

---

## Alternatives considered

| Decision | Alternative | Why not |
|---|---|---|
| D-MBK1 drawer config | Keep #50 D1 (manifest-only gate) | The whole point of v1.2 is a learner-controlled cluster; a manifest-fixed count can't teach "resize/kill and observe." Human-confirmed reversal. |
| D-MBK2 per-broker host port | Single shared host port (broker 1 only) | RF=3 spreads leaders across brokers; a host client that can only reach broker 1 hangs on the second hop the moment a leader is elsewhere — R-K6 guaranteed, not hypothetical. |
| D-MBK2 combined mode | Dedicated controller-only quorum | Speculative generality nothing here teaches; #50's single-node was already combined mode — replicate it, don't redesign it. |
| D-MBK5 CLI shellouts | `KafkaAdminClient` / a Python Kafka lib | #50 proved `NodeNotReadyError`; and the CLI idiom is already this project's established Kafka-tooling discipline. No new dependency. |
| D-MBK5 ~10s sub-cadence, concurrent | Run all CLI pulls every 2s base cycle | ~7 JVM-startup CLI invocations (~7–14s sum) can't fit a 2s cycle and would peg the broker; a slower sub-cadence + `asyncio.gather` + last-snapshot reuse is the honest fit. |
| D-MBK6 baked exporter agent | Raw JMX over a published port | A published JMX/RMI port violates the minimal-host-port-surface posture; the agent's loopback HTTP endpoint scraped via `docker exec` adds zero host surface. |
| D-MBK6 baked exporter agent | Read MBeans via `JmxTool` shellout (no agent, no image) | Would need a JMX RMI port enabled and is per-MBean chatty; the Prometheus agent gives one `/metrics` scrape and is the standard operator path. |
| D-MBK7 4th tab in existing panel | New standalone Kafka panel | Duplicates the collector/SSE/OOB machinery and a second SSE connection for no gain; the shared-collector panel already fans N views over one stream. |
| D-MBK7 always-present tab | Manifest-gate the Kafka tab (v1.1 price-panel pattern) | The panel is READY-gated, not manifest-gated (its three existing tabs already are); Kafka is infrastructure available on any topic, not streaming-topic-only. |

Simpler options rejected because a real constraint forbids them (ADR discipline): dropping the
`min(3, N)` RF clamp (a 1/2-broker spawn would request an unsatisfiable RF), dropping the
`>= 2` ISR conditional (single-broker min-isr can't exceed 1), and dropping the resource-ceiling
per-broker scaling (US-MBK1 requires staying in budget). None simplified away.

---

## Consequences

**Accepted trade-offs:**

- **The `sparkpb` project's blast radius grows by up to N-1 more broker containers.** Teardown, the
  #38 guard, and `--remove-orphans` all operate at project granularity and absorb it for free
  (D2 unchanged), but `docker ps` for a 3-broker streaming spawn now shows more service types and a
  wedged broker is one more failed-spawn diagnosis input.
- **Up to 5 loopback host ports (9092/9192/9292/9392/9492) on Kafka spawns** instead of one. Scoped
  to `127.0.0.1` only, never `0.0.0.0`, on Kafka spawns only, no auth — consistent with the
  single-trusted-user loopback threat model (`public-deploy.md`). Security-auditor should re-confirm
  this port-surface change stays within the minimal-surface posture (a re-confirmation, not a fresh
  full pass — no new external API/secrets; that's v1.1's concern).
- **The observability layer is real code with real upkeep** — seven CLI parsers with self-checks,
  broker-fallback logic, a JMX Prometheus-text parser, and ISR-diff tracking. More than a scratch
  read, the intended cost of a monitoring surface future Kafka topics build on.
- **`kafka_broker_count` threads through the same four layers `include_kafka` already does**
  (`ClusterParams` → `validate()`/ceiling → `render()` context → template loop, + the CLI mirror) —
  boilerplate, not new architecture, but four edits that must stay in sync.
- **A custom broker image (US-MBK3) is a new build artifact** — `compose/build.sh` now builds two
  images; a Spark/Kafka version bump must re-pin the exporter jar (R-K3-class).

**What becomes harder:** Kafka state that survives a respawn, or a controller-only quorum, remain
deliberately out of reach — the ephemeral combined-mode multi-broker cluster is the floor that
teaches replication/ISR/leader-election, not a production Kafka. That is the intended boundary.

---

## Component / data design

```
BROWSER  cluster drawer (Kafka section: [x] Include Kafka, broker count [3])
   │ POST /topics/<id>/spawn  (include_kafka, kafka_broker_count, + existing fields)
   ▼
app/web/routes/topics.py::spawn_cluster   ← reads Kafka fields from FORM (D1 reversal)
   ▼
app/lifecycle/renderer.py
   validate(): total += KAFKA_MEMORY_GB * kafka_broker_count ; range-check 1–5   ← D-MBK4
   render():  context["kafka_broker_count"] = params.kafka_broker_count          ← NEW
   ▼
compose/templates/docker-compose.yml.j2
   {% for i in range(1, kafka_broker_count+1) %} spark-kafka-{{i}} (D-MBK2/3) {% endfor %}
   │  docker compose -p sparkpb up -d  (unchanged: manager → compose_ops → #38 guard)
   ▼
┌── project sparkpb, network sparkpb-net ─────────────────────────────────────────────┐
│  spark-master  spark-worker-1..N  spark-driver                                        │
│  spark-kafka-1 (kafka-1:9092 / :29092→127.0.0.1:9092 / :9093 controller)  [+JMX 7071] │
│  spark-kafka-2 (kafka-2:9092 / :29092→127.0.0.1:9192 / :9093)             [+JMX 7071] │
│  spark-kafka-3 (kafka-3:9092 / :29092→127.0.0.1:9292 / :9093)             [+JMX 7071] │
│    quorum voters: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093 (identical on all)     │
└───────────────────────────────────────────────────────────────────────────────────────┘
        ▲ docker stats (project label, 2s)          ▲ docker exec <live broker> kafka-*.sh (~10s)
        │  broker CPU/RAM/disk/net (reused)          │  topics/URP/groups/quorum/log-dirs/offsets
        │                                            │  + docker exec curl localhost:7071/metrics (JMX)
        └────────── app/monitoring/collector.py::_build_kafka() ──────────┘
                       ├ KafkaSnapshot (brokers, topics, groups, leader dist, health strip)
                       ├ ISR-diff vs _prev_isr → _isr_events ring buffer (US-MBK5)
                       └ offset delta vs _prev_kafka_offsets → throughput msg/s
                                     │  Snapshot.kafka
                                     ▼
   dashboard.py::_render_oob_payload → +kafka_oob.html  → SSE → #kafka-content (4th .dash-view)
```

**Files (developer handoff):**

*New:* `app/monitoring/kafka_stats.py` (shellouts + 7 CLI parsers + Prometheus-text parser, each
with a `demo()` self-check); `app/web/templates/dashboard/fragments/_kafka_body.html`,
`kafka_oob.html`; `compose/Dockerfile.kafka` (US-MBK3).

*Changed:* `compose/templates/docker-compose.yml.j2` (single kafka block → N-broker loop, D-MBK2/3;
US-MBK3 swaps `image:` + adds `KAFKA_OPTS`); `app/lifecycle/renderer.py` (`ClusterParams.kafka_broker_count`,
ceiling formula, range check, render context); `compose/cli.py` (`--kafka-broker-count`, DEFAULTS,
`_validate_ranges` formula + the 48→32 reconciliation); `app/config.py`
(`KAFKA_BROKER_COUNT_RANGE=(1,5)`, `DEFAULTS["kafka_broker_count"]=3`, `KAFKA_COLLECTOR_SUBCADENCE_CYCLES=5`,
`KAFKA_ISR_EVENT_HISTORY_LENGTH`, `KAFKA_JMX_EXPORTER_PORT=7071`, host-port base/stride constants);
`app/monitoring/model.py` (the 5 Kafka dataclasses + `Snapshot.kafka`); `app/monitoring/collector.py`
(`_cpu_limits` + brokers, `_expected_kafka_containers`, `_build_kafka`, ISR/offset state,
`_reset_deltas`); `app/web/routes/topics.py::spawn_cluster` + `_panel_context` (Kafka form fields +
range + `requires_kafka` default checked-state); `app/web/templates/fragments/_cluster_form.html`
(Kafka fieldset); `app/topics/loader.py::ClusterDefaults` (+ optional `kafka_broker_count`);
`app/web/routes/dashboard.py::_render_oob_payload` (+4th fragment) + `_dashboard_body.html`
(4th view + nav + JS). `compose/build.sh` (build the kafka image, US-MBK3).

**`KafkaSnapshot` shape (in-memory only, per sub-cadence cycle):**

- `running: bool`; health strip: `brokers_online`/`brokers_total`, `under_replicated_count`,
  `active_controller_id`, `throughput_label`, `consumer_group_count`, `total_lag`, `p99_latency_label`
  (JMX or `—`), `jmx_available: bool`.
- `brokers: [KafkaBrokerStat]` — node_id, container_name, online, cpu/ram/disk/net (reused), colors,
  partitions_led, is_controller, heap_pct+label+color / produce_p99 / fetch_p99 / rh_idle (JMX, `—`
  until MBK3).
- `topics: [KafkaTopicRow]` — name, partitions, replication_factor, under_replicated_count,
  isr_health_label+color, size_label.
- `consumer_groups: [ConsumerGroupRow]` — group, state, members, total_lag, `partitions: [PartitionLagRow]`
  (topic, partition, current_offset, log_end_offset, lag) for drill-down.
- `isr_shrink_events: [IsrShrinkEvent]` (from the ring buffer) — timestamp_label, topic, partition,
  dropped_replica_id, detail_label. **No suggestion field on any of these** (D-MBK8).

---

## Visual design (Kafka Cluster Monitor tab — UI-facing)

Source of truth is `Kafka Cluster Monitor.dc.html` (DesignSync projectId
`911c0961-ad6e-4cb2-bee2-e117ad1e3f2e`), **not re-inspected at design time** (see D-MBK8) — the
written spec below is the buildable target; the real mockup is checked at acceptance. The tab reuses
the existing panel chrome (dark header, threshold color system from `config.py`) so it reads as the
same family as the current Overview/Job/Node views.

**Layout (top to bottom), inside the 4th `.dash-view`:**

- **Health strip** (single row of stat tiles): Brokers online (`N/M`), Under-replicated partitions
  (green 0 / red > 0), Active controller (broker id), Throughput (msg/s), Consumer groups + total
  lag, p99 latency (`—` until JMX).
- **Diagnostics / incident cards** (conditional, only on a real fault): factual title + quantified
  detail + deep link. **No remedy text** (D-MBK8). Empty absent a fault.
- **Broker card grid** (`repeat(auto-fill,minmax(280px,1fr))`, like the Spark node grid): per broker
  — status dot + `kafka-{i}` + controller badge; CPU% / RAM% color bars; Disk I/O / Net I/O; heap %
  / produce p99 / fetch p99 / request-handler-idle (`—` until JMX); partitions-led count. Offline
  brokers marked unavailable (not frozen), same as `NodeStat.available`. Clickable → broker detail.
- **Leader distribution:** per-broker partitions-led bars (real, flat when balanced).
- **ISR-shrink events feed:** reverse-chronological list from the ring buffer; honest empty state
  ("No ISR changes observed") until a broker is killed.
- **Topics table:** Topic / Partitions / RF / Under-replicated / ISR health / Size.
- **Consumer-groups table:** Group / State / Members / Total lag, expandable to per-partition lag rows.

**Distinct states to verify (beyond "it works"):**
- *Kafka not running* (excluded or no cluster): single clear "Kafka not running" empty state, not an
  error/blank/stale-from-prior-spawn.
- *Kafka up, no fault:* health strip all green, 0 URP, ISR feed empty, leader distribution flat,
  broker grid live; JMX fields `—` until MBK3.
- *Broker killed (US-MBK5):* killed broker marked offline, its led partitions re-elected, URP > 0
  (red), ISR-shrink events appear, incident card present — **no suggestion text anywhere**.
- *Killed broker restarted:* it rejoins (online, ISR heals) but historical ISR-shrink events remain.
- *JMX not yet landed (before MBK3):* heap/latency/idle render `—`, never a fabricated number.

---

## Open questions (flagged for human/developer, not blocking)

- **OQ-MBK1 — JMX MBean names not live-verified at design time (D-MBK6).** Resolved from strong
  knowledge with per-metric confidence; the `request=FetchConsumer` tag and the exact
  idle-% rate attribute (`OneMinuteRate` vs `MeanRate`) are the two Medium-confidence details to
  confirm against a real 3.9 broker's MBean tree during US-MBK3. Heap (`java.lang:type=Memory`) and
  the produce/percentile family are High confidence.
- **OQ-MBK2 — apache/kafka 3.9 native-image JMX plumbing (D-MBK6).** Confirm live that
  `KAFKA_OPTS` is honored by the image launcher and that the image (or the new Dockerfile.kafka)
  provides an HTTP client for the `docker exec ... /metrics` scrape.
- **OQ-MBK3 — Kafka mockup not re-inspected (D-MBK8).** DesignSync tool unavailable to this agent;
  developer/test-engineer must fetch `Kafka Cluster Monitor.dc.html` and confirm any prescriptive
  element's removal is documented as intentional. G3 applied by construction regardless.
- **OQ-MBK4 — `compose/cli.py` 48GB→32GB ceiling drift (D-MBK4).** Pre-existing; reconcile while
  editing `_validate_ranges`. Not a v1.2 design decision, but must not be left divergent.

---

## Risks

- **R-MBK1 — Advertised-address mismatch per broker (R-K6 at N).** A wrong per-broker
  `PLAINTEXT_HOST` advertised port (e.g. all brokers advertising 127.0.0.1:9092) makes a host client
  reach the wrong broker or hang on the second hop. *Noticed by:* a producer/consumer that connects
  then times out on produce/consume against a non-broker-1 leader. *Mitigation:* D-MBK2 pins each
  broker's advertised host port to its own published port; a live smoke test from a host shell
  against a 3-broker spawn (produce with `acks=all`, force a leader onto broker 2/3) before v1.1
  builds on this.
- **R-MBK2 — Auto-created topic under-replicated at birth.** If a producer's first send auto-creates
  a data topic before all N brokers register, RF/ISR can be below `min(3,N)`. *Noticed by:*
  `kafka-topics.sh --describe` showing RF < expected right after spawn. *Mitigation:* v1.1's explicit
  `kafka-init --replication-factor` for its topics; for v1.2 demos, create/produce after the cluster
  is READY, or add a broker-readiness poll (YAGNI until observed).
- **R-MBK3 — ~7 concurrent JVM-startup CLI execs spike a 1-core broker.** *Noticed by:* broker CPU
  saturating on each ~10s sub-cadence tick, or the broker itself slowing. *Mitigation:* the
  sub-cadence is a tunable config knob (D-MBK5, `ponytail`-tagged); widen it or serialize a subset if
  observed; target the exec at whichever online broker has headroom.
- **R-MBK4 — Broker-fallback picks a dead broker first.** If broker 1 is killed (US-MBK5) and the
  collector still tries it first, each cycle wastes a failed exec before falling through. *Noticed
  by:* a ~sub-cadence lag after a kill. *Mitigation:* order the fallback by the `docker stats`
  online set (skip brokers absent from this cycle's stats) rather than always `1,2,3,…`.
- **R-MBK5 — ISR-diff false positives from a transient describe hiccup.** A single failed/partial
  `--describe` could read an empty/shrunk ISR and emit a spurious shrink event. *Noticed by:* ISR
  events with no corresponding broker kill. *Mitigation:* only diff when the describe returned a
  well-formed partition line for that partition (skip, don't zero, on parse gaps); the buffer is
  historical/ephemeral, so a stray entry is cosmetic, not corrupting.
- **R-MBK6 — Signal→conclusions scope creep in Kafka diagnostics (R-Dash-6 at N).** *Noticed by:*
  review/acceptance vs D-MBK8. *Mitigation:* the Kafka models have no suggestion field by
  construction — adding a remedy forces a conscious G3 conversation.
- **R-MBK7 — #38 guard assumed, not confirmed, unaffected by broker count.** *Noticed by:* a foreign
  worktree's Kafka spawn tearing down another's live cluster (or being wrongly refused). *Mitigation:*
  `running_owner()`/`list_container_ids()` are project-label-scoped and container-count-agnostic
  (confirmed by reading — neither enumerates names); US-MBK1's dedicated acceptance criterion tests
  a foreign-worktree Kafka spawn is refused exactly as a non-Kafka one.
- **R-MBK8 — CLI/app resource-ceiling drift (existing 48/32 + the new per-broker formula).**
  *Noticed by:* the CLI accepting a config the app rejects or vice versa. *Mitigation:* D-MBK4
  reconciles both in one pass; code-review checks the mirror (the standing CLI-mirror obligation).
```
