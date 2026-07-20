# Multi-Broker Kafka Cluster & Monitor — Requirements (v1.2 — Multi-Broker Kafka Cluster & Monitor)

Status: Draft for architect handoff
Owner: requirements-analyst
Date: 2026-07-19
Traceability: GitHub milestone [`v1.2 — Multi-Broker Kafka Cluster & Monitor`](https://github.com/hoanghaithanh/Spark-Playbook/milestone/15)
(#15), backlog row #40. Formalizes the human-approved plan
`C:\Users\hoang\.claude\plans\for-18-i-want-lazy-candle.md` (2026-07-19) into testable requirements;
it does not re-open the plan's confirmed decisions, only turns them into acceptance criteria and
flags what's genuinely still open (JMX MBean names, exact CLI-output parsing).

## Amends

**This doc amends `docs/architecture/kafka-streaming-infra.md`, explicitly reversing Decision D1.**

D1 ("Conditional" means an opt-in template flag driven by the topic manifest, not a runtime
profile) stated plainly: *"It is **not** a user choice in the cluster drawer... `include_kafka` is
a boolean Jinja2 template variable... set server-side [from] `topic.requires_kafka`."* That was the
correct call for #50's scope (a single always-off-unless-streaming broker) and is not being
second-guessed as a past decision — but this release deliberately reverses it going forward:
**Kafka becomes a user-facing config section in the cluster-config drawer**, alongside broker
count, mirroring the existing `worker_count` field's pattern exactly. `topic.requires_kafka` is
demoted from **the sole gate** to a **default checked-state** — it still pre-checks the "Include
Kafka" box and suggests a default broker count on topics that want it (e.g. the streaming topic),
but the learner can now enable, disable, or resize Kafka on any topic page. This is a deliberate,
human-confirmed scope decision made when approving this milestone (see
`docs/backlog.md` row #40 and its "New release milestone: v1.2..." section), not an oversight of
D1 or a silent contradiction of it. Same "state the supersession explicitly" pattern this repo
already used for `docs/requirements/live-market-data-streaming.md`'s supersession of US-C7.

Everything else in `kafka-streaming-infra.md` (D2's same-compose-project collision safety, D4's
baked-jar discipline, D5's producer design, D6's ephemeral/bounded-retention posture) is unaffected
and still holds — only D1's "not a user-facing toggle" framing is reversed, and only for broker
count/inclusion. The two-listener host/in-cluster pattern (D3) is *extended* (one host-published
loopback port per broker instead of one fixed port), not reversed.

## Problem statement

The single-node Kafka broker shipped in #50 (backlog row #19) proves the compose-lifecycle
plumbing works, but a broker with no peers can't demonstrate the things that make Kafka *Kafka*:
replication, in-sync-replica tracking, leader election, and surviving a broker failure. The human
has stated a specific, standing interest in going deeper on Kafka mechanics — not just using Kafka
as plumbing under a PySpark exercise — and this project's own forward direction (per the plan's
Context section) is shifting from "Spark Playbook" toward a "PySpark + Kafka Playbook," where Kafka
becomes a first-class second subject with its own curriculum topics over time (partitioning
strategy, consumer-group rebalancing, exactly-once semantics, transactional producers, and more).
A single-broker cluster is structurally incapable of teaching any of that. This release replaces it
with a real, user-configurable multi-broker KRaft cluster and a matching observability panel so a
learner can watch replication, ISR, and leader election happen — laying the infrastructure
foundation that future Kafka topics build on, the same way the existing Spark cluster harness
underpins every PySpark topic today, not a one-off enhancement scoped narrowly to the still-pending
v1.1 streaming demo.

## Goals / Non-goals

### Goals

- **G-MBK1 — Real multi-broker replication, not a cosmetic broker count.** RF=3 (or
  `min(3, kafka_broker_count)`) and `min.insync.replicas=2` are actually enforced, so "kill one
  broker, writes still succeed" is a demonstrable property, not an assumption.
- **G-MBK2 — Broker count is a genuine user-facing config choice.** 1-5 brokers, default 3,
  configured in the existing cluster-config drawer, folded into the single existing Spawn/Teardown
  action — no second lifecycle control.
- **G-MBK3 — The learner can actually see the cluster's internal state.** A new Kafka Cluster
  Monitor tab surfaces broker health, replication/ISR, topics, consumer groups, and (once JMX
  lands) JVM-level metrics — not just "is the process running."
- **G-MBK4 — Broker failure is a real, observable, exercisable demo.** Killing a broker manually
  produces a real ISR shrink and leader re-election the monitor can show, mirroring the existing
  Fault Tolerance & Lineage (worker-kill) pattern for Spark.
- **G-MBK5 — Reuse existing platform seams.** The CLI-shellout idiom (`docker_stats.py`'s
  `asyncio.create_subprocess_exec` pattern), the single-slot spawn/teardown state machine, the
  existing collector/SSE/HTMX-OOB dashboard mechanism, and the #38 cross-worktree collision guard
  are all reused as-is — this is new data and a new tab, not a new platform mechanism.

### Non-goals (explicit, not implicit)

- **No dedicated controller-only quorum.** Every broker runs combined `broker,controller` mode
  (matches #50's shipped single-node topology, just replicated) — a separate controller-only
  quorum is speculative generality nothing here needs.
- **No JMX host-port publishing.** The Prometheus JMX exporter's `/metrics` endpoint is scraped
  in-cluster only, via `docker exec ... curl localhost:<port>/metrics` — no new host-published
  port, preserving the minimal-host-port-surface discipline `public-deploy.md` established.
- **No automated broker-kill control.** The fault-tolerance demo uses documented manual
  `docker stop`/`docker kill` against a target broker container — the same established pattern as
  Structured Streaming's checkpoint-recovery demo and Fault Tolerance & Lineage (US-C9)'s
  worker-kill demo. No new in-app "kill a broker" button or safety UX is built.
- **No change to v1.1's own scope.** This doc amends `kafka-streaming-infra.md` (#50's ADR), not
  `live-market-data-streaming.md` (v1.1's requirements) directly. v1.1's not-yet-implemented
  `kafka-init` container (`docs/architecture/live-market-data-streaming.md`, D-LMD4) will need its
  `--replication-factor` flags authored against this release's RF=3 policy once v1.2 lands — that
  is a **forward dependency v1.1 will need to account for**, not something this release itself
  implements or modifies. See "Sequencing" below.
- **No persisted Kafka observability history.** Mirrors the existing Cluster Monitor panel's
  posture (`realtime-monitoring-dashboard.md` D-E: no queryable time-series storage) — the ISR-shrink
  events ring buffer is bounded and in-memory, lost on process/cluster restart, same as the existing
  node CPU/RAM sparkline buffers.
- **No new frontend dependency.** The Kafka tab is server-rendered fragments over the existing
  SSE/HTMX-OOB mechanism, same as the panel's other three tabs.

## User stories and acceptance criteria

**US-MBK1 (sub-story a) — Multi-broker topology + drawer config.**
As a learner, I want to configure how many Kafka brokers spawn (1-5, default 3) from the same
cluster-config drawer I already use for worker count, so that Kafka becomes a real replicated
cluster I control, without a second lifecycle action to remember.

- *Given* the cluster-config drawer, *when* I open it, *then* a "Kafka" section is present with an
  "Include Kafka" checkbox and a broker-count number input (range 1-5, default 3), mirroring the
  existing `worker_count` field's min/max/default pattern exactly.
- *Given* a topic whose manifest sets `requires_kafka: true`, *when* I open that topic's drawer,
  *then* the "Include Kafka" checkbox is pre-checked and the broker-count field is pre-populated
  with the default — but *when* I uncheck the box or change the broker count before spawning,
  *then* my choice is honored, not overridden by the manifest. This is the explicit test that
  `requires_kafka` is now a default, not the sole gate (the D1 reversal).
  Conversely, *given* a topic whose manifest sets `requires_kafka: false` (or omits it), *when* I
  open that topic's drawer, *then* the checkbox is unchecked by default but I can still check it
  and spawn Kafka manually — Kafka is available on any topic, not gated to streaming topics only.
- *Given* the drawer's existing single Spawn/Teardown action, *when* I submit it with "Include
  Kafka" checked at N brokers, *then* exactly one `docker compose up` brings up the whole `sparkpb`
  project including N Kafka broker containers — no second button, no separate Kafka lifecycle call.
- *Given* Kafka included at broker count N, *when* the compose project renders, *then* every broker
  runs combined `broker,controller` mode, `KAFKA_CONTROLLER_QUORUM_VOTERS` lists all N brokers
  identically on every broker, and `CLUSTER_ID` is the single fixed value shared across all N
  (matching #50's fixed-CLUSTER_ID convention, just replicated).
- *Given* broker `i` (1..N), *when* the stack is up, *then* it is reachable in-cluster at
  `kafka-{i}:9092` and has its own host-published loopback port (`127.0.0.1:{9092 + (i-1)*100}` →
  internal `:29092`), so a host client (e.g. the FastAPI app or a host-run producer) can reach
  whichever broker a given partition's leader actually is, not just broker 1. Broker 1 keeps
  today's `9092`, so existing bootstrap defaults (`config.CLUSTER_HOST`, `produce.py`'s default)
  need no change. The controller port `9093` stays in-cluster-only on every broker, never
  published.
- *Given* a topic/broker-count combination, *when* the compose template renders topic-level
  replication settings, *then* `KAFKA_DEFAULT_REPLICATION_FACTOR` and the bumped
  `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`/`KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` equal
  `min(3, kafka_broker_count)` (so a 1- or 2-broker spawn never requests an RF exceeding the actual
  broker count), and `min.insync.replicas=2` is set on data topics whenever `kafka_broker_count >=
  2` (at RF/ISR=1 with a single broker, min-isr can't exceed 1 either — the topic must not request
  an unsatisfiable ISR floor).
- *Given* a spawn request, *when* `renderer.validate()` computes the resource ceiling, *then* the
  Kafka contribution is `config.KAFKA_MEMORY_GB * kafka_broker_count` (not a flat `+2GB` as #50
  shipped), and the same formula is mirrored in `compose/cli.py`'s `_validate_ranges` — a
  combination whose total exceeds the 32GB ceiling is rejected with a clear error, matching the
  existing ceiling-rejection behavior for worker count/memory.
- *Given* worktree B attempting to spawn a Kafka-included cluster while worktree A already owns a
  running `sparkpb` project (Kafka included or not), *when* the #38 ownership guard runs, *then*
  worktree B's spawn is refused exactly as it would be for a non-Kafka cluster — confirming
  `compose_ops.running_owner()`/`docker_stats.list_container_ids()`'s project-label scoping
  (`com.docker.compose.project=sparkpb`) is unaffected by broker count, since neither reads
  container names.
- *Given* a live 3-broker spawn, *when* I run `docker exec spark-kafka-1 kafka-topics.sh
  --describe` against a data topic, *then* it reports RF=3 and a 3-member ISR set.
- *Given* the same live spawn, *when* I run `docker stop spark-kafka-2`, *then*
  `kafka-topics.sh --describe` (run against a still-live broker) shows the partition's leader
  re-elected to a surviving broker and the ISR set shrunk to exclude broker 2, while producer
  writes with `acks=all` against `min.insync.replicas=2` continue to succeed.

**US-MBK2 (sub-story b) — Kafka observability data layer.**
As a learner, I want the monitoring system to pull real broker/topic/consumer-group state from the
running Kafka cluster, so the eventual dashboard panel shows facts, not fabricated placeholders.

- *Given* a running multi-broker Kafka cluster, *when* the new `app/monitoring/kafka_stats.py`
  collects a snapshot, *then* it does so via `docker exec` CLI shellouts (mirroring
  `docker_stats.py`'s `asyncio.create_subprocess_exec` idiom) — not `KafkaAdminClient` or any new
  Python Kafka library dependency (the CLI-shellout approach was chosen specifically because
  #50's ADR already establishes this project's Kafka-tooling discipline, and the plan explicitly
  rejects `KafkaAdminClient` as a dead end for this use).
- *Given* broker 1 is not the one issuing the CLI commands (e.g. it was killed per US-MBK4/the
  fault-tolerance demo), *when* the collector next runs, *then* it tries `spark-kafka-1`, then
  `-2`, then `-3`, ... in order, falling back to the next live broker on failure, and succeeds via
  whichever broker is actually up — never hardcoding broker 1 as the sole shellout target, since
  the fault-tolerance demo means broker 1 itself may be the down one exactly when the monitor
  matters most.
- *Given* the collector's per-cycle CLI pull, *when* it runs, *then* it surfaces (at minimum):
  topics/partitions/RF/leader/ISR (`kafka-topics.sh --describe`), under-replicated partitions
  specifically (`kafka-topics.sh --describe --under-replicated-partitions`), consumer groups + lag
  (`kafka-consumer-groups.sh --describe --all-groups[+--state]`), the active controller
  (`kafka-metadata-quorum.sh describe --status`), per-partition disk size
  (`kafka-log-dirs.sh --describe`), and a produce/consume rate derived from an offset delta across
  two polls (`kafka-run-class.sh kafka.tools.GetOffsetShell`, the same delta idiom
  `collector.py` already uses for disk/net). Exact output-parsing shapes are to be re-confirmed
  live during implementation (the CLI command list above is drafted from documented behavior, not
  guessed field-by-field) — flagged for the architect/developer step, not asserted as final here.
- *Given* a non-Kafka spawn (no `spark-kafka-*` containers present), *when* a collector cycle runs,
  *then* it skips the Kafka CLI shellouts entirely — no wasted subprocess calls when Kafka isn't
  running.
- *Given* a Kafka-included spawn, *when* the collector runs its normal ~2s cycle, *then* broker
  CPU/RAM/disk/net piggyback on the existing `docker stats` batch call at zero added cost (already
  covers `spark-kafka-*` today via the project-label filter), while the heavier CLI shellouts above
  run on a slower sub-cadence within the same cycle (a tick counter, e.g. every 5th cycle, ≈10s),
  reusing the last `KafkaSnapshot` between refreshes.
- *Given* a live multi-broker spawn, *when* `collect_once()` is called directly (no UI), *then*
  the resulting `KafkaSnapshot` is populated with real values matching what the equivalent manual
  CLI commands report — this is the sub-story's testable-standalone bar per the plan's sequencing,
  no UI work is required to verify it.

**US-MBK3 (sub-story c) — JMX exporter.**
As a learner, I want to see JVM-level broker health (heap usage, produce/fetch latency, request-
handler saturation) in addition to OS-level container stats, so I can reason about broker
performance the way a real operator would.

- *Given* the broker image build, *when* it's built, *then* it bakes in the Prometheus JMX exporter
  Java agent at build time (same "bake at build time, not runtime fetch" discipline #50's ADR
  already applied to the Kafka SQL connector jar) and attaches it via `KAFKA_OPTS`/`EXTRA_ARGS` as a
  `-javaagent`, exposing an HTTP `/metrics` endpoint bound to the container's own loopback only —
  no new host-published port (see Non-goals).
- *Given* a running broker with the exporter attached, *when* `kafka_stats.py` scrapes it via
  `docker exec spark-kafka-{i} curl -s localhost:<port>/metrics`, *then* the resulting metrics feed
  into the same `KafkaSnapshot` structure US-MBK2 established, using the same CLI-shellout idiom as
  everything else in that module.
- *Given* the exporter is wired up, *when* the monitor eventually renders broker cards, *then* it
  can show heap usage %, produce/fetch latency percentiles, and request-handler-idle % as real
  numbers.
  **Flagged, not asserted:** the exact MBean names backing these three metrics on the specific
  Kafka broker version this project ships must be verified live against the real broker's MBean
  tree during implementation — this doc does not assert specific MBean names or exact metric
  formulas as given, since they were not independently confirmed while writing these requirements.
  This is explicitly called out for the architect/developer step to resolve empirically (per this
  project's "verify by running it" discipline), not guessed from documentation.
- *Given* sub-story (b) is already merged, *when* (c) lands, *then* it is additive to the existing
  `KafkaSnapshot`/collector wiring — sequenced after (b), not a prerequisite for the rest of the
  observability layer or the monitor panel.

**US-MBK4 (sub-story d) — Kafka Cluster Monitor panel UI.**
As a learner, I want a Kafka tab in the same Cluster Monitor panel I already use for Spark node/job/
task detail, so Kafka's health lives in one familiar place rather than a separate tool.

- *Given* the existing Cluster Monitor panel (`_dashboard_body.html`'s Overview/Job Detail/Node
  Detail views, all fed by one shared SSE connection per `topic-shell-redesign.md` Decision B),
  *when* this story lands, *then* a 4th "Kafka" tab is added to the *same* panel/collector — not a
  new standalone panel, not a manifest-gated feature.
- *Given* any topic page, regardless of whether that topic's manifest sets `requires_kafka`, *when*
  I open the Cluster Monitor panel, *then* the Kafka tab is always present (matching the panel's
  existing READY-gated-not-manifest-gated posture, the same posture the other three tabs already
  have) — this is explicitly *not* the separate-panel, manifest-flag-gated pattern used for v1.1's
  price panel (`live-market-data-streaming.md` US-LMD3).
- *Given* no Kafka broker containers exist for the current spawn (Kafka excluded, or no cluster
  spawned), *when* I open the Kafka tab, *then* it renders a clear empty "Kafka not running" state
  — not an error, blank panel, or stale data from a prior spawn.
- *Given* a live Kafka-included spawn, *when* I open the Kafka tab, *then* the following sections
  render with **real data, sourced from US-MBK2's collector layer, from day one** (no JMX
  dependency): the broker grid (CPU/disk/net utilization and online/offline count, reusing the
  existing `docker stats` container-stats plumbing), the topics table, and the consumer-groups
  table with per-partition lag drill-down; leader distribution and the under-replicated-partition
  count also render real data from day one and are naturally flat/zero absent a fault.
  **Depends on US-MBK3 landing** for the following sections to show real numbers instead of an
  honest "—"/pending state: per-broker heap %, produce/fetch latency percentiles, and
  request-handler-idle %.
  **Structurally present but only populates once the broker-kill demo (US-MBK5) is actually
  exercised:** the ISR-shrink-events feed and any diagnostics/incident cards tied to it — these
  render an honest empty state, never a fabricated event, until a real ISR shrink is observed.
- *Given* the design mockup at `https://claude.ai/design/p/911c0961-ad6e-4cb2-bee2-e117ad1e3f2e`
  (file `Kafka Cluster Monitor.dc.html`) as the visual spec, *when* the Kafka tab is built and later
  checked (test-engineer's acceptance-validation step, per this repo's established screenshot-
  comparison convention), *then* its layout — health strip, diagnostics cards, throughput/latency
  charts, broker card grid with drill-down, leader distribution, ISR-shrink feed, topics table,
  consumer-groups table with lag drill-down — is faithfully represented, with any deliberate
  deviation (e.g. dropping a prescriptive "fix" element, matching this repo's `realtime-monitoring-
  dashboard.md` D-A precedent, if the mockup contains one) documented as intentional rather than a
  missed element. The mockup's demo data (5 brokers, a simulated incident) is illustrative only —
  not a literal spec for what values must appear.
- *Given* the new templates `_kafka_body.html` (initial render) and `kafka_oob.html` (SSE push),
  *when* a collector cycle completes, *then* `app/web/routes/dashboard.py::_render_oob_payload()`
  appends the Kafka fragment as a 4th OOB swap, alongside the existing three, over the same shared
  SSE connection — no second SSE connection is opened for the Kafka tab.

**US-MBK5 (sub-story e) — Broker-kill fault-tolerance demo.**
As a learner, I want to manually kill a Kafka broker and watch the monitor show a real ISR shrink
and leader re-election, so I can observe Kafka's replication guarantees the same hands-on way I
already observe Spark's worker-kill recovery (Fault Tolerance & Lineage, US-C9).

- *Given* a live multi-broker (N>=2) spawn with the monitor's Kafka tab open, *when* I manually run
  `docker stop spark-kafka-2` (or `docker kill`) outside the app, *then* the collector's next
  KafkaSnapshot reflects the broker as offline (broker grid) and any partition it led as
  re-elected to a surviving broker, sourced from the same CLI-shellout layer as US-MBK2 — no new
  in-app control triggers the kill.
- *Given* consecutive `KafkaSnapshot`s taken across that kill, *when* a partition's ISR set loses a
  replica id between one snapshot and the next, *then* an ISR-shrink event (partition, dropped
  replica id, timestamp) is appended to a bounded in-memory ring buffer (mirroring the existing
  `DASHBOARD_HISTORY_LENGTH` idiom already used for the node CPU/RAM sparkline buffers) — this is
  what turns the Kafka tab's ISR-shrink-events feed and under-replicated-partitions diagnostics
  from "structurally present, always empty" (US-MBK4) into genuinely exercisable.
- *Given* the ISR-shrink diff logic, *when* a killed broker is later restarted and rejoins the ISR
  set, *then* the corresponding ring-buffer entries are not retroactively removed (the buffer
  records historical events, not current state) — current cluster health is read from the live
  snapshot, not inferred from the event history.
- *Given* the broker-kill demo, *when* a learner wants to try it, *then* a short `docs/` or
  `concept.md` callout documents the manual step (e.g. "try `docker stop spark-kafka-2` while
  watching the monitor") — matching this project's existing documented-manual-kill convention
  (Structured Streaming's checkpoint-recovery demo, Fault Tolerance & Lineage's worker-kill demo).
  No new UI control (button, confirmation dialog, etc.) is built to trigger the kill from inside
  the app.
- *Given* this sub-story's dependency on having something to observe, *when* it is sequenced,
  *then* it lands last, after both US-MBK2 (data layer) and US-MBK4 (panel UI) exist.

## Scope note — sequencing across the five sub-stories

Matches the plan's own execution sequence and this repo's established precedent (v1.0's 6-issue
split, v1.1's 4-issue split, both under one release milestone): US-MBK1 (topology + drawer config)
is testable standalone via live `docker exec`/`kafka-topics.sh` commands, with no UI or
observability-layer work needed — it should land and be reviewed first, since every other
sub-story either builds on the multi-broker topology it establishes or is independently reviewable
against it. US-MBK2 (observability data layer) depends only on US-MBK1 being mergeable, and is
itself testable standalone via `collect_once()` with no UI. US-MBK3 (JMX exporter) is additive to
US-MBK2's data structures — sequenced after it, not a prerequisite for the rest of the panel.
US-MBK4 (monitor panel UI) depends on US-MBK2 for its real-data sections and is checked against the
design mockup once built; it does not require US-MBK3 to land first, since several of its sections
render real data from day one independent of JMX. US-MBK5 (broker-kill demo) is sequenced last,
since it needs both US-MBK2's data layer and US-MBK4's panel to have something to observe. Splitting
these into five separate, independently-reviewable GitHub issues happens after the architect ADR,
not at this requirements stage — mirroring how v1.1's four sub-story issues (#52-#55) were filed
only after its architect pass, and #50's single issue before it.

**Cross-release sequencing (restated for traceability, established in `docs/backlog.md` row #40):**
this release must land **before** v1.1's still-unstarted sub-story issues
([#52](https://github.com/hoanghaithanh/Spark-Playbook/issues/52),
[#53](https://github.com/hoanghaithanh/Spark-Playbook/issues/53),
[#54](https://github.com/hoanghaithanh/Spark-Playbook/issues/54),
[#55](https://github.com/hoanghaithanh/Spark-Playbook/issues/55)) begin active development. Those
sub-stories build the streaming producer, Spark job, and dashboard against whatever broker topology
is running; they should target this release's multi-broker cluster and RF=3 policy from the start,
rather than being built against #50's single-node broker and needing rework once v1.2 lands. This
mirrors how row #19 (Kafka infra, Sprint 10) was itself a prerequisite consumed by row #18 (v1.1) —
the same "infra before the thing that depends on it" sequencing, one level up.

## Open questions

1. **Exact JMX MBean names for heap %, produce/fetch latency percentiles, and request-handler-idle
   %** — explicitly not resolved by this doc (see US-MBK3). Must be verified live against the real
   broker's MBean tree during implementation, not guessed from documentation. Flagged for the
   architect/developer step.
2. **Exact Kafka CLI output-parsing shapes** for `kafka-topics.sh --describe`,
   `kafka-consumer-groups.sh --describe`, `kafka-metadata-quorum.sh describe --status`,
   `kafka-log-dirs.sh --describe`, and `kafka-run-class.sh kafka.tools.GetOffsetShell` — the plan
   drafts these as "verified shapes, not assumed" but flags them for re-confirmation live during
   implementation per this project's "verify by running it" discipline. Left to the architect/
   developer, not asserted as final here.
3. **Exact bounded-latency/collector-sub-cadence figure** for the heavier CLI shellouts (drafted as
   "every 5th ~2s cycle ≈10s" in the plan) — a reasonable placeholder, not a locked number; left to
   the architect/developer to confirm or adjust based on how expensive the CLI calls prove to be
   live.
4. **Design-mockup deviations, if any** (e.g. a prescriptive "fix"/"suggestion" element analogous to
   the one `realtime-monitoring-dashboard.md` Decision D-A removed from that panel's mockup) — not
   pre-judged here since the mockup itself was not re-inspected line-by-line as part of writing this
   doc; if the architect finds prescriptive content in `Kafka Cluster Monitor.dc.html`, the same
   G3 "signal, not conclusions" precedent should apply, documented explicitly as a deliberate
   deviation, not silently dropped.

## Constraints

- **Broker count: user-configurable, 1-5, default 3** — mirrors the existing `worker_count` field's
  min/max/default pattern exactly (confirmed decision, not reopened by this doc).
- **Resource ceiling formula**: Kafka's contribution to `renderer.validate()`'s total becomes
  `config.KAFKA_MEMORY_GB * kafka_broker_count` (replacing #50's flat `+2GB`), mirrored in
  `compose/cli.py`'s `_validate_ranges`. At the default 3 brokers this is +6GB; the streaming
  topic's existing 3×4GB-worker default totals `1+12+2+6 = 21GB`, comfortably under the 32GB
  ceiling.
- **RF=3, min-insync-replicas=2** as the default replication policy, downgraded to
  `min(3, kafka_broker_count)` when the user configures fewer than 3 brokers — RF can never exceed
  the broker count.
- **Design mockup source**: `https://claude.ai/design/p/911c0961-ad6e-4cb2-bee2-e117ad1e3f2e`
  (project "Kafka cluster monitoring dashboard", file `Kafka Cluster Monitor.dc.html`, fetched via
  the DesignSync MCP tool) is the visual spec for US-MBK4 specifically — not for any other
  sub-story.
- **No new host-published ports beyond the per-broker loopback data-listener ports already
  described in US-MBK1** — the JMX exporter is scraped in-cluster only (US-MBK3), no new port is
  published for it.
- **Cross-worktree collision safety (#38) must remain intact**, confirmed rather than assumed
  (US-MBK1's dedicated acceptance criterion) — `compose_ops.running_owner()` and
  `docker_stats.list_container_ids()`'s project-label scoping is unaffected by broker count.
- **Security-auditor re-confirmation, not a fresh full pass**: this work is not newly triggered by
  auth/secrets/PII/payments concerns (no new external API or secrets — that remains v1.1's
  concern), but per the plan's own execution sequence, security-auditor should re-confirm the
  port-surface change (N loopback ports instead of 1) stays within the existing minimal-surface
  posture established by `public-deploy.md`.
- **Notebook cleanliness**: per CLAUDE.md, any `content/*/notebook.ipynb` executed during
  development/verification of this work must be reset (`git checkout -- <path>`) before the work is
  considered done — no notebook is expected to be touched by this release's scope, but the
  constraint is restated for completeness since live cluster testing is extensive here.
- **Standard repo hygiene**: full `docker ps -a` / `git status` cleanup after any live testing
  (multiple sub-stories require live multi-broker spawns and broker-kill exercises).

## Sequencing note (restated per task instructions)

This release must land before v1.1's `#52`-`#55` (backlog row #18) begin active development —
already established in `docs/backlog.md` row #40's "New release milestone" section; restated here
for traceability so this requirements doc is self-contained on the point.
