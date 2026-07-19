# ADR: Conditional Kafka (KRaft) in the compose lifecycle + synthetic producer (Phase 3 infra)

Status: Accepted (both open questions resolved by human 2026-07-19) · Date: 2026-07-19
Drives: GitHub issue #50 (backlog row #19), Sprint 10 milestone #12
Requirements: `docs/requirements/spark-playbook-mvp.md` (G9, US-3.1, US-3.2, US-3.3), PLAN.md §1/§2/§5 Phase 3
Unblocks: backlog row #18 (Structured Streaming + Kafka curriculum topic — out of scope here)
Builds on: `compose/templates/docker-compose.yml.j2`, `compose/templates/spark-defaults.conf.j2`,
`compose/cli.py`, `compose/Dockerfile.spark`, `app/lifecycle/renderer.py`, `app/topics/loader.py`,
`app/web/routes/topics.py`, `app/config.py`, `docs/architecture/worktree-cluster-isolation.md`,
`docs/architecture/public-deploy.md`

---

## Context

Every sprint since Phase 1 added curriculum *content* against a settled engine. This story is
different in kind: it puts a **brand-new service (Kafka) into the compose lifecycle itself**, the same
class of infra work as the public-deploy pass. Three things must land together and stay within the
existing lifecycle's guarantees: (1) Kafka appears in the stack **only for streaming topics** and
stays absent otherwise; (2) it plugs into the single-slot spawn/teardown state machine and the
issue-#38 cross-worktree collision guard **without** reintroducing a collision or port-surface
problem; (3) a **synthetic producer** exists that publishes bounded, rate-controlled traffic a
streaming notebook can genuinely exercise watermarks and checkpoint-recovery against (G1: real
behaviour, not toy data).

The groundwork already exists and constrains the design: the manifest schema already carries
`requires_kafka: bool` (loaded into `Topic.requires_kafka`, `app/topics/loader.py:233`), PLAN.md §2
already reserves an `include_kafka` template variable and a Kafka slot in the resource ceiling, and
PLAN.md R4 already relies on `docker compose down --remove-orphans` to sweep a stray Kafka container
left by a prior streaming stack. So the wiring path is mostly pre-cut; this ADR resolves the four
open questions the Sprint 10 proposal flagged and specifies the concrete changes.

The single load-bearing decision is **D2 — Kafka is a new service inside the *same* `sparkpb`
compose project.** Get that wrong (a separate project) and both the single-slot state machine and the
#38 ownership guard silently stop covering it.

---

## Decision

### D1 — "Conditional" means an opt-in template flag driven by the topic manifest, not a runtime profile

`include_kafka` is a boolean Jinja2 template variable (PLAN.md §2's reserved row), defaulting `false`.
It is **not** a user choice in the cluster drawer and **not** a compose `profile` — it is a property
of the *topic*: the spawn route already loads the `Topic`, so it sets `include_kafka =
topic.requires_kafka` server-side. A non-streaming topic (`requires_kafka: false`, the value in all 14
existing manifests) renders no `kafka` service (US-3.1 given/then #1); the streaming topic
(`requires_kafka: true`) renders exactly one KRaft broker (US-3.1 given/then #2). Because the flag is
baked into the rendered compose file at spawn time, teardown/respawn between a streaming and a
non-streaming topic naturally drops or adds the broker, and `down --remove-orphans` (already in
`compose_ops.down()`) sweeps a leftover Kafka container from a prior streaming stack (PLAN.md R4).

Rejected the always-on alternative outright: an always-running broker would sit in **every** spawn
(13 of 14 topics don't need it), burning ~2 GB against the 32 GB ceiling for nothing and adding a
listener to the surface on every non-streaming exercise — directly against G1 (spend the budget on
cluster realism where it's taught) and the public-deploy minimal-surface posture.

### D2 — Kafka is a new service in the *same* `sparkpb` compose project (load-bearing)

The broker is one more service in the existing `docker-compose.yml.j2`, under the existing
`name: sparkpb` project and on the existing `sparkpb-net` bridge — **not** a separate compose project
or a sidecar stack. This is the load-bearing choice and it is what makes collision-safety free:

- **Single-slot state machine (`manager.py`) covers it unchanged.** `up`/`down` are project-scoped
  (`docker compose -p sparkpb ...`); a service inside that project is spawned and torn down by the
  exact same awaited `down`-before-`up` barrier (PLAN.md R4 / D5 cancel-and-replace). No new lifecycle
  code.
- **The #38 ownership guard covers it unchanged.** `compose_ops.running_owner()` reads
  `com.docker.compose.project.working_dir` off the *first* container of the `sparkpb` project; every
  container in the project (Kafka included) carries that label, so a foreign-worktree streaming
  cluster is refused before `up` by the existing guard — no template or guard change
  (`docs/architecture/worktree-cluster-isolation.md`).
- **Fixed `container_name: spark-kafka` / `hostname: kafka`** follows the same fixed-name convention
  as `spark-master`/`spark-driver`. Fixed names *do* collide across worktrees — but that is already
  true of every existing service and is exactly what the #38 guard (not per-name uniqueness) is there
  to prevent. Adding Kafka to a separate project would put it *outside* that guard and outside the
  single-slot `down`, which is precisely the collision class #38 fixed. So "same project" is the
  collision-safe choice, not merely the convenient one.

### D3 — Two listeners: in-cluster (`kafka:9092`) + loopback host-published (`127.0.0.1:9092`)

The broker exposes **two PLAINTEXT data listeners** (KRaft's standard multi-listener pattern), each
advertising the address its client type can actually reach:

- **`PLAINTEXT` on `:9092`, advertised as `kafka:9092`** — the in-cluster listener. Both Kafka
  consumers on `sparkpb-net` use it: the Spark **driver** (Structured Streaming source) and the
  **producer** when run via `docker exec` in the driver (D5). Resolved by container DNS, exactly as
  `spark://spark-master:7077` is.
- **`PLAINTEXT_HOST` on `:29092` in-container, published to `127.0.0.1:9092` on the host, advertised
  as `127.0.0.1:9092`** — the host listener, so `produce.py` can also run from a **host shell**
  outside Docker (human-confirmed requirement, OQ-1 resolved 2026-07-19). The two listeners must be on
  *distinct in-container ports* under distinct names (Kafka rejects two listeners on one port), and
  each advertised address must be the one its own client can reach — an in-cluster client handed
  `127.0.0.1:9092` would dial its own container (or fail outright), and a host client handed
  `kafka:9092` couldn't resolve it. This is the well-known dual-listener KRaft idiom, not a bespoke
  scheme.
  **Implementation deviation (127.0.0.1 vs the `localhost` drafted below):** Kafka's client protocol is
  two-hop — the client bootstraps, then reconnects to whatever address the broker's metadata response
  advertises for that listener. `produce.py`'s host-run default was fixed to `127.0.0.1:9092` (D5)
  because `localhost` resolves IPv6-first on the dev host and nothing is published on `::1`. If
  `PLAINTEXT_HOST` still advertised `localhost:9092`, a host client's *bootstrap* would succeed via
  `127.0.0.1` but the *second-hop* reconnect (using the advertised address verbatim) would hit the same
  IPv6 dead end. Advertising `127.0.0.1:9092` instead keeps both hops consistent — the same fix pattern
  as `produce.py`'s default, applied to the other half of the connection. This is R-K6's mismatch class,
  caught in implementation rather than needing a live smoke test to surface it.

This **honors PLAN.md §1's host-port-map row (line 195)** — which listed `9092` host-published for
"Producer + driver" — scoped to **loopback (`127.0.0.1`) only**, matching the public-deploy D2
minimal-surface posture (the same loopback scoping already applied to `8080`/`4040`/`8888`). The added
surface is one loopback-bound port, on streaming spawns only: small and deliberate, not nil (see
Consequences).

KRaft listener config (single combined broker+controller node, no ZooKeeper — G9):

```yaml
  kafka:
    image: apache/kafka:3.9.0        # official ASF image, native KRaft; matches apache/spark provenance
    container_name: spark-kafka
    hostname: kafka
    environment:
      KAFKA_NODE_ID: "1"
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      # Two data listeners (D3): PLAINTEXT for in-cluster (advertised as the DNS
      # name kafka:9092), PLAINTEXT_HOST for the loopback host publish (advertised
      # as 127.0.0.1:9092). Distinct in-container ports (9092 vs 29092) are required.
      #
      # DEVIATION (implementation, not this draft): PLAINTEXT_HOST advertises
      # 127.0.0.1:9092, not localhost:9092 as drafted below. Kafka's client
      # protocol is two-hop -- bootstrap, then reconnect to whatever address
      # this listener advertises for the actual produce/consume traffic. A
      # host client bootstrapping via 127.0.0.1:9092 (required because
      # "localhost" resolves IPv6-first with nothing published on ::1, see
      # produce.py's docstring) that then got redirected to "localhost:9092"
      # for the second hop would hit the exact same resolution failure one
      # hop later -- textbook R-K6. Both hops must agree, hence 127.0.0.1 on
      # both. Caught and fixed in the implementation, not discovered live.
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092,PLAINTEXT_HOST://127.0.0.1:9092"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"     # producer's first send creates the demo topic
      KAFKA_HEAP_OPTS: "-Xmx512m -Xms512m"        # bounded heap under the 2GB container limit
      KAFKA_LOG_RETENTION_MINUTES: "10"           # bounded disk (D6)
      KAFKA_LOG_RETENTION_BYTES: "536870912"      # 512MB size cap per partition (D6)
      CLUSTER_ID: "sparkpb-kafka-kraft-0001"      # fixed → deterministic storage format
    ports:
      - "127.0.0.1:9092:29092"   # host 9092 → PLAINTEXT_HOST listener (29092), loopback only (D3)
    networks: [sparkpb-net]
    depends_on: [spark-master]
    deploy:
      resources:
        limits:
          cpus: "1"
          memory: 2g
    restart: "no"
```

No `volumes:` for Kafka log dirs — the broker's data lives in the container's ephemeral writable
layer, so `docker compose down` wipes it (D6). The whole repo is still bind-mounted at `/workspace` on
the driver as today; Kafka needs no repo mount.

### D4 — Spark's Kafka SQL connector is baked into the image, not fetched at runtime

`compose/Dockerfile.spark` gains the `spark-sql-kafka-0-10_2.13:4.0.3` jar and its transitive deps
(`kafka-clients`, `commons-pool2`, `spark-token-provider-kafka-0-10`) into `$SPARK_HOME/jars/` at
build time. The connector is inert unless a query actually reads/writes Kafka, so baking it costs a
few MB of image size and **nothing** at runtime for the 13 non-streaming topics — while making every
spawn deterministic and offline-safe. Rejected the `spark.jars.packages=spark-sql-kafka-0-10...`
runtime-`--packages` mechanism (PLAN.md line 63's sketch): it triggers a Maven fetch on first
`readStream`, which is slow and fails on an offline/firewalled box — the opposite of the reproducible
"real cluster on demand" the tool promises. The same layer adds the **`kafka-python`** client library
(pure-Python, no native build deps) for the producer (D5).

### D5 — The synthetic producer is a real, structured dependency that runs in-cluster

Per PLAN.md §5 it is *not* a throwaway: `tools/kafka_producer/produce.py` is a standalone,
rate-controlled CLI (mirroring the `tools/datagen/generate.py` two-layer pattern PLAN.md already
established), with a thin importable wrapper `driver/playbook/producer.py` the streaming notebook
(#18) can call. It can run either **inside the driver container** (`docker exec ... python /workspace/tools/
kafka_producer/produce.py --topic events --rate 100`, reaching `kafka:9092` by DNS) **or from a host
shell** (`python tools/kafka_producer/produce.py --bootstrap 127.0.0.1:9092 ...`, reaching the loopback
host publish, D3 / OQ-1). The bootstrap server is a `--bootstrap` flag defaulting to `kafka:9092` for
the common in-driver case; host runs pass `127.0.0.1:9092` (not `localhost:9092` — see D3's
implementation-deviation note and R-K6). A host run needs `kafka-python` installed
on the host (`pip install kafka-python`); the in-driver run gets it from the baked image (D4).

Scope and shape (production-adjacent care, since #18 depends on it and it must survive a
checkpoint-recovery demo):

- **Rate control (US-3.2):** `--rate <events/sec>` paced with a simple sleep-per-batch loop; publishes
  until `SIGINT`/`SIGTERM`. Approximate, not hard-real-time — matches US-3.2's "approximately the
  requested rate".
- **Bounded, realistic-but-modest volume (G1 / OQ-4):** default topic `events`, `--partitions 3`
  (matches the default 3-worker parallelism so the stream fans out across executors), JSON payload
  with an **event-time timestamp** field (so watermark/late-data lessons are possible), a **key**
  (drawn from a small key space so stateful aggregation has real groups), and a value. A `--late-frac`
  knob emits a small fraction of events with a back-dated timestamp so #18 can demonstrate
  past-watermark dropping. These are the *minimal viable* schema/knobs for the infra to be exercisable;
  #18 refines the exact lesson schema (flagged OQ-2).
- **Independent of the streaming query's lifecycle.** The producer is launched as its own process
  (a background cell / driver terminal in #18), **not** from the same kernel that runs the streaming
  query — so stopping and restarting the query against its checkpoint (US-3.2 / US-3.3) doesn't stop
  the data. The `produce.py` CLI is the same either way; how #18 launches it is #18's call.
- **Input validation at the boundary** (G4's datagen precedent): reject non-positive `--rate`,
  non-positive `--partitions`, empty `--topic` with a clear error, not a silent no-op.
- **One runnable check:** a `--self-check` mode (or `test_produce.py`) that publishes N events to a
  throwaway topic and asserts N were accepted — the smallest thing that fails if the produce loop or
  connection breaks.

### D6 — Bounded retention so a long-running demo broker can't fill the disk

Two independent bounds, both on the broker (D3's config block): **time** (`LOG_RETENTION_MINUTES=10`)
and **size** (`LOG_RETENTION_BYTES=512MB` per partition), and **ephemeral storage** (no named/host
volume, so `down` wipes it). At the default 100 ev/s with small JSON payloads, 10 minutes is well
under the size cap, so steady-state disk is bounded regardless of how long a single streaming session
runs. This answers the retention/cleanup open question without any host-side cron or manual sweep —
the disposable-cluster model already does the coarse cleanup on teardown; retention handles the
within-session bound.

### Resource-ceiling accounting (US-3.1 "within the resource budget")

`renderer.validate()` (and its `compose/cli.py::_validate_ranges` mirror) add **2 GB for Kafka when
`include_kafka`** to the existing `master(1) + Σworker + driver(2)` total, matching PLAN.md §2's
`+ (kafka ≈ 2GB if included)`. The streaming topic's `cluster_defaults` (3×4 GB workers) totals
`1 + 12 + 2 + 2 = 17 GB` — comfortably under the 32 GB ceiling, so no ceiling change is needed and no
default worker reduction is forced. The 2 GB is a conservative reservation (the broker's actual heap
is capped at 512 MB); it is a tunable number, not a design constraint.

---

## Alternatives considered

| Decision | Alternative | Why not |
|---|---|---|
| D1 manifest-driven flag | Always-on Kafka in every spawn | ~2 GB + a listener in 13/14 topics that never touch streaming; against G1 and the minimal-surface posture. |
| D1 template flag | Compose `profiles:` (`--profile kafka`) | The app shells `docker compose up -d` with a fixed invocation; a profile means threading `--profile` through `compose_ops`/`cli.py` for no gain over a render-time boolean that already fits the existing template-variable machinery. |
| D2 same project | Separate `sparkpb-kafka` compose project | Puts Kafka outside the single-slot `down` and outside the #38 ownership guard — reintroducing exactly the cross-stack collision class #38 closed. Strictly worse. |
| D3 loopback host publish + in-cluster listener | Bind the host publish to `0.0.0.0:9092` | Loopback (`127.0.0.1`) is the public-deploy D2 scoping already used for every other host port; `0.0.0.0` would expose the broker on all interfaces for no benefit on a single-user tool. |
| D3 dual listener | In-cluster listener only, no host port | Rejected per OQ-1: the human wants `produce.py` runnable from a host shell, not only via `docker exec`. One loopback-bound port on streaming spawns is the deliberate, minimal cost of that. |
| D4 baked jar | Runtime `spark.jars.packages` Maven fetch | Slow first-`readStream`, fails offline; breaks the reproducible on-demand promise. Baking is inert when unused. |
| D5 in-driver producer | A `producer` compose service that auto-produces | Can't rate-control on demand or start/stop from the notebook; runs even when unwanted; and an always-producing service muddies the checkpoint-recovery lesson. A CLI the learner launches is simpler and matches US-3.2's "started with a rate … until stopped". |
| D6 ephemeral + retention | Named volume for Kafka logs | Durable Kafka state across respawns is pointless for a disposable teaching cluster and only accumulates disk; ephemeral + short retention is less config and self-cleaning. |

Simpler options rejected because a real constraint forbids them (per the ADR discipline): dropping the
resource-ceiling Kafka accounting (US-3.1 requires staying in budget), dropping producer input
validation (G4's boundary-validation precedent), and dropping retention bounds (D6 — an unbounded
demo broker fills the disk). None were simplified away.

---

## Consequences

**Accepted trade-offs:**

- **`include_kafka` must be threaded through four layers** — `Topic.requires_kafka` → `ClusterParams`
  → `renderer.render()` context → the `{% if include_kafka %}` template block, plus a `--include-kafka`
  flag on the standalone `compose/cli.py`. This is the same threading `aqe_enabled` already has; it is
  boilerplate, not new architecture, but it is four small edits that must stay in sync (the CLI mirror
  is the easy one to forget — `compose/README.md` already notes the mirror obligation).
- **A fifth service widens the `sparkpb` project's blast radius by one container.** Teardown, the #38
  guard, and `--remove-orphans` all already operate at project granularity, so they absorb it for free
  — but `docker ps` for a streaming spawn now shows five service types, and a wedged Kafka container is
  one more thing a failed-spawn diagnosis has to consider.
- **One loopback-bound host port (`127.0.0.1:9092`) is published on streaming spawns** (OQ-1
  resolution) so `produce.py` can run from a host shell. This is small but nonzero added surface — not
  the zero-surface option — accepted deliberately for the host-run convenience. It is scoped to
  loopback only (never `0.0.0.0`), on streaming spawns only, and there is no auth on it: consistent
  with the tool's single-trusted-user, loopback-only threat model (same posture as the unauthenticated
  `8080`/`4040`/`8888` publishes), not a new exposure class. A host run also requires `kafka-python`
  installed on the host, unlike the in-driver run.
- **The producer is real code with real upkeep.** Unlike a throwaway, `produce.py` + the
  `driver/playbook` wrapper are a dependency #18 builds on, so they carry validation, a self-check, and
  a stable CLI contract — more care than a scratch script, which is the intended cost of it being a
  curriculum dependency rather than a one-off.
- **2 GB of the 32 GB ceiling is reserved for Kafka on streaming spawns** even though the broker heap
  is capped far lower — a deliberate conservative margin (JVM off-heap, page cache) that slightly
  narrows the worker budget for streaming topics only.

**What becomes harder:** multi-broker / partitioned-broker realism, or Kafka state that survives a
respawn, are now *further* away — the single ephemeral broker is deliberately the floor that teaches
the Structured Streaming semantics interviews probe, not a production Kafka. That is the intended
boundary (G1: interview-depth on Spark's streaming behaviour, not Kafka operations), not an oversight.

---

## Component / data design

Wiring path (each arrow is an existing seam this reuses, except the two marked NEW):

```
content/structured-streaming/manifest.yaml
   requires_kafka: true
        │  (loaded, already supported)
        ▼
app/topics/loader.py  Topic.requires_kafka  ──────────────┐
        │                                                  │
app/web/routes/topics.py::spawn_cluster                    │
   params = ClusterParams(..., include_kafka=topic.requires_kafka)   ← NEW field
        │
        ▼
app/lifecycle/renderer.py
   validate(): total += 2GB if include_kafka   ← NEW (ceiling)
   render():  context["include_kafka"] = params.include_kafka   ← NEW
        │
        ▼
compose/templates/docker-compose.yml.j2
   {% if include_kafka %}  kafka service (D3 block)  {% endif %}   ← NEW
        │  docker compose -p sparkpb up -d  (unchanged: manager → compose_ops)
        ▼
┌── project: sparkpb, network sparkpb-net (all existing services + …) ──────────┐
│  spark-master  spark-worker-1..N  spark-driver (Jupyter, /workspace mount)     │
│                                                                                 │
│  kafka (spark-kafka)                                                            │
│    listener PLAINTEXT      → advertised kafka:9092       (in-cluster, D3) ──┐     │
│    listener PLAINTEXT_HOST → advertised 127.0.0.1:9092   (host publish) ──┐ │     │
│        ▲ produce (in driver)                        ▲ readStream         │ │     │
│  tools/kafka_producer/produce.py ──► kafka:9092 ◄── Spark driver ────────┘ │     │
│        (baked kafka-python)              (baked spark-sql-kafka jar, D4)    │     │
└──────────────────────────────────────────────────────────────────────────┼──────┘
              host loopback publish  127.0.0.1:9092 → container :29092 ──────┘
                     ▲
       produce.py --bootstrap 127.0.0.1:9092   (host shell, OQ-1 resolved; both hops now agree)
        ▲
   #38 ownership guard + single-slot down/up already cover the whole project (D2)
```

**Files touched (developer handoff):**
- `compose/templates/docker-compose.yml.j2` — add the `{% if include_kafka %}` `kafka` service (D3),
  including the dual-listener KRaft env + the `127.0.0.1:9092:29092` loopback host publish (OQ-1);
  document it in the header block (which currently says "No Kafka service … No `include_kafka`
  conditional yet").
- `compose/Dockerfile.spark` — add the `spark-sql-kafka-0-10` connector jar set + `kafka-python` (D4);
  update the "Deliberately NOT included yet" note.
- `compose/cli.py` — `--include-kafka` flag on `render`; add 2 GB to `_validate_ranges` when set;
  pass `include_kafka` into the render context (mirror of the app path).
- `app/lifecycle/renderer.py` — `ClusterParams.include_kafka: bool = False`; `validate()` ceiling +2 GB;
  `render()` context key.
- `app/web/routes/topics.py::spawn_cluster` — set `include_kafka=topic.requires_kafka` on the params.
- `tools/kafka_producer/produce.py` (NEW; `--bootstrap` defaults to `kafka:9092`, host runs pass
  `127.0.0.1:9092`, OQ-1/R-K6) + `driver/playbook/producer.py` (NEW thin wrapper) +
  `tools/kafka_producer/README.md`.
- `content/structured-streaming/manifest.yaml` — created by #18, not here; #50 only guarantees
  `requires_kafka: true` will flip Kafka on. **No** streaming notebook/concept is in #50's scope.

No change to `manager.py`, `compose_ops.py`, the #38 guard, the annotation engine, or any existing
topic's content.

## Visual / UX surface

Not a UI-facing feature — no new page, drawer control, or layout. The cluster drawer is unchanged
(Kafka is topic-driven, not a user toggle, D1). The only observable UI difference is second-order and
belongs to #18 (the live query-progress chart, US-3.3). So there is no mockup/visual-spec to check
here; the acceptance evidence for #50 is functional (US-3.1/US-3.2), per the acceptance criteria.

---

## Open questions (flagged, not blocking — resolvable at the #50 or #18 checkpoint)

Consistent with this repo's "flag, don't block" pattern (`topic-shell-redesign.md`), two items are
surfaced for a human preference rather than guessed silently. Neither blocks starting #50.

- **OQ-1 — RESOLVED (human, 2026-07-19): publish `127.0.0.1:9092` too.** The question was whether the
  producer would only ever run in-cluster (no host port) or also from a host shell outside Docker. The
  human confirmed they want to run `produce.py` from a host shell, not only via `docker exec`. D3 now
  specifies the dual-listener KRaft pattern: in-cluster `PLAINTEXT`/`kafka:9092` **plus** a loopback
  host publish `127.0.0.1:9092 → PLAINTEXT_HOST` advertised as `localhost:9092`. The added surface (one
  loopback-bound port on streaming spawns) is noted in Consequences as a deliberate, accepted trade-off.
  *Sections updated for this resolution:* D3 (heading, prose, YAML `KAFKA_LISTENERS`/
  `KAFKA_ADVERTISED_LISTENERS`/`KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` + new `ports:` block), D5
  (producer `--bootstrap` flag, host vs in-driver run), Alternatives (D3 rows), Consequences (host-port
  trade-off bullet), and the component/data-design diagram.

- **OQ-2 — RESOLVED (human, 2026-07-19): minimal now, #18 refines later** — exactly as recommended.
  #50 ships the minimal viable producer schema (a generic keyed JSON event with an event-time field, a
  small key space, a `--late-frac` knob, default topic `events`, 3 partitions, ~100 ev/s) and does
  **not** pre-bake #18's lesson schema; #18 refines `produce.py`'s defaults/knobs as it builds the
  streaming notebook (fields, key semantics, window size, the US-C7 state-growth-vs-plateau shape). No
  design change to this ADR from the resolution — the D5 minimal-set decision stands as written.

---

## Risks

- **R-K1 — A non-streaming spawn still stands up Kafka (US-3.1 regression).** If `include_kafka` is
  wired to a default-`true`, or the template `{% if %}` is malformed, every spawn gets a broker.
  *Noticed by:* US-3.1 given/then #1 — spawn a non-streaming topic, assert `docker ps` shows no
  `spark-kafka`. *Mitigation:* flag defaults `false`; the streaming topic is the *only* manifest with
  `requires_kafka: true`; test-engineer asserts both branches.
- **R-K2 — Cross-worktree collision if Kafka is (mis)placed in its own project.** The whole
  collision-safety story (D2) rests on Kafka being *inside* `sparkpb`. A refactor that splits it out
  silently drops it from the #38 guard and the single-slot `down`. *Noticed by:* a streaming spawn from
  worktree B tearing down worktree A's live Kafka, or a `spark-kafka` container surviving a `-p sparkpb
  down`. *Mitigation:* D2 is explicit; the service lives in the one template; `--remove-orphans`
  (already present) is the backstop.
- **R-K3 — Baked connector jar version drift on a Spark bump.** `spark-sql-kafka-0-10_2.13` must match
  the Spark version (4.0.3) exactly, like every other Spark jar. A future image bump that forgets the
  connector re-pin gets a `ClassNotFound`/version-mismatch on `readStream`. *Noticed by:* the streaming
  notebook failing at the Kafka source, not at spawn. *Mitigation:* pin the connector version to the
  `apache/spark` tag in `Dockerfile.spark` next to the base `FROM`, with a comment tying them together
  (same discipline as the existing py4j-glob note).
- **R-K4 — KRaft broker not ready when the driver connects.** `depends_on` only waits for container
  start, not broker readiness; a `readStream` issued in the first second can hit a not-yet-listening
  broker. *Noticed by:* a transient connection-refused on the streaming notebook's first run, clearing
  on retry. *Mitigation:* the producer's own connect (and Spark's source) retry; if it proves flaky,
  the readiness step (`app/lifecycle/readiness.py`) can gain a cheap `kafka:9092` TCP-open poll — noted
  as a small follow-up, not built speculatively (YAGNI until observed).
- **R-K5 — Producer outpaces or under-feeds the demo, or fills disk.** A high `--rate` with the 512 MB
  retention cap, or a runaway loop, could stress the broker. *Noticed by:* Kafka OOM/restart, or the
  streaming batch input-rate chart (#18) reading flat/zero. *Mitigation:* D6's time+size retention and
  the 512 MB heap cap bound it; `--rate` validation rejects nonsense; the default 100 ev/s is
  deliberately modest for a 4 GB/2-core worker (G1: a clear signal, not a stress test).
- **R-K6 — Dual-listener advertised-address mismatch (OQ-1's cost).** The classic Kafka footgun: a
  client connects to the bootstrap port, then reconnects to whatever *advertised* address the broker
  hands back. If `PLAINTEXT` advertised a container-unreachable address (or the two listeners shared an
  in-container port), an in-cluster client would be redirected to its own container and hang, and vice
  versa for a host client. *Noticed by:* a producer/`readStream` that connects then times out on the
  *second* hop (metadata fetch succeeds, produce/consume stalls) — the tell-tale of a wrong advertised
  address, not a down broker. *Mitigation:* D3 pins each advertised address to the reachable-by-that-
  client value (`kafka:9092` for `PLAINTEXT`, `127.0.0.1:9092` for `PLAINTEXT_HOST`) on distinct
  in-container ports (9092 vs 29092).
  **Status update:** this exact instance — `PLAINTEXT_HOST` drafted as `localhost:9092`, which would
  have reproduced the same IPv6-resolution failure `produce.py`'s `--bootstrap` default already worked
  around, one hop later — was caught and fixed pre-emptively during implementation (D3's
  implementation-deviation note), by reasoning through the two-hop protocol rather than needing a live
  smoke test to discover it. This specific host-vs-container-DNS pairing is now correct-by-construction.
  R-K6's *general* class of risk still stands, though: a future person editing
  `KAFKA_ADVERTISED_LISTENERS` (e.g. changing a hostname, adding a listener) could reintroduce a
  mismatch, and nothing in the template enforces the invariant beyond this comment — a live smoke check
  from both a `docker exec` and a host shell (test-engineer) remains worthwhile before #18 builds on
  this, and is the right way to catch any *future* regression in this config.
