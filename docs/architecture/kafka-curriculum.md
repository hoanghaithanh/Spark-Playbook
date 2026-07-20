# ADR: Kafka Curriculum — topics-index grouping, schema-registry infra (US-KC8), exactly-once spike (US-KC11)

Status: Draft for human review · Date: 2026-07-20
Drives: `docs/requirements/kafka-curriculum.md` (G-KC5, G-KC6, US-KC8, US-KC11, Open Questions 2 & 5), backlog rows #41–52
Formalizes the architect pass the requirements doc's "Resolved decisions" section and Open Questions 2/5 handed off.

**Scope of this doc:** the *three* things the Kafka curriculum needs decided before its blocked/gated
GitHub issues can be filed — (1) the topics-index Kafka/Spark grouping mechanism (G-KC5), (2) the
schema-registry + Avro infrastructure for US-KC8 (G-KC6), (3) the US-KC11 `kafka-python` transactional
feasibility spike (Open Question 5). The other 9 topics (US-KC1–7, 9, 10, 12) need **no** app/infra
decision — they are content-only against the shipped shell and the multi-broker cluster, and are not
covered here beyond the grouping change that affects how *all* 27 topics render.

**Relationship to existing ADRs (not a supersession):** this is additive. It reuses, without changing,
`docs/architecture/kafka-streaming-infra.md` (D2 same-project, D4 bake-don't-fetch, D6
ephemeral/bounded) and `docs/architecture/multi-broker-kafka-cluster.md` (D-MBK1 drawer-driven Kafka,
D-MBK2 per-broker listeners). The one prior decision this **amends** is the requirements doc's own
earlier framing, not an ADR: `kafka-curriculum.md`'s original Non-goals ruled out both a grouping
manifest field and a schema registry; the human reversed both on 2026-07-20 (G-KC5/G-KC6), which is
what this doc designs against.

---

## Context

The 12-topic Kafka curriculum was written to be *content-only* against the existing topic shell. Two
human decisions on 2026-07-20 pushed a small amount of app/infra work back into scope, and one
requirements open question asked for an empirical answer before a design could be committed:

1. **Grouping (G-KC5).** The topics-index page today renders one flat `order`-sorted grid of all
   topics (`topics_index.html`, `loader.list_topics()` sorted by `order`). With 12 Kafka topics landing
   next to 15 Spark topics, the human wants two visually distinct tracks, not one interleaved list.
2. **Schema registry (G-KC6).** US-KC8 (`kafka-serialization-schema-evolution`) was originally
   JSON-only because a registry was out of scope. The human reversed that: US-KC8 now needs a *real*
   registry that actually rejects an incompatible schema change — which means a new compose-stack
   service and the first pinned dependency departure from "`kafka-python` + baked connector jar only."
3. **Exactly-once spike (US-KC11, OQ-5).** `kafka-python==2.0.2` (the version baked into the driver
   image) has "a documented history of limited/flaky" transactional support; the doc explicitly refused
   to assume the answer and asked the architect to resolve it empirically before designing US-KC11.

The load-bearing decision is **D-KC2 (schema registry)** — it is the only one that adds a service, a
port, a resource line item, and a new dependency. D-KC1 (grouping) is a small loader+template change;
D-KC3 (spike) is a decision *not* to add app code, backed by a live API check.

---

## Decision

### D-KC1 — Grouping: a `track:` manifest field defaulting to `spark`; per-track `order`; two rendered sections

**Manifest field.** Add one optional field, `track: kafka | spark`, to the manifest schema.
`loader.load_topic()` reads `track = data.get("track", "spark")`; `Topic` gains `track: str = "spark"`.

**No edits to the 15 existing manifests (the load-bearing laziness).** The default is `spark`, so every
existing manifest — none of which has a `track` field — lands in the Spark track unchanged. Only the 12
new Kafka manifests carry `track: kafka` explicitly. This is 12 new-file lines vs. 15 existing-file
edits; the smaller, lower-risk diff, and it can't regress a shipped topic because it touches none of
them. The one cost — `spark` is an implicit default rather than declared on every topic — is acceptable
because the entire existing corpus is Spark and the default is documented at the field's one read site.

**`order` becomes per-track, not one global space.** Each track is sorted independently by its own
`order` (Spark keeps its existing 1..15; Kafka gets a fresh 1..12). Because the page now renders two
*separate* sections, a Spark `order: 1` and a Kafka `order: 1` never compete — they live in different
grids. This is simpler than re-basing the 12 Kafka topics into a global 16..27 space (which would
couple Kafka numbering to the Spark count and force a renumber every time either track grows). Grouping
is by `track`; ordering is by `order` *within* a track.

**Recommended `order` values for the 12 Kafka topics** (basic→advanced, matching the requirements doc's
G-KC2 ladder — this is the architect recommendation feeding Open Question 2; **final confirmation is the
human's**, since the doc explicitly deferred exact values to be revisited after this pass):

| order | topic id | tier |
|---|---|---|
| 1 | `kafka-architecture-kraft` | Basic |
| 2 | `kafka-topics-partitions` | Basic |
| 3 | `kafka-producers-delivery` | Basic |
| 4 | `kafka-consumers-groups` | Basic |
| 5 | `kafka-replication-fault-tolerance` | Intermediate |
| 6 | `kafka-log-compaction-retention` | Intermediate |
| 7 | `kafka-spark-structured-streaming` | Intermediate |
| 8 | `kafka-serialization-schema-evolution` | Intermediate |
| 9 | `kafka-performance-tuning` | Advanced |
| 10 | `kafka-monitoring-observability` | Advanced |
| 11 | `kafka-exactly-once-transactions` | Advanced |
| 12 | `kafka-multi-broker-cluster-ops` | Advanced |

**Rendering.** Section order is **Spark first, Kafka second** (the established 15 topics are the app's
primary identity; Kafka is the parallel second track). A small loader helper returns ordered groups:

```python
# app/topics/loader.py
_TRACK_ORDER = ["spark", "kafka"]          # section display order
_TRACK_LABELS = {"spark": "Spark", "kafka": "Kafka"}

def list_topics_by_track() -> list[tuple[str, list[Topic]]]:
    """Topics grouped for the two-section index (G-KC5). Returns
    [(label, [topics sorted by order]), ...] in _TRACK_ORDER; a track with no
    topics is omitted, so the page never renders an empty 'Kafka' heading before
    the first Kafka topic ships."""
    by_track: dict[str, list[Topic]] = {}
    for t in list_topics():                # reuse existing discovery + per-track order falls out of t.order
        by_track.setdefault(t.track, []).append(t)
    groups = []
    for track in _TRACK_ORDER:
        if by_track.get(track):
            groups.append((_TRACK_LABELS[track], sorted(by_track[track], key=lambda t: t.order)))
    # any unknown/future track value renders after the known ones rather than vanishing
    for track, topics in by_track.items():
        if track not in _TRACK_ORDER:
            groups.append((track.title(), sorted(topics, key=lambda t: t.order)))
    return groups
```

`routes/topics.py::index()` passes `list_topics_by_track()` instead of `list_topics()`;
`topics_index.html` iterates `{% for label, topics in tracks %}` rendering a `<h2 class="topics-track-heading">`
+ one `.topics-grid` per group (the existing card markup and `.topics-grid`/`.topic-card` CSS are
reused verbatim inside each section). `list_topics()` itself is unchanged — everything else that calls
it (the breadcrumb switcher's `all_topics`, `_shell_context`) keeps its flat list.

Ponytail check to leave behind: a `test_topics_loader.py` assert that a manifest without `track`
resolves to `"spark"`, and that `list_topics_by_track()` returns Spark before Kafka with each group
internally `order`-sorted.

### D-KC2 — Schema registry (US-KC8): Karapace as a manifest-gated ephemeral service; Avro via `fastavro`; loopback `:8081`

US-KC8 is the one topic that needs infra beyond the shipped stack. Four sub-decisions:

**(a) Product: Karapace** (`ghcr.io/aiven-open/karapace`, Apache-2.0). It implements the *exact*
Confluent Schema Registry REST API (register subject version, GET schema, `PUT /config` compatibility
mode, `POST /compatibility/...` checks) and — critically — **stores its schemas in a `_schemas` Kafka
topic on the existing broker**, needing no separate datastore or volume. That makes it fit the
ephemeral model perfectly: `docker compose down` wipes the broker's writable layer, and the registry's
state goes with it (D6 unchanged). Apache-2.0 keeps the repo's open-source-hygiene posture
(`release:v1.0` LICENSE pass) clean, and it is lighter than a second JVM. `concept.md` names Confluent
Schema Registry as the industry-standard sibling a learner will meet in interviews — the REST API, the
Avro-on-the-wire format, and the compatibility-mode semantics US-KC8 teaches are identical between the
two, so teaching value is preserved while the runtime stays Apache-2.0 and small.

**(b) Format: Avro only** (not Protobuf, not both). Avro is the canonical Kafka+registry pairing and
has the cleanest compatibility story for exactly the three moves US-KC8 must show — baseline
register/produce/consume, a backward-compatible change (add a field *with a default*), and a
backward-*incompatible* change (remove a required field) that the registry *rejects*. One format for a
single-notebook demo; Protobuf proves nothing additional here (YAGNI).

**(c) Client library: `fastavro` (the one new pinned dependency)** — pure-Python wheels, no system/C
build deps, so it layers into `Dockerfile.spark` next to the existing `kafka-python`/`pyarrow` installs.
The notebook keeps **`kafka-python` as the Kafka transport** (identical to the other 11 topics) and
talks to the registry over its plain REST API (register/compat-check via `urllib`/`requests`), doing
the Confluent wire-format framing explicitly (`magic byte 0x00` + 4-byte big-endian schema id + Avro
body). This is deliberately *more* educational than a black-box serializer — a learner who has framed
the bytes themselves understands what the registry-id-in-the-payload actually is — and it isolates the
*one* new concept (registry + Avro) instead of also swapping Kafka clients. See Alternatives for why
`confluent-kafka[avro]` was rejected.

**(d) Compose placement + ports + ceiling.** Manifest-gated exactly like Kafka itself, via a new
`requires_schema_registry: bool` manifest field → `include_schema_registry` render flag. The service
block is rendered only when *both* `include_kafka` and `include_schema_registry` are set (the registry
needs a broker to store `_schemas` in), inside the same `sparkpb` project on `sparkpb-net` (D2
unchanged, so the #38 guard and single-slot down/up cover it for free). US-KC8's manifest sets
`requires_kafka: true`, `requires_schema_registry: true`, `kafka_broker_count: 1` (a single broker is
enough to teach registry compatibility — replication is a different topic's job; `_schemas` and the demo
topics render at RF=1 under the existing `min(3, N)` clamp).

```yaml
{% if include_schema_registry %}
  schema-registry:
    image: ghcr.io/aiven-open/karapace:4.1.1   # Apache-2.0; Confluent-SR-API compatible; pin + re-check latest at build (R-K3 discipline)
    container_name: spark-schema-registry
    hostname: schema-registry
    depends_on: [spark-kafka-1]
    environment:
      KARAPACE_BOOTSTRAP_URI: "kafka-1:9092"       # in-cluster listener (D-MBK2)
      KARAPACE_PORT: "8081"
      KARAPACE_HOST: "0.0.0.0"                       # bind inside the container; host publish is loopback-scoped below
      KARAPACE_COMPATIBILITY: "BACKWARD"             # default subject compat; the notebook flips it per-subject to show rejection
      KARAPACE_REGISTRY_HOST: "schema-registry"
      KARAPACE_REGISTRY_PORT: "8081"
    ports:
      - "127.0.0.1:8081:8081"                        # loopback only (public-deploy D2); 8081 is free of every existing publish
    networks: [sparkpb-net]
    deploy:
      resources:
        limits:
          cpus: "1"
          memory: 1g
    restart: "no"
    # No volumes: schema state lives in the broker's ephemeral _schemas topic (D6).
{% endif %}
```

- **Reachability.** In-cluster the notebook (driver) uses `http://schema-registry:8081`; a host shell
  uses `http://127.0.0.1:8081`. `8081` is the registry's conventional port and collides with none of
  the existing loopback publishes (8080 master, 8888 Jupyter, 4040–4042 driver, 9092+ brokers).
- **Resource ceiling.** `renderer.validate()` and its `compose/cli.py::_validate_ranges` mirror add a
  flat `config.SCHEMA_REGISTRY_MEMORY_GB` (**1 GB**, conservative — the process is light) to `total_gb`
  when `include_schema_registry`. US-KC8's spawn totals `1 (master) + 3×4 (workers) + 2 (driver) +
  2×1 (1 Kafka broker) + 1 (registry) = 18 GB`, well under the 32 GB ceiling. New config:
  `SCHEMA_REGISTRY_MEMORY_GB = 1`; a `--include-schema-registry` flag on the CLI mirror; the
  `include_schema_registry` render-context key. Same four-layer threading (`ClusterParams` →
  `validate()`/ceiling → `render()` context → template `{% if %}`, + CLI mirror) that `include_kafka`
  already has — boilerplate, not new architecture, but four edits that must stay in sync.
- **Dependency:** `Dockerfile.spark` adds `pip install fastavro==<pin>` in the existing Kafka layer;
  update its "Deliberately NOT included" note. This is the curriculum's first dependency beyond
  `kafka-python` + the baked connector jar — deliberate, minimal, and confined to US-KC8.

### D-KC3 — US-KC11 exactly-once: `kafka-python==2.0.2` cannot do transactions at all; fallback is CLI-driven, no app-code decision needed (spike resolved)

**The spike ran and the answer is definitive — and it needed no live cluster.** I installed
`kafka-python==2.0.2` (the exact version baked into `Dockerfile.spark`, isolated in a scratch dir) and
introspected its API. The transactional producer surface is not "limited/flaky" — it is **entirely
absent**:

| Capability US-KC11 needs | `kafka-python==2.0.2` | source of truth checked |
|---|---|---|
| `producer.init_transactions()` | **absent** | `hasattr(KafkaProducer, ...)` → False |
| `producer.begin_transaction()` | **absent** | False |
| `producer.commit_transaction()` / `abort_transaction()` | **absent** | False |
| `producer.send_offsets_to_transaction()` | **absent** | False |
| `transactional_id` producer config | **absent** | not in `KafkaProducer.DEFAULT_CONFIG` |
| `enable_idempotence` producer config | **absent** | not in `KafkaProducer.DEFAULT_CONFIG` |
| `isolation_level` consumer config (`read_committed`) | **absent** | not in `KafkaConsumer.DEFAULT_CONFIG` |

Because the API *does not exist in this version*, the result is version-intrinsic — no broker topology
can make an absent method appear. **That is why the spike correctly did not require spinning up the
multi-broker cluster:** the load-bearing question ("can the Python client we ship drive a transaction?")
is answered at the client's API surface, and a live cluster could only have re-confirmed a `False`. This
is the correctly-scoped architecture-time answer; the *remaining* work is a bounded developer-time
implementation task (below), which is the right place for a live cluster.

**Decision:** US-KC11 is built on the **CLI fallback**, driven from the notebook via `subprocess`
(`docker exec spark-kafka-1 ...`), exactly the mechanism US-KC11's third acceptance criterion already
names — commit/abort visibility contrasted between a `read_committed` and a `read_uncommitted`
consumer, both via `kafka-console-consumer.sh --isolation-level ...`. **No new pinned Kafka client
dependency** (honoring the requirements doc's explicit constraint). No app-code or compose change — this
is a `content/`-only topic like the other Kafka topics, just one whose demo shells out to the broker's
own CLI rather than to a Python client.

**Bounded item handed to the developer (not resolved here, and honestly flagged rather than assumed):**
the requirements doc's literal `kafka-console-producer.sh --transactional-id` incantation is itself
**not yet verified** — `kafka-console-producer.sh` produces messages but does not obviously drive an
explicit `begin/commit/abort` cycle from flags alone. The developer must confirm a working CLI
transactional path at implementation time against a live broker (candidates, in order of preference:
`kafka-console-producer.sh --producer-property transactional.id=... [+ enable.idempotence=true]` if it
wraps a transaction; else `kafka-run-class.sh kafka.tools.TransactionalMessageCopier`; else a minimal
transactional snippet run via `kafka-run-class.sh`). This is an implementation spike, not an
architecture decision — the architecture-level fact (Python client is out, CLI/subprocess is the path,
no new dependency) is settled.

**Knock-on finding beyond US-KC11 — flag to requirements-analyst/PM (see Open Questions).** The same
`enable_idempotence`-absent fact means **US-KC3** (`kafka-producers-delivery`, backlog row #43, marked
"fully buildable today, no blockers") is **not** fully buildable via the Python client for its
idempotent-producer criterion. Its second acceptance criterion requires `acks=all` +
`enable.idempotence=true` demonstrating no-duplicate dedup — `kafka-python==2.0.2` has no
`enable_idempotence` config, so that specific criterion needs the same CLI/`--producer-property` path
(or a rephrase), not the Python client. The rest of US-KC3 (`acks=0`/`acks=1` behavior, the summary
table) is fine on the Python client. This is a real status correction, not a blocker: US-KC3 is still
buildable, just with the idempotence slice done via CLI like US-KC11.

---

## Alternatives considered

| Decision | Alternative | Why not |
|---|---|---|
| D-KC1 default `track: spark`, no existing edits | Add `track: spark` to all 15 existing manifests explicitly | 15 file edits touching shipped topics for zero behavior gain over a documented default; larger, riskier diff. Rejected on YAGNI — the simpler design already covers every acceptance criterion. |
| D-KC1 per-track `order` | One global `order` space, Kafka re-based to 16..27, sorted by `(track, order)` | Couples Kafka numbering to the Spark count; every time either track grows the other may need renumbering. Two rendered sections already remove any cross-track collision, so a shared space buys nothing. |
| D-KC2 Karapace | Confluent Schema Registry (`cp-schema-registry`) | Confluent Community License (not OSI-OSS) and a heavier second JVM; identical REST API and Avro semantics to Karapace, so no teaching loss from the Apache-2.0, lighter, Kafka-topic-backed choice. Close call — named as the drop-in fallback if Karapace config friction appears at build time (same client code). |
| D-KC2 Karapace | Apicurio Registry | Apache-2.0 but typically wants a SQL/dedicated storage backend and is heavier; Karapace's `_schemas`-topic storage fits the ephemeral model with no datastore. |
| D-KC2 `fastavro` + kafka-python transport | `confluent-kafka[avro]` (bundled SR client + AvroSerializer) | Drags in librdkafka (C build) and a *second* Kafka client, making US-KC8's produce/consume code diverge from the other 11 topics' `kafka-python`. Bundling also hides the wire format the topic exists to teach. |
| D-KC2 Avro only | Avro + Protobuf | A single-notebook demo doesn't need to prove two formats exist; the compatibility lesson is identical. YAGNI. |
| D-KC3 CLI fallback, no new dep | Bump to a Kafka client with transactions (`confluent-kafka`, `kafka-python` 2.1+/2.2+) | The requirements doc explicitly forbids a new pinned Kafka client for US-KC11; the CLI path meets the same acceptance bar without one. (If the human later reverses that constraint, `confluent-kafka` is the natural client — flagged, not decided.) |
| D-KC3 resolve at API surface, no cluster | Spin up the multi-broker cluster to test transactions live | The API is absent in 2.0.2 — a live cluster cannot make an absent method exist, so it would only re-confirm the introspection result at the cost of ~20 GB and minutes. The live test that *does* have value (which exact CLI incantation drives a transaction) is a developer-time task, not an architecture gate. |

Simpler options rejected because a real constraint forbids them (ADR discipline): dropping the
schema-registry resource-ceiling accounting (US-KC8 must stay in budget), dropping the
`include_schema_registry` gate to make the registry always-on (a second unused service in every
non-US-KC8 spawn, against the minimal-surface posture), and dropping the `track` default in favor of
editing every manifest (churn without benefit). None were simplified away silently.

---

## Consequences

**Accepted trade-offs:**
- **`track` is an implicit `spark` default**, not declared on every topic — a tiny bit of "magic," paid
  down by documenting it at the single read site and by the fact that the whole existing corpus is Spark.
- **A sixth service type on US-KC8 spawns** (`spark-schema-registry`). Teardown, the #38 guard, and
  `--remove-orphans` absorb it for free (D2), but `docker ps` for that one topic shows one more
  container and a wedged registry is one more failed-spawn diagnosis input.
- **One loopback host port (`127.0.0.1:8081`) on US-KC8 spawns**, unauthenticated, consistent with the
  single-trusted-user loopback threat model (same posture as 8080/8888/4040/9092). No new exposure class.
- **`fastavro` is the first dependency beyond `kafka-python` + the baked connector jar** — a deliberate,
  minimal, US-KC8-confined departure; a future image bump must keep it pinned (R-K3-class).
- **`include_schema_registry` threads the same four layers** `include_kafka`/`kafka_broker_count`
  already do (plus the CLI mirror) — boilerplate that must stay in sync.
- **US-KC11 and US-KC3's idempotence slice shell out to the broker CLI** rather than the Python client,
  so those two notebooks look slightly different from the other Kafka topics (subprocess-driven vs.
  in-Python). That is the honest cost of `kafka-python==2.0.2`'s missing transactional/idempotence API,
  documented rather than papered over.

**What becomes harder:** a Protobuf demo, multiple registries, or a durable registry surviving respawns
are all further away — the single ephemeral Karapace + Avro is the floor that teaches registry-enforced
compatibility, not a production schema-management setup. Intended boundary, not oversight.

---

## Component / data design

```
TOPICS-INDEX (D-KC1)
  routes/topics.py::index()  →  loader.list_topics_by_track()
        │   [("Spark",[order 1..15]), ("Kafka",[order 1..12])]   (empty tracks omitted)
        ▼
  topics_index.html : {% for label, topics in tracks %} <h2>label</h2> <div.topics-grid>…</div> {% endfor %}
        ▲  Topic.track = manifest.get("track","spark")   ← 15 existing manifests untouched (default),
                                                            12 new set track: kafka

US-KC8 SCHEMA REGISTRY (D-KC2)   content/kafka-serialization-schema-evolution/manifest.yaml
   requires_kafka: true · requires_schema_registry: true · kafka_broker_count: 1
        │  (loader → ClusterParams.include_schema_registry)
        ▼
   renderer.validate(): total += SCHEMA_REGISTRY_MEMORY_GB (1) when include_schema_registry
   renderer.render():   context["include_schema_registry"] = ...
        ▼
   docker-compose.yml.j2 : {% if include_schema_registry %} schema-registry (Karapace :8081) {% endif %}
        │  docker compose -p sparkpb up -d   (unchanged: manager → #38 guard → single-slot down/up)
        ▼
   ┌ project sparkpb, network sparkpb-net ───────────────────────────────────────┐
   │ spark-master  spark-worker-1..3  spark-driver                                │
   │ spark-kafka-1 (kafka-1:9092 / 127.0.0.1:9092)                                │
   │ schema-registry (schema-registry:8081 / 127.0.0.1:8081)                      │
   │      └─ stores schemas in the broker's ephemeral _schemas topic (D6)         │
   └──────────────────────────────────────────────────────────────────────────────┘
        ▲ notebook (driver): kafka-python transport + fastavro encode + REST to schema-registry:8081
        ▲ host shell: 127.0.0.1:8081

US-KC11 EXACTLY-ONCE (D-KC3)   content/kafka-exactly-once-transactions/  (content-only)
   notebook → subprocess: docker exec spark-kafka-1 kafka-console-*.sh (transactional CLI)
   (no compose/app change; no new dependency; Python client cannot do transactions in 2.0.2)
```

**Files (developer handoff):**

*D-KC1 (grouping):* `app/topics/loader.py` (`Topic.track` + `list_topics_by_track()`);
`app/web/routes/topics.py::index()` (pass grouped tracks); `app/web/templates/topics_index.html`
(two-section render); `app/web/static/style.css` (one `.topics-track-heading` rule); the 12 new Kafka
manifests set `track: kafka` + their `order`. **No existing manifest edited.**

*D-KC2 (registry):* `compose/templates/docker-compose.yml.j2` (`{% if include_schema_registry %}` block);
`compose/Dockerfile.spark` (`fastavro` pin); `app/lifecycle/renderer.py`
(`ClusterParams.include_schema_registry`, ceiling +1 GB, render key); `compose/cli.py`
(`--include-schema-registry`, DEFAULTS, `_validate_ranges` +1 GB); `app/config.py`
(`SCHEMA_REGISTRY_MEMORY_GB = 1`); `app/topics/loader.py` (parse `requires_schema_registry`);
`app/web/routes/topics.py::spawn_cluster` (set `include_schema_registry=topic.requires_schema_registry`
— or a drawer field if the human wants it learner-toggleable; **recommend manifest-driven only**, since
unlike broker-count there is no pedagogical "resize the registry" exercise). US-KC8 `content/` (notebook
+ concept + manifest).

*D-KC3 (exactly-once):* `content/kafka-exactly-once-transactions/` only. No app/compose change.

## Visual design (topics-index grouping — UI-facing)

No mockup was supplied for the grouped index; this written spec is the buildable target (check a
screenshot against it at acceptance).

**Layout.** The existing single-column page (`topics-eyebrow` "SPARK PLAYBOOK" → `topics-title`
"Topics" → `topics-subhead`) is unchanged. Below the subhead, the flat `.topics-grid` is replaced by
**two labeled sections in fixed order**:

1. **"Spark"** section heading (`<h2 class="topics-track-heading">`), then the existing `.topics-grid`
   of the 15 Spark cards (unchanged card markup: `TOPIC NN` eyebrow, notebook name, title, blurb),
   ordered by their existing `order` 1..15.
2. **"Kafka"** section heading, then a second `.topics-grid` of the 12 Kafka cards, ordered 1..12.

**Distinct states to verify (beyond "it renders"):**
- *Both tracks present (steady state):* two headings, Spark grid above Kafka grid, each internally
  ordered; card layout identical to today.
- *Kafka track empty (before the first Kafka topic ships):* **only** the Spark heading + grid render —
  no empty "Kafka" heading (the helper omits empty groups). This is the state on the day this loader
  change merges but before any `content/kafka-*` folder exists, so it must look identical to today's
  page plus a "Spark" heading.
- *A card's `TOPIC 01` eyebrow* appears in *both* sections (Spark 01 and Kafka 01) — correct and
  unambiguous under the section headings; not a bug.

The heading style should read as a section divider consistent with the page's existing dark/eyebrow
visual family (reuse the eyebrow/subhead type scale rather than inventing a new one).

---

## Open questions (flagged, not blocking)

- **OQ-KC-A — exact Kafka `order` values (requirements OQ-2) are a recommendation, not a decision.**
  D-KC1's table (1..12, basic→advanced) is the architect's recommended sequence; the requirements doc
  explicitly deferred final confirmation to the human after this pass. Needs a human yes/adjust before
  the 12 manifests are authored. Nothing else in D-KC1 depends on the exact numbers.
- **OQ-KC-B — US-KC3 is not fully buildable via the Python client as currently written (D-KC3
  knock-on).** `kafka-python==2.0.2` has no `enable_idempotence`, so US-KC3's idempotent-producer
  criterion needs the CLI/`--producer-property` path (or a rephrase). **For requirements-analyst/PM:**
  backlog row #43 and US-KC3's status ("fully buildable today, no blockers") should be corrected to
  note the idempotence slice is CLI-driven like US-KC11. Not a blocker — still buildable.
- **OQ-KC-C — the exact CLI transactional incantation for US-KC11 is a developer-time spike (D-KC3).**
  The requirements doc's literal `kafka-console-producer.sh --transactional-id` is unverified; the
  developer confirms a working CLI transaction path live at implementation. Architecture-level direction
  (CLI/subprocess, no new dep) is settled.
- **OQ-KC-D — registry as manifest-only vs. a drawer toggle.** D-KC2 recommends manifest-driven only
  (no learner "resize the registry" exercise exists, unlike broker count). If the human wants it in the
  drawer for symmetry with the Kafka section, that's a small addition — flagged, recommend against.
- **Not this doc's to decide (per the requirements framing), recommendations only:** (1) US-KC7
  standalone-vs-fold-into-v1.1 (requirements OQ-1) — recommend keeping standalone as the doc argues
  (minimal-mechanic vs. real-data application are genuinely distinct), but human sign-off needed;
  (2) per-topic `kafka_broker_count` defaults (requirements OQ-6) — an implementation default for the
  developer; only US-KC8's `1` is asserted here (registry demo needs no replication) and US-KC12's 3–5
  is already in the requirements.

---

## Risks

- **R-KC1 — Karapace config/behavior friction at build time.** Karapace is less ubiquitously documented
  than Confluent SR; an env-var or compatibility-mode surprise could cost the developer time.
  *Noticed by:* the registry container failing to serve `/subjects` on a US-KC8 spawn, or compatibility
  checks behaving differently than Confluent SR. *Mitigation:* the REST API and client code are
  identical to Confluent SR, so `cp-schema-registry` is a documented drop-in fallback (D-KC2 alternative)
  — swap the `image:` and the env keys, notebook code unchanged.
- **R-KC2 — `_schemas` topic under-replicated / registry racing broker readiness.** On US-KC8's
  single-broker spawn the `_schemas` topic is RF=1 (fine), but the registry may start before the broker
  is listening. *Noticed by:* registry logs showing bootstrap retries, or the notebook's first register
  call 503-ing. *Mitigation:* `depends_on: [spark-kafka-1]` + Karapace's own bootstrap retry; if flaky,
  a readiness poll (YAGNI until observed, per the R-K4 precedent).
- **R-KC3 — the four-layer `include_schema_registry` threading drifts** (the CLI mirror is the usual
  miss). *Noticed by:* the CLI accepting a config the app rejects, or the registry not rendering despite
  the manifest flag. *Mitigation:* mirror both paths in one pass; code-review checks the standing
  CLI-mirror obligation (same as D-MBK4's 48/32 lesson).
- **R-KC4 — grouping change regresses the flat-list callers.** `list_topics()` still feeds the
  breadcrumb switcher and `_shell_context`; only `index()` moves to the grouped helper. *Noticed by:*
  the topic-switcher dropdown losing topics, or the index rendering an empty Kafka heading.
  *Mitigation:* `list_topics()` is unchanged; `list_topics_by_track()` omits empty groups; the
  leave-behind loader test pins both.
- **R-KC5 — US-KC11's CLI path proves as awkward as the Python one.** If no clean CLI transactional
  incantation exists (OQ-KC-C), US-KC11's demo could stall. *Noticed by:* the developer-time CLI spike
  failing all three candidate paths. *Mitigation:* the fallback-of-the-fallback is a minimal
  transactional snippet via `kafka-run-class.sh` against the broker's own bundled Java client (which
  fully supports transactions); this stays within "no new *pinned Python* dependency." If even that is
  rejected, escalate the "bump the Kafka client" constraint to the human (D-KC3 alternative).

---

Empirical verification performed at design time (this pass): `kafka-python==2.0.2` transactional/
idempotence/isolation API surface introspected directly (D-KC3 table) — the API is absent, resolving
Open Question 5 without a live cluster. No cluster was spawned; no notebook or repo file was modified
(`docs/backlog.md` M and `docs/requirements/kafka-curriculum.md` untracked are prior-stage inputs, not
touched by this pass).
