# ADR: Live Market Data Streaming — real-feed producer, compacted control topic, price dashboard, streaming job

Status: Draft for human review · Date: 2026-07-19
Drives: GitHub milestone [`v1.1 — Live Market Data Streaming`](https://github.com/hoanghaithanh/Spark-Playbook/milestone/13) (#13), backlog row #18
Requirements: `docs/requirements/live-market-data-streaming.md` (US-LMD1–US-LMD4, Open Questions 1–3)
Formalizes: `C:\Users\hoang\.claude\plans\for-18-i-want-lazy-candle.md` (human-approved plan, 2026-07-19)
Builds on / reuses (no modification): `docs/architecture/kafka-streaming-infra.md` (#50 — broker, dual-listener KRaft, resource-ceiling accounting, `produce.py`/`producer.py` CLI+wrapper shape); `docs/architecture/topic-shell-redesign.md` (Decision B — slide-in panel + shared-collector SSE lifecycle); `app/monitoring/collector.py` (`DashboardCollector` pattern); `driver/playbook/annotate.py` + `app/config.py::SHARED_DIR` (pull-not-push `scratch/shared/`).

---

## Context

Backlog row #18 pivots from a synthetic event stream to two real, free market-data feeds (Coinbase's public `ticker` WebSocket, Finnhub's free-tier stock WebSocket). The requirements doc formalized the plan's confirmed decisions and handed the architect three open questions to close (control-topic wire protocol, bounded-latency figure, watermark window size) plus full implementation detail across four sub-stories. The load-bearing constraint from #50 is unchanged and honored throughout: **this adds topics and content to the existing broker; it does not touch the broker's lifecycle, the `include_kafka` gate, the dual-listener config, or the +2GB ceiling reservation.**

Two facts discovered while designing against the real seams shape the whole ADR and are worth stating up front:

1. **A log-compacted control topic cannot be auto-created.** `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` (D6 of #50) produces topics with the broker default `cleanup.policy=delete`, and #50 already proved (its `produce.py` deviation note) that `kafka-python`'s `KafkaAdminClient.__init__` raises `NodeNotReadyError` against this single-node KRaft broker — so Python cannot create the compacted topic or alter its config either. This forces one genuine compose addition (D-LMD4): a one-shot `kafka-init` container that runs `kafka-topics.sh`. It sits inside the existing `{% if include_kafka %}` block, so it is invisible to the 13 non-streaming topics.
2. **The query-progress evidence is a genuine third pattern, not the annotation engine.** The self-check signal for US-LMD4 is "does `progress.json` show growth-vs-plateau," a continuous pull-not-push file read — neither a plan-node fact (`engine.annotate_plan`) nor a reveal-time `:4040` REST spotlight (Decision A of the shell ADR). D-LMD6 keeps it out of the annotation engine deliberately.

The single decision most expensive to get wrong is **D-LMD1 — full-snapshot control messages under one compacted key.** Choose deltas instead and log compaction silently deletes all but the last add/remove, corrupting restart-resync.

---

## Decision

### D-LMD1 — Control topic carries a full snapshot of the desired symbol set under one key; "nothing selected" is an explicit `[]`, never a tombstone (resolves Open Question 1)

`price-subscriptions` messages are **full snapshots**, not deltas:

- **Key:** the constant string `"selection"` (single key, as the plan fixed).
- **Value:** the complete desired symbol set as a **sorted JSON array**, e.g. `["AAPL","BTC-USD","ETH-USD"]`. Sorted so byte-identical selections produce byte-identical values (cleaner compaction, easier diffing in logs).
- **Empty selection is `[]`** — a real, retained value — **never a null tombstone.** This is the one correctness rule the whole resync story rests on (see below).

Why full-snapshot is not merely simpler but *required* by the compacted-single-key framing already fixed by the plan:

- **Compaction keeps only the latest value per key.** Under one key, a stream of delta events (`+SOL-USD`, `-AAPL`, …) is exactly the access pattern compaction destroys: after cleanup only the *last* delta survives, so a restarting producer replaying the compacted log reconstructs a wrong set. Deltas would only survive compaction if keyed *per symbol* (key = symbol, value = subscribed / tombstone) — a genuinely different, more complex model with tombstone-timing hazards, adopted for no benefit here.
- **Restart-resync is one read.** US-LMD1's fifth criterion (resync from the compacted log on startup, no browser resend) becomes trivial: seek to the beginning, read the single retained `"selection"` record, done. No fold-over-history, no ordering assumptions.
- **The producer stays stateless w.r.t. the control topic.** Every message is the complete truth, so a missed/duplicated/out-of-order message self-heals on the next snapshot — the producer just re-diffs desired-vs-active.

**Why `[]` not a tombstone for the empty set:** `cleanup.policy=compact` retains the latest value for a key indefinitely, *but* a null value is a tombstone, eligible for physical deletion after `delete.retention.ms`. If "deselect everything" published a tombstone, a producer restarting after that retention window would resync to *no record* — indistinguishable from "topic never written" — and would have no way to know the user's actual state is "nothing." Publishing `[]` keeps a durable, unambiguous "nothing selected" value that compaction never evicts. The publisher (D-LMD7) therefore always sends an array, empty or not.

### D-LMD2 — A single form-level change trigger with a 400 ms debounce; subscription-change latency budget is ≤ 3 s (resolves Open Question 2)

**Publishing:** the price panel wraps all ticker checkboxes in one `<form>` whose `hx-post` fires on `change` with `hx-trigger="change delay:400ms"` and `hx-include` covering every checkbox. Because HTMX's `delay` modifier resets the timer on each triggering event and `change` bubbles from any checkbox to the form, a burst of rapid toggles (the "3 checkboxes in 2 s" case) collapses into **one** Kafka write carrying the final full selection — not three. Each POST carries the complete current checkbox state, so the published snapshot (D-LMD1) is always the true desired set; a mid-burst publish is harmless because the next one supersedes it and is itself a full snapshot. No server-side session state is needed — the form *is* the state.

Per-checkbox immediate publish was rejected: with independent per-element triggers, HTMX's `delay` debounces each checkbox separately and cannot coalesce a burst across different boxes, yielding N writes and N upstream WS subscribe/unsubscribe churns for one logical selection change. The form-level trigger is one attribute and gets true burst coalescing.

**Latency budget** (checkbox click → producer's live WS subscription set actually changes — the thing US-LMD1 AC3/AC4 test):

| Hop | Bound |
|---|---|
| client debounce | ~0.4 s |
| HTTP POST + FastAPI `KafkaProducer.send`+flush | < 0.2 s |
| producer control-consumer poll (`poll(timeout_ms=1000)`) | ≤ 1.0 s |
| diff + provider WS subscribe frame sent | < 0.1 s |
| **Total to subscription-set change** | **~1.7 s, bounded ≤ 3 s** |

**Confirmed figure: ≤ 3 seconds** for the producer's live subscription set to change (tighter than the requirements' 5 s placeholder, justified by the debounce + 1 s poll math). Update US-LMD1 AC3/AC4 and US-LMD3's second criterion to 3 s.

**Explicit carve-out:** the *chart showing new price points* additionally waits for a real market tick to arrive on the newly-subscribed symbol. For crypto (Coinbase) that is effectively immediate; for a thinly-traded stock outside US market hours it may be seconds-to-minutes, or nothing — **this tail is bounded by real market activity, not by us.** The 3 s bound applies to the *subscription change* (observable in producer logs), which is the criterion that distinguishes this design from a client-side filter. Acceptance criteria that mention the chart must say "once ticks arrive," not assert a hard visible-point deadline.

### D-LMD3 — 10-second tumbling OHLC window, 30-second watermark, producer late-injection default 120 s (resolves Open Question 3)

- **OHLC window: 10-second tumbling** (`window("event_time", "10 seconds")`) — confirms the plan's sketch. Fast enough that a learner watches bars form live without waiting minutes; both feeds tick often enough during active hours to populate a 10 s bar for a subscribed symbol.
- **Watermark: 30 seconds** (`withWatermark("event_time", "30 seconds")`). Chosen against three constraints simultaneously:
  - *Observable plateau, quickly.* With 10 s windows, a window finalizes and its state row is evicted ~30–40 s after the window closes, so the no-watermark→with-watermark state-count contrast is visible inside ~1 minute of a live run (US-LMD2 AC1/AC2, US-LMD4's growth-vs-plateau).
  - *Clean-cut late-data drop.* The producer's injected-late ticks are back-dated well beyond 30 s, so they land unambiguously past the watermark and are dropped (US-LMD2 AC3), while naturally-jittery real ticks (sub-second out-of-order) sit comfortably inside it and are kept.
  - *Not so tight that real feed jitter causes spurious drops.* 30 s dwarfs the real inter-arrival jitter of both feeds.
- **Producer late-injection: `--late-seconds` default 120** (vs. `produce.py`'s 60) so injected-late ticks are ~4× the watermark old — no ambiguity about which side of the 30 s boundary they fall on. `--late-frac` default 0.05, mirroring `produce.py`.

OHLC aggregation note for the developer (avoids a real streaming trap): `first()`/`last()` are **not** time-ordered inside a streaming aggregation. Compute open/close with the struct-ordering idiom — `open = min(struct(event_time, price)).price`, `close = max(struct(event_time, price)).price`, `high = max(price)`, `low = min(price)` — so open/close are the earliest/latest *by event_time*, not by arrival.

### D-LMD4 — Two topics; `prices` auto-creates, `price-subscriptions` is created by a one-shot `kafka-init` container with explicit compaction config

`prices` (data plane):

| Setting | Value | Rationale |
|---|---|---|
| partitions | 3 | matches broker `KAFKA_NUM_PARTITIONS` default and `events`; fans per-symbol groups across the 3 default workers |
| key | symbol string (`"BTC-USD"`) | required for correct per-symbol windowed aggregation; same key → same partition → per-symbol ordering |
| cleanup.policy | `delete` (broker default) | ephemeral live data, no replay (non-goal: no persisted history) |
| retention | inherits broker `LOG_RETENTION_MINUTES=10` / `LOG_RETENTION_BYTES=512MB` (#50 D6) | bounded disk, self-cleaning |
| creation | **auto-create on first produce** | delete-policy + 3 partitions is exactly what auto-create yields; no explicit creation needed |

`price-subscriptions` (control plane):

| Setting | Value | Rationale |
|---|---|---|
| partitions | **1** | single-key control topic; one partition guarantees total order of selection changes and keeps compaction trivial (auto-create's 3 would leave 2 permanently-empty partitions — harmless but pointless) |
| key | constant `"selection"` (D-LMD1) | |
| cleanup.policy | **`compact`** (not `compact,delete`) | pure compaction retains the latest `"selection"` value indefinitely and is **exempt from the global 10-min time-retention** — the live selection must survive a learner leaving the panel idle > 10 min then toggling |
| `min.cleanable.dirty.ratio` | `0.1` | lets compaction collapse superseded snapshots sooner, keeping the log tiny; cosmetic — correctness never depends on compaction running, since the latest value is always readable |
| `segment.ms` | `60000` (1 min) | rolls the active segment often enough that old duplicate snapshots become compaction-eligible on a rarely-changing topic; cosmetic, same caveat |
| `min.compaction.lag.ms` | `0` | no reason to delay |
| creation | **explicit, via `kafka-init`** | auto-create cannot set `cleanup.policy=compact`; `KafkaAdminClient` is blocked (`NodeNotReadyError`, #50) |

**`kafka-init` — one-shot topic-creation container** (the one genuine compose addition, inside `{% if include_kafka %}`):

```yaml
{% if include_kafka %}
  kafka-init:
    image: apache/kafka:3.9.0          # same image as the broker; ships kafka-topics.sh
    container_name: spark-kafka-init
    depends_on: [kafka]
    networks: [sparkpb-net]
    restart: "no"                      # runs once, creates topics, exits
    entrypoint: ["/bin/sh", "-c"]
    command:
      - |
        BIN=/opt/kafka/bin/kafka-topics.sh
        # Broker-readiness retry loop (R-K4 class: depends_on only waits for
        # container start, not a listening broker).
        for i in $(seq 1 30); do
          "$BIN" --bootstrap-server kafka:9092 --list >/dev/null 2>&1 && break
          sleep 1
        done
        "$BIN" --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic prices --partitions 3 --replication-factor 1
        "$BIN" --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic price-subscriptions --partitions 1 --replication-factor 1 \
          --config cleanup.policy=compact \
          --config min.cleanable.dirty.ratio=0.1 \
          --config segment.ms=60000 \
          --config min.compaction.lag.ms=0
{% endif %}
```

`--if-not-exists` makes it idempotent across respawns within a session. It creates `prices` explicitly too (cheap, deterministic partition count independent of the broker default). It exits after creating both, so it is **not** a standing resource consumer — the #50 +2GB Kafka reservation is unchanged (D-LMD8). This is a targeted correction to #50's "Kafka topics don't need compose-level declaration": that held for the delete-policy `events` topic (auto-create sufficed); a *compacted* topic is the exception that auto-create cannot serve.

### D-LMD5 — One asyncio producer process: two provider WS adapters + one control consumer, sharing a desired-set, each provider reconciling independently

`tools/price_producer/produce.py` mirrors `tools/kafka_producer/produce.py`'s CLI conventions exactly (argparse, `--self-check`, `--bootstrap` default `kafka:9092`, `--late-frac`/`--late-seconds`, `SIGINT`/`SIGTERM` → clean flush-and-exit, input validation at the boundary). Internally it runs one event loop with three long-lived tasks over shared state:

```
                         ┌───────────────────────────────────────────────┐
                         │  desired: set[str]   (the truth from control)  │
                         └───────────────────────────────────────────────┘
                            ▲ writes                    ▼ reads/reconciles
  price-subscriptions ──► control_consumer_task    coinbase_adapter_task ──► WS ws-feed.exchange.coinbase.com
   (kafka-python poll,     (asyncio.to_thread        finnhub_adapter_task ──► WS ws.finnhub.io?token=<key>
    wrapped in to_thread)   around blocking poll)         │ normalize tick
                                                          ▼
                                                    KafkaProducer.send("prices", key=symbol, value=…)
```

- **`control_consumer_task`.** On startup, seeks to the beginning of `price-subscriptions` and reads the one retained snapshot to prime `desired` (resync, US-LMD1 AC5). Then loops `records = await asyncio.to_thread(consumer.poll, timeout_ms=1000)` — the blocking `kafka-python` poll goes through `asyncio.to_thread` so it never stalls the WS tasks (the #19 lesson, reused). On each new snapshot: validate every symbol against the curated list (reject/ignore invalid with a clear log line, US-LMD1 AC last), update `desired`, and signal each provider adapter to reconcile.
- **`coinbase_adapter_task` / `finnhub_adapter_task`.** Each owns its slice of the curated universe (crypto → Coinbase, stocks → Finnhub) and an `active: set[str]` of what it is *currently* subscribed to on its live socket. Reconcile = diff its slice of `desired` against `active`, send subscribe frames for `desired − active`, unsubscribe frames for `active − desired`. Empty `desired` slice → no active subscriptions → no ticks published (US-LMD1 AC2, the client-filter-distinguishing behavior). Incoming messages are parsed to the `prices` schema and `KafkaProducer.send`-ed (send is non-blocking in `kafka-python` — a background sender thread handles I/O — so no `to_thread` needed on the publish path; a periodic `flush` bounds latency).
- **Per-provider adapter, not one shared code path.** The two providers have genuinely different wire formats, so each adapter is a small subclass over a shared reconnect/reconcile loop, overriding three format-specific methods. Two concrete implementations of a three-method shape is the justified minimum — not a speculative abstraction (both providers concretely exist):

  | | Coinbase (`ticker` channel) | Finnhub (trades) |
  |---|---|---|
  | subscribe frame | `{"type":"subscribe","product_ids":[…],"channels":["ticker"]}` | one per symbol: `{"type":"subscribe","symbol":"AAPL"}` |
  | unsubscribe frame | `{"type":"unsubscribe","product_ids":[…],"channels":["ticker"]}` | `{"type":"unsubscribe","symbol":"AAPL"}` |
  | tick parse | `{"type":"ticker","product_id","price","time","last_size"}` → tick | `{"type":"trade","data":[{"s","p","t","v"}]}` → tick(s) |
  | asset_class / source | `crypto` / `coinbase` | `stock` / `finnhub` |

- **Reconnection/backoff.** Each adapter wraps connect+run in `while not shutdown:` with exponential backoff (1→2→4→…→30 s cap) on `ConnectionClosed`/connect failure. **On every (re)connect it re-subscribes the full current `desired` slice** — WS subscriptions are per-connection state that a drop wipes, so a reconnect must replay them, not assume `active` survived.
- **Late-injection.** On publish to `prices`, a `--late-frac` fraction of ticks get `event_time` back-dated by `--late-seconds` (real price, synthetic delay only) — the same honest corner-cut `produce.py` documents, now the *only* reliable trigger for US-LMD2's late-drop demo since live feeds won't produce late ticks on demand.

`driver/playbook/price_producer.py` is the thin `start()`/`stop()` subprocess wrapper mirroring `driver/playbook/producer.py` exactly (launches `produce.py` as its own OS process so the streaming query's kill/restart never stops the feed — #50 D5's independence rule).

`prices` schema (confirms the plan): `{"symbol":"BTC-USD","price":67321.5,"event_time":"<ISO-8601 UTC>","asset_class":"crypto","source":"coinbase","volume":0.014}`.

### D-LMD6 — Spark job as a real `readStream`; two self-check surfaces, neither is the annotation engine

`content/structured-streaming/` follows the 3-file convention (`manifest.yaml`, `concept.md`, `notebook.ipynb`).

**Manifest** — two new data-driven boolean flags, kept separate because they gate independent surfaces with independent dependencies:

```yaml
id: structured-streaming
title: "Structured Streaming & Watermarks"
order: <next>
content: concept.md
notebook: notebook.ipynb
requires_kafka: true            # #50 D1 — flips the broker (and now kafka-init) on
live_price_dashboard: true      # NEW — gates the price panel (US-LMD3); depends on the producer
query_progress_widget: true     # NEW — gates the query-progress widget (US-LMD4); depends on this notebook
cluster_defaults:               # 3×4GB → 1+12+2+2 = 17GB, under the 32GB ceiling (#50 accounting, unchanged)
  worker_count: 3
  worker_cores: 2
  worker_memory_gb: 4
  shuffle_partitions: 200
  aqe_enabled: false
```

No `annotation:` plan-node rules — the streaming semantics are not a query-plan fact. The self-check evidence lives in **two surfaces, neither routed through `engine.annotate_plan()` nor the reveal-time `:4040` REST spotlight** (Decision A of the shell ADR):

1. **Notebook-native OHLC correctness asserts** (US-LMD2 AC5). The `foreachBatch` sink writes to an in-notebook `memory` table; a notebook cell asserts `high >= low` and `open, close ∈ [low, high]` per bar. This is a `demo()`-style runnable check *in the notebook*, not an app-side reveal — real prices have no clean-generator guarantee, so the check is meaningful.
2. **Query-progress widget reading `progress.json`** (US-LMD4) — **the third evidence pattern, explicitly flagged.** A `StreamingQueryListener.onQueryProgress` writes `scratch/shared/streaming/progress.json` (atomic temp-write + rename) every batch with `{batchId, inputRowsPerSecond, processedRowsPerSecond, batchDuration, numStateRows}` (from `stateOperators[0].numRowsTotal`). The app's widget reads that file on a timed refresh. This is structurally the `driver/playbook/annotate.py` pull-not-push idiom (driver writes a file under `SHARED_DIR`, app reads on demand) but **continuous** rather than one-shot-on-Reveal. It is neither of the shell ADR's two categories: not a static plan node, not a point-in-time REST pull. **Recommendation: keep it a standalone widget with its own route and its own file reader — do not shoehorn it into the annotation engine or the dashboard collector.** The growth-vs-plateau contrast (US-LMD4 AC3) falls straight out of `numStateRows` across the no-watermark and with-watermark runs.

**Checkpoint location:** `scratch/shared/streaming/checkpoints/` (SHARED_DIR convention). Kill-and-restart against the same dir demonstrates exact resume (US-LMD2 AC4). Uses the documented manual/external kill mechanism (Open Question 4, unchanged — no new in-app control).

**Notebook cell structure (developer handoff):** (1) setup + start producer via `price_producer.start()`; (2) publish an initial selection to `price-subscriptions` so the notebook is self-contained without the browser; (3) `readStream` from `prices`, parse JSON, **no** watermark, 10 s windowed OHLC, `foreachBatch` → memory table + `StreamingQueryListener` → `progress.json`; observe state growth; (4) stop, add `withWatermark(30s)`, restart, observe plateau; (5) restart producer with `--late-frac` raised, observe late-drop; (6) OHLC correctness asserts; (7) checkpoint-recovery: kill query, restart against same checkpoint, observe resume; (8) cleanup: stop producer + query. Reset the notebook (`git checkout --`) after any live run per CLAUDE.md.

### D-LMD7 — `PriceCollector` mirrors `DashboardCollector`; server-computed inline SVG polylines; panel mirrors the monitor panel

`app/prices/collector.py::PriceCollector` is structurally identical to `DashboardCollector` (module-level singleton `price_collector`; `subscribe`/`unsubscribe`/`ensure_running`/`is_running`; gated on `manager.state == READY` **and** `subscriber_count() > 0`, so no background consumption when the panel is closed — US-LMD3 AC4). Differences:

- **Source is Kafka, not Docker/Spark REST.** A `kafka-python` `KafkaConsumer` on `prices`, bootstrap `f"{config.CLUSTER_HOST}:9092"` — the **loopback host-published listener** #50 D3 built (`127.0.0.1:9092` in local dev), the same seam the app already uses to reach the cluster. The blocking `consumer.poll(timeout_ms=…)` goes through `asyncio.to_thread` (the #19 lesson, reused) so a stuck broker only stalls this cycle, not the whole app. On connect failure (broker absent / producer not started) it emits an empty "waiting for ticks" snapshot and retries next cycle — `DashboardCollector`'s cycle-failed handling shape.
- **State = per-symbol ring buffer.** `Dict[str, deque]`, each `deque(maxlen=config.PRICE_HISTORY_LENGTH)` (recommend **60** points) of `(event_time, price)`. Ephemeral, in-memory, no persistence (non-goal). The collector renders whatever symbols currently have ticks — because only subscribed symbols ever reach `prices`, there is no client-side filtering to do (US-LMD3 AC3).
- **OOB fragment shape mirrors `_dashboard_body.html`.** The panel body carries one SSE-connect element (`sse-connect="/prices/stream" sse-swap="message" hx-swap="none"`). Each SSE push is one HTML blob of per-symbol OOB fragments — `<div id="price-chart-{symbol}" hx-swap-oob="true">` containing the latest-price label + the SVG polyline — so one connection keeps every visible chart current, exactly as the dashboard fans overview/job/node updates out over one stream.

**SVG helper** (`app/prices/svg.py` or inline): a few-line `polyline_points(prices, width, height) -> str` that min/max-normalizes a symbol's price deque into an SVG `<polyline points="x,y …">` over a fixed viewBox. Server-computed markup, no client JS, no dependency — the exact "server computes the number, template emits styled markup" pattern the CSS-bar sparklines already use (non-goal: no new frontend dependency).

**Routes** (`app/web/routes/prices.py`, new): `GET /prices/panel` (panel body, rendered inline with a fresh snapshot so first paint isn't blank), `GET /prices/stream` (SSE, subscribes to `price_collector` — the `DashboardCollector`/`dashboard.py` stream generator copied structurally), `POST /prices/subscriptions` (reads the full checkbox form, validates against the curated list, publishes the sorted-array snapshot to `price-subscriptions` via a module-level `KafkaProducer`). The publish route is where the app produces to Kafka — see D-LMD8 for the dependency.

**Panel gating in the shell.** `live_price_dashboard: true` adds a "Live Prices" button to `_top_bar.html` and a slide-in `#price-panel` to `shell.html`, both mirroring the existing Cluster Monitor panel's open/close/backdrop/SSE-inject lifecycle in `shell.js` (a fifth small handler, structurally identical to `initMonitorPanel`). Rendered only when the topic's manifest sets the flag — threaded through `_shell_context` the same way `requires_kafka` already flows, no per-topic branching. The query-progress widget (US-LMD4), gated by `query_progress_widget: true`, is a small `hx-get`-on-a-timer fragment (`GET /prices/progress` reads `progress.json`) — reuses the app's existing timed-refresh idiom, not a new push mechanism.

### D-LMD8 — Secrets via `.env`, `websockets` into the driver image, `kafka-python` into the app, no broker/ceiling change

- **Secrets.** A gitignored repo-root `.env` (already covered — `.gitignore` lines 28–29 match `.env`/`*.env`) holds `FINNHUB_API_KEY` (required) and `COINBASE_API_KEY` (unused placeholder — the public `ticker` channel needs no key). Threaded into `spark-driver`'s `environment:` block exactly as `SPARKPB_PUBLIC_ORIGIN` already is: `FINNHUB_API_KEY: "${FINNHUB_API_KEY:-}"`, `COINBASE_API_KEY: "${COINBASE_API_KEY:-}"`. The producer reads them from its own env. **No secrets code in `app/config.py`** — the FastAPI app never touches these keys; only the driver container does (the app talks to Kafka, not to the upstream feeds).
- **`websockets` pip dependency** added to `compose/Dockerfile.spark` next to the existing `kafka-python==2.0.2` line (line 69) — asyncio-native, pure Python, no native build deps.
- **`kafka-python` added to `app/requirements.txt`.** New this release: the app process both *consumes* `prices` (`PriceCollector`) and *produces* to `price-subscriptions` (the subscription publish route), so `kafka-python` (pinned `==2.0.2` to match the image) must be an app dependency, not only baked into the driver image. This is the first time the FastAPI process itself is a Kafka client.
- **Confirmed unchanged:** the broker service, `include_kafka` gate, dual-listener config, `KAFKA_NUM_PARTITIONS=3`, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`, and the +2GB Kafka ceiling reservation (`config.KAFKA_MEMORY_GB`). `kafka-init` is a one-shot (exits immediately, no steady-state footprint), and both new topics live on the same broker, so no ceiling accounting changes. The `KAFKA_NUM_PARTITIONS=3` default does **not** fight `price-subscriptions`' single-partition need, because `kafka-init` creates that topic explicitly with `--partitions 1` (explicit creation overrides the auto-create default).

---

## Alternatives considered

| Decision | Alternative | Why not |
|---|---|---|
| D-LMD1 full snapshot | Delta add/remove events under one key | Log compaction keeps only the last value per key → deltas are silently destroyed → wrong resync. Would require per-symbol keying (a more complex, tombstone-hazard model) for no benefit. |
| D-LMD1 `[]` for empty | Null tombstone for empty selection | Tombstones are physically deleted after `delete.retention.ms`; a later resync then can't distinguish "nothing selected" from "never written." `[]` is a durable, unambiguous value compaction never evicts. |
| D-LMD2 form-level debounce | Immediate publish per checkbox toggle | Per-element `delay` can't coalesce a burst across different checkboxes → N writes and N upstream WS churns per logical change. Form-level `change delay` is one attribute and coalesces the burst. |
| D-LMD4 `kafka-init` container | Auto-create the control topic | Auto-create yields `cleanup.policy=delete`, not `compact` — no compaction, and subject to the 10-min global deletion that would drop the live selection. |
| D-LMD4 `kafka-init` container | Create the topic from Python (`KafkaAdminClient`) | `#50` proved `KafkaAdminClient.__init__` raises `NodeNotReadyError` against this single-node KRaft broker — construction fails before any create call. |
| D-LMD5 per-provider adapters | One shared subscribe/parse code path | Coinbase (`product_ids`+`channels`) and Finnhub (`{"symbol":…}` per symbol, `data[]` trade batches) have genuinely different wire formats; a single path would be a tangle of provider `if`s. Two subclasses over a shared reconcile loop is the honest minimum. |
| D-LMD6 standalone progress widget | Route query-progress through the annotation engine | It is neither a plan-node fact nor a reveal-time REST pull — forcing it into `engine.annotate_plan()` breaks the matcher's one-dump→labels contract with continuous-metric logic it was never shaped for (same reasoning the shell ADR used to keep runtime metrics out of the engine). |
| D-LMD7 raw ticks from `prices` | Dashboard reads Spark's aggregated OHLC output | Coupling the live dashboard to the Spark job means no live prices until the learner starts the notebook. Reading raw `prices` makes the dashboard work the instant the producer runs (US-LMD3 AC5), independent of the Spark job. |
| D-LMD7 inline SVG | A charting library | No `package.json`/bundler exists; the frontend is hard-locked to server-rendered Jinja2 + HTMX + vanilla JS. A server-computed `<polyline>` is a few lines and fits the existing sparkline pattern (non-goal: no new frontend dependency). |

Simpler options rejected because a real constraint forbids them (ADR discipline): dropping the `kafka-init` container (a compacted topic genuinely cannot be created any other way here); dropping curated-list validation in the control consumer (US-LMD1's invalid-symbol criterion + a real external-input boundary); dropping per-provider reconnect re-subscription (WS subscriptions don't survive a drop). None were simplified away.

---

## Consequences

**Accepted trade-offs:**

- **One new one-shot container on streaming spawns.** `docker ps` for a streaming spawn now shows six service types (five standing + `spark-kafka-init`, which exits). A failed topic-create (broker never ready within 30 retries) would leave the control topic absent — noticed as the producer resync finding no topic and the subscription publish failing; mitigated by the retry loop and `--if-not-exists` idempotency.
- **The FastAPI process is now a Kafka client.** `kafka-python` becomes an app dependency, and the app opens a consumer (`PriceCollector`) and a producer (subscription publish) against the loopback broker listener. Both are gated/lazy (consumer only while the panel is open and cluster READY; producer is a lightweight module singleton), but it is genuinely new I/O surface in the app — a torn-down broker degrades the price panel to its empty state, reusing the collector's cycle-failed handling.
- **Real external APIs are now a runtime dependency of the topic.** The producer's health depends on Coinbase/Finnhub uptime, Finnhub free-tier rate limits, and (for stocks) US market hours — an off-hours stock selection may show a subscribed-but-silent chart. This is inherent to "real data, not synthetic" (G1) and is why the injected-late knob exists (live feeds can't be relied on for the late-data lesson). Mandatory security-auditor pass (first external-API integration + secrets handling).
- **The producer is real code with real upkeep** — two WS protocol adapters, reconnection, a control consumer, validation, a self-check — more than a scratch script, the intended cost of it being a curriculum dependency four sub-stories build on.

**What becomes harder:** multi-provider realism beyond these two feeds, arbitrary-symbol entry, and any persisted price history are now *further* away — the curated fixed list + live-only + single-broker floor is deliberately the minimum that teaches the Structured Streaming + compacted-control-topic semantics interviews probe, not a production market-data platform.

---

## Component / data design

```
BROWSER  price panel (checkboxes in one <form>, hx-post change delay:400ms, hx-include all)
   │ POST /prices/subscriptions (full checkbox set)                 ▲ SSE /prices/stream
   ▼                                                                │ (per-symbol OOB <polyline>)
app/web/routes/prices.py ── KafkaProducer.send ──►  price-subscriptions   PriceCollector ◄── prices
   (validate vs curated list,                        (compact, 1 part,      (to_thread poll,
    publish sorted [] snapshot)                        key="selection")       per-symbol ring buffer)
                                                            │  ▲                    ▲
   127.0.0.1:9092  (loopback host listener, #50 D3) ────────┘  │                    │ 127.0.0.1:9092
                                                               │                    │
┌── project sparkpb, network sparkpb-net ──────────────────────┼────────────────────┼──────────────┐
│  kafka (spark-kafka)   kafka-init (one-shot: creates both topics, exits)           │              │
│      │ kafka:9092 in-cluster                                 │                      │             │
│  spark-driver ── tools/price_producer/produce.py (asyncio) ──┘  publishes ► prices ─┘             │
│      │   ├ control_consumer_task  (poll price-subscriptions, resync on start, diff→reconcile)     │
│      │   ├ coinbase_adapter_task ──► wss://ws-feed.exchange.coinbase.com  (crypto)                │
│      │   └ finnhub_adapter_task  ──► wss://ws.finnhub.io?token=$FINNHUB_API_KEY  (stocks)         │
│      │                                                                                             │
│  spark-driver notebook: readStream(prices) → withWatermark(30s) → window(10s) OHLC                │
│      ├ foreachBatch → in-notebook memory table (OHLC correctness asserts)                         │
│      └ StreamingQueryListener → scratch/shared/streaming/progress.json  ──┐                       │
│         checkpoints → scratch/shared/streaming/checkpoints/               │ pull-not-push         │
└──────────────────────────────────────────────────────────────────────────┼───────────────────────┘
                                                                            ▼
                                 app GET /prices/progress reads progress.json → query-progress widget
```

**Files (developer handoff):**

*New:* `tools/price_producer/produce.py`, `tools/price_producer/README.md`; `driver/playbook/price_producer.py`; `app/prices/__init__.py`, `app/prices/collector.py`, `app/prices/svg.py`; `app/web/routes/prices.py`; `app/web/templates/fragments/_price_panel.html`, `_price_progress.html`; `content/structured-streaming/` (`manifest.yaml`, `concept.md`, `notebook.ipynb`).

*Changed:* `compose/templates/docker-compose.yml.j2` (add `kafka-init` in the `{% if include_kafka %}` block; add `FINNHUB_API_KEY`/`COINBASE_API_KEY` to `spark-driver` env); `compose/Dockerfile.spark` (add `websockets`); `app/requirements.txt` (add `kafka-python==2.0.2`); `app/topics/loader.py` (`Topic.live_price_dashboard`, `Topic.query_progress_widget` booleans, mirroring `requires_kafka`); `app/web/routes/topics.py::_shell_context` (thread the two flags); `app/web/templates/shell.html` + `fragments/_top_bar.html` (price panel + Live Prices button, flag-gated); `app/web/static/shell.js` (a fifth panel handler); `app/config.py` (`PRICE_HISTORY_LENGTH`, curated symbol list, `PRICES_TOPIC`/`SUBSCRIPTIONS_TOPIC` names); `app/main.py` (register the prices router).

**Curated symbol universe** (single source of truth, `app/config.py`, shared by validation and the panel): crypto `BTC-USD, ETH-USD, SOL-USD` (Coinbase); stocks `AAPL, TSLA, MSFT, GOOGL, AMZN` (Finnhub).

---

## Visual design (price panel — UI-facing)

No human-supplied mockup exists; written layout spec below (the price panel is a new slide-in, mirroring the Cluster Monitor panel's chrome so it reads as the same family).

**Live Prices panel** (slides from the right, ~70% width, dimmed backdrop, dark 40px header with × — identical chrome to `_monitor_panel`/Cluster Monitor):

- **Header:** "Live Prices" title + close ×. A small live-dot + "updates every ~Ns" indicator matching the dashboard header's style.
- **Ticker selector (top):** one `<form>` with a labelled `<input type="checkbox">` per curated symbol, grouped visually into **Crypto** (BTC-USD, ETH-USD, SOL-USD) and **Stocks** (AAPL, TSLA, MSFT, GOOGL, AMZN). Plain checkboxes, no new control class. Toggling any box (debounced 400ms) publishes the new selection.
- **Charts (below):** one card per *subscribed* symbol, each showing the symbol name, the latest price (right-aligned, monospace), and an inline SVG `<polyline>` sparkline of recent ticks. Cards appear/disappear as symbols are checked/unchecked.

**Distinct states to verify (beyond "it works"):**
- *Empty / nothing selected:* no chart cards; a "Select a ticker to start streaming" hint. Checkboxes all unchecked.
- *Subscribed but no ticks yet* (e.g. a stock off-hours, or within the ≤3s subscription window before the first tick): the symbol's card is present with its label but an empty/flat chart and a "waiting for ticks" state — **not** an error, **not** a fabricated line.
- *Populated:* one polyline per subscribed symbol, updating live; latest price refreshes as ticks arrive.
- *Cluster not running / broker absent:* panel shows a clear "no active cluster" state (mirrors the monitor panel's empty state), not a stack trace.
- *Refresh / reopen:* chart starts fresh from newly-arriving ticks — no restored history (ephemeral non-goal).
- *Unsubscribed symbol:* its ticks never reach the browser (server-side, not a client hide) — unchecking a box stops new points for that symbol within ≤3s of the subscription change plus feed latency.

**Query-progress widget** (US-LMD4, on the Notebook/Self-check surface of this topic): a small metrics block — batch id, input rate, processing rate, batch duration, state row count — refreshing on a timer. States: *not running* ("query not started" when `progress.json` is absent — not an error/blank/stale), and *running* (latest batch metrics). Across the no-watermark and with-watermark runs the state-row-count value visibly grows then plateaus.

---

## Open questions (flagged for human input, not blocking the developer)

- **OQ-LMD1 — Finnhub API key procurement (dependency/blocker for sub-story (a)'s stock half only).** Development on the Finnhub adapter cannot be validated live without a free-tier key in `.env`. Coinbase's public channel needs none, so the crypto half and everything downstream (b/c/d) is unblocked. This is the requirements doc's stated blocker, restated here — a human action, not a design gap.
- **OQ-LMD2 — Finnhub free-tier limits vs. the curated 5 stocks.** The free tier's WebSocket symbol/connection limits should be confirmed against 5 concurrent stock subscriptions before acceptance. If the tier caps below 5, options are: reduce the stock list, or accept that only the first N checked stocks stream (with a clear log/UI note). Flagged rather than guessed — needs a quick check against current Finnhub terms.
- **OQ-LMD3 — Query-restart safety UX** remains open per the requirements (Open Question 4) and the shell ADR — US-LMD2's checkpoint-recovery uses the documented manual/external kill, no new in-app control. Unchanged by this ADR.

---

## Risks

- **R-LMD1 — Compacted control topic created with the wrong policy (or not at all).** If `kafka-init` fails, is edited to drop the `--config cleanup.policy=compact`, or the topic pre-exists with `delete`, the live selection is lost to the 10-min retention and resync breaks. *Noticed by:* producer resync finding no/empty selection after an idle period; `kafka-topics.sh --describe price-subscriptions` not showing `compact`. *Mitigation:* `kafka-init` `--if-not-exists` + explicit config; an acceptance step asserting the topic's `cleanup.policy=compact`.
- **R-LMD2 — WS reconnect drops subscriptions silently.** If an adapter reconnects without replaying the full `desired` slice, symbols go quiet after a transient drop with no error. *Noticed by:* a chart that stops updating after a network blip and never recovers. *Mitigation:* D-LMD5 mandates full re-subscribe on every (re)connect; a test that kills/restores a socket and asserts ticks resume.
- **R-LMD3 — Late-injection vs. watermark mis-tuned.** If `--late-seconds` ≤ the 30s watermark, injected ticks land *inside* the window and the late-drop demo shows nothing dropped. *Noticed by:* US-LMD2 AC3 showing no dropped rows. *Mitigation:* default `--late-seconds=120` (~4× the watermark); the notebook documents the relationship.
- **R-LMD4 — Blocking Kafka call stalls the app/producer event loop.** `kafka-python`'s `poll()` is blocking; a direct `await`-less call would freeze every coroutine (the exact #19 failure). *Noticed by:* the price panel / whole app hanging when the broker is slow. *Mitigation:* every blocking `poll()` goes through `asyncio.to_thread` (D-LMD5, D-LMD7) — the already-learned lesson, applied to both the producer's control consumer and `PriceCollector`.
- **R-LMD5 — Secrets leakage.** `FINNHUB_API_KEY` in `.env`, threaded into the driver env and onto the Finnhub WS URL as a query param. *Noticed by:* key appearing in committed files, logs, or the notebook. *Mitigation:* `.env` gitignored (verified); the producer must not log the full WS URL/token; notebook cleanliness reset after any live run; mandatory security-auditor pass.
- **R-LMD6 — Off-hours / thin-liquidity stock looks broken.** A subscribed stock with no trades during off-hours shows a silent chart, easily misread as a bug. *Noticed by:* acceptance against a subscribed-but-flat stock. *Mitigation:* the "subscribed but waiting for ticks" visual state (Visual design) is explicit and distinct from an error; crypto (24/7) is the reliable live-demo path.
- **R-LMD7 — `numStateRows` shape varies by Spark version.** `progress.stateOperators[0].numRowsTotal` is the state-row count the growth-vs-plateau contrast depends on; a Spark bump could change the field name/shape. *Noticed by:* the widget showing no state count while the notebook works. *Mitigation:* verify the field against the target Spark (4.0.3) `lastProgress` JSON before authoring the listener; degrade to "—" rather than fabricating a number if absent.
