# Live Market Data Streaming — Requirements (v1.1 — Live Market Data Streaming)

Status: Draft for architect handoff
Owner: requirements-analyst
Date: 2026-07-19
Traceability: GitHub milestone [`v1.1 — Live Market Data Streaming`](https://github.com/hoanghaithanh/Spark-Playbook/milestone/13)
(#13), backlog row #18. Formalizes the human-approved plan
`C:\Users\hoang\.claude\plans\for-18-i-want-lazy-candle.md` (2026-07-19) into testable requirements;
it does not re-open the plan's confirmed decisions, only turns them into acceptance criteria and
flags what's genuinely still open.

## Supersedes

This doc **supersedes** the synthetic-data framing of backlog row #18 in two existing documents —
that divergence is stated explicitly here, not left to silently contradict what's on record (same
pattern already used for row #26's Skew & Salting redesign and row #30/US-C9's sharpening):

- **`docs/requirements/curriculum-topics-2026-07.md`, US-C7 (Structured Streaming topic).** All 4 of
  US-C7's acceptance criteria describe the topic running against the *synthetic* producer built for
  #50 (`tools/kafka_producer/produce.py`, US-3.2) and a generic query-progress framing. US-LMD2 and
  US-LMD4 below replace US-C7's scope for backlog row #18 — the watermark/no-watermark state-growth-
  vs-plateau demonstration and the checkpoint-recovery criterion survive conceptually (same lesson),
  but now run against real Coinbase/Finnhub prices, with a real-tick late-data injection knob
  standing in for the synthetic producer's late-data controls.
- **`docs/architecture/redesign-2026-07/topics-content-spec.md`, "## 11 — Structured Streaming"**
  (referred to as "§11" in prior planning). Its notebook walkthrough and self-check hypothesis are
  written against an unspecified generic event stream; US-LMD1 through US-LMD4 below give that
  walkthrough concrete real-data shape (curated ticker list, dual-provider ingestion, dynamic
  subscribe/unsubscribe) that §11's original text did not anticipate.

Everything else about backlog row #18 (its position in the curriculum ladder, its Phase 3/Kafka
dependency, the underlying watermark/OHLC/checkpoint-recovery *concepts* being taught) is unchanged
— this supersession is about *what data the topic runs against and how the learner interacts with
it*, not about replacing the topic's Spark-engine learning goals.

This doc also closes out the remainder of `docs/requirements/spark-playbook-mvp.md`'s original
Phase 3 stories: US-3.1 (Kafka conditionally available) and the first half of US-3.2 (rate-controlled
synthetic producer) already shipped under #50 (`docs/architecture/kafka-streaming-infra.md`,
backlog row #19, Done Sprint 10). What's left — US-3.2's checkpoint-recovery-resumes-correctly
criterion and all of US-3.3 (watermarks, stateful aggregation, checkpoint recovery, live
query-progress chart) — is covered by US-LMD2 and US-LMD4 below, now against real data plus the new
producer/dashboard/dynamic-subscription scope the approved plan adds on top.

## Problem statement

The synthetic producer built for #50 proves the Kafka-plumbing half of Structured Streaming works,
but a fabricated event stream teaches nothing about the *data* side of streaming — there's no reason
to reason about a real upstream feed's connection lifecycle, no genuine motivation for a control
topic, and no interview-relevant "this is what a production stream actually looks like" texture.
Per G1 (interview-depth and cluster realism win over platform polish when the two compete), and per
the human's own stated interest in this pivot — going deeper on Kafka mechanics specifically
(log-compacted control topics, consumer-driven dynamic subscription), not just the PySpark side —
backlog row #18 is rebuilt against two real, free, keyless-or-cheap market-data feeds (Coinbase's
public `ticker` WebSocket channel, Finnhub's free-tier stock WebSocket) instead of a synthetic
generator. This also motivates a second Kafka topic that the synthetic version had no reason to
need: a log-compacted control topic driving genuine subscribe/unsubscribe behavior on the producer's
live upstream WebSocket connections, not a client-side filter over an always-on firehose — the
harder design, chosen deliberately because it's the one that actually exercises Kafka consumer/
producer mechanics rather than routing around them.

## Goals / Non-goals

### Goals

- **G-LMD1 — Real market data, not synthetic, drives the Structured Streaming topic.** Prices
  flowing through `prices` originate from genuine Coinbase/Finnhub WebSocket connections, not a
  generator (matches the plan's Confirmed Decision 1/2).
- **G-LMD2 — Genuine dynamic subscribe/unsubscribe, not a client-side filter.** A learner's browser
  ticker selection changes what the upstream producer is actually subscribed to on Coinbase/Finnhub,
  via a Kafka control topic — this is the deliberate scope increase over "always ingest everything,
  filter in the browser" (Confirmed Decision 5), chosen specifically to exercise Kafka mechanics.
- **G-LMD3 — The Spark job remains real Structured Streaming, not a bypass.** Watermarking,
  windowed OHLC aggregation, and checkpoint recovery run as an actual `readStream` job against
  `prices` — real data changes what's flowing through the pipe, not whether Spark is really doing
  the streaming work (Confirmed Decision 1).
- **G-LMD4 — Reuse existing platform seams, add nothing structurally new.** The dashboard bridge
  (`DashboardCollector`/SSE/HTMX-OOB pattern), the `scratch/shared/` pull-not-push convention, the
  slide-in-panel UI pattern, and the producer CLI shape from #50 are all reused as-is — this is new
  *content and data*, not a new platform mechanism (matches the plan's "Reused as-is" list).

### Non-goals

- **No persisted price history.** The dashboard and producer are live-only and ephemeral, matching
  the Kafka broker's own disposable-cluster posture (10-min/512MB retention, no volume, per #50's
  ADR D6). A browser refresh or panel reopen starts the chart fresh from newly-arriving ticks —
  there is no backing store to replay from.
- **No free-text ticker entry.** The tracked symbol universe is the curated fixed list (BTC-USD,
  ETH-USD, SOL-USD; AAPL, TSLA, MSFT, GOOGL, AMZN) — the learner selects among these via checkboxes,
  not an arbitrary-symbol search box. Widening the universe is future scope, not this doc's.
- **No client-side-only filtering, by deliberate design choice (not an oversight).** The plan
  explicitly rejected the simpler "producer always ingests every curated symbol, the browser just
  hides unchecked ones" design in favor of real upstream subscribe/unsubscribe, because the human
  wants hands-on Kafka dynamic-subscription experience as part of this project. Acceptance criteria
  below test for the harder design's actual behavior, not the simpler alternative's.
- **No new frontend dependency.** Charting is hand-rolled server-computed inline SVG polylines,
  consistent with the app's existing "server computes the number, template emits styled markup"
  pattern (CSS-bar sparklines) — no bundler, no `package.json`, no charting library.
- **No in-app worker-kill/query-restart control.** The checkpoint-recovery demo (US-LMD2, AC4) uses
  the same documented manual/external kill mechanism already established for Fault Tolerance &
  Lineage (US-C9) and left open for Structured Streaming by `curriculum-topics-2026-07.md` Open
  Question 2 — no new in-app "restart the query" safety UX is built here.
- **No change to public-deploy scope.** This work has no interaction with the (waived) v1.0 remote-
  deploy surface; it is local-cluster-only, same as every other curriculum topic.

## Dependency / blocker — sub-story (a) specifically

**A Finnhub API key is required before development work starts on sub-story (a)'s stock-side
WebSocket feed.** Free signup at finnhub.io, no credit card required — this blocks only the
Finnhub half of the producer, not the whole story (Coinbase's crypto feed needs no key: its public
`ticker` channel is unauthenticated; only Coinbase's separate authenticated order/account endpoints,
not used here, need one). This blocker does not apply to sub-stories (b), (c), or (d), which consume
`prices`/`price-subscriptions` once (a) exists and are agnostic to which upstream feed produced a
given tick.

## User stories and acceptance criteria

**US-LMD1 (sub-story a) — Producer with real feeds, both Kafka topics, and genuine dynamic
subscribe/unsubscribe.**
As a learner, I want a producer that ingests genuine Coinbase and Finnhub ticks and only forwards
ticks for currently-subscribed symbols, with its live upstream subscriptions driven by a Kafka
control topic, so that changing my ticker selection is a real Kafka consumer/producer interaction,
not a display-layer filter.

- *Given* the producer running with `--self-check` against the real Coinbase and Finnhub feeds,
  *when* self-check runs, *then* it confirms a live connection to both feeds and validates it can
  publish a well-formed `prices` message (`symbol`, `price`, `event_time`, `asset_class`, `source`,
  `volume`) for at least one crypto symbol and one stock symbol from the curated list.
- *Given* the producer running with no symbols currently subscribed (an empty or not-yet-set
  desired-symbol-set on `price-subscriptions`), *when* observed, *then* no ticks are published to
  `prices` for any symbol — only actively-subscribed symbols ever produce ticks. This is the
  specific behavior that distinguishes the chosen design from an always-ingest/client-filter
  alternative, so it needs its own explicit check, not an assumed pass.
- *Given* the producer running with some symbols subscribed, *when* a changed desired-symbol-set
  adding a new symbol is published to `price-subscriptions` (e.g., via a manual test publish, no
  browser/UI needed to validate this sub-story standalone), *then* the producer's live upstream
  WebSocket subscriptions actually change to include the new symbol within a bounded, observable
  window (target: within 5 seconds of the control message being consumed — see Open Questions for
  the exact figure), confirmed via the producer's own logging. **This is the acceptance criterion
  that proves real subscribe behavior, not a client-side filter** — it tests observable behavior
  (does the live subscription set change?) without prescribing the control topic's wire format
  (full-snapshot vs. delta, debounce timing), which is left to the architect step next.
- *Given* the producer running with some symbols subscribed, *when* a changed desired-symbol-set
  removing a symbol is published, *then* the producer stops publishing ticks for the removed symbol
  within the same bounded window, while continuing to publish for symbols still selected — the
  matching unsubscribe half of the criterion above.
- *Given* the producer process restarts (crash, or the driver container restarting), *when* it comes
  back up, *then* it resyncs its live subscriptions from the last-known desired-symbol-set by reading
  the compacted `price-subscriptions` log on startup, without requiring the browser to resend its
  selection — the compacted topic is the durable source of truth for "what's currently wanted," not
  in-memory producer state.
- *Given* the `--late-frac`/`--late-seconds` knob (mirroring the convention already established by
  `tools/kafka_producer/produce.py`), *when* enabled, *then* a fraction of real ticks (real price,
  synthetic delay only) are deliberately delayed and back-dated before publishing to `prices`, so
  sub-story (b)'s late-data demo has a reliable trigger — live feeds cannot be relied on to produce
  late ticks on demand.
- *Given* a `price-subscriptions` message naming a symbol outside the curated ticker list, *when*
  consumed, *then* the producer rejects/ignores the invalid entry with a clear log message rather
  than silently subscribing to an unsupported symbol or crashing.

**US-LMD2 (sub-story b) — Spark Structured Streaming job against real prices.**
As a learner, I want a Structured Streaming job that ingests real price ticks and demonstrates
watermarking, windowed OHLC aggregation, and checkpoint recovery, so I see genuine streaming-state
behavior (state growth, plateau, late-data drop, exact resume) against real market data rather than a
synthetic stand-in.

- *Given* the `prices` topic receiving real ticks for actively-subscribed symbols (US-LMD1), *when*
  a `readStream` windowed OHLC aggregation (`groupBy(window("event_time", ...), "symbol")`) runs with
  **no** watermark, *then* the notebook/dashboard shows state row count growing every batch and never
  dropping — real data works fine for this demonstration (every new symbol/window pair is a new state
  group regardless of feed size), no synthetic assistance needed.
- *Given* the same query with `withWatermark("event_time", "<N> minutes")` added and restarted,
  *when* run, *then* state row count plateaus instead of growing unbounded — additive on top of the
  criterion above, not a replacement for it (unchanged framing from superseded US-C7's second
  criterion, now against real data).
- *Given* the producer's real-tick late-injection knob (US-LMD1) enabled, *when* deliberately delayed
  ticks arrive past the configured watermark, *then* the notebook demonstrates and the learner can
  observe (via output or state metrics) that those specific late ticks are dropped from the
  aggregate, while ticks within the watermark window are included — **this is the one place real
  data alone falls short** (live feeds don't reliably produce late ticks on demand), so this
  criterion specifically requires the injected-delay knob rather than naturally-occurring lateness.
- *Given* a checkpoint directory under `scratch/shared/streaming/checkpoints/` (mirroring the
  existing pull-not-push `scratch/shared/` convention), *when* the streaming job is killed and
  restarted against the same checkpoint, *then* it resumes processing from the correct offset/state
  per Kafka + Structured Streaming checkpoint semantics — no reprocessing or dropping of data beyond
  what the watermark semantics being taught would already predict.
- *Given* the job computes OHLC bars per symbol/window from real Coinbase/Finnhub-sourced prices,
  *when* the `foreachBatch` sink writes to an in-notebook memory table, *then* the notebook includes
  a correctness self-check specific to using real (not synthetic) prices — e.g., confirming
  `high >= low` and `open`/`close` fall within `[low, high]` for each computed bar — since real data
  has no guaranteed-clean generator behind it the way the synthetic producer did.

**US-LMD3 (sub-story c) — Live price dashboard panel.**
As a learner, I want a live-updating price dashboard where I can select which curated tickers to
watch and see their prices update in real time, so I can directly observe the effect of my own
subscribe/unsubscribe choices end-to-end, not just read about it in producer logs.

- *Given* the structured-streaming topic's manifest sets `live_price_dashboard: true`, *when* I open
  the topic page, *then* a slide-in price panel (mirroring `_monitor_panel.html`'s pattern) is
  available, showing one checkbox per curated ticker (both crypto and stock symbols).
- *Given* the panel is open and the producer (US-LMD1) is running, *when* I check or uncheck a
  ticker, *then* the new full desired-symbol-set is published to `price-subscriptions`, and — within
  the same bounded window as US-LMD1's third/fourth criteria — the panel's chart begins or stops
  showing new price points for that ticker.
- *Given* at least one ticker is subscribed and the producer is publishing real ticks, *when* the
  panel is open, *then* an SSE connection sourced from a new `PriceCollector` (structurally
  identical to `DashboardCollector`, consuming raw ticks directly from `prices`) streams new ticks to
  the browser, and each subscribed ticker renders as a hand-rolled inline `<polyline>` SVG chart that
  updates as new ticks arrive — no client-side filtering of unsubscribed symbols occurs, because
  unsubscribed symbols' ticks never reach the browser in the first place.
- *Given* the panel is closed (no active SSE subscriber), *when* observed, *then* `PriceCollector`'s
  background consumption task is not running, consistent with `DashboardCollector`'s existing
  ≥1-subscriber-gated pattern — no wasted background work when nobody's watching the panel.
- *Given* the dashboard reads raw ticks directly from `prices` rather than Spark's aggregated OHLC
  output, *when* the Spark Structured Streaming job (US-LMD2) has not been started, *then* the live
  price dashboard still shows real-time prices — the dashboard's correctness does not depend on the
  Spark job running, so a learner can watch live prices before ever touching the notebook.
- *Given* the ephemeral/no-persistence decision, *when* the browser is refreshed or the panel is
  reopened, *then* no prior price history is restored — the chart starts fresh from newly-arriving
  ticks only, matching the live-only, no-persisted-history non-goal.

**US-LMD4 (sub-story d) — Query-progress widget.**
As a learner, I want a widget showing the running Structured Streaming query's live progress, so I
can observe streaming-specific behavior (batch duration, input/processing rate, state row count) in
real time while the job runs, without reading raw notebook output.

- *Given* the Structured Streaming job's `StreamingQueryListener` writes
  `scratch/shared/streaming/progress.json` on every batch (mirroring `driver/playbook/annotate.py`'s
  existing checkpoint-dump pull-not-push pattern), *when* the widget reads that file, *then* it
  displays the latest batch's metrics — batch id, input rate, processing rate, batch duration, and
  state row count for the windowed aggregation — sourced from `query.lastProgress`/`recentProgress`.
- *Given* the query has not been started yet (no `progress.json` present), *when* the widget is
  viewed, *then* it shows a clear "not running" state rather than an error, a blank chart, or stale
  data from a prior run.
- *Given* the watermark-vs-no-watermark demonstration (US-LMD2, first two criteria), *when* the
  widget is viewed across both runs, *then* the state-row-count metric it displays visibly shows the
  same growth-vs-plateau contrast — the same underlying data, surfaced live in the app rather than
  only readable from notebook output.
- *Given* the widget is open while the query runs, *when* a new batch completes, *then* the displayed
  metrics refresh within a few seconds (consistent with the existing SSE/dashboard refresh cadence
  used elsewhere in the app) — via the pull-not-push `scratch/shared/` file read, not a new push
  mechanism.

## Scope note — sequencing across the four sub-stories

Matching the plan's own execution sequence and this repo's precedent (public-deploy's 6-issue split
under one release milestone): US-LMD1 is testable standalone via `--self-check` plus manually
publishing to `price-subscriptions`, with no Spark or web changes needed — it should land and be
reviewed first, since it de-risks the one genuinely new technical unknown (real-feed late-data
injection) before anything else depends on it. US-LMD2 depends only on US-LMD1 being mergeable.
US-LMD3 depends on US-LMD1 for real data but is independent of US-LMD2, since the dashboard reads raw
ticks, not Spark's aggregated output (see US-LMD3's fifth criterion). US-LMD4 depends on US-LMD2's
notebook existing, to know the exact progress-metric shape it's reading. Splitting these into four
separate, independently-reviewable GitHub issues happens after the architect ADR, not at this
requirements stage — mirroring how #50's Kafka infra requirements existed before its single issue was
filed following the architect pass.

## Open questions

1. **Subscribe/unsubscribe wire protocol — explicitly the architect's call, not resolved here.**
   Whether `price-subscriptions` messages carry a full snapshot of the desired symbol set on every
   change (simpler, matches the compacted-topic-as-source-of-truth framing) or a delta
   (add/remove events), and what debounce timing (if any) applies between a checkbox click and the
   control message being published, are both left open by the plan on purpose. US-LMD1's third and
   fourth criteria test the *resulting observable behavior* (does the live subscription set change
   within N seconds?) precisely so this doc doesn't accidentally pre-empt that decision.
2. **Exact bounded-latency figure for subscribe/unsubscribe (currently drafted as "within 5
   seconds").** This is a reasonable placeholder, not a locked number — it should be confirmed or
   adjusted by the architect once the wire protocol (Open Question 1) and any debounce timing are
   decided, since debounce timing directly affects what's achievable.
3. **Exact watermark window size for US-LMD2.** The plan doesn't specify a concrete "<N> minutes"
   value for `withWatermark`. Left to the developer/architect to pick based on the curated feeds'
   real tick rates and the OHLC window size chosen — same kind of implementation judgment call this
   repo already delegates in similar cases (e.g., Executor Tuning's dataset-size choice).
4. **Structured Streaming's query-restart safety UX remains genuinely open**, per
   `curriculum-topics-2026-07.md` Open Question 2 (Fault Tolerance & Lineage's half was resolved
   2026-07-19; Structured Streaming's half was explicitly left unresolved at that time, since #18 was
   still sequenced well behind Kafka infra). This pivot does not change that: US-LMD2's
   checkpoint-recovery criterion (fourth bullet) still uses a documented manual/external kill-and-
   restart mechanism, not a new in-app control, consistent with that still-open question's current
   disposition (documented manual step, no in-app control being built).

## Constraints

- **Curated fixed ticker list**: BTC-USD, ETH-USD, SOL-USD (crypto, via Coinbase's public `ticker`
  channel) and AAPL, TSLA, MSFT, GOOGL, AMZN (stocks, via Finnhub) — confirmed decision, not
  reopened by this doc (see Non-goals).
- **Two Kafka topics on the existing broker** (same `sparkpb` compose project as #50, no new
  compose service): `prices` (data plane, keyed by symbol, 3 partitions) and `price-subscriptions`
  (control plane, log-compacted, single key). Both reuse #50's existing broker/resource-ceiling
  accounting — no change to the +2GB Kafka reservation.
- **Producer runs inside `spark-driver`, notebook-launched, not a standing compose service** — same
  reasoning #50's ADR already used to reject an always-on producer service (can't rate/session-
  control it; muddies the checkpoint-recovery lesson if it's always running).
- **Secrets**: a gitignored `.env` (repo root `.gitignore` already covers this from the public-deploy
  work) holds `FINNHUB_API_KEY` (required) and an unused `COINBASE_API_KEY` placeholder (not
  required — the public channel used needs no key), threaded into `spark-driver`'s `environment:`
  block the same way `SPARKPB_PUBLIC_ORIGIN` already is. No new secrets-handling code in
  `app/config.py` — the FastAPI app itself never touches these keys, only the driver container does.
- **No new frontend dependency** — hand-rolled inline SVG, consistent with the existing static-assets
  set (`htmx.min.js`, `htmx-sse.js`, `shell.js`, `style.css`, nothing else).
- **Security-auditor pass is mandatory**, unlike #50 — this is the first real external-API
  integration plus secrets handling in the codebase, per the plan's own execution sequence (run at
  minimum after sub-story a lands).
- **Notebook cleanliness**: per CLAUDE.md, any `content/structured-streaming/notebook.ipynb`
  executed during development/verification must be reset (`git checkout -- <path>`) before the work
  is considered done, same as every other topic notebook in this repo.
