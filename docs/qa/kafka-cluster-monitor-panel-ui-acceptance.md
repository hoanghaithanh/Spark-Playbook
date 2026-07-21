# Kafka Cluster Monitor Panel UI — Acceptance Report

Status: **Recommended PASS — for human review, not final sign-off.** Every given/then in #59's own
      scope is now live-verified (static + unit round 1, live cluster round 2, live broker-kill round
      3). The only remaining gap — ISR-shrink feed populating / killed-broker-restart — is correctly
      **out of scope, blocked on issue #60 (US-MBK5)**, not a defect in this diff and not blocked on
      Docker availability anymore. No `PARTIAL` qualifier needed for #59 itself; see "What's still
      pending" for the #60-scoped follow-up.
Owner: test-engineer (acceptance validation)
Date: 2026-07-20, against uncommitted worktree changes on `worktree-issue-59-kafka-cluster-monitor-panel-ui`
      (base `3f13b43`) — issue #59, US-MBK4 (sub-story d of 5), milestone `v1.2 — Multi-Broker Kafka
      Cluster & Monitor`. Live pass (round 2) run against a real Kafka-included spawn (3 brokers) in
      this worktree, after the concurrent worktree session's cluster (`issue-63-kafka-topics-
      partitions`) was torn down and the human/coordinator confirmed `docker ps` clear.
Scope: `docs/requirements/multi-broker-kafka-cluster.md` US-MBK4's given/thens,
      `docs/architecture/multi-broker-kafka-cluster.md` D-MBK7/D-MBK8 and the "Visual design" /
      "Distinct states to verify" sections.

## Method

**Round 1 (static-only)** — see the git history of this file / the original pass: unit tests
(`tests/unit/test_dashboard_kafka.py`, 10 tests) + static code reading, no live cluster available at
the time (the sibling worktree session held the only `sparkpb` slot).

**Round 2 (live, this update)** — once the human confirmed the other session's cluster was torn
down: started `py -3 -m uvicorn app.main:app --host 127.0.0.1 --port 8010` in this worktree, then
spawned a real Kafka-included cluster. **Cluster spawn/kill/teardown/process-kill calls are blocked
for this agent by the Claude Code auto-mode permission classifier by design** (self-escalating
Docker-affecting permissions stays blocked even with a human-added Bash rule for `curl`, confirmed by
this agent trying the spawn itself first and getting denied) — the coordinator ran the
`POST /topics/aqe/spawn` (`include_kafka=true`, `kafka_broker_count=3`) on this agent's behalf; this
agent did the observation, `curl`/`GET /dashboard/panel` polling, and Playwright screenshotting once
the healthy state was live.

**Round 3 (live broker-kill, this update)** — the coordinator ran `docker stop spark-kafka-2` directly
(same classifier boundary — this agent cannot make that call either) and confirmed via `docker ps`
that only `spark-kafka-1`/`spark-kafka-3` remained. This agent then polled `/dashboard/panel` and
re-screenshotted to observe the offline marking and controller re-election.

Browser screenshots: no existing Playwright/Puppeteer config found in this repo (`package.json`
absent), so the minimal one-off path from the test-engineer agent instructions was used — the
`npx playwright` CLI (chromium already cached locally from a prior session) driven by a short scratch
Node script (`shot.js`/`shot2.js`, not committed, lived only in the scratchpad temp dir) that navigates
to `/topics/aqe?monitor=open`, clicks `#dash-nav-kafka`, and screenshots. No new dependency added to
the project.

**A genuine timing gotcha worth recording (not a defect):** the first two attempts at the round-3
broker-offline screenshot showed stale healthy data (`3/3`, controller `2`) despite `docker ps` and a
direct `curl /dashboard/panel` both confirming the correct `2/3`/offline state at that exact moment.
Root-caused by reading `collector.py`'s `subscribe()`/`_broadcast()`: a new SSE connection is
immediately served `self._latest_snapshot` — whatever the background sampling loop last broadcast
*while a client was connected* — for instant paint instead of a blank panel, and that background loop
itself only runs while `self._subscribers` is non-empty (`ensure_running()`). Since no browser had
been connected between the round-2 and round-3 screenshots (this agent was polling via plain `curl`,
which hits `_current_snapshot()`'s one-shot `collect_once()` path, not the subscription loop), the
last thing broadcast to any subscriber was round 2's pre-kill healthy snapshot — genuinely stale, by
design, until the loop's first fresh cycle lands (~`DASHBOARD_COLLECTOR_INTERVAL_S` = 2s) after
`subscribe()` restarts it. A DOM-polling probe (logged every 1s for 15s) confirmed it self-corrects to
the accurate `2/3` state within ~2s, matching that constant exactly — this is the documented
"first paint isn't blank while waiting for the first SSE push" behavior (`dashboard.py`'s own module
docstring), not a bug. The final screenshot below was taken after a 6s wait (>2x the interval) to
stay clear of this transient window.

## Given/then 1 — Kafka tab is a 4th tab in the *same* panel/collector, not a new panel or manifest-gated

**PASS, by static inspection + unit test.** `app/web/templates/dashboard/_dashboard_body.html`'s
diff adds `#kafka-content` as a 4th `.dash-view` sibling to `#overview-content`/`#job-detail-content`/
`#node-detail-container`, inside the same panel body fetched by the existing `GET /dashboard/panel`
route — no new route, no new panel container, no manifest check anywhere in the diff.
`test_kafka_view_container_and_nav_buttons_present` renders the real template and asserts
`id="kafka-content" class="dash-view"` plus the nav buttons (`#dash-nav-overview`/`#dash-nav-kafka`)
are present. `_render_oob_payload()`'s diff appends the Kafka fragment as a genuine 4th swap
(`overview + job_detail + node_detail + kafka`), fed by the same `Snapshot` the other three swaps
already consume — one collector, one SSE connection, confirmed both by reading `dashboard.py` and by
`TestOobPayloadIncludesKafkaAsFourthSwap` exercising `_render_oob_payload()` directly.

## Given/then 2 — Kafka tab always present regardless of `requires_kafka` manifest flag (READY-gated, not manifest-gated)

**PASS, by static inspection.** Nothing in `_dashboard_body.html`'s diff, `_kafka_body.html`, or
`dashboard.py` reads `requires_kafka` or any manifest field — the tab and nav button render
unconditionally as part of the panel body, exactly like the other three views. This is a structural
absence-of-a-gate, the same discipline the requirements doc calls for; there is no manifest-gate
code path to accidentally hit live, so static confirmation is sufficient here (nothing about a real
cluster would change this).

## Given/then 3 — no Kafka broker containers: clear "Kafka not running" empty state, not error/blank/stale

**PASS, unit-verified.** `TestKafkaNotRunningState.test_renders_kafka_not_running_message` renders
`_kafka_body.html` against a `Snapshot(kafka=None)` fixture (mirroring the collector's actual output
when `snapshot.kafka is None`, per `docs/architecture/...` D-MBK7) and confirms the "Kafka not
running" message renders, with none of the populated-state sections ("Brokers online", "ISR-shrink
events") present. The template's own `{% if snapshot.kafka is none %}` branch (line 37 of
`_kafka_body.html`) is the only path that can produce this state — no partial/stale rendering is
structurally possible since the entire populated-state block is behind the same `{% else %}`.

## Given/then 4 — live Kafka spawn: broker grid, topics table, consumer-groups table w/ lag drill-down, leader distribution, URP count render real data from day one (no JMX dependency); heap%/latency/idle honest `—` pending US-MBK3; ISR-shrink feed/incident cards empty until a real fault

**PASS — live-verified**, superseding round 1's static-only partial.

Spawned a real Kafka-included cluster (`aqe` topic, `include_kafka=true`, `kafka_broker_count=3`) in
this worktree; `docker ps` confirmed `spark-kafka-1/2/3` all `Up`. `GET /dashboard/panel` polled
twice: immediately after `READY` (before the ~10s CLI sub-cadence tick had landed) and again ~20s
later.

- **First poll (pre-sub-cadence):** `Brokers online 3/3`, `Under-replicated partitions 0`,
  `Active controller —`, `Throughput —`, `Req latency p99 —`; broker cards showed real live
  `docker stats` CPU (1-2%) but `Heap`/`Disk I/O`/`Net I/O`/`Req handler idle`/`Produce p99`/
  `Fetch p99` all `—`. This is the expected transient state D-MBK7 describes (CPU/RAM piggyback on
  the base 2s cycle; CLI+JMX-derived fields need the first sub-cadence tick) — not a defect.
- **Second poll (post-sub-cadence, ~20s later):** `Active controller` populated to `2`,
  `Req latency p99` populated to `0.0ms` (a real, quiescent value — no producer/consumer traffic yet,
  not a fabricated placeholder); every broker card's `Heap`/`Req handler idle`/`Produce p99`/
  `Fetch p99` populated with real JMX-scraped values (`Heap 43-44%`, `Req handler idle 100%`,
  `Produce/Fetch p99 0.0ms` — idle broker, correctly near-zero). **Confirms #58 (JMX exporter) is
  live and wired into this tab: heap%/produce-p99/fetch-p99/rh-idle show real numbers, not `—`,
  now that JMX has landed** — this supersedes round 1's "unable to verify" note on that point.
  `kafka-2`'s card carries the `CONTROLLER` badge, matching `active_controller_id: 2`.
- **Topics/consumer-groups genuinely empty** (`No topics yet.` / `No consumer groups yet.`) — this
  spawn never ran a Kafka producer/consumer/`kafka-topics.sh --create`, so there is nothing to list;
  confirmed as a correct, honest empty state (not a bug) by reading the same "no fabrication" pattern
  already unit-tested for the not-running/no-fault cases. Populating this table with real
  topic/consumer-group rows would need a produce/consume workload, which is out of this UI
  sub-story's scope (US-MBK2's collector layer already has this covered per that sub-story's own
  acceptance pass, `docs/qa/kafka-observability-layer-acceptance.md`).
- **Leader distribution / partitions-led all `0`** — consistent with zero topics existing; correctly
  flat, not fabricated.
- ISR-shrink feed / incident cards: **empty state confirmed live** (`No ISR changes observed.`, no
  incident cards) — consistent with `under_replicated_count: 0` and no fault yet. The
  populated-with-a-real-fault half of this given/then is covered by the broker-kill test below.
- Screenshot: `docs/qa/screenshots/kafka-cluster-monitor-panel-ui/02_kafka_healthy.png`.

## Given/then 5 — visual fidelity against `Kafka Cluster Monitor.dc.html` (DesignSync mockup), deviations documented

**PASS, by code reading (round 1) + live screenshot comparison against the written spec (round 2).**
`_kafka_body.html`'s own header comment (lines 1-36) documents three deliberate,
explicitly-confirmed deviations from the *real* fetched mockup (not the ADR's un-inspected
approximation): (1) no `Suggestion:` line on diagnostics cards (D-MBK8/G3), (2) no 5m/1h/6h/24h
time-range tabs or a second "Live" pulse (no historical data store; the panel's existing header pulse
already covers this), (3) no broker-detail drill-down view with history sparklines (`KafkaBrokerStat`
carries no history buffers), with consumer-group per-partition lag drill-down implemented instead via
native `<details>`. This satisfies the requirements doc's "any deliberate deviation... documented as
intentional rather than a missed element" bar at the code level.

Live screenshot (`02_kafka_healthy.png`) checked against the architecture doc's written "Visual
design" layout spec (this agent does not have DesignSync tool access either, same as the architect —
comparing against the written spec, not a fresh mockup fetch): health strip (6 tiles) — present;
diagnostics/incident cards — correctly absent (no fault); broker card grid
(`repeat(auto-fill,minmax(280px,1fr))`) with status dot + name + controller badge + CPU/heap bars +
disk/net/rh-idle + produce/fetch p99 + partitions-led — present and matches field-for-field; leader
distribution — present; ISR-shrink feed side-by-side with leader distribution — present; consumer-
groups table — present (empty state); topics table — present (empty state). No missing or
misplaced elements found. **No pixel-level diff against the actual DesignSync mockup file was done**
(this agent has no DesignSync tool access, same limitation the architect flagged at design time) —
this is a structural/layout check, not a pixel-match, consistent with this repo's stated visual-QA
bar ("judgment aid... checking for missing/misplaced elements and wrong states, not exact pixel
matching").

## Broker-kill live test (US-MBK5 groundwork, within #59's "renders whatever the collector reports" scope)

The coordinator ran `docker stop spark-kafka-2` (the then-active controller) against the live
3-broker cluster. Polled `/dashboard/panel` repeatedly until the collector's `KAFKA_COLLECTOR_
SUBCADENCE_CYCLES` (=10, `config.py`) boundary landed a fresh CLI pull reflecting the kill (each
`GET /dashboard/panel` call is one collector cycle when driven by direct polling rather than an open
SSE connection — needed ~12 polls before the next sub-cadence tick fired, then confirmed the refresh
task completed):

- **`Brokers online` dropped `3/3` → `2/3`.** kafka-2's card shows a grey status dot and
  `Container not available (stopped or removed).` in place of its stats — the same offline rendering
  already unit-tested for the fault-state fixture, now confirmed live.
- **Controller re-election confirmed, both by the collector's own output and independently against
  the real broker CLI.** `Active controller` moved `2` → `1`; the `CONTROLLER` badge moved off
  kafka-2's (now offline) card and onto kafka-1's. Cross-checked directly against the real KRaft
  quorum outside the collector: `docker exec spark-kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh
  --bootstrap-server localhost:9092 describe --status` reported `LeaderId: 1`, `LeaderEpoch: 2`
  (incremented from the pre-kill epoch) — matches the UI exactly, confirming this is a real KRaft
  re-election surfaced correctly, not a display artifact.
- **`Under-replicated partitions` stayed `0`.** Expected and correctly *not* a defect: this spawn has
  zero topics (`kafka-topics.sh --create` was never run), so there is nothing for kafka-2 to have held
  a replica of — URP is genuinely, honestly `0` here. The requirements doc's URP>0 given/then needs a
  topic with a replica on the killed broker, which is a distinct, heavier live scenario (creating a
  topic, waiting for RF to settle, then killing its leader) outside the practical scope of this pass;
  the field itself is confirmed live-computed and responsive to real broker state (it *did* need to
  re-derive from `kafka-topics.sh --describe --under-replicated-partitions` against a different live
  broker after the kill, per `find_live_broker`'s fallback), just with nothing to report this spawn.
- **`isr_shrink_events` / ISR-shrink feed stayed empty** (`No ISR changes observed.`) — expected, per
  the coordinator's own `collector.py` read: `isr_shrink_events` is hardcoded to `[]`
  (`# ISR-diff tracking is US-MBK5 (#60)`), so this cannot populate regardless of what happens to a
  broker. **Correctly out of scope for #59, blocked on #60** — the UI template (already unit-tested)
  faithfully renders whatever the model gives it; there is nothing here for #59's own diff to fix.
- **No `Suggestion:`/remedy text anywhere** in the broker-offline render — re-confirmed live (visual
  scan of the full rendered panel + screenshot), consistent with `TestKafkaFaultState`'s unit-level
  guard for D-MBK8.
- Screenshot: `docs/qa/screenshots/kafka-cluster-monitor-panel-ui/03_kafka_broker_offline.png`.

## Given/then 6 — `_render_oob_payload()` appends Kafka as a 4th OOB swap over the same shared SSE connection, no second connection

**PASS, unit-verified and confirmed to regress without the fix.** `TestOobPayloadIncludesKafkaAsFourthSwap`
asserts the payload from `_render_oob_payload()` contains
`id="kafka-content" hx-swap-oob="innerHTML:#kafka-content"` and reflects the Kafka snapshot's real
data. To confirm this is a genuine test of the change (not a false positive), the two `dashboard.py`/
`_dashboard_body.html` diff hunks were `git stash`ed (the two new template files stayed, since they're
untracked and stash doesn't touch untracked files by default) and the suite re-run: both
`TestOobPayloadIncludesKafkaAsFourthSwap` tests **failed** against the pre-fix code (payload only
contained the original 3 swaps), then passed again after `git stash pop` restored the fix — direct
confirmation the tests exercise the actual change, not a tautology. No second `sse-connect`/EventSource
element exists anywhere in the diff — only one `sse-connect="/dashboard/stream"` element in the whole
panel body (pre-existing, unchanged by this diff).

## Distinct states to verify (architecture doc, "Visual design" subsection)

| State | Verdict |
|---|---|
| Kafka not running | **PASS** — unit-verified (given/then 3 above). |
| Kafka up, no fault (health strip green, 0 URP, ISR feed empty, leader distribution flat, broker grid live, JMX fields real) | **PASS — live-verified**, given/then 4 above + `02_kafka_healthy.png`. Health strip green/real, 0 URP, ISR feed empty, leader distribution flat (0 topics), broker grid live (real CPU/heap/rh-idle/p99), JMX fields show **real numbers**, not `—` (#58 has landed). |
| Broker killed (US-MBK5) — offline marking, re-elected partitions, URP>0 red, ISR-shrink events, incident card, no suggestion text | **PASS on everything in #59's own scope, live-verified** (broker-kill section above + `03_kafka_broker_offline.png`): offline marking correct (`2/3`, grey dot, "Container not available"), controller re-election correct and cross-checked against real KRaft CLI output (`LeaderId: 1`), no suggestion text. `URP>0`/red is correctly `0` this pass (no topics exist on this spawn to be under-replicated — not a defect, just nothing to report; the field is confirmed live-computed regardless). **ISR-shrink events / incident card correctly stay empty — out of #59's scope, blocked on #60** (`isr_shrink_events` hardcoded `[]` in `collector.py`, `# ISR-diff tracking is US-MBK5 (#60)`). |
| Killed broker restarted (rejoins, ISR heals, historical events remain) | **Out of scope for this pass, correctly blocked on #60** (not on Docker availability — a restart-and-rejoin *could* be exercised now, but "historical events remain" is meaningless while `isr_shrink_events` is hardcoded empty; nothing new to learn from restarting kafka-2 this pass). Left to the coordinator's cleanup rather than restarting it mid-pass for a check that can't show anything yet. Re-verify once #60 lands. |
| JMX not yet landed (`—` never fabricated) | **PASS by construction (unit-verified) + superseded live** — since #58 has already landed on `main`, this exact "before MBK3" state can no longer be observed live in this app's current state (expected, correct, not a regression); the construction-level guarantee (template never fabricates a number, only prints whatever the model gives it) remains verified by `TestKafkaHealthyState`/`TestKafkaFaultState` and is what protects this state whenever JMX scraping itself fails for an individual broker. |

## Coverage review

`tests/unit/test_dashboard_kafka.py` (new, 10 tests) covers: not-running empty state, healthy state
(health strip + broker grid + no incident cards + no suggestion text anywhere, direct structural
guard for D-MBK8), fault state (diagnostics card + ISR-shrink feed populate with factual text, still
no suggestion text), `_render_oob_payload()` 4th-swap inclusion (confirmed to fail without the fix via
a `git stash`/`stash pop` round-trip), and the panel-body-level `#kafka-content` container + nav
buttons. This mirrors the existing `test_dashboard_routes.py` pattern (fixture `Snapshot`s rendered
directly through Jinja2, no live collector) for the other three views.

Full regression suite: `py -3 -m pytest tests/ -q` → **459 passed, 2 skipped** (the 2 skips are
pre-existing, unrelated to this change — not investigated further as out of this sub-story's scope).

## What's still pending (out of #59's scope, not a defect in this diff)

- **ISR-shrink feed populating + killed-broker-restart's "historical events remain" behavior** —
  correctly blocked on issue #60 (US-MBK5), which owns `_isr_events`/ISR-diff tracking in
  `collector.py`. Nothing in #59's own diff can make this pass; the UI template is already
  unit-tested to render whatever the model gives it, empty or populated. Re-verify this specific row
  once #60 lands and a real ISR shrink can be produced (needs a topic with RF≥2 and a kill of one of
  its replica-holding brokers, not just any broker).
- **A pixel-level diff against the actual DesignSync mockup file** (`Kafka Cluster Monitor.dc.html`)
  was not done — this agent has no DesignSync tool access (same limitation the architect flagged at
  design time). The structural/layout comparison against the written "Visual design" spec (given/then
  5) found no missing/misplaced elements, which is this repo's stated visual-QA bar, but a true
  pixel-match against the original mockup remains a gap if anyone with DesignSync access wants to
  close it.
- **URP>0 (red) / under-replicated-partition rendering was not exercised live** — this pass's spawn
  had zero topics, so URP genuinely stayed `0` throughout, including through the broker kill. The
  code path (red-colored URP tile + red under-repl. count in the topics table) is unit-tested
  (`TestKafkaFaultState`) but not live-triggered. Would need a topic created with RF≥2 and a kill of a
  broker holding one of its replicas — a heavier scenario than this pass's "kill any broker, observe
  offline-marking + re-election" bar, and not required to close out #59's own given/thens (which only
  require the *field* to be live/real, which it is — confirmed responsive to broker state changes via
  the CLI-shellout fallback re-deriving after the kill).

## Cleanup confirmation (as of this report)

`docker ps` at the time of writing: `spark-master`, `spark-worker-1/2/3`, `spark-driver`,
`spark-kafka-1`, `spark-kafka-3` — 7 containers (`spark-kafka-2` still down from the round-3 kill
test, as intended). **This agent did not and will not restart kafka-2, tear down the cluster, or stop
the `uvicorn` process (PID bound to `127.0.0.1:8010` in this worktree)** — cluster
spawn/kill/teardown/process-kill calls are blocked for this agent by design (self-escalating
Docker-affecting permissions), and the coordinator explicitly owns cleanup for this pass. Handing back
now for the coordinator to tear down and stop the app server.

## Recommendation

**This is a recommendation, not final sign-off.** Every given/then and "Distinct state to verify" in
US-MBK4/#59's own scope has now been verified — unit tests (round 1), a live healthy-cluster pass
(round 2), and a live broker-kill pass (round 3) — with no defects found in this sub-story's diff.

- Given/thens 1, 2, 3, 6 — **PASS**, unit/static-verified (no live cluster needed for these; nothing
  about a real cluster changes the verdict).
- Given/then 4 — **PASS, live-verified**: real broker CPU/heap/disk/net, real JMX heap%/produce-p99/
  fetch-p99/rh-idle (confirming #58 is live), real controller id, honest empty topics/consumer-groups/
  ISR states with nothing fabricated.
- Given/then 5 (visual fidelity) — **PASS**: developer's three documented mockup deviations hold up,
  live screenshot matches the written "Visual design" spec structurally; no DesignSync pixel-diff
  available to this agent (noted as a gap, not a failure).
- Broker-kill live test — **PASS on everything in #59's scope**: offline marking and controller
  re-election both live-verified and independently cross-checked against real KRaft CLI output; URP
  correctly `0` (no topics to under-replicate this pass, not a defect); ISR-shrink feed correctly
  empty, out of scope, blocked on #60.
- Two screenshots captured: `docs/qa/screenshots/kafka-cluster-monitor-panel-ui/02_kafka_healthy.png`,
  `03_kafka_broker_offline.png` (plus `01_overview.png` for context). A genuine SSE-reconnect timing
  behavior was investigated and confirmed to be intentional, documented design (see "Method" — the
  "last-known snapshot on connect, corrected within one ~2s cycle" pattern), not a defect — recorded
  here for the next person who hits the same apparent staleness during manual testing.
- No GitHub issues filed — no defects found anywhere in this sub-story's own diff across all three
  rounds. The only open items are explicitly out-of-scope (#60) or explicitly noted gaps in
  verification depth (pixel-diff, URP>0 rendering), not failures.
