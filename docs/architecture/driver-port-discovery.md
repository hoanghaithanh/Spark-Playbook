# ADR: Driver REST port discovery across the 4040–4042 range (issue #24)

Status: accepted · 2026-07-15 · scope: bug fix touching a shared interface contract

## Context

`app/config.py` hardcodes `DRIVER_APP_UI_URL = "http://localhost:4040"` and every
function in `app/spark_api/app_client.py` builds its URL from that single fixed
port. In real use, a learner switching topic notebooks without shutting the prior
Jupyter kernel leaves the first driver holding `:4040`; Spark's `SparkUI` silently
falls back to `:4041` (then `:4042`) for the second, still-alive SparkContext
(confirmed live: kernel 1 `app-…-0001` on `:4040`, kernel 2 `app-…-0002` on
`:4041`). The collector re-runs `fetch_current_app_id()` every cycle (that part is
fine), but because it only ever queries `:4040` it stays locked onto whichever app
first grabbed that port — hence the frozen Job Detail view. The compose template
already publishes `4040-4042` (issue #15 comment), so the multi-port scenario was
anticipated but never wired into the client. `annotation.py` carries the identical
latent bug.

Confirmed *not* the SSE/OOB delivery layer (issue #22) — a live stream capture
showed correct per-cycle regeneration of stale-but-faithfully-rendered REST data.

## Decision

Port discovery lives in `app_client.py` (it already owns "which driver do I talk
to"; `docker_stats.py` is a separate concern and doesn't know app-ids). The client
gains a small resolution step that probes every candidate port's
`/api/v1/applications` once, and returns an **`AppRef(app_id, base_url)`** value.
`base_url` is then threaded — as the `AppRef` itself — into every downstream call
so all calls in one cycle/request provably agree on the same port. **No internal
module-level cache of "last resolved port"**: the collector runs these calls in
separate `asyncio.to_thread` workers and annotation runs concurrently, so hidden
shared state is exactly the "re-resolve and disagree mid-cycle" hazard we're
avoiding. Resolution happens once per cycle/request; the resolved `AppRef` is the
single source of truth passed everywhere downstream.

"Current" application = **most recent `startTimeEpoch` among all still-running
attempts across all reachable ports** (edge case #5, confirmed: the dashboard
should follow whatever the user most recently kicked off). Use the numeric
`startTimeEpoch` field, not the ISO `startTime` string — no parsing.

### Alternatives considered
- **Internal last-resolved-port cache, keep signatures unchanged** — rejected:
  unsafe under `to_thread`/concurrent annotation access, and doesn't actually
  guarantee intra-cycle agreement unless resolve+use are atomic, which a cache
  isn't.
- **Discover the port in `docker_stats.py`** (it already inspects the driver
  container) — rejected: it inspects containers by name/IP, has no notion of
  app-ids or which port a SparkContext bound; wrong layer.
- **Return a bare `base_url` string alongside the existing bare `app_id`** —
  rejected: two parallel values that callers must keep paired by hand; one
  `AppRef` makes "these travel together" the type.

### Consequences
- Every downstream fetcher signature changes (see contract below) — that's the
  point of escalating; the unit tests in `tests/unit/test_app_client.py`,
  `test_collector.py`, `test_annotation_routes.py` that monkeypatch these must be
  updated to the new signatures.
- Worst-case first-paint latency rises: resolution probes up to 3 ports
  sequentially, so an all-ports-hung driver costs up to `3 × timeout_s` instead of
  one. Reachable ports answer fast; only genuinely-down ports eat their timeout.
  Still fully inside the existing `to_thread` offload (issue #19), so it stalls
  only that dashboard cycle's data, never the event loop. Consider a slightly
  shorter per-probe `timeout_s` (~1.5s) inside resolution if it bites.
- Deep links now point at the correct port (a latent bonus fix): `stage_ui_url`
  built from the resolved `base_url` means a `:4041` app's Spark-UI links actually
  resolve, instead of always sending the browser to `:4040`.

## Component / data design

### Port range constant — `config.py`, next to the URL constants

```python
# Driver Spark-UI/REST ports. Spark rebinds upward when :4040 is held by an
# already-live SparkContext (orphaned kernel; issue #15/#24); compose publishes
# this same 4040-4042 range. Fixed small list, not derived — it mirrors the
# literal port mapping in compose/templates/docker-compose.yml.j2, not a
# variable-driven range like WORKER_COUNT_RANGE.
DRIVER_APP_UI_PORTS = (4040, 4041, 4042)
```

Keep `DRIVER_APP_UI_URL = "http://localhost:4040"` as the canonical default (used
for the page-header "open Spark UI" link fallback below); the probe list drives
discovery.

### `app_client.py` — new/changed contract

```python
@dataclass(frozen=True)
class AppRef:
    app_id: str
    base_url: str          # e.g. "http://localhost:4041"

# --- resolution (new) ---
def resolve_current_app(timeout_s: float = 3.0) -> Optional[AppRef]:
    """The most-recently-started still-running application across all
    DRIVER_APP_UI_PORTS. Replaces fetch_current_app_id() as the collector's
    entry point (issue #24)."""

def resolve_app(app_id: str, timeout_s: float = 3.0) -> Optional[AppRef]:
    """Locate which port currently serves a *specific* app_id (running OR
    completed). For annotation's checkpoint path, whose app_id comes from the
    checkpoint file, not from live discovery."""

# --- shared probe helper (new, private) ---
def _probe_ports(timeout_s) -> List[Tuple[str, list]]:
    """[(base_url, applications_json), ...] for every port that answered with a
    JSON list. Ports that are unreachable / wrong-shaped are simply absent."""

# --- fetchers: now take the resolved AppRef instead of a bare app_id ---
def fetch_stages(app: AppRef, timeout_s=3.0) -> Optional[List[dict]]: ...
def fetch_executors(app: AppRef, timeout_s=3.0) -> Optional[List[dict]]: ...
def fetch_task_list(app: AppRef, stage_id, attempt_id=0, length=1000, timeout_s=3.0) -> Optional[List[dict]]: ...
def stage_ui_url(app: AppRef, stage_id, attempt_id=0) -> str:      # base_url from app

# --- fetch_all_app_ids: now aggregates across ALL ports ---
def fetch_all_app_ids(timeout_s=3.0) -> Optional[List[str]]:
    """Union of app-ids (running + completed) across every reachable port.
    Returns None only if NO port is reachable/valid (preserves the existing
    degrade contract: None == 'nothing reachable', [] == 'reachable, empty')."""
```

Resolution algorithm (both `resolve_*` use `_probe_ports`):
- `resolve_current_app`: over all `(base_url, apps)`, for each app whose latest
  attempt `_attempt_is_running(...)`, track `(startTimeEpoch, AppRef(id, base_url))`;
  return the max-`startTimeEpoch` one. `None` if none running anywhere.
- `resolve_app(app_id)`: return `AppRef(app_id, base_url)` for the first port whose
  apps list contains that id; else `None`.
- `fetch_all_app_ids`: `None` if `_probe_ports` is empty, else the union of ids.

Keep the `_TRANSIENT_ERRORS` / return-`None`-never-raise contract and the existing
`_attempt_is_running` sentinel-endTime logic unchanged — resolution is layered on
top of them, `_get_json` per port is the only I/O.

### Caller changes

`collector.collect_once()` — resolve once, thread the `AppRef`:
```python
app_ref = await asyncio.to_thread(app_client.resolve_current_app, timeout_s=2.0)
app_id = app_ref.app_id if app_ref else None          # JobSummary/_build_job still want the str
executors_raw = await asyncio.to_thread(app_client.fetch_executors, app_ref, timeout_s=2.0) if app_ref else None
stages_raw    = await asyncio.to_thread(app_client.fetch_stages,   app_ref, timeout_s=2.0) if app_ref else None
# fetch_task_list(app_ref, stage_id, attempt, length=..., timeout_s=2.0)
# stage_ui_url(app_ref, stage_id, attempt)  in _build_signal_cards
```
`_build_job`/`JobSummary` keep using the bare `app_id` string (unchanged) — only
the fetch/deep-link calls take `app_ref`.

`annotation.py`:
- `_stages_context`: resolve one `AppRef` and thread it.
  - checkpoint present → `app_ref = resolve_app(checkpoint_data["app_id"])`
    (find the port serving the checkpoint's app, running or completed);
    keep `app_id` in the template context = the checkpoint's id string even if
    `app_ref is None`, matching today's "show the id regardless of reachability".
  - no checkpoint → `app_ref = resolve_current_app()`; template `app_id =
    app_ref.app_id if app_ref else None`.
- `_stage_rows(app_ref, manifest)` takes the `AppRef`; `fetch_stages(app_ref)` and
  `stage_ui_url(app_ref, …)` use it. Returns `None` when `app_ref is None`.
- `_stale_checkpoint_warning`: unchanged call — `fetch_all_app_ids()` now
  aggregates across ports, which is exactly the right semantics ("is this id known
  to *any* live driver?"). Its two distinct messages (None vs not-in-list) still
  work.

`dashboard.py` (minor, same bug class): the page-header `driver_ui_url` link is
rendered once from `config.DRIVER_APP_UI_URL`. Best-effort: set it from a
`resolve_current_app()` at page render, falling back to `config.DRIVER_APP_UI_URL`.
Low priority; page link only, not the freeze.

## Risks
- **All-ports-down latency** (covered above) — noticed as a slower-than-usual first
  dashboard paint; mitigated by the existing `to_thread` offload and an optional
  shorter per-probe timeout.
- **Two genuinely-concurrent running apps** — resolved deterministically by
  max `startTimeEpoch`; if a driver ever omitted `startTimeEpoch` the tie-break
  degrades to "0", so default it defensively and it just deprioritizes that app.
- **Test drift** — the monkeypatched signatures in the three unit test modules will
  fail loudly until updated; that's the intended tripwire, not a silent regression.
