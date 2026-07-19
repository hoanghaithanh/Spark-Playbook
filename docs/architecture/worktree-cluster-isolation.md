# ADR: Cross-Worktree Cluster Collision — Ownership Guard, Not Concurrent Multi-Cluster

Status: Proposed (architect design for issue #38, Sprint 8; awaiting human sign-off before implementation)
Date: 2026-07-18
Issue: #38 ("compose/cli.py's fixed Docker Compose project name causes cross-worktree cluster collisions")
Related: `docs/architecture/public-deploy.md` (D2 — the fixed 127.0.0.1 host-port surface this
collides on); `docs/architecture/driver-port-discovery.md` (the 4040-4042 range); PLAN.md §2/D5
(single-slot lifecycle state machine)

---

## Context

This repo routinely runs several git worktrees (`.claude/worktrees/*`) at once, each a full checkout
running its own FastAPI app instance for a QA/dev session on a feature branch — an explicitly normal
practice per CLAUDE.md, not an edge case. But **every** worktree spawns its cluster under the same
fixed Compose project name (`PROJECT_NAME = "sparkpb"`, hardcoded in both `app/config.py` and
`compose/cli.py`), with the same hardcoded `container_name`s (`spark-master`, `spark-worker-N`,
`spark-driver`) and the same hardcoded 127.0.0.1 host ports (`8080`, `8888`, `4040-4042`). Because
`spawn()`/`teardown()` both begin with `docker compose -p sparkpb down`, a spawn or teardown issued
by worktree B silently tears down worktree A's *live* cluster — reproduced during issue #36's
acceptance pass, where all three containers were killed mid-job by a concurrent worktree's app. The
harm is **silent, unattributed data loss mid-job**. The issue's suggested fix (derive the project
name from the worktree) does not by itself resolve this, and the filer explicitly flagged that an
architect must decide how port allocation interacts with it — hence this ADR.

The constraint that shapes the whole decision: `spark-master`/`spark-worker-N`/`spark-driver` are
Docker-daemon-global names, and `8080`/`8888`/`4040-4042` are host-global ports bound to 127.0.0.1.
**Two live clusters cannot coexist on one Docker daemon without distinct project names, distinct
container names, *and* distinct host ports** — and a large amount of app code assumes those exact
fixed identities (`monitoring/docker_stats.py`'s container-name keying, `config.DRIVER_APP_UI_PORTS`,
`MASTER_JSON_URL`/`JUPYTER_URL`, readiness polling, the driver's CSP/iframe URL construction).

Spark Playbook is, by deliberate documented design, a **single-user, single-process, single-slot**
product (`manager.py`: "at most one Spark stack exists at a time"; README trust model: no
multi-tenancy). Concurrent worktrees are a *development-time* convenience for this project's own
multi-agent workflow, not a product feature end users need. And that workflow is already serialized
by CLAUDE.md's human-checkpoint orchestration rule — agents advance one pipeline stage at a time
under human confirmation, not as an N-way-parallel free-for-all.

---

## Decision — Option B: keep single-slot, add a cross-worktree **ownership guard** that fails loudly. Explicitly decline true concurrent multi-cluster (Option A).

Keep the single-slot design and the fixed `sparkpb` project/container/port identity exactly as they
are. Before `spawn()` or `teardown()` touches any container, check whether a `sparkpb` cluster is
**already running and owned by a *different* worktree**; if so, **refuse the operation with a clear
error naming the owning worktree**, instead of clobbering it. Same-worktree respawn/teardown
(the D5 cancel-and-replace flow) is unaffected — the guard only fires on a *foreign* owner.

This directly fixes the actual observed harm (silent, unattributed teardown of another worktree's
live cluster) with a small, additive diff, and it does not build a concurrent-multi-cluster
capability the product design explicitly rejects. It is the ponytail-consistent call: fix the
reported harm, do not add speculative multi-tenancy the product is defined not to have.

### Why not Option A (true concurrent multi-cluster)

Option A — dynamically allocate a distinct project name **and** host-port range **and**
container-name prefix per worktree so N worktrees run N live clusters simultaneously — is a large,
cross-cutting change: a port-allocation scheme, template parameterization of `container_name`s (not
just ports), and rework of every place that assumes fixed identity (`docker_stats.py`'s name keying,
`DRIVER_APP_UI_PORTS`, `MASTER_JSON_URL`/`JUPYTER_URL`/`MASTER_UI_URL`, readiness polling, the CSP
`frame-ancestors` / iframe `src` construction). It delivers *simultaneous* live clusters — but the
workflow needs clusters to be *non-destructive* of each other, not *simultaneous*. Live-cluster use
(acceptance passes) is comparatively rare and already human-serialized; paying Option A's full cost
to remove a serialization constraint the orchestration model already imposes is speculative
generality. Declined — see "Explicitly out of scope" for the upgrade path if that ever changes.

### Identity: reuse Compose's own per-worktree label (no template change)

Docker Compose already stamps every container it creates with labels that are **unique per
worktree** — verified live on this host (Compose v2.38.2):

```
com.docker.compose.project             = sparkpb                       (shared — the collision)
com.docker.compose.project.working_dir = <worktree>\compose\rendered   (UNIQUE per worktree)
com.docker.compose.project.config_files= <worktree>\compose\rendered\docker-compose.yml (UNIQUE)
```

Because each worktree renders its compose file into its *own* `<worktree>/compose/rendered/`, the
`project.working_dir` label is already a reliable, human-readable, per-worktree owner identity —
**with zero template change and no new label to add**. The guard reads it off the running
containers; `str(config.RENDERED_DIR)` is this worktree's expected value.

> **Load-bearing empirical caveat the developer must confirm:** the label value is an OS-native
> absolute path — on this Windows host it came back backslash-style
> (`C:\\...\\lbltest`). `config.RENDERED_DIR` from `pathlib` may differ in separator/case/`.`-`..`
> normalization. **Compare normalized:** `os.path.normcase(os.path.normpath(x))` on both the label
> value and `str(config.RENDERED_DIR)` before deciding "foreign." Verify against one real spawn that
> a same-worktree spawn's label matches (guard must *not* fire) and a hand-faked foreign path does
> not. Get this normalization wrong in the "too strict" direction and the guard blocks your own
> respawn; wrong in the "too loose" direction and it fails to protect. This is the one thing to
> check live, not by inspection.

### Mechanism sketch (concrete enough to implement without re-deriving)

**1. New helper — `app/lifecycle/compose_ops.py`:**

```python
async def running_owner() -> Optional[str]:
    """Normalized working_dir of the worktree that owns the currently-running
    `sparkpb` cluster, or None if nothing is running. Never raises."""
    ids = docker ps -q --filter label=com.docker.compose.project={config.PROJECT_NAME}
    if not ids:
        return None
    label = docker inspect <first id> \
        --format '{{ index .Config.Labels "com.docker.compose.project.working_dir" }}'
    return _norm(label)   # os.path.normcase(os.path.normpath(label))
```

Reuse the module's existing `_run()` subprocess wrapper (same pattern `docker_stats.list_container_ids`
already uses). Degrade to `None` on any docker error — a guard that can't read state must not itself
become a spawn-blocking failure; `up`'s own container-name/port collision remains the last-resort
safety net (see Risks).

**2. Guard at the top of both mutating paths — `app/lifecycle/manager.py`:**

```python
SELF = _norm(str(config.RENDERED_DIR))

async def spawn(self, params, timeout_s=...):
    result = self.validate(params)
    if not result.ok: ... (unchanged)

    owner = await compose_ops.running_owner()
    if owner is not None and owner != SELF:
        self.state = ClusterState.FAILED
        self.error = (f"A 'sparkpb' cluster is already running, owned by another "
                      f"worktree ({owner}). Refusing to spawn — it would tear down "
                      f"that worktree's live cluster. Tear it down there first, or wait.")
        self.message = f"Refused: {self.error}"
        return SpawnOutcome(ok=False, status=self.status())

    async with self._mutate_lock:
        await self._cancel_and_teardown_locked()   # unchanged from here down
        ...
```

`teardown()` gets the identical guard, before `_cancel_and_teardown_locked()`. The guard must sit
**before** any `compose_ops.down()` call, because that `down -p sparkpb` is exactly what does the
clobbering. Same-owner (`owner == SELF`) or nothing-running (`owner is None`) → proceed exactly as
today; the D5 cancel-and-replace single-slot behavior within a worktree is completely unchanged.

**3. Mirror the guard in the standalone CLI — `compose/cli.py`:** `cmd_up` and `cmd_down` have the
identical collision surface (the module docstring already notes it mirrors the app path). Add a
synchronous equivalent of `running_owner()` (plain `subprocess.run`, comparing against
`str(RENDERED_DIR)`) and refuse with the same clear, worktree-naming error. Keep it a small local
helper — the CLI is deliberately self-contained and does not import `app/`.

**4. No change needed elsewhere.** `monitoring/docker_stats.py`, `collector.py`,
`DRIVER_APP_UI_PORTS`, readiness polling, CSP/iframe construction, and the template all stay as-is —
that is the whole point of choosing B over A. One benign interaction to note (not fix): the collector
samples only while *this* worktree's `manager.state == READY`; a worktree that just got *refused* is
`FAILED`, not `READY`, so its dashboard never runs and never displays the foreign cluster's stats.

---

## Alternatives considered

| Alternative | Why not chosen |
|---|---|
| **A — true concurrent multi-cluster** (per-worktree project name + host-port allocation + container-name prefix, + make `docker_stats`/`DRIVER_APP_UI_PORTS`/URLs/readiness/CSP worktree-aware) | Large cross-cutting change to deliver *simultaneous* clusters; the workflow needs *non-destructive*, not simultaneous, and live-cluster use is already human-serialized. Speculative multi-tenancy the product is defined not to have. Retained as the documented upgrade path if simultaneity is ever a real requirement. |
| **Issue's literal suggestion — derive the project name from the worktree, stop there** | A distinct project name alone does **not** prevent collision: the hardcoded `container_name`s (Docker-global) and host ports (`8080`/`8888`/`4040-4042`, host-global) still clash — the second `up` fails on duplicate name / bound port. It's Option A's first step without the rest, so it either breaks loudly at `up` or, worse, forces the full Option A ripple. Rejected as a half-measure. |
| **Per-worktree lockfile on the host filesystem** | The shared state that collides lives on the *Docker daemon*, not any one worktree's filesystem — a file under `<worktree>/` can't see another worktree's running cluster. The daemon (via the label the running containers already carry) is the correct single source of truth; a lockfile would be a second, drift-prone copy of it. |
| **New custom compose label (`com.sparkpb.owner`) added to the template** | Unnecessary — Compose's built-in `project.working_dir` label already uniquely identifies the worktree. Adding a template label is extra surface for zero gain. |

---

## Consequences

**Accepted trade-offs:**

- **Two worktrees cannot hold live clusters at the same time.** The second worktree to try is
  **blocked with a clear error** naming the owner, until the first tears down. This is a real
  serialization constraint on the dev workflow, accepted deliberately: it matches the single-slot
  product design and CLAUDE.md's already-serialized orchestration, and it trades "sometimes wait /
  coordinate" for "never silently lose a running job." This is the corner being cut, stated plainly
  so it is not later mistaken for a bug.
- **The guard is a check, not a distributed lock (TOCTOU).** Two worktrees issuing a *cold* spawn
  within the same ~second can both observe "nothing running" and both proceed. But this is *no worse
  than today*, and strictly better in the case that actually caused harm: a **long-running** cluster
  is stamped well before any later spawn checks, so the mid-job clobber (#36's exact failure) is
  fully prevented. In the rare simultaneous-cold-start race, Docker's own duplicate-`container_name`
  error makes the second `up` fail loudly rather than silently — the same last-resort net PLAN.md R4
  already relies on. Mark it with a `# ponytail: naive check, real lock only if simultaneous cold
  spawns become common` comment; do not build a lock now.
- **What becomes harder:** an agent doing a live acceptance pass may now hit "refused — worktree X
  owns the cluster" and must coordinate/wait. That surfaced error is the intended behavior, and
  should be understood as such by whoever reads it (and documented in the dev workflow notes by
  tech-writer when this ships).

**What does *not* change:** the single-slot state machine, D5 cancel-and-replace within a worktree,
the compose template, all monitoring/URL/port code, and the product's single-user trust model.

---

## Component / data design

```
Docker daemon (shared across all worktrees)
  └─ project "sparkpb" containers  ── carry label ──▶ com.docker.compose.project.working_dir
                                                       = <owning-worktree>\compose\rendered
        ▲                                   ▲
        │ down/up (clobbers)                │ reads label (guard)
        │                                   │
  worktree A app                      worktree B app
  RENDERED_DIR = A\compose\rendered   RENDERED_DIR = B\compose\rendered
  SELF = norm(A\...)                  SELF = norm(B\...)
        │                                   │
   spawn()/teardown():                 spawn()/teardown():
     owner = running_owner()             owner = running_owner()  ── sees A's working_dir
     owner in {None, SELF} → proceed     owner != SELF → REFUSE, name A, touch nothing
```

Files touched (implementation, next pipeline step — not this ADR):

```
app/lifecycle/compose_ops.py   # + async running_owner() helper (docker ps + inspect, reuse _run)
app/lifecycle/manager.py       # + foreign-owner guard at top of spawn() and teardown()
compose/cli.py                 # + sync owner-guard mirror in cmd_up / cmd_down
app/config.py                  # (optional) a _norm() path helper if shared; RENDERED_DIR already exists
```

No data-model change, no schema change, no template change, no new dependency.

---

## Explicitly out of scope (so it is not silently expected later)

- **Concurrent live clusters across worktrees.** This ADR delivers *collision safety*, not
  *concurrency*. The second worktree is blocked, not accommodated. If the workflow ever genuinely
  needs two live clusters at once, that is Option A (per-worktree project name + host-port allocation
  + container-name parameterization + making `docker_stats`/`DRIVER_APP_UI_PORTS`/URLs/readiness/CSP
  worktree-aware) and is a separate, larger design — do not treat it as implied by this fix.
- **A real cross-process lock.** The guard is a best-effort check; the sub-second cold-start race is
  knowingly left to Docker's own name/port collision (see Consequences).
- **Scoping the monitoring dashboard to owned containers.** Not needed under B (a refused worktree
  never reaches READY, so its collector never runs); revisit only if that assumption changes.

---

## Risks

- **R-WT-1 — Path-normalization mismatch makes the guard misfire.** The `working_dir` label is an
  OS-native path (backslashes/case on Windows) that may not string-equal `str(config.RENDERED_DIR)`.
  *Noticed by:* either your own respawn gets refused ("owned by <your own path>"), or a genuine
  foreign cluster is not detected. *Mitigation:* normalize both sides with
  `os.path.normcase(os.path.normpath(...))`; verify live that same-worktree respawn does **not** fire
  the guard (the mandatory empirical check called out in the Decision).
- **R-WT-2 — `docker ps`/`inspect` unavailable when the guard runs.** If Docker is unreachable,
  `running_owner()` returns `None` and the spawn proceeds unguarded. *Noticed by:* the guard silently
  not protecting when the daemon is flaky. *Mitigation:* this is intentional fail-open — a guard that
  can't read state must not block all spawns; the existing `up` duplicate-name/bound-port failure
  remains the last-resort safety net (PLAN.md R4), and this matches how `docker_stats` already treats
  "can't reach docker" as "nothing running" rather than an error.
- **R-WT-3 — Simultaneous cold-start TOCTOU.** Covered under Consequences; degrades to today's
  behavior plus Docker's own loud collision, never to silent mid-job loss. Accepted, `ponytail`-tagged.
- **R-WT-4 — CLI/app guard drift.** The guard exists in two places (`compose_ops.running_owner` and
  `cli.py`'s mirror) that must agree on the identity source and normalization. *Noticed by:* the CLI
  and app disagreeing on ownership of the same cluster. *Mitigation:* both key off the same
  `project.working_dir` label and the same `normcase(normpath(...))` normalization; keep the compared
  value (`RENDERED_DIR`) the single source in each. (They can't share code — the CLI deliberately
  doesn't import `app/` — so this is a convention to hold, and a natural thing for code-review to
  check.)
```
