# Spark Playbook

A local, single-user, self-hosted web app for learning PySpark at a mid/advanced level — the kind
of depth that holds up under interview questions about shuffle, joins, AQE, memory management, and
streaming.

## What it is

Generic tutorials and toy datasets don't produce the real cluster behavior (spill, skew, broadcast
decisions, stage-level shuffle costs) that Spark interview questions actually probe. Spark Playbook
spins up a real Spark Standalone cluster on demand (master + N workers + a driver/Jupyter
container, all in Docker), lets you run notebooks against it, generates data large/skewed enough to
trigger real Spark behavior, and pairs that with a guided curriculum plus — once built — a
self-check annotation engine and a realtime monitoring dashboard.

Day to day it runs entirely on your own machine (Windows/WSL2 or Linux, Docker + Docker Compose),
with no other users and no network exposure beyond `localhost` — this is the default, unchanged
mode, covered in Quickstart below. A one-command, single-user public deploy path also exists (see
[Deploy (single-user, remote)](#deploy-single-user-remote)) if you want to reach it remotely
instead — it's a separate, opt-in mode that does not change the single-user, no-multi-tenancy trust
model, and does not affect the local workflow.

**The project's real differentiator (G1 — interview-depth over platform polish):** where a choice
exists between spending effort on UI/platform sophistication versus on curriculum depth or
realistic cluster/data behavior, curriculum depth and cluster realism win. Raw, unguided cluster
access is never gated behind unfinished app features, and the self-check annotation engine is
designed to be consulted *after* you've formed a hypothesis, not to explain things for you. See
`docs/requirements/spark-playbook-mvp.md` for the full problem statement and goals.

## Current status

- **Phase 0 — cluster harness.** Built, tested, and working: render a Jinja2-templated
  `docker-compose.yml`, bring up master + N workers + a driver/JupyterLab container, reach the
  master UI/REST API and the driver's app UI/REST API from the host, run a real shuffle job, and
  generate tunable skewed synthetic data. No web app required — see `compose/README.md`.
- **Phase 1 — partitioning/shuffle topic, end-to-end in the web app.** Built, tested, and
  acceptance-validated: a FastAPI + Jinja2/HTMX app with a topic page, a cluster control panel
  (spawn/teardown with configurable workers/cores/memory/shuffle-partitions/AQE), and an embedded
  JupyterLab iframe wired to the spawned cluster. Full acceptance results in
  [`docs/acceptance/phase-1.md`](docs/acceptance/phase-1.md).
- **Phase 2 — annotation engine + join strategies + bucketing + AQE.** Built, tested, and closed
  out in Sprint 1: the self-check annotation engine, plus the join-strategies, bucketing, and AQE
  curriculum topics.
- **Phase 2.5 — realtime monitoring dashboard.** Built and bug-fixed. Acceptance report in
  [`docs/acceptance/phase-2-5.md`](docs/acceptance/phase-2-5.md), pending final human sign-off
  (all findings from the acceptance pass, including issue #22, are now resolved — see that
  report's addendum for the corrected status).
- **Sprint 3 — topic-page shell redesign.** Built, tested, and closed out: a unified topic-page
  shell (Concept/Notebook/Self-check tabs, cluster-config drawer, breadcrumb topic switcher) and
  the Phase 2.5 dashboard moving from a standalone route to an in-page slide-in panel. See
  [`docs/requirements/topic-shell-redesign.md`](docs/requirements/topic-shell-redesign.md) and
  [`docs/architecture/topic-shell-redesign.md`](docs/architecture/topic-shell-redesign.md).
  Six new curriculum topics (DAG & Lazy Evaluation, Skew & Salting, Executor Tuning,
  Checkpointing, Serialization Formats, Fault Tolerance & Lineage) are scoped alongside this in
  [`docs/requirements/curriculum-topics-2026-07.md`](docs/requirements/curriculum-topics-2026-07.md)
  but sequenced after the shell per a shell-first prioritization decision — not yet built.
- **Sprint 4 — Catalyst plans topic + data-driven topics-index landing page.** Built, tested, and
  closed out: a dedicated `content/catalyst-plans/` curriculum topic (parse → analyze → optimize
  → physical-plan phases, DataFrame/SQL/UDF predicate-pushdown comparison, three-cell notebook
  walkthrough), and `GET /` now rendering a real topics-index landing page — one card per
  `content/*/manifest.yaml` topic (id, title, order, a blurb derived from each topic's
  `concept.md`), sorted by `order`, with no hardcoded topic list — replacing the previous
  307-redirect to the first topic. See [`docs/requirements/topic-shell-redesign.md`](docs/requirements/topic-shell-redesign.md)
  (US-SH5, US-SH8) and [`docs/backlog.md`](docs/backlog.md) rows #31/#24 for acceptance evidence.
- **Sprint 5 — four more curriculum topics.** Built, tested, and closed out: `content/dag-lazy-evaluation/`
  (DAG & Lazy Evaluation, issue #27), `content/caching-persistence/` (Caching & Persistence, issue
  #28), `content/window-functions/` (Window Functions, issue #29), and `content/serialization-formats/`
  (Serialization Formats, issue #30) are each built, tested, code-reviewed with no blockers, and
  live-acceptance-validated against a real cluster (all acceptance criteria PASS, human sign-off
  given). These brought the topics-index landing page to 9 real topics. See
  [`docs/backlog.md`](docs/backlog.md) rows #25/#14/#15/#29 and
  [`docs/qa/serialization-formats-acceptance.md`](docs/qa/serialization-formats-acceptance.md) for
  the latest acceptance evidence.
- **Sprint 6 — executor tuning, memory management, skew & salting.** In progress:
  `content/executor-tuning/` (Executor Tuning, issue #34) is built, tested, code-reviewed with no
  blockers, and live-acceptance-validated against a real cluster (all 3 US-C3 acceptance criteria
  PASS — see [`docs/qa/executor-tuning-acceptance.md`](docs/qa/executor-tuning-acceptance.md)),
  human sign-off given. `content/memory-management/` (Memory Management, issue #36) is built,
  tested, code-reviewed with no blockers, and live-acceptance-validated against a real cluster (all
  5 US-C10 acceptance criteria PASS — see
  [`docs/qa/memory-management-acceptance.md`](docs/qa/memory-management-acceptance.md)), pending
  final human sign-off. Both topics share the same `executor_metrics` annotation-manifest mechanism
  (a reveal-time REST pull from `/api/v1/applications/<id>/executors`, mirroring the existing
  `stage_metrics` mechanism — see
  [`docs/architecture/topic-shell-redesign.md`](docs/architecture/topic-shell-redesign.md) Decision
  A). Skew & Salting (issue #35) remains open. This brings the topics-index landing page to 11 real
  topics.
- **Sprint 7 — public, single-user, authenticated deploy (v1.0 — Public Deploy).** Implemented and
  security-audited (all audit findings remediated): a one-command `./deploy.sh` stands up a
  containerized app + nginx reverse proxy on a VM, gated by HTTP basic auth and Let's Encrypt TLS,
  with the spawned Spark cluster's ports bound to loopback only (never `0.0.0.0`). Final human
  acceptance sign-off is still pending per this project's Definition of Done. See
  [`docs/requirements/public-deploy.md`](docs/requirements/public-deploy.md),
  [`docs/architecture/public-deploy.md`](docs/architecture/public-deploy.md), and the
  [Deploy (single-user, remote)](#deploy-single-user-remote) section below for the full design and
  operator checklist.
- **Phase 3 (streaming/Kafka) and remaining Phase 4 curriculum topics** are backlogged, not yet
  started.

For the full prioritized list of remaining work, see [`docs/backlog.md`](docs/backlog.md). For the
phased roadmap and architecture in detail, see [`PLAN.md`](PLAN.md).

## Quickstart

### Prerequisites

- Docker Desktop with the WSL2 backend, Compose v2 (`docker compose ...`) on PATH.
- Python 3.9+, with `pip install -r app/requirements.txt` (Phase 1 web app) and `pip install
  jinja2` (Phase 0 CLI, if used standalone).

(The public/remote deploy path below has different, host-Python-free prerequisites of its own.)

### Phase 0 — cluster harness only (no web app)

```bash
# Build the custom Spark image (once, and after editing Dockerfile.spark)
bash compose/build.sh

# Render the compose stack with defaults (3 workers x 2 cores/4GB, driver 2GB)
python compose/cli.py render

# Bring the cluster up (tears down any previous stack first)
python compose/cli.py up

# Wait for all workers to register (polls :8080/json/)
python compose/cli.py wait-for-ready

# Run the shuffle smoke test
docker exec -it spark-driver /opt/spark/bin/spark-submit /workspace/compose/smoke_test.py

# Tear down
python compose/cli.py down
```

See [`compose/README.md`](compose/README.md) for full details, including known deviations from a
literal reading of `PLAN.md` and Windows/Git Bash notes.

### Phase 1 — the web app

The web app drives the same cluster lifecycle through its own routes (spawn/teardown), rather than
`compose/cli.py` directly:

```bash
pip install -r app/requirements.txt
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Then open **`http://localhost:8000`** in a browser (`http://127.0.0.1:8000` also works — the
embedded JupyterLab iframe's CSP allowlist covers both loopback origins, since browsers otherwise
treat them as distinct origins for `frame-ancestors` purposes even though they resolve to the same
host).

From there: open the partitioning/shuffle topic, configure and spawn a cluster from the control
panel, and the topic's notebook opens in an embedded JupyterLab pointed at that cluster.

### Public / remote deploy

A one-command deploy path also exists if you want to reach the app remotely instead of only on
`localhost`. It's a separate, opt-in mode from everything above — see
[Deploy (single-user, remote)](#deploy-single-user-remote) below for the full walkthrough and its
security model.

## Deploy (single-user, remote)

This is a second, opt-in way to run Spark Playbook — reachable over the internet at a domain you
control, gated by a password, instead of only on `localhost`. It does not replace the local-dev
workflow above (which is still the default and is unaffected by any of this); it's for when you
want to use the app from somewhere other than the machine running Docker.

**What it gives you:** a containerized FastAPI app + nginx reverse proxy on a VM, reachable at
`https://<your-domain>/`, with TLS (Let's Encrypt, auto-renewing) and HTTP basic auth in front of
everything — the app itself, the embedded JupyterLab, and the Spark Master UI. It is still
single-user: one shared password, no per-user accounts, no multi-tenancy. Every capability that
works locally (spawn/teardown a cluster, run notebooks, the monitoring dashboard) works the same
way through the proxied, authenticated path.

Unlike the two local paths above, `./deploy.sh` needs **no Python on the host at all** — it
containerizes the app itself, so the only host prerequisites are Docker Engine, the Compose v2
plugin, and `git`:

```bash
git clone <this-repo-url> && cd Spark-Playbook
bash deploy.sh
```

On first run it prompts for a domain, a Let's Encrypt contact email, and a basic-auth
username/password (minimum 16 characters, typed twice to confirm), then brings up the containerized
app + nginx behind that gate. Before running it, you also need:

- **A domain's A-record already pointing at the VM's public IP** (required for the Let's Encrypt
  HTTP-01 challenge — `deploy.sh` fails loud with an actionable message if this isn't in place).
- **Inbound firewall/security-group restricted to 22 (SSH), 80, 443 only** — every other service
  (app, Jupyter, Spark UIs) binds to the VM's loopback interface only, but the firewall is the
  primary enforcement.
- **A VM sized for the app's cluster-spawn ceiling** — recommended minimum 8 vCPU / 48 GB RAM /
  80 GB SSD.

That's the overview; **[`deploy/README.md`](deploy/README.md) is the full, concrete operator
checklist** (exact OS packages, `ufw`/security-group commands, VM-sizing rationale, Windows/WSL2
notes) — read it before you provision anything.

**Redeploy and password rotation.** To pick up a code change, `git pull && ./deploy.sh` again — it
is idempotent: it rebuilds/restarts only the app + nginx containers, re-prompts for nothing it
already has on disk (domain, email, basic-auth credential, TLS cert), and never touches a Spark
cluster you already spawned (that's a separate, independent Docker Compose project). To rotate the
basic-auth password, run `./deploy.sh --reset-auth` — it re-prompts for a new username/password and
overwrites the credential; nothing else about the stack changes. See `deploy/README.md` / the ADR's
OQ7 for exactly what a redeploy does and doesn't disturb (in short: the running cluster survives; the
app's own in-memory "is a cluster spawned?" state resets and self-heals on your next spawn).

**Security model — read this before deploying somewhere reachable from the internet.** The entire
system sits behind **one gate**: nginx HTTP basic auth. There are no per-user accounts and no
additional layer behind it. Concretely:

- The app container mounts the host's Docker socket (it needs this to spawn Spark clusters), so
  **anyone who has the basic-auth password has effective root on the VM** — full Docker API access,
  not just this app's functionality.
- The embedded JupyterLab is token-less by design (same as local dev), so anyone through the auth
  gate also has **arbitrary code execution** via a notebook cell.
- A strong, unique password is therefore not a suggestion — it is the entire security of the
  deployment. There is no lockout after failed attempts (nginx applies a blunt rate limit, not a
  lockout), so password strength alone is what stands between "reachable" and "compromised."

This is an accepted, deliberate trade-off for **single-user** use — it has been through this
project's mandatory security-auditor pass, and the findings from that pass have been remediated.
**Multi-user or multi-tenant access is an explicit non-goal and is not safe on this design** — do
not share the password with anyone you would not trust with root on the box. See
[`docs/architecture/public-deploy.md`](docs/architecture/public-deploy.md) for the full design
rationale, the resolved open questions (credential provisioning, TLS renewal, rate limiting, VM
sizing), and the accepted trade-offs in detail.

### Running the public deploy on Windows

The design above was built and documented against a Linux VM (`docs/architecture/public-deploy.md`
assumes Debian/Ubuntu). It also works on a Windows machine with Docker Desktop + Git already
installed, but only if you run it from **WSL2**, not from Git Bash/PowerShell directly — for the
same reason Phase 0's cluster harness already recommends WSL2 (`compose/README.md`): this stack's
core trick (D1, the DooD path-alignment) depends on a bind-mount path meaning the exact same thing
to the app container and to the Docker daemon that creates the *nested* spawned-cluster containers.
Git Bash rewrites `/`-leading paths to Windows-style paths (the same MSYS quirk documented in
`compose/README.md`), which breaks that alignment for a **sibling container** created from inside
another container — a known rough edge on Docker Desktop for Windows, not just a Spark Playbook
one.

1. **Install a WSL2 distro** (e.g. `wsl --install -d Ubuntu`) if you don't have one, and enable
   Docker Desktop's WSL2 integration for it (Settings → Resources → WSL Integration).
2. **Clone the repo *inside* the WSL2 filesystem** — e.g. `~/Spark-Playbook` — not under
   `/mnt/c/...` or `/mnt/d/...`. A path under `/mnt/<drive>` is still a Windows path underneath and
   reintroduces the same host-path-translation risk; a path that lives natively in the WSL2 distro
   doesn't.
3. **Enable Docker Desktop's host networking**: Settings → Features in development → *Enable host
   networking* → restart Docker Desktop. This is required for `nginx`'s and the app's
   `network_mode: host` to actually publish 80/443/8000 (available since Docker Desktop 4.34;
   incompatible with Enhanced Container Isolation — turn that off if it's on).
4. From an Ubuntu-on-WSL2 terminal, inside the cloned repo: `bash deploy.sh`, same as the Linux
   path above.
5. **If you're running this on a home/office PC rather than a cloud VM**, you additionally need
   your router to port-forward 80/443 to this machine's LAN IP for the domain's A-record to
   actually reach it — Docker Desktop's host networking makes the ports reachable *on your Windows
   machine*, it doesn't put you on the public internet by itself.

This Windows/WSL2 combination (WSL2-native clone + host networking) is an adaptation of the
Linux-VM design the security audit actually covered — treat it as self-verify. Before trusting it,
do the same check the ADR uses to catch a silent DooD mismatch: spawn a cluster and confirm
`docker exec <a-spawned-container> ls /workspace` shows the real repo, not an empty directory.

## Project structure

```
compose/      Phase 0 cluster harness — Dockerfile, Jinja2 compose templates, standalone CLI
app/          Phase 1+ FastAPI web app — cluster lifecycle, topic pages, (later) annotation engine
content/      Curriculum topics — one folder per topic (concept.md, notebook.ipynb, manifest.yaml)
deploy/       Public-deploy base stack — nginx config, compose file, gitignored secrets/certs
Dockerfile.app  Containerizes the app itself for the public-deploy base stack (deploy/)
deploy.sh     One-command public deploy — see "Deploy (single-user, remote)" above
docs/         Requirements, backlog, acceptance reports, architecture notes
PLAN.md       Full technical design: architecture, key decisions, phased roadmap, named risks
```

## Further reading

- [`PLAN.md`](PLAN.md) — full architecture and design (component diagram, cluster lifecycle,
  annotation engine design, phased roadmap, named risks and mitigations).
- [`CLAUDE.md`](CLAUDE.md) — how this repo's own development process works (the SDLC pipeline,
  sprint cadence, definition of done). The mechanics of the underlying agent team are documented
  separately in [`docs/AGENT_TEAM.md`](docs/AGENT_TEAM.md).
