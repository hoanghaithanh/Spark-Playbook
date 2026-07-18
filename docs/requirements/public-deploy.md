# Public Deploy — Requirements (v1.0 — Public Deploy)

Status: Draft for architect handoff
Owner: requirements-analyst
Date: 2026-07-17
Traceability: GitHub issues #39–#44 (milestone "v1.0 — Public Deploy"). This doc formalizes the
already-approved implementation plan (`summarize-the-repository-and-compiled-quill.md`) into
testable requirements; it does not re-open decisions the human already confirmed in that plan.

## Problem statement

Spark Playbook today is **localhost-only, single-user, no-auth**: the FastAPI app runs as a host
process, the browser talks directly to three host ports (`:8000` app, `:8888` Jupyter, `:8080`
Spark Master UI) via hardcoded `localhost` URLs, and the spawned cluster's compose template
publishes roughly ten ports to `0.0.0.0` with nothing gating access to any of them — including a
tokenless JupyterLab (`--allow-root`, whole repo bind-mounted), which is arbitrary remote code
execution the instant it's reachable from the network. The human wants to reach the tool remotely,
as its one user, safely, brought up by a single command — and to open-source the repo, which
currently has no LICENSE despite already being public on GitHub. This doc turns the
already-approved plan into acceptance-testable requirements for that work.

## Goals / Non-goals

### Goals

- **G-PD1 — One-command deploy.** A single script (`deploy.sh`) brings up the full stack —
  containerized app + nginx reverse proxy — on a fresh VM with Docker + Compose installed.
- **G-PD2 — Minimal internet-facing port surface.** Only 80/443 (nginx) are reachable from the
  network; every other service (app, Spark Master UI, JupyterLab, driver Spark UI, worker UIs)
  binds to host loopback only or is not published at all.
- **G-PD3 — Authenticated, encrypted access.** HTTP basic auth (nginx) gates all traffic; TLS
  (Let's Encrypt) is terminated at nginx; plaintext HTTP redirects to HTTPS.
- **G-PD4 — Functional parity behind the proxy.** Every existing capability (cluster spawn/
  teardown, embedded JupyterLab with a live kernel, Spark Master UI links, the monitoring
  dashboard) continues to work when accessed remotely through the proxied, authenticated path —
  this is a deployment/access change, not a feature rewrite.
- **G-PD5 — Open-source hygiene.** The repo carries a LICENSE, contains no secrets in the tree or
  git history, and documents the deploy path and its security model in the README.

### Non-goals (explicit — do not build)

- **Multi-user access.** This is single-user remote access for the repo owner, not a
  multi-tenant service. No user accounts, no per-user sessions.
- **Per-user cluster isolation / Jupyter sandboxing.** The Spark cluster and JupyterLab driver
  remain a single shared, trusted-user surface — the same trust model as today, just reachable
  remotely instead of only on localhost. Declined "open multi-user" model.
- **OAuth/SSO or any auth beyond nginx basic auth.** Basic auth + a strong password is the entire
  trust boundary by design (see Constraints/risk notes below), not a placeholder for something
  stronger.
- **Kubernetes, Terraform, or other IaC.** A single VM running Docker Compose is the target
  deployment shape; no orchestration or provisioning automation beyond `deploy.sh` itself.
- **Certificate auto-renewal automation, monitoring/alerting on the deploy, or backup/DR for the
  VM.** Not addressed by this body of work (see Open Questions — renewal in particular is flagged
  as ambiguous, not silently assumed out of scope).
- **Changes to the cluster's execution model, curriculum content, or the annotation/dashboard
  features themselves.** This is a deployment and access-control change; G-PD4 requires those
  features keep working, not that they change.

## User stories and acceptance criteria

**US-PD1 — One-command deploy of the base stack.**
As the operator (the repo owner, deploying to a VM they control), I want a single script that
builds and starts the entire base stack, so that going from a fresh clone to a running, reachable
instance requires no manual multi-step setup.

- *Given* a fresh VM with Docker + Docker Compose installed and the repo cloned, *when* I run
  `./deploy.sh`, *then* the base stack (containerized app + nginx) comes up with no further manual
  steps beyond whatever `deploy.sh` itself documents as prerequisites (domain DNS, firewall — see
  US-PD5).
- *Given* the stack is up, *when* I run `docker compose -f deploy/docker-compose.yml ps`, *then*
  both the `app` and `nginx` services show a healthy/running state.
- *Given* `deploy.sh` has already been run once successfully, *when* I run it again (e.g. after a
  `git pull` to pick up changes), *then* it does not fail or duplicate resources — it is
  idempotent (rebuilds/restarts as needed, doesn't error on "already exists" conditions, doesn't
  re-issue a still-valid TLS cert or overwrite an already-configured basic-auth credential without
  being asked to).
- *Given* the app spawns a Spark cluster from inside its own container (Docker-out-of-Docker
  against the host socket), *when* a cluster is spawned post-deploy, *then* the spawned
  containers start successfully AND the bind-mounted repo inside those containers contains the
  real repo contents (not an empty directory) — proving the identical host-path bind mount
  resolved correctly from inside the containerized app.

**US-PD2 — Minimal internet-facing port surface.**
As the operator, I want only standard web ports reachable from the internet, so that the cluster's
internal services (Spark UIs, Jupyter, worker ports) are never directly exposed, regardless of
what firewall rules I remember to set.

- *Given* the stack is deployed and a cluster is spawned, *when* I run `ss -tlnp` (or equivalent)
  on the VM, *then* only nginx is bound to `0.0.0.0:80` and `0.0.0.0:443`; the app, Spark Master
  UI, driver Spark UI(s), and JupyterLab are bound to `127.0.0.1` only (or, for worker UIs, not
  published to the host at all).
- *Given* the stack is deployed, *when* I attempt to connect from a separate machine to the VM's
  IP on ports 8080 (Master UI), 8888 (Jupyter), 4040 (driver UI), 8081+ (worker UI), or 6066
  (Spark REST submission), *then* every one of those connections is refused.
- *Given* a security group / firewall is configured per US-PD5, *when* I inspect its rules,
  *then* only 22 (SSH), 80, and 443 are permitted inbound — the port-surface reduction above is
  defense-in-depth on top of this, not a substitute for it.

**US-PD3 — Authenticated, encrypted remote access.**
As the operator, I want every request to the deployed instance to require credentials and travel
over TLS, so that the app (and everything reachable behind it) isn't exposed to anyone who finds
the URL.

- *Given* a deployed instance at `https://<domain>/`, *when* I request it without credentials,
  *then* the server responds `401 Unauthorized` and no app content is returned.
- *Given* correct basic-auth credentials, *when* I request `https://<domain>/`, *then* the app
  loads normally.
- *Given* a plaintext request to `http://<domain>/`, *when* it's received, *then* the server
  responds with a redirect to the `https://` equivalent (no basic-auth prompt or app content is
  ever served over plaintext HTTP, so credentials cannot be sent unencrypted).
- *Given* the deployed instance, *when* I inspect the TLS certificate, *then* it is a valid,
  browser-trusted Let's Encrypt certificate for the configured domain (no self-signed-cert
  warnings).
- *Given* HSTS is configured, *when* a browser that has previously visited the site is redirected
  again, *then* it upgrades to HTTPS without an intermediate plaintext round-trip.

**US-PD4 — Functional parity for all UI surfaces behind the proxy.**
As the operator, I want the app, Jupyter, and the Spark Master UI to work exactly as they do
locally today, once reached through the authenticated HTTPS proxy, so that deploying remotely
doesn't degrade the tool I already rely on.

- *Given* I've logged in through basic auth, *when* I spawn a cluster and open a topic, *then*
  the embedded JupyterLab iframe renders (not blank/CSP-blocked) and I can run a notebook cell to
  completion — proving the WebSocket kernel connection survives being proxied through nginx
  behind basic auth over HTTPS.
  - Reflects the CSP `frame-ancestors` directive (`driver/jupyter_config.py`) allowing the public
    HTTPS origin, and Jupyter served at a `/jupyter` subpath (`--ServerApp.base_url=/jupyter/`).
- *Given* I've logged in, *when* I follow the Spark Master UI link from the app, *then* it loads
  at a `/spark-master` subpath and correctly shows cluster state (using Spark's own
  `reverseProxy` support so worker UIs remain reachable via the master without their own host
  ports).
- *Given* I've logged in and a job is running, *when* I open the monitoring dashboard, *then* it
  populates with live data exactly as it does in the current localhost deployment (the app's
  server-side reads of the Spark REST APIs and Docker stats are unaffected by the access-control
  change, since they happen inside the deployment, not through the public proxy path).
- *Given* any of the above, *when* I inspect what the browser actually loaded, *then* no
  service is reached by URL/port other than the public `https://<domain>/...` origin — no
  fallback to a directly-addressed internal port leaks through.

**US-PD5 — Deploy prerequisites are known and checked.**
As the operator, I want the prerequisites for a successful deploy stated up front and, where
feasible, checked by `deploy.sh` itself, so that a failed deploy fails fast with a clear reason
rather than partially succeeding into a broken or insecure state.

- *Given* the README's deploy section, *when* I read it before deploying, *then* it states the
  required VM shape (Docker + Compose installed), the requirement for a domain A-record pointed
  at the VM's IP (needed for Let's Encrypt), and the requirement to restrict the VM's
  firewall/security group to ports 22/80/443 only.
- *Given* `deploy.sh` is run without a working domain resolving to the VM, *when* the certbot step
  is reached, *then* it fails with a clear, actionable error rather than silently leaving the
  stack running without TLS.
- *Given* the deploy prerequisites, *when* I check them against the existing resource-ceiling
  logic in `app/config.py` (`RESOURCE_CEILING_GB`), *then* the README states a minimum VM
  memory/CPU recommendation consistent with what the app can legitimately request for a spawned
  cluster (see Open Questions — exact figure to be confirmed).

**US-PD6 — Open-source hygiene for a public repo.**
As the maintainer, I want the repo to meet baseline open-source hygiene, so that its existing
public visibility on GitHub is backed by a clear license and no leaked secrets.

- *Given* the repo, *when* I look for a `LICENSE` file at the repo root, *then* one exists and
  names a specific, recognized open-source license (MIT recommended — see Open Questions for
  final confirmation).
- *Given* the full git history (not just the current tree), *when* it's scanned for
  credentials/tokens/keys, *then* none are found — or, if any are found, they are rotated and
  documented as remediated before this story is considered done.
- *Given* the new deploy artifacts this work introduces (nginx htpasswd file, TLS certs, `.env`
  or equivalent secret/config file, any host-specific compose override), *when* I check
  `.gitignore`, *then* all of them are excluded from version control.
- *Given* the README, *when* I read it, *then* it documents `deploy.sh`, its prerequisites, and
  states plainly that basic auth is the entire trust boundary — i.e. that anyone with the
  password has full code-execution access to the box (tokenless Jupyter, repo bind-mounted,
  Docker-socket-mounted app) — so a future reader isn't surprised by the security model.

## Open questions

These are genuine ambiguities the human/architect should resolve before or during design — not
resolved here by assumption:

1. **LICENSE choice.** The plan recommends MIT but flags it as "not yet final." Needs an explicit
   human decision before US-PD6 can close (MIT vs. Apache-2.0 vs. something else — the choice has
   downstream consequences, e.g. Apache-2.0's patent grant, that are worth a deliberate pick
   rather than defaulting silently).
2. **Basic-auth credential provisioning.** The plan says `deploy.sh` "creates the htpasswd entry
   on first run" but doesn't specify the mechanism: does it prompt interactively for a password,
   generate one and print it once, or read one from an env var the operator sets beforehand? This
   affects both `deploy.sh`'s UX and whether a password ever transits shell history/logs.
   Left to the architect, but the human should pick a preference before implementation.
3. **TLS certificate renewal.** Let's Encrypt certs expire in 90 days. The plan's scope
   (`deploy.sh` obtains a cert on first run via certbot) does not mention a renewal mechanism
   (cron/systemd timer, or `certbot renew` documented as a manual operator task). Left
   unaddressed by the approved plan — flagging so it's a deliberate non-goal or explicitly added,
   not silently forgotten. Recommend either a short follow-up story or an explicit non-goal
   with a documented manual renewal procedure in the README.
4. **Minimum VM sizing.** `app/config.py`'s `RESOURCE_CEILING_GB` (32GB) and the MVP doc's 64GB
   reference host assume a specific scale. US-PD5 asks the README to state a minimum VM spec —
   the exact number (e.g., is 32GB the floor, or is a smaller VM acceptable if the operator only
   ever spawns default-sized clusters?) is not decided by this doc; recommend the architect or
   devops-engineer set a concrete recommended minimum during design.
5. **Docker-socket-mount risk — explicit sign-off.** The approved plan is candid that mounting
   the host Docker socket into the containerized app gives the app (and thus, transitively,
   anyone who has the basic-auth password) effective root on the host. This isn't new risk
   (the current host-process app already drives the socket), but containerizing it and exposing
   it to the internet behind only a password is a materially different exposure than
   localhost-only. This doc surfaces it as a named risk requiring the mandatory
   security-auditor pass per the plan and the project's Definition of Done — it should not be
   treated as resolved until that pass explicitly signs off on it.
6. **Brute-force protection on basic auth.** nginx basic auth has no built-in rate limiting or
   lockout. Is a strong password alone considered sufficient (matching the plan's stated
   "acceptable only because single-user" framing), or should `deploy.sh`/nginx config include
   basic rate limiting (e.g. `limit_req`) as cheap defense-in-depth? Not decided by the approved
   plan; flagging for the architect to make an explicit call rather than defaulting either way.
7. **Update/redeploy path.** US-PD1's idempotency criterion covers re-running `deploy.sh`
   unchanged, but the operational path for picking up a new app image after a code change
   (`git pull` + re-run, versus a documented rolling-restart step) isn't specified in the
   approved plan. Likely small, but worth an explicit one-line answer before `deploy.sh` is
   built so its idempotency behavior actually covers the real update flow.

## Constraints

- Builds on the existing architecture (PLAN.md) without changing the cluster execution model,
  curriculum content, or annotation/dashboard features — this is deploy/access-layer work only
  (see Non-goals).
- Must not break the existing localhost/dev workflow described in the current README Quickstart —
  the containerized deploy path is additive; running `uvicorn app.main:app` directly for local
  development should still work.
- Depends on Docker-out-of-Docker path alignment: the containerized app must bind-mount the host
  repo at the identical absolute path used on the host, since `docker compose` resolves relative
  bind-mount paths (e.g. `../../:/workspace`) inside the app container, but the host daemon
  performs the actual mount. This is a load-bearing implementation detail the architect must
  design around, not an incidental one.
- Touches auth, input exposure (Jupyter kernel execution reachable from the internet), and a
  trust boundary (Docker socket mount) — a **security-auditor pass is mandatory** before go-live,
  per this project's Definition of Done (CLAUDE.md).
- Target deployment shape is a single VM with Docker + Docker Compose — no serverless, no
  container orchestration platform, consistent with the Non-goals above.
- Per CLAUDE.md's notebook-cleanliness rule: any `content/*/notebook.ipynb` executed during
  verification of this work must be reset (`git checkout -- <path>`) before the work is
  considered done.

## Scope note — this is several independently shippable stories, not one

Matching the already-filed issue breakdown (#39–#44), the six user stories above are sized to be
pulled into a sprint independently: US-PD1 (containerize + one-command deploy, #39) and US-PD2
(port surface, #40) can proceed in parallel once the architecture is settled; US-PD3 (auth/TLS,
#42) and US-PD4 (functional parity behind the proxy) depend on US-PD1's base stack existing;
US-PD5 (prerequisites, #43) and US-PD6 (open-source hygiene, #44) are largely independent of the
others and could be pulled in early. Sequencing across sprints is the project-manager's call, not
this doc's.
