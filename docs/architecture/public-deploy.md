# ADR: Public, single-user, one-command authenticated deploy (v1.0 — Public Deploy)

Status: Accepted (architect handoff)
Date: 2026-07-17
Requirements: `docs/requirements/public-deploy.md` (US-PD1–US-PD6, Open Questions 1–7)
Approved plan: `summarize-the-repository-and-compiled-quill.md`
Traceability: GitHub issues #39–#44, milestone "v1.0 — Public Deploy"
Builds on: `app/config.py`, `app/lifecycle/` (`manager.py`, `compose_ops.py`), `compose/cli.py`,
`compose/templates/{docker-compose.yml.j2,spark-defaults.conf.j2}`, `driver/jupyter_config.py`

> **Addendum A1 (2026-07-17):** the `app` service's networking (the `host.docker.internal` /
> `extra_hosts` parts of **D1** and **D3**) was found not to work on plain Linux Docker and was
> corrected to `network_mode: host`. Superseded passages below are struck through and point here;
> the full addendum is the [Addendum A1](#addendum-a1-2026-07-17--app-service-uses-network_mode-host-supersedes-the-hostdockerinternalextra_hosts-parts-of-d1--d3)
> section after the Decision.

---

## Context

Spark Playbook today is a localhost-only, single-user, no-auth tool: the FastAPI app runs as a
**host process** on `:8000`, the browser talks directly to three host ports (`:8000` app, `:8888`
Jupyter iframe, `:8080` Master UI) via hardcoded `localhost` URLs, and the app spawns a real Spark
Standalone cluster **per session** by shelling out to `docker compose` against the host Docker
socket (`app/lifecycle/compose_ops.py`). The spawn template publishes ~10 ports to `0.0.0.0` with
no auth anywhere — and the driver runs a **tokenless JupyterLab** (`--allow-root`, whole repo
bind-mounted), i.e. arbitrary remote code execution the instant it is network-reachable.

The goal is to reach the tool **remotely, as its single user, safely, via one `deploy.sh`**, and
open-source the repo. Three problems must be solved together: shrink the internet-facing port
surface to 80/443; put the whole thing behind auth + TLS (the proxy alone is necessary but not
sufficient — tokenless Jupyter behind it is still RCE for whoever holds the password); and make the
whole stack come up with one command. The approved plan already fixed the shape (containerize the
app; base compose stack of nginx + app; app keeps spawning the cluster via the host socket —
Docker-out-of-Docker). This ADR records that design concretely and resolves the seven open
questions the requirements doc left for the architect.

The single load-bearing detail is the **DooD bind-mount path alignment** (below): the containerized
app must render compose files whose relative bind mounts (`../../:/workspace`) resolve to the *same
absolute path* on both the app container and the host daemon that actually performs the mount.

---

## Decision

Four decisions, plus concrete resolutions to Open Questions 1–7. **D1 (the DooD path alignment) is
the load-bearing one** — get it wrong and every spawned cluster silently mounts an empty
`/workspace`.

### D1 — Base compose stack (`nginx` + `app`) + Docker-out-of-Docker, one command

Ship a new `deploy/` base stack, separate from the per-session `sparkpb` cluster the app spawns:

- **`Dockerfile.app`** — Python base, `pip install -r app/requirements.txt`, plus the **Docker CLI +
  Compose v2 plugin** (the app shells out to `docker compose`, see `compose_ops.py`). Entrypoint:
  `uvicorn app.main:app --host 0.0.0.0 --port 8000`.
- **`deploy/docker-compose.yml`** — the base stack (compose project name **`sparkpb-deploy`**,
  deliberately *distinct* from the `sparkpb` project the app spawns, so the two never collide and
  `docker compose -p sparkpb down` from the app never touches the base stack):
  - **`app`**: built from `Dockerfile.app`. Mounts the **host Docker socket**
    (`/var/run/docker.sock:/var/run/docker.sock`) and the **repo at the identical host path**
    (`${REPO_HOST_PATH}:${REPO_HOST_PATH}`, with `working_dir: ${REPO_HOST_PATH}`).
    ~~`extra_hosts: ["host.docker.internal:host-gateway"]` so its server-side reads reach the spawned
    cluster on host loopback. Publishes `127.0.0.1:8000:8000` only.~~ **[SUPERSEDED by Addendum A1
    (2026-07-17): this does not work on plain Linux Docker. The `app` service uses
    `network_mode: host` (no `extra_hosts`, no `ports:` publish); uvicorn is pinned to
    `127.0.0.1:8000` to keep the loopback-only exposure. See addendum below.]**
  - **`nginx`**: `network_mode: host` (binds `0.0.0.0:80/443`, reaches app + spawned cluster on
    `127.0.0.1`). Mounts nginx conf, htpasswd, TLS certs, and the ACME webroot (all gitignored /
    outside the repo tree).
  - **`certbot`**: renewal sidecar (see OQ3).
- **`deploy.sh`** — the one command. Captures `REPO_HOST_PATH=$(pwd)` (so it works wherever cloned);
  builds the Spark image via `compose/build.sh` **only if absent** (idempotent) and the app image;
  provisions htpasswd on first run (OQ2); brings up nginx; obtains the cert (OQ3); then
  `docker compose -f deploy/docker-compose.yml up -d --build`. Idempotent on re-run.

**DooD path alignment (the load-bearing detail).** The app renders compose to
`compose/rendered/docker-compose.yml` with bind mounts like `../../:/workspace`, resolved by the
compose CLI **relative to the compose file's directory inside the app container**, then handed to
the **host daemon** which performs the actual mount. So the resolved absolute path must be valid on
the host. `app/config.py` derives `REPO_ROOT` from `__file__`; mounting the host repo into the app
container at the **identical absolute path** (`${REPO_HOST_PATH}:${REPO_HOST_PATH}`) makes `../../`
resolve identically on both sides. Verified by US-PD1's acceptance check: after a spawn,
`/workspace` inside the spawned containers must contain the real repo, not an empty dir.

### D2 — Port surface: spawned cluster publishes to loopback only, unused ports dropped

One edit to `compose/templates/docker-compose.yml.j2` covers every spawn:

| Service | Today | After |
|---|---|---|
| spark-master | `8080:8080`, `6066:6066` | `127.0.0.1:8080:8080`; **drop `6066`** (unused REST submission) |
| spark-worker-N | `8081+i:8081` | **drop entirely** — worker UIs reached via master's reverse-proxy (D3) |
| spark-driver | `8888:8888`, `4040-4042`, `7078`, `7079` | `127.0.0.1:8888:8888`, `127.0.0.1:4040-4042:...`; **drop `7078`/`7079`** (intra-cluster only, reached by container DNS `spark-driver:7078`) |

Port `7077` stays unpublished (already is). Keep the `spark.driver.port`/`blockManager.port`
pinning in `spark-defaults.conf.j2` — only stop *publishing* them. Net: the spawned cluster
publishes nothing to `0.0.0.0`; only loopback. Firewall/security-group restricted to 22/80/443
(US-PD5) is the real enforcement; loopback binding is defense-in-depth on top.

### D3 — Config URL/host split: server-side cluster host vs. browser-facing proxy subpaths

`app/config.py` currently uses one set of `localhost` URLs for **both** the app's server-side
fetches **and** the browser. These are now two different audiences and must split:

- **App → cluster (server-side).** The app is now a *container*, so `localhost` no longer means the
  host. Introduce `CLUSTER_HOST` env var (**default `localhost` for dev; ~~`host.docker.internal` in
  the container~~ — SUPERSEDED by Addendum A1: under `network_mode: host` the default `localhost`
  is the correct in-container value, so no override is set**). It rewrites the *host* in the three
  server-side URLs:
  - `MASTER_JSON_URL` → `http://{CLUSTER_HOST}:8080/json/` (read by `master_client.py:24`,
    `manager.py:226`)
  - `DRIVER_APP_UI_URL` and the probe base in `app_client.py:85`
    (`f"http://{CLUSTER_HOST}:{port}"`) — this line currently hardcodes `localhost`, so it **must**
    move to `CLUSTER_HOST` or the dashboard/annotation REST reads break in-container.
- **Browser-facing (via proxy subpaths).** These flow to templates and are what the browser dials —
  they become **relative proxy paths**, never host:port:
  - `JUPYTER_URL` → `/jupyter` (used by `topics.py:40` → iframe src
    `{{ jupyter_url }}/lab/tree/...` in `_cluster_right_pane.html:50`, becomes `/jupyter/lab/tree/...`)
  - `MASTER_UI_URL` → `/spark-master` (used by `topics.py:41`, `dashboard.py:113`,
    `_dashboard_body.html:26`, `_cluster_right_pane.html:45`)
  - `dashboard.py:90` (`app_ref.base_url`, from `DRIVER_APP_UI_URL`) is a **server-side** read used
    for deep-links — audit confirms it stays server-side; it inherits `CLUSTER_HOST`, not the proxy
    path. (Deep-links that a *browser* follows to the Spark UI go through `/spark-master`; the
    `app_client` `base_url` is for the app's own `:4040` REST reads.)
- **Public origin.** New `PUBLIC_ORIGIN` env var (scheme+host, e.g. `https://spark.example.com`) —
  consumed by the CSP allowlist (D4 / `jupyter_config.py`). Empty in dev = current localhost
  behavior preserved (US constraint: don't break the localhost/dev workflow).

Backward compatibility: all three env vars default to today's values, so `uvicorn app.main:app`
run directly on a host with no env set behaves exactly as now.

### D4 — nginx is the only internet-facing process: reverse proxy + basic auth + TLS

nginx on `network_mode: host` terminates TLS and gates everything with basic auth, reverse-proxying
three loopback upstreams:

```nginx
# --- rate limit: blunt brute-force mitigation, sized to clear the app's own HTMX/SSE traffic ---
limit_req_zone $binary_remote_addr zone=auth:10m rate=10r/s;   # see OQ6

# HTTP :80 — ACME challenge + redirect everything else to HTTPS
server {
    listen 80;
    server_name spark.example.com;
    location /.well-known/acme-challenge/ { root /var/www/certbot; }   # webroot for certbot renew
    location / { return 301 https://$host$request_uri; }
}

# HTTPS :443 — the whole app, behind one basic-auth gate
server {
    listen 443 ssl;
    http2 on;
    server_name spark.example.com;

    ssl_certificate     /etc/letsencrypt/live/spark.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/spark.example.com/privkey.pem;
    add_header Strict-Transport-Security "max-age=63072000" always;   # HSTS (US-PD3)

    # server-level auth => covers /, /jupyter/, /spark-master/, and the Jupyter WS upgrade
    auth_basic           "Spark Playbook";
    auth_basic_user_file /etc/nginx/htpasswd;
    limit_req            zone=auth burst=20 nodelay;

    location / {                              # FastAPI app
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto https;
    }
    location /jupyter/ {                       # JupyterLab — WebSocket kernels need upgrade headers
        proxy_pass http://127.0.0.1:8888;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 86400;             # long-lived kernel/SSE connections
    }
    location /spark-master/ {                  # Spark Master UI (reverseProxy handles worker UIs)
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
    }
}
```

Supporting changes so the subpaths actually work behind the proxy:

- **Jupyter** — add `--ServerApp.base_url=/jupyter/` and `--ServerApp.allow_remote_access=True` to
  the driver `command:` in the compose template. The iframe src already builds
  `{{ jupyter_url }}/lab/tree/...` so `/jupyter` + `base_url=/jupyter/` line up for free.
- **CSP/framing** — `driver/jupyter_config.py` currently hardcodes `frame-ancestors 'self'
  localhost:8000 127.0.0.1:8000`. Add the **public HTTPS origin** from an env var
  (`SPARKPB_PUBLIC_ORIGIN`, matching `PUBLIC_ORIGIN`) so the iframe renders on the deployed origin
  and not blank. `allow_origin` likewise widened to the public origin. (This file runs in the driver
  container with no access to `app/config.py`, so it reads its own env var — same
  can't-import-config rationale already documented in the file.)
- **Spark Master UI** — in `spark-defaults.conf.j2` set `spark.ui.reverseProxy=true` and
  `spark.ui.reverseProxyUrl=/spark-master`. This proxies worker UIs *through* the master (why worker
  host ports are dropped in D2). Verify it doesn't break the app's direct loopback reads of `/json/`
  and `:4040/api/v1/...` (those are separate endpoints, unaffected by UI reverse-proxy, but this is
  a named verification step — see Risks R-PD5).

---

## Addendum A1 (2026-07-17) — `app` service uses `network_mode: host` (supersedes the `host.docker.internal`/`extra_hosts` parts of D1 & D3)

*Source: devops-engineer operational review during deploy bring-up. The correction is already applied
in `deploy/docker-compose.yml`; this addendum records it so the ADR isn't silently stale. It is a
correction to make D1/D3 work on a real Linux VM, not a new design decision.*

**What D1/D3 got wrong.** D1 gave the `app` service `extra_hosts:
["host.docker.internal:host-gateway"]` and D3 set `CLUSTER_HOST=host.docker.internal` in the
container, on the assumption that the app's server-side reads would reach the spawned cluster's
loopback-published ports via the bridge gateway. **This fails on plain Linux Docker** (not Docker
Desktop, whose VM-based networking model is more permissive). D2 publishes the spawned cluster's
ports as `127.0.0.1:PORT:PORT` (loopback-scoped). Docker implements a loopback-scoped publish with
an **iptables DNAT rule matched on destination `127.0.0.1`** specifically. A packet the app
container sends to the bridge-gateway IP that `host.docker.internal` resolves to (e.g. `172.17.0.1`)
carries a *different* destination address, never matches that DNAT rule, and is refused. So the
"reachable from a container via host-gateway" reasoning is simply false for a loopback-scoped
publish on Linux Docker.

**Corrected decision.** The `app` service now uses **`network_mode: host`** — the same pattern D4
already approved for nginx. Under host networking the app's own `localhost`/`127.0.0.1` *is* the
host loopback, so it reaches the cluster's `127.0.0.1:8080` / `:4040` / `:8888` publishes directly.
Resulting changes to D1/D3:

- **`CLUSTER_HOST` reverts to its default (`localhost`).** No override is set in the container; the
  `app/config.py` default is now the correct in-deployment value. The `CLUSTER_HOST` env-var
  *mechanism* itself (D3) still stands — as does the browser-facing URL split
  (`JUPYTER_URL=/jupyter`, `MASTER_UI_URL=/spark-master`) and `PUBLIC_ORIGIN`, all unaffected by
  this change.
- **`extra_hosts` and the `ports:` publish are dropped** for the `app` service (host networking has
  no port-publish concept, and there is no bridge on which to add a `host.docker.internal` alias).
- **uvicorn is pinned to `127.0.0.1:8000` via an explicit `command:`.** Under host networking there
  is no `ports: 127.0.0.1:8000:8000` mapping to constrain the bind, and `Dockerfile.app`'s default
  CMD binds `0.0.0.0`. The explicit `--host 127.0.0.1` restores the loopback-only exposure that the
  D1 publish used to provide, so nginx (also host-net) still reaches the app on `127.0.0.1:8000`
  while nothing outside loopback can.

**Security-relevant consequence (routed to the security-auditor — see handoff item 5).** The `app`
container now shares the **host network namespace** rather than living on an isolated bridge. Versus
a bridged container this changes two things: (a) the app process can now reach **every** host
loopback service, not just the intended cluster ports — anything else bound to `127.0.0.1` on the VM
is in its reach; and (b) any port the app (or anything in that container) binds **without an
explicit interface** lands on **all** host interfaces, including public ones, with no Docker publish
step to gate it. The mitigation for (b) is the explicit uvicorn `--host 127.0.0.1` pin above;
combined with the firewall/security-group restricted to 22/80/443 (US-PD5), the intended exposure is
preserved. (a) is bounded by the same single-trusted-user threat model that already accepts the
Docker socket mount (OQ5/R-PD7) — the auditor should confirm both.

---

## Resolved open questions (OQ1–OQ7)

**OQ1 — LICENSE = MIT.** Add `LICENSE` (MIT) at repo root; add the SPDX/`author` year. Human-confirmed.

**OQ2 — Basic-auth credential provisioning = interactive prompt in `deploy.sh`, writes htpasswd.**
On first run only (idempotent — skip if the htpasswd file already exists, never overwrite silently),
`deploy.sh`:

```bash
HTPASSWD_FILE=./deploy/secrets/htpasswd          # gitignored, mounted into nginx
if [ ! -f "$HTPASSWD_FILE" ]; then
  read -rp  "Basic-auth username: " BA_USER
  read -rsp "Basic-auth password (min 16 chars, confirmed by retype): " BA_PASS; echo
  # generate bcrypt entry (cost 12) via a throwaway container -> no host apache2-utils
  # dependency, password never hits shell history (read -s) and never a command arg
  # that shows in `ps`
  docker run --rm httpd:2.4-alpine htpasswd -niB -C 12 "$BA_USER" <<<"$BA_PASS" > "$HTPASSWD_FILE"
  chmod 600 "$HTPASSWD_FILE"
  unset BA_PASS
fi
```

bcrypt (`-B`) at cost 12 (`-C 12`, security audit MED-1 — htpasswd's own default is cost 5), not
apr1/MD5. `read -s` keeps the password off the terminal and out of history; piping via stdin keeps
it off the process argv (`ps`). The password is rejected if empty or under 16 characters, and must
be re-typed to confirm before it's accepted (security audit HIGH-1) so a typo can't lock the
operator out. A `--reset-auth` flag can force a rewrite when the operator wants to rotate, subject
to the same validation. README states plainly this password is the entire trust boundary.

**OQ3 — TLS auto-renewal = certbot renewal sidecar (implemented, not manual).** Standard
nginx+certbot webroot pattern, fully containerized so it survives reboots with no host cron:

- *Initial issuance* (`deploy.sh`, first run): bring up nginx serving `/.well-known/acme-challenge/`
  from the shared webroot volume, then a one-shot
  `docker run ... certbot/certbot certonly --webroot -w /var/www/certbot -d $DOMAIN
  --email $EMAIL --agree-tos -n`. Fails loud with an actionable message if the domain doesn't resolve
  to the VM (US-PD5).
- *Auto-renewal* — a `certbot` service in `deploy/docker-compose.yml`:
  `entrypoint: sh -c 'trap exit TERM; while :; do certbot renew --webroot -w /var/www/certbot; sleep 12h & wait $!; done'`
  sharing the `letsencrypt` + `certbot-webroot` volumes with nginx.
- *nginx picks up renewed certs* — a companion reload loop wrapping the nginx command:
  `sh -c 'while :; do sleep 6h & wait $!; nginx -s reload; done & nginx -g "daemon off;"'`.

No host cron/systemd; the sidecar restarts with the stack on VM reboot. `certbot renew` is itself a
no-op until ~30 days before expiry, so the 12h loop is cheap and idempotent.

**OQ4 — VM sizing = my recommendation, below.** See the dedicated section after Consequences.

**OQ5 — Docker-socket-mount risk = documented trust-boundary decision; security-auditor signs off
separately.** Mounting `/var/run/docker.sock` into the app container makes the app **effectively
root on the host**. This is *not new* risk (the current host-process app already drives the socket),
but exposing it to the internet behind a single password is a **materially different exposure** than
localhost-only. It is accepted here because (a) the whole tool's threat model is single-trusted-user,
(b) the app genuinely needs broad Docker API access (create/start/stop/exec/networks/volumes) to
spawn clusters, so a restrictive socket-proxy (e.g. `tecnativa/docker-socket-proxy`) would have to
allow nearly the full API and buys little, and (c) basic auth + TLS + the loopback-only port surface
are the compensating controls. **This is flagged for the mandatory security-auditor pass and is NOT
considered resolved until that pass explicitly signs off** (per requirements Constraints / DoD). A
socket-proxy was considered and is noted as a possible auditor-driven hardening, not a default.

**OQ6 — Brute-force protection = nginx `limit_req` (added).** `rate=10r/s` with `burst=20 nodelay`
at server level (see D4). This is **deliberately blunt**: the limit must clear the app's own legit
traffic (HTMX pollers at ~1 req/6s, one long-lived SSE connection per dashboard, iframe/asset bursts
on load), so it is sized to stop an automated brute-forcer hammering hundreds/sec — not to lock out
after N failures (nginx OSS can't rate-limit selectively on 401). Combined with the mandatory strong
password (the real defense), this is cheap defense-in-depth. `fail2ban` on the nginx 401 log is noted
as an optional future hardening if the auditor wants a true lockout, but is out of scope for v1.0.

**OQ7 — Redeploy path after a code change (designed here; plan omitted it).**

- *Procedure:* `git pull && ./deploy.sh` (or directly
  `docker compose -f deploy/docker-compose.yml up -d --build`). This rebuilds the **app** image and
  recreates the `app` + `nginx` (+ `certbot`) containers of the **`sparkpb-deploy`** project only.
- *What it does NOT disturb:* the spawned Spark cluster is a **separate compose project (`sparkpb`)**
  created via the host socket, not part of the base stack — so recreating the base stack **does not
  stop the running cluster's containers**. TLS certs, htpasswd, and the rendered compose file all
  live on gitignored host paths / volumes and survive the rebuild. The Spark image
  (`sparkpb/spark:4.0.3`) is rebuilt only if `compose/build.sh` is re-run (deploy.sh skips it when
  present).
- *What it DOES disturb:* restarting the `app` container resets the in-memory `manager` singleton
  (`manager.py:240`) to `IDLE` — there is **no reconciliation with existing containers on startup**.
  So after an app restart the UI shows "no cluster" even though `sparkpb` containers may still be
  running (a benign desync). It **self-heals on the next spawn**, because `spawn()` /
  `_cancel_and_teardown_locked()` runs an idempotent `docker compose -p sparkpb down` first
  (`compose_ops.down():54`). A JupyterLab kernel's in-memory/JVM state is lost only when the driver
  container is actually town down — a bare app rebuild alone does not do that; a re-spawn or explicit
  `docker compose -p sparkpb down` does.
- *Recommended:* if you want a clean slate, tear the cluster down from the UI **before** redeploying;
  otherwise redeploy freely and re-spawn. No persistent app data is at risk — the only durable state
  is annotation checkpoints under `scratch/shared/annotations/` (on the bind-mounted host repo path,
  survives everything) and curriculum content in the repo.

---

## Alternatives considered

| Decision | Alternative | Why not |
|---|---|---|
| D1 containerize app + DooD | Keep app as a host process (systemd) + host nginx (apt) | Defeats the one-command goal — deploy becomes apt/systemd/host-nginx mutation instead of `docker compose up`. Containerizing is what makes the deploy a single command; the one cost (DooD path alignment) is bounded and standard. |
| D1 DooD (mount host socket) | Docker-in-Docker (privileged dind) | Heavier, its own daemon/storage, and *more* privileged than a socket mount for no gain here — spawned containers would be nested and their bind mounts wouldn't see the host repo without the same path gymnastics anyway. |
| D1 identical-path mount | Mount repo at a fixed `/workspace` in the app container | Breaks the DooD path resolution: the host daemon would try to bind `/workspace/../../` which doesn't exist on the host. Identical absolute path is the only clean fix given the existing relative mounts. |
| D1/D3 app networking (Addendum A1) | `extra_hosts: host.docker.internal:host-gateway` + `CLUSTER_HOST=host.docker.internal` (the original D1/D3 text) | Doesn't work on plain Linux Docker: the cluster's loopback-scoped publishes match an iptables DNAT rule keyed on destination `127.0.0.1`, which a packet to the bridge-gateway IP never hits. `network_mode: host` makes the app's own `127.0.0.1` the real host loopback — the same fix D4 already uses for nginx. See Addendum A1. |
| D3 proxy subpaths | Subdomains (`jupyter.example.com`, `spark.example.com`) | Needs multiple DNS records + certs and more nginx servers; subpaths need one domain, one cert, one auth gate. Subpaths are strictly simpler for single-user. |
| D4 basic auth | OAuth/SSO, or an app-level login | Explicit non-goal (requirements). Basic auth + strong password is the entire, deliberate trust boundary for a single user; anything more is unrequested complexity. |
| D4 nginx `network_mode: host` | nginx on a bridge network publishing 80/443, reaching upstreams via `host.docker.internal` | Host network lets nginx reach the loopback-bound app *and* the loopback-bound spawned cluster uniformly on `127.0.0.1`, matching where the app already binds; bridge would need every loopback upstream re-exposed to the bridge. |
| OQ3 renewal sidecar | Manual `certbot renew` documented as an operator task | Human confirmed auto-renewal must be implemented. A 90-day manual chore is exactly the kind of thing that lapses and takes the site down. |
| OQ2 interactive prompt | Read password from an env var / generate-and-print | Human-chosen: prompt avoids the password transiting `.env` files, shell history, or CI logs. |

Simpler designs I rejected because the real constraints forbid them (recorded per the ADR
discipline): dropping TLS renewal automation (OQ3 — would lapse), dropping rate limiting (OQ6 —
cheap defense-in-depth on an internet-facing auth gate), and a fixed-path repo mount (D1 — silently
mounts an empty `/workspace`). None of these were simplified away.

---

## Consequences

**Accepted trade-offs:**

- **The app container is effectively root on the host** via the socket mount, now reachable from the
  internet behind one password (OQ5). This is the central accepted risk; it gates on the security
  auditor.
- **The `app` container shares the host network namespace** (Addendum A1, superseding D1/D3's
  `host.docker.internal` approach — which doesn't work on plain Linux Docker). This widens the app's
  reach to *all* host loopback services (not just the cluster ports) and means any unqualified bind
  by the app process hits all host interfaces; the uvicorn `--host 127.0.0.1` pin + the 22/80/443
  firewall are the mitigations. Routed to the security-auditor (handoff item 5).
- **Two compose projects now coexist** (`sparkpb-deploy` base stack vs. `sparkpb` spawned cluster).
  The separation is deliberate (redeploy doesn't kill running clusters) but is a new mental model:
  operators must know `docker compose -f deploy/... ps` shows the base stack, `docker ps` shows both.
- **The app no longer reconciles cluster state on restart** (OQ7) — a redeploy leaves a benign
  UI/reality desync until the next spawn self-heals it. We accept this rather than build startup
  reconciliation (YAGNI for single-user; the self-heal path already exists).
- **`limit_req` is blunt, not a lockout** (OQ6) — it stops automated hammering but a slow,
  distributed guesser is bounded only by password entropy. Honest: the password *is* the security.
- **Dev/prod config divergence** — three new env vars (`CLUSTER_HOST`, browser proxy paths via
  templates, `PUBLIC_ORIGIN`). Defaulted to preserve the localhost workflow, but there are now two
  configurations to keep working (constraint: don't break dev).
- **What becomes harder:** multi-user, per-user isolation, or Jupyter sandboxing are now *further*
  away, not closer — the whole design leans on single-trusted-user. That's the intended boundary
  (explicit non-goal), not an oversight.

---

## Component / interaction design

Two compose projects. The base stack is long-lived; the cluster is spawned/torn-down per session by
the app driving the host socket.

```
                         Internet (only 22/80/443 open via firewall/SG)
                                     │  443 (TLS + basic auth + limit_req)
                                     ▼
   ┌──────────────────────────  VM (single host)  ──────────────────────────────┐
   │                                                                             │
   │   ┌── project: sparkpb-deploy (base stack, `deploy/docker-compose.yml`) ──┐ │
   │   │  nginx (network_mode: host)          certbot (renew loop, 12h)        │ │
   │   │    / ───────────► 127.0.0.1:8000     shares letsencrypt + webroot vols│ │
   │   │    /jupyter/ ───► 127.0.0.1:8888 (WS upgrade)                         │ │
   │   │    /spark-master/ ► 127.0.0.1:8080                                    │ │
   │   │                                                                       │ │
   │   │  app  (FastAPI, network_mode: host — uvicorn binds 127.0.0.1:8000)    │ │
   │   │    • mounts /var/run/docker.sock  (DooD — drives host daemon)         │ │
   │   │    • mounts ${REPO_HOST_PATH}:${REPO_HOST_PATH} (identical path)      │ │
   │   │    • host net (Addendum A1) ─► server-side reads:                     │ │
   │   │        MASTER_JSON_URL / :4040 REST → CLUSTER_HOST=localhost (default)│ │
   │   └───────────────────────────┬───────────────────────────────────────────┘ │
   │                               │ docker compose -p sparkpb up/down            │
   │                               ▼  (via host socket)                           │
   │   ┌── project: sparkpb (spawned cluster, rendered template) ──────────────┐ │
   │   │  spark-master (127.0.0.1:8080, reverseProxy=/spark-master)            │ │
   │   │  spark-worker-1..N (no host ports; UIs via master reverse-proxy)      │ │
   │   │  spark-driver (127.0.0.1:8888 Jupyter base_url=/jupyter/,             │ │
   │   │                127.0.0.1:4040-4042 app UI/REST)                       │ │
   │   │  all mount ../../:/workspace  → resolves to ${REPO_HOST_PATH} (DooD)  │ │
   │   └───────────────────────────────────────────────────────────────────────┘ │
   └─────────────────────────────────────────────────────────────────────────────┘
```

**Request flows:**
- *Browser → app:* `https://domain/` → nginx (TLS, basic auth, limit_req) → `127.0.0.1:8000`.
- *Browser → Jupyter iframe:* iframe src `/jupyter/lab/tree/<notebook>` → nginx (same auth, WS
  upgrade) → `127.0.0.1:8888` (Jupyter `base_url=/jupyter/`). Kernel WebSocket rides the same
  upgraded, authenticated connection.
- *Browser → Master UI:* link `/spark-master/` → nginx → `127.0.0.1:8080` (Spark
  `reverseProxyUrl=/spark-master`; worker UIs reached through the master, no host ports).
- *App → cluster (server-side, never through the proxy):* `manager`/`master_client`/`app_client`
  read `http://localhost:8080/json/` and `:4040/api/v1/...` via `CLUSTER_HOST` (default `localhost`,
  reaching the cluster's loopback publishes directly under `network_mode: host` — Addendum A1).
- *App → host daemon:* `compose_ops` shells `docker compose -p sparkpb ...` over the mounted socket;
  the daemon performs the `../../:/workspace` bind mount resolving to `${REPO_HOST_PATH}`.

**Files touched (developer handoff):** new `Dockerfile.app`, `deploy/docker-compose.yml`,
`deploy/nginx/*.conf`, `deploy.sh`, `LICENSE`, `.gitignore` (htpasswd/certs/`.env`); edits to
`compose/templates/docker-compose.yml.j2` (ports + Jupyter `base_url`), `spark-defaults.conf.j2`
(reverseProxy), `app/config.py` (URL/host split + env vars), `driver/jupyter_config.py` (CSP public
origin). No change to the lifecycle state machine, annotation engine, or curriculum content.

---

## VM sizing recommendation (OQ4)

Sized against the app's own hard limit: `RESOURCE_CEILING_GB = 32` (`app/config.py:127`) is the most
memory any legitimate UI spawn can request — `1 (master) + worker_count·worker_memory_gb + 2
(driver) ≤ 32`, so the practical ceiling spawn is ~31 GB of **container memory limits**
(`deploy.resources.limits.memory`). The default spawn is `1 + 3·4 + 2 = 15 GB`.

Memory must be sized for **limits, worst case**, not average use — a spill/skew demo really does push
workers to their `-Xmx` limit, and RAM exhaustion OOM-kills containers (unlike CPU, which merely
throttles). Reserve ~4–6 GB above the spawn for the host OS, the Docker daemon, the base stack
(nginx + app ≈ 0.5 GB), and JVM/off-heap overhead beyond the configured `-Xmx`.

**Recommended minimum (safe for any in-range spawn, including the 32 GB ceiling):**

| Resource | Spec | Rationale |
|---|---|---|
| RAM | **48 GB** | 32 GB ceiling spawn + ~6 GB OS/Docker/base-stack/JVM overhead, with margin. A 48 GB cloud shape is a common size and matches the original 64 GB dev host's intent. |
| vCPU | **8** | Default spawn's CPU *limits* total `1 + 3·2 + 2 = 9`; a max spawn (`1 + 5·4 + 2 = 23`) oversubscribes 8 vCPU, which is acceptable (CPU throttles gracefully; RAM does not). 8 vCPU comfortably runs default and moderate spawns. |
| Disk | **80 GB SSD** | Spark image (~few GB) + app image + base images + repo + `scratch/` demo data + logs + Let's Encrypt. Comfortable headroom. |

**Headroom assumption (state this in the README):** RAM is sized for container memory *limits* at the
32 GB ceiling plus ~6 GB reserved for host OS + Docker daemon + base stack + JVM overhead. On this
budget, *every* in-range UI spawn (up to the ceiling) runs without OOM risk.

**Genuine fork the operator must decide (surfaced, not guessed):** a cheaper **32 GB / 8 vCPU / 60 GB**
VM is viable **only if** the operator also lowers `RESOURCE_CEILING_GB` to ~24 — otherwise a
ceiling-max spawn would OOM (32 GB of limits + OS/base leaves no headroom on a 32 GB box). The clean
invariant to document: **VM RAM ≥ `RESOURCE_CEILING_GB` + ~6 GB**. So the ceiling and the VM size are
coupled knobs; devops-engineer/operator picks the pair (48 GB VM + 32 GB ceiling *or* 32 GB VM + ~24 GB
ceiling). I recommend the 48 GB shape as the default so the shipped ceiling is safe out of the box.

---

## Visual / UX surface (for the later acceptance screenshot check)

This is a deploy/access change, not a UI feature — functional parity (US-PD4) is the bar, not a new
layout. Two access-surface things a visual check should confirm, so "wrong" is distinguishable from
"works":

- **Auth gate:** hitting `https://<domain>/` unauthenticated shows the **browser's native basic-auth
  dialog** (not an app login page); wrong/blank creds → `401`; `http://<domain>/` → `301` to https.
- **Subpath URLs, no leaked ports:** after login, the app renders normally; the Jupyter iframe loads
  (not blank/CSP-blocked) from `https://<domain>/jupyter/lab/tree/...` and a kernel runs a cell; the
  Master UI link opens `https://<domain>/spark-master/`; the monitoring dashboard populates. In
  browser devtools, **every** request is to the `https://<domain>/...` origin — no `:8888`, `:8080`,
  `:4040`, or bare-IP request leaks through. That "no direct-port request in the network tab" is the
  concrete, checkable signal that the proxy/URL split is correct.

---

## Risks

- **R-PD1 — DooD path mismatch silently mounts an empty `/workspace`.** If `REPO_HOST_PATH` isn't
  captured/passed correctly, spawns "succeed" but containers see an empty repo (no notebooks, no
  `spark-defaults.conf`). *Noticed by:* US-PD1's post-spawn check (`ls /workspace` inside a spawned
  container must show the real repo); Spark falling back to `local[*]` because the conf mount is
  empty. *Mitigation:* `deploy.sh` sets `REPO_HOST_PATH=$(pwd)` and `working_dir` to it; the
  acceptance test asserts real contents, not just container start.
- **R-PD2 — Jupyter subpath / WebSocket breaks behind the proxy.** Kernels need the WS upgrade
  through nginx *and* through basic auth *and* Jupyter served at `base_url=/jupyter/`. Any one wrong
  → blank iframe or "kernel connecting…" forever. *Noticed by:* US-PD4 (cell won't run). *Mitigation:*
  the `Upgrade`/`Connection` headers + long `proxy_read_timeout` in D4; server-level auth so the WS
  handshake carries creds; explicit acceptance step.
- **R-PD3 — CSP `frame-ancestors` missing the public origin → blank iframe.** *Noticed by:* iframe
  renders blank, console shows a `frame-ancestors` refusal for the https origin. *Mitigation:* D4
  adds `PUBLIC_ORIGIN` to `jupyter_config.py`'s allowlist; verify with the deployed origin, not
  localhost.
- **R-PD4 — Cert issuance fails (DNS not pointed / rate-limited).** *Noticed by:* `deploy.sh`
  certbot step erroring (US-PD5 requires a clear, actionable failure, not a silent no-TLS stack).
  *Mitigation:* fail loud with the domain-resolution hint; the renewal sidecar retries every 12h.
- **R-PD5 — Spark `reverseProxy` breaks the app's direct REST reads.** Turning on
  `spark.ui.reverseProxy` could interfere with the app's own `:8080/json/` and `:4040/api/v1/...`
  loopback reads. *Noticed by:* dashboard/annotation views going blank while the cluster is clearly
  up. *Mitigation:* those are REST/JSON endpoints, distinct from the HTML UI the reverse-proxy
  rewrites, so they should be unaffected — but this is a **named verification step** in D4, not an
  assumption; test-engineer confirms the dashboard still populates.
- **R-PD6 — Redeploy desync surprises the operator** (OQ7). *Noticed by:* UI shows "no cluster" after
  a redeploy while `docker ps` shows `sparkpb` containers. *Mitigation:* documented procedure;
  self-heals on next spawn; README notes it.
- **R-PD7 (SECURITY, gates release) — socket-mounted app behind one password = internet-reachable
  host root.** *Noticed by:* by design, not by failure — this is the standing exposure. *Mitigation /
  handoff:* strong password (the real control), TLS + HSTS, loopback-only ports, firewall 22/80/443,
  `limit_req`. **This ADR does not resolve it — the mandatory security-auditor pass must explicitly
  sign off before go-live** (requirements Constraints / DoD).
- **R-PD8 (SECURITY, Addendum A1) — `app` on `network_mode: host` binds too broadly.** Under host
  networking a bind without an explicit interface lands on all host interfaces, and the app can reach
  all host loopback services. *Noticed by:* a port scan of the public IP showing `:8000` (or any app
  listener) reachable from outside; devtools/`ss -ltnp` on the VM showing a `0.0.0.0` bind.
  *Mitigation:* uvicorn pinned `--host 127.0.0.1`; firewall 22/80/443. Routed to the security-auditor
  (handoff item 5).

---

## Security-auditor handoff

Per the requirements Constraints and the project DoD, this work touches auth, internet-reachable
input execution (tokenless Jupyter behind the proxy), secrets (htpasswd, TLS keys), and a trust
boundary (Docker socket mount) — a **security-auditor pass is mandatory before go-live** and is a
separate pipeline stage (not resolved in this ADR). Specific items to route to the auditor:

1. **Socket mount (OQ5 / R-PD7)** — accept as-is, require a socket-proxy, or add other compensating
   controls? This ADR accepts it with rationale but defers the sign-off.
2. **`limit_req` sufficiency (OQ6)** — is blunt rate-limiting + strong password enough, or add
   `fail2ban` on 401s?
3. **htpasswd/TLS-key handling (OQ2/OQ3)** — file perms (`chmod 600`), gitignore coverage, no secret
   in argv/history/logs; confirm the throwaway-container htpasswd generation leaks nothing.
4. **Open-source hygiene (US-PD6)** — full git-history secret scan before flipping visibility (the
   plan's exploration found none; the auditor confirms).
5. **`app` on `network_mode: host` (Addendum A1 / R-PD8)** — the `app` container now shares the
   **host network namespace** (correcting D1/D3, which didn't work on plain Linux Docker). This
   changes the app's exposure versus a bridged container in two ways the auditor must weigh: (a) the
   app process can reach **all** host loopback services, not just the intended cluster ports
   (`127.0.0.1:8080/:4040/:8888`) — anything else bound to loopback on the VM is now in its reach;
   and (b) any port the app process binds **without an explicit interface** lands on **all** host
   interfaces (public included), with no Docker publish step to gate it. The mitigation for (b) is
   the explicit uvicorn `--host 127.0.0.1` pin in `deploy/docker-compose.yml`; the firewall
   (22/80/443) is the backstop. Confirm: no other listener in the app container binds `0.0.0.0`
   unintentionally, the `127.0.0.1` pin is present and effective, and (a) is acceptable under the
   single-trusted-user threat model that already governs the socket mount (OQ5).
