# Public Deploy — Acceptance Report + On-VM Checklist

Status: Part A validated on the dev host (Windows + Docker Desktop) and accepted as the sole
live-verification evidence for v1.0. **Part B (on-VM live acceptance) is explicitly waived, not
pending** — human decision, 2026-07-19: this project will only ever run locally, so a real Linux
VM + domain will never exist to execute Part B against. The Part B checklist below is kept as
reference documentation of what a hypothetical future VM deploy would need to verify, not as open
work blocking anything.
Owner: test-engineer (acceptance validation)
Date: 2026-07-17
Scope: US-PD1 through US-PD6 (`docs/requirements/public-deploy.md`), against
`docs/architecture/public-deploy.md` (incl. Addendum A1) and the implemented artifacts
(`Dockerfile.app`, `deploy/docker-compose.yml`, `deploy/nginx/default.conf.template`,
`deploy.sh`, `deploy/README.md`, modified `app/config.py` / `app/spark_api/app_client.py` /
`app/lifecycle/renderer.py` / `compose/templates/*.j2` / `driver/jupyter_config.py`).

## Method / environment constraint

This session has **no cloud VM and no domain** — the host is Windows + Docker Desktop. Docker
Desktop's networking model doesn't reproduce plain-Linux `network_mode: host` behavior (the whole
reason Addendum A1 exists), so nothing that depends on real host-network semantics, a real
firewall, a real Let's Encrypt issuance, or a real browser hitting a public IP could be exercised
here. Everything in Part A below is either (a) actually run and observed on this host, or (b)
statically confirmed by reading the rendered/generated artifacts — each item says which. Part B is
the checklist for the parts that gate on real Linux + real DNS.

---

## Part A — validated in this environment

### A1. Unit suite

**RUN.** `py -3 -m pytest tests/unit -q`

```
316 passed in 3.63s
```

**PASS.** Matches the expected post-remediation count. Includes the dedicated
`tests/unit/test_public_deploy_config.py` (dev-default preservation, `CLUSTER_HOST` override
scoping, `app_client.py` probe URL construction, renderer forwarding `PUBLIC_ORIGIN` into the
compose template).

### A2. Base-stack compose validity

**RUN.** `REPO_HOST_PATH=/tmp/fakepath DOMAIN=spark.example.com PUBLIC_ORIGIN=https://spark.example.com docker compose -f deploy/docker-compose.yml config`

**PASS.** Renders cleanly, no errors. Confirmed in the rendered YAML:
- `app`: `network_mode: host`, `command: uvicorn app.main:app --host 127.0.0.1 --port 8000`,
  volumes = `/var/run/docker.sock:/var/run/docker.sock` + `${REPO_HOST_PATH}:${REPO_HOST_PATH}`
  (identical-path DooD mount), `working_dir` = the same path, `environment.JUPYTER_URL=/jupyter`,
  `MASTER_UI_URL=/spark-master`, `PUBLIC_ORIGIN` passed through.
- `nginx`: `network_mode: host`, mounts the nginx template/htpasswd/cert/webroot paths, reload-loop
  `command`.
- `certbot`: renewal-loop `entrypoint`, shares the letsencrypt + webroot volumes with nginx.
- Two distinct compose project concerns visible: this file's `name: sparkpb-deploy` vs. the
  spawned cluster's `name: sparkpb` (`compose/templates/docker-compose.yml.j2`) — confirmed
  textually, they don't collide.

Note: `docker compose config` on Windows resolves the bind-mount `source:` to a Windows path
(`C:/Users/.../fakepath`) purely because the dummy `REPO_HOST_PATH` I supplied doesn't exist as a
real Linux path — this is a rendering/display artifact of running the check on Windows, not a
defect; the template itself only ever substitutes `${REPO_HOST_PATH}` literally.

### A3. Renderer — port surface, Jupyter subpath, reverseProxy

**RUN.** Called `app/lifecycle/renderer.py::render()` directly (the app's real renderer, not a
hand-inspection) with default `ClusterParams()`, then inspected the actual rendered
`compose/rendered/docker-compose.yml` and `compose/rendered/spark-defaults.conf`.

**PASS**, all assertions held:
- Published ports found: `127.0.0.1:8080:8080` (master), `127.0.0.1:8888:8888` (Jupyter),
  `127.0.0.1:4040-4042:4040-4042` (driver UI/REST + 2 fallback ports). No other `ports:` entries.
- **No worker ports published** — `spark-worker-N` services have no `ports:` block at all
  (confirmed by direct read of `docker-compose.yml.j2`, lines 82-111).
- **No `6066` or `7078`/`7079` published** — those strings appear only in template comments
  explaining why they're intentionally *not* published, not in any `ports:` list entry (checked by
  isolating actual `- "..."` port-mapping lines via regex; no `6066`/`7078`/`7079` among them).
- Jupyter `command:` contains `--ServerApp.base_url=/jupyter/ --ServerApp.allow_remote_access=True`.
- `SPARKPB_PUBLIC_ORIGIN` env var is forwarded into the driver service and carries the
  `PUBLIC_ORIGIN` value (also covered by the existing unit test
  `test_renderer_forwards_public_origin_into_compose_template`).
- `spark-defaults.conf` contains `spark.ui.reverseProxy true` and
  `spark.ui.reverseProxyUrl /spark-master`.

`compose/rendered/` is gitignored (confirmed `git status --short compose/rendered/` is empty
before and after) — no cleanup needed.

### A4. nginx config — static confirmation

**Read** (binary-flagged by the file tool, inspected via `cat`/`file`; UTF-8 text, not actually
binary) `deploy/nginx/default.conf.template`. Confirmed statically:
- `auth_basic`/`auth_basic_user_file` are set at the **`server` block level** inside the `:443`
  server, so they apply to every `location` under it — `/`, `/jupyter/` (including the WebSocket
  upgrade request, since there's no separate unauthenticated WS location), and `/spark-master/`.
- `/jupyter/` location has `proxy_http_version 1.1`, `proxy_set_header Upgrade $http_upgrade`,
  `proxy_set_header Connection "upgrade"`, and a long `proxy_read_timeout 86400` for long-lived
  kernel/SSE connections.
- The **only unauthenticated location** anywhere is `/.well-known/acme-challenge/` on the `:80`
  server (outside the `auth_basic` server block, as required for ACME HTTP-01).
- `:80` server's `location /` returns `301 https://$host$request_uri` (plaintext-to-HTTPS
  redirect); `:443` server sends `Strict-Transport-Security "max-age=63072000"` (HSTS) on every
  response (`always`).
- `limit_req_zone` + `limit_req zone=auth burst=20 nodelay` present at server level (OQ6 blunt
  brute-force mitigation).

**PASS** — matches D4 exactly as specified in the ADR; no discrepancy found between the ADR's
inline nginx snippet and the shipped template.

### A5. `Dockerfile.app` build

**RUN.** `docker build -f Dockerfile.app -t sparkpb-app-test .`

**PASS.** Builds successfully (fully cached from a prior identical build in this environment, but
the layer graph and final image both resolved correctly). Confirmed the LOW-1 fix is present:
`CMD ["uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8000"]` (loopback-only default,
not `0.0.0.0`) — this matters because under `network_mode: host` there's no `ports:` publish step
to constrain the bind, so the image's own default CMD binding loopback is a real second line of
defense, not redundant with the compose `command:` override alone. Test image removed after the
check (`docker rmi sparkpb-app-test`).

**No `docker compose up` / live cluster spawn was run** — the renderer check (A3) exercises the
real render path without needing a running cluster, and `network_mode: host` runtime behavior on
Docker Desktop would not represent real Linux Docker behavior anyway (explicitly out of scope per
the task). No `content/*/notebook.ipynb` was executed; `git status` confirms no notebook is
modified.

### A6. Open-source hygiene artifacts (US-PD6, static check)

- `LICENSE` exists at repo root. **PASS** (existence confirmed; not re-litigating the MIT-vs-other
  choice, already human-confirmed per the ADR's OQ1).
- `.gitignore` diff adds `deploy/secrets/*` (keeping `.gitkeep`), `deploy/certs/*` (keeping
  `.gitkeep`), `.env`, `*.env`. **PASS** — covers htpasswd, TLS certs/keys, and the
  domain/email config file (`deploy/secrets/deploy.env`), all of which exist as real untracked
  files under `deploy/` in this working tree and are correctly excluded (`git status --short
  deploy/` shows only the top-level `deploy/` directory as untracked-as-a-whole, not individual
  secret files leaking through — Git treats the whole ignored subtree as invisible to `status`).
- Full git-history secret scan: **not re-run here** — this was already covered by the plan's
  exploration and is explicitly named as a security-auditor responsibility (item 4 of the ADR's
  security-auditor handoff list), not re-verified independently by this pass.

### A7. Local-environment note (not a defect)

While inspecting `deploy/certs/`, found a stray `deploy/certs/letsencrypt;C` directory left over
from an earlier local dry-run on this Windows/Docker-Desktop host — a `docker run -v
<path>:/etc/letsencrypt` where `<path>` contained a Windows drive letter's `:` got misparsed by the
`-v` flag's own colon-delimited syntax, minting a literal `letsencrypt;C` directory. This is
already a documented caveat in `deploy/README.md` §6 ("Windows (Docker Desktop)" — run from WSL2
inside the Linux filesystem, not `/mnt/c/...`), so it's expected debris from a non-target platform,
not a defect in `deploy.sh`/`docker-compose.yml`. Gitignored, no cleanup required, flagged here
only for transparency.

---

## US-PD1–US-PD6 traceability summary

| Criterion | Status | Where |
|---|---|---|
| US-PD1: `./deploy.sh` brings up base stack, no manual steps | VM-deferred | Checklist §1 |
| US-PD1: `docker compose ... ps` shows app+nginx healthy | VM-deferred | Checklist §1 |
| US-PD1: re-running `deploy.sh` is idempotent | VM-deferred | Checklist §1 (read-through of script logic done; **statically** looks idempotent — see below) |
| US-PD1: DooD spawn — `/workspace` has real repo, not empty | VM-deferred | Checklist §2 |
| US-PD2: only nginx on `0.0.0.0:80/443`, everything else loopback (`ss -tlnp`) | VM-deferred | Checklist §3 |
| US-PD2: 8080/8888/4040/8081/6066 refused from another host | VM-deferred | Checklist §3 |
| US-PD2: firewall/SG restricts to 22/80/443 | VM-deferred | Checklist §3 |
| US-PD3: unauthenticated request → 401 | VM-deferred | Checklist §4 |
| US-PD3: authenticated request → app loads | VM-deferred | Checklist §4 |
| US-PD3: plaintext → 301 redirect | VM-deferred | Checklist §4 |
| US-PD3: valid LE cert, browser-trusted | VM-deferred | Checklist §4 |
| US-PD3: HSTS upgrades without plaintext round-trip | VM-deferred | Checklist §4 |
| US-PD4: Jupyter iframe renders + kernel cell runs via proxied WS | VM-deferred | Checklist §5 |
| US-PD4: Master UI loads at `/spark-master`, shows cluster state | VM-deferred | Checklist §5 |
| US-PD4: monitoring dashboard populates live (R-PD5) | VM-deferred | Checklist §6 (named risk, dedicated section) |
| US-PD4: no direct-port leak in browser network tab | VM-deferred | Checklist §5 |
| US-PD5: README states VM shape / DNS / firewall prereqs | **Verified here** | `deploy/README.md` §1-4 read directly, matches ADR OQ4/US-PD5 |
| US-PD5: certbot step fails loud on bad domain | **Verified here (static)** | `deploy.sh` lines 141-166: explicit `exit 1` with actionable message + issuer-string assertion; not exercised live (needs real DNS failure) |
| US-PD6: LICENSE exists | **Verified here** | A6 |
| US-PD6: git-history secret scan | Deferred to security-auditor | A6 (explicitly routed, not this pass's job) |
| US-PD6: deploy artifacts excluded via `.gitignore` | **Verified here** | A6 |
| US-PD6: README documents `deploy.sh` + trust-boundary statement | **Verified here** | `deploy/README.md` + main README's deploy section confirmed to exist and state the password-is-the-boundary point (per requirements US-PD6) |

**Verified here (this session):** unit suite (316/316), base compose validity, renderer port
surface + Jupyter subpath + reverseProxy config, nginx static auth/redirect/HSTS/rate-limit
structure, `Dockerfile.app` build + CMD loopback pin, `.gitignore`/`LICENSE` hygiene, README
prerequisites content.

**Genuinely deferred to the VM (cannot be observed here):** every live-network, live-DNS,
live-TLS-issuance, live-firewall, and live-browser behavior — i.e. all of US-PD1's "stack actually
comes up and is idempotent on a real host," all of US-PD2 and US-PD3's live checks, and all of
US-PD4's functional-parity checks. `network_mode: host` specifically cannot be meaningfully
exercised on Docker Desktop (Addendum A1's whole point is that Docker Desktop's networking model
is *more permissive* than plain Linux, so a "works here" result would be a false positive, not
evidence).

---

## Part B — on-VM acceptance checklist (WAIVED — out of scope, 2026-07-19)

**This checklist will never be executed.** The human has decided Spark Playbook is local-only
permanently, so there will never be a real Linux VM + domain to run it against; v1.0's
Definition of Done was met on Part A + the unit suite alone (see the Status line above and
`docs/backlog.md`'s v1.0 rescope section). Kept below only as reference documentation for what a
hypothetical future VM deploy would need to verify.

Execute in order on the target Linux VM, after `deploy/README.md`'s prerequisites (VM sizing, OS
packages, DNS A-record, firewall) are in place. Each item is tied to its US-PD acceptance
criterion. Replace `<domain>` with the real deployed domain throughout.

### 1. One-command deploy (US-PD1)

- [ ] **Fresh deploy.** From a clean clone: `./deploy.sh`. Expect: prompts for domain, contact
      email, basic-auth username/password (min 16 chars, confirmed by retype); completes with
      `Deployed. https://<domain>/ (basic auth required).` and no error exit.
- [ ] **Stack health.** `docker compose -p sparkpb-deploy -f deploy/docker-compose.yml ps` — expect
      `app` and `nginx` both `Up` (nginx additionally healthy once it reloads with the real cert).
- [ ] **Idempotent re-run.** `./deploy.sh` again (unchanged). Expect: it does **not** re-prompt for
      domain/email/credentials (reads `deploy/secrets/deploy.env` + skips existing htpasswd), does
      **not** re-issue a still-valid cert (skips the certbot step because
      `deploy/certs/letsencrypt/live/<domain>/fullchain.pem` already exists and is not the
      bootstrap self-signed one), and exits 0 with the stack still up.
- [ ] **Redeploy after a change.** `git pull` (or touch a file) then `./deploy.sh` again. Expect:
      `app` image rebuilds and the `app`/`nginx` containers recreate; exits 0.
- [ ] **`--reset-auth` rotates credentials on demand.** `./deploy.sh --reset-auth` — expect a fresh
      username/password prompt, `deploy/secrets/htpasswd` rewritten, and the old credentials
      subsequently rejected (see §4).

### 2. DooD spawn — real repo, not empty `/workspace` (US-PD1)

- [ ] Through the deployed UI (or `curl` against the app's own spawn route through nginx, logged
      in), spawn a cluster.
- [ ] `docker ps` shows `spark-master`, `spark-worker-1..N`, `spark-driver` all `Up` under project
      `sparkpb` (distinct from `sparkpb-deploy`).
- [ ] `docker exec spark-driver ls /workspace` — expect the **real repo tree** (`app/`, `compose/`,
      `content/`, `driver/`, `README.md`, ...), not an empty directory. This is the concrete proof
      the identical-absolute-path DooD bind mount (`${REPO_HOST_PATH}:${REPO_HOST_PATH}`) resolved
      correctly from inside the containerized app — the single load-bearing detail in the ADR (D1).
- [ ] `docker exec spark-driver cat /opt/spark/conf/spark-defaults.conf` — expect the real rendered
      conf (not empty/missing), confirming the mount isn't just present but has real content Spark
      actually reads (`spark.master spark://spark-master:7077` line present).

### 3. Minimal port surface (US-PD2)

- [ ] With the base stack up **and** a cluster spawned (§2), run `ss -tlnp` on the VM. Expect:
      - Only `nginx` (or a process whose cmdline shows `nginx`) bound to `0.0.0.0:80` and
        `0.0.0.0:443`.
      - `app` (uvicorn), `spark-master` (`:8080`), `spark-driver` (`:8888`, `:4040-4042`) bound to
        `127.0.0.1` only — never `0.0.0.0` or a public interface IP.
      - No process at all bound to `:8081`+ (worker UIs), `:6066` (Spark REST submission),
        `:7078`/`:7079` (driver/blockManager ports).
- [ ] From a **separate machine**, attempt TCP connections to the VM's public IP on each of:
      `8080`, `8888`, `4040`, `8081`, `6066`. Expect every one **refused** (connection refused /
      timeout, not a Spark/Jupyter response). Quick check:
      `nc -zv -w3 <vm-ip> 8080 8888 4040 8081 6066` (or `Test-NetConnection` from Windows) — expect
      all closed/refused.
- [ ] Confirm the firewall/security-group rule set itself: only `22`, `80`, `443` inbound allowed
      (`sudo ufw status` or the cloud provider's SG console). This is the primary enforcement per
      the ADR; the loopback binding above is defense-in-depth on top of it, not a substitute.

### 4. Authenticated, encrypted access (US-PD3)

- [ ] `curl -i https://<domain>/` with **no** credentials → expect `401 Unauthorized`, no app HTML
      body returned.
- [ ] `curl -i -u '<user>:<pass>' https://<domain>/` with the real basic-auth credentials → expect
      `200 OK` and the app's actual page content.
- [ ] `curl -i http://<domain>/` (plaintext) → expect `301` with a `Location: https://<domain>/`
      header; confirm the response body carries **no** app content and **no** basic-auth prompt
      (credentials must never be inducible over plaintext).
- [ ] `curl -vI https://<domain>/ 2>&1 | grep -i strict-transport` → expect a
      `Strict-Transport-Security: max-age=63072000` header present on the HTTPS response.
- [ ] In a real browser, open `https://<domain>/`: expect a valid, **browser-trusted** padlock —
      no self-signed-cert warning. Inspect the cert (browser cert viewer or
      `openssl s_client -connect <domain>:443 -servername <domain> | openssl x509 -noout -issuer`)
      → issuer should be Let's Encrypt (`R3`/`E5`/etc., not the bootstrap self-signed CN).
- [ ] Revisit `https://<domain>/` in a browser that already has an HSTS record for the domain
      (i.e. visited once already) by typing `http://<domain>/` in the address bar → expect the
      browser to upgrade to `https://` itself (no visible plaintext round-trip in devtools'
      Network tab — the request should show as `https` from the start, not a redirected `http`
      request).

### 5. Functional smoke through the proxy (US-PD4)

- [ ] Log in via the browser (basic-auth dialog, not an app login page — confirms D4's design:
      the native browser prompt, not a custom form).
- [ ] Spawn a cluster from the UI, open a topic. Expect the **Jupyter iframe renders** (not blank,
      no CSP console error) at `https://<domain>/jupyter/lab/tree/...`.
- [ ] Run a notebook cell to completion. Expect it executes and returns output — proves the kernel
      WebSocket connection survives nginx's proxy + the WS upgrade headers + basic auth + TLS,
      all at once.
- [ ] Follow the Spark Master UI link from the app. Expect it loads at
      `https://<domain>/spark-master/` and shows live cluster state (workers listed, applications
      running/completed).
- [ ] From the Master UI, follow a link into a **worker's** UI (via Spark's own reverse-proxy).
      Expect it resolves under the same `/spark-master/...` origin/subpath — no separate worker
      host:port ever appears.
- [ ] Open the monitoring dashboard while a job is running. Expect it **populates with live data**
      exactly as the localhost deployment does — see §6 below, this is the named risk R-PD5.
- [ ] With browser devtools' Network tab open throughout the above, confirm **every** request goes
      to `https://<domain>/...` — grep the Network tab for `:8888`, `:8080`, `:4040`, or a bare VM
      IP; expect zero matches. This is the concrete "no leaked port" signal the ADR names.

### 6. R-PD5 — Spark `reverseProxy` must not break the app's own REST reads (named risk)

This is the ADR's explicitly-named verification step, not assumed safe by the design — check it
directly, not just via the dashboard "looking fine":

- [ ] With `spark.ui.reverseProxy=true` / `reverseProxyUrl=/spark-master` active (confirmed
      rendered in §A3 above), from **inside** the VM (or via `docker exec app curl ...` if the app
      container doesn't have `curl`, adapt to `python3 -c "import urllib.request; ..."`):
      `curl http://127.0.0.1:8080/json/` → expect a valid JSON payload describing cluster state
      (workers, status), **not** an HTML page or a reverse-proxy rewrite artifact.
- [ ] `curl http://127.0.0.1:4040/api/v1/applications` → expect a valid JSON array (driver's Spark
      UI REST API), unaffected by the master's UI-level reverse-proxy setting.
- [ ] With a job actually running, open the monitoring dashboard (`/dashboard` through the proxy)
      and confirm live data populates — this exercises the exact same server-side reads as the two
      `curl` checks above, end-to-end through the real deployed app, not just the raw endpoints.
- [ ] If any of the three above is blank/broken while `docker ps` shows the cluster healthy, this
      is R-PD5 materializing — file it as a defect referencing this checklist item; do **not**
      treat "dashboard looks empty" as a cluster-liveness issue without first confirmed via the
      `curl` checks that the reverse-proxy setting, not the cluster itself, is the cause.

### 7. MED-2 follow-up — XSRF stays enabled with same-origin deploy topology

`driver/jupyter_config.py` disables Jupyter's XSRF check only when `PUBLIC_ORIGIN` is unset (dev,
cross-origin embed); a deployed instance sets `PUBLIC_ORIGIN` and is same-origin, so
`c.ServerApp.disable_check_xsrf` should be `False` (XSRF **enabled**) in production. Confirm this
doesn't silently break cell execution:

- [ ] On the deployed instance (already logged in via §5), open a notebook and run a cell that
      involves a Jupyter **REST** action typically gated by XSRF (not just the kernel WebSocket
      channel, which doesn't carry XSRF tokens the same way) — e.g. save the notebook
      (`Ctrl+S`/File → Save), or create/rename a file from the Jupyter file browser inside the
      iframe. Expect it succeeds (no `403`/XSRF-cookie-mismatch error in the browser console or a
      visible "Forbidden" toast in the Jupyter UI).
- [ ] Confirm in browser devtools that the request to `/jupyter/api/contents/...` (or similar) is
      same-origin (`https://<domain>/jupyter/...`), not a separate origin — this is what makes the
      XSRF cookie readable/sendable in the first place; if it's ever cross-origin in the deployed
      topology, that's a real regression from the assumption XSRF re-enablement rests on.
- [ ] Run at least one ordinary kernel cell (not just REST actions) to confirm the WebSocket path
      (already covered in §5) is unaffected by XSRF being enabled — kernel execution over WS is a
      different channel and should be unaffected, but confirm rather than assume.
