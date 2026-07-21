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
instead, and a manual LAN-only deploy path (see
[Deploy (LAN-only, home server)](#deploy-lan-only-home-server)) if you want it reachable on your
home network without a domain or TLS — both are separate, opt-in modes that do not change the
single-user, no-multi-tenancy trust model, and do not affect the local workflow.

**The project's real differentiator (G1 — interview-depth over platform polish):** where a choice
exists between spending effort on UI/platform sophistication versus on curriculum depth or
realistic cluster/data behavior, curriculum depth and cluster realism win. Raw, unguided cluster
access is never gated behind unfinished app features, and the self-check annotation engine is
designed to be consulted *after* you've formed a hypothesis, not to explain things for you. See
`docs/requirements/spark-playbook-mvp.md` for the full problem statement and goals.

## Current status

**Built and human-signed-off:** the Phase 0 cluster harness (`compose/README.md`), the Phase 1 web
app (topic page, cluster control panel, embedded JupyterLab — `docs/acceptance/phase-1.md`), the
Phase 2 self-check annotation engine, the Phase 2.5 realtime monitoring dashboard
(`docs/acceptance/phase-2-5.md`), the unified topic-page shell (Concept/Notebook/Self-check tabs,
cluster-config drawer, breadcrumb switcher), and a data-driven topics-index landing page (`GET /`,
one card per `content/*/manifest.yaml` topic, no hardcoded list, now grouped into two sections — see
below). 15 curriculum topics are built in the original Spark track: partitioning/shuffle, join
strategies, bucketing, AQE, Catalyst plans, DAG & lazy evaluation, caching & persistence, window
functions, serialization formats, executor tuning, memory management, skew & salting, checkpointing,
fault tolerance & lineage, and UDF vs pandas UDF serialization cost.

**A second, parallel Kafka curriculum track has now started** (learning Kafka itself, not just as
plumbing under a Spark job): three topics are built and signed off so far — `kafka-architecture-kraft`
(brokers, controllers, KRaft quorum vs. legacy ZooKeeper — GitHub issue #62),
`kafka-topics-partitions` (partition count/key choice, keyed vs. unkeyed produce, per-partition
ordering — GitHub issue #63), and `kafka-producers-delivery` (`acks=0/1/all` producer delivery
semantics and idempotent-producer dedup under a real induced broker restart — GitHub issue #64). The
topics-index page (`GET /`) now renders two labeled sections, **"Spark"** (the 15 topics above) and
**"Kafka"** (these three topics so far), each independently ordered — the first consumer of the new
per-topic `track:` manifest field (`app/topics/loader.py`, `docs/architecture/kafka-curriculum.md`
D-KC1). 9 more Kafka topics are scoped and backlogged (consumer groups, replication, and further
intermediate/advanced topics) but not yet started — see
[`docs/requirements/kafka-curriculum.md`](docs/requirements/kafka-curriculum.md) and
[`docs/architecture/kafka-curriculum.md`](docs/architecture/kafka-curriculum.md) for the full
12-topic curriculum plan and design, and
[`docs/qa/kafka-architecture-kraft-acceptance.md`](docs/qa/kafka-architecture-kraft-acceptance.md),
[`docs/qa/kafka-topics-partitions-acceptance.md`](docs/qa/kafka-topics-partitions-acceptance.md), and
[`docs/qa/kafka-producers-delivery-acceptance.md`](docs/qa/kafka-producers-delivery-acceptance.md)
for these topics' acceptance evidence.

**Shipped:**
- **v1.0 — Public Deploy** (GitHub milestone #8, closed 2026-07-19) — implemented and
  security-audited: a one-command `./deploy.sh` stands up a containerized app + nginx reverse
  proxy on a VM, gated by HTTP basic auth and Let's Encrypt TLS, with the spawned Spark cluster's
  ports bound to loopback only. Verified locally (unit suite, 317 tests, plus a local acceptance
  pass — `docs/acceptance/public-deploy.md` Part A). **Live-VM acceptance (Part B) is explicitly
  waived, not pending**: this project is only ever run locally, so a real Linux VM + domain will
  never exist to verify against — that checklist is kept in `docs/acceptance/public-deploy.md`
  as reference documentation, not tracked as open work. See
  [`docs/requirements/public-deploy.md`](docs/requirements/public-deploy.md),
  [`docs/architecture/public-deploy.md`](docs/architecture/public-deploy.md), and
  [Deploy (single-user, remote)](#deploy-single-user-remote) below for the full design and operator
  checklist.

- **Kafka infra** (Sprint 10, GitHub issue #50, closed 2026-07-19) — conditional Kafka (KRaft)
  in the compose template, since upgraded to a real, user-configurable **multi-broker** cluster
  (1–5 brokers, default 3, RF=3 / `min.insync.replicas=2`) as `v1.2 — Multi-Broker Kafka Cluster
  & Monitor`'s first sub-story (issue #56, closed 2026-07-20). A "Kafka" section now lives in the
  same cluster-config drawer used for Spark — check the box, pick a broker count, spawn/tear down
  together in one action, on any topic page, not just a streaming one. See
  [`docs/architecture/kafka-streaming-infra.md`](docs/architecture/kafka-streaming-infra.md) and
  [`docs/architecture/multi-broker-kafka-cluster.md`](docs/architecture/multi-broker-kafka-cluster.md).
- **Sprint 11 — UDF vs pandas UDF Serialization Cost** (GitHub milestone #14, closed 2026-07-20) —
  a new curriculum topic (`content/udf-pandas-udf/`) comparing a row-at-a-time `udf()` against a
  vectorized `pandas_udf()`, with a measured, never-hardcoded timing comparison and a live-verified
  plan-node distinction (`BatchEvalPython` vs `ArrowEvalPython`). Acceptance-validated live against
  a real cluster, all 5 acceptance criteria PASS (`docs/qa/udf-pandas-udf-acceptance.md`).

**Currently in flight:**
- **v1.1 — Live Market Data Streaming** (GitHub milestone #13) — real Coinbase/Finnhub-sourced
  prices flowing through Kafka into a real Spark Structured Streaming job, plus a live price
  dashboard. Requirements and architecture are done; 4 sub-story issues (#52–#55) are filed;
  development hasn't started yet (blocked on a Finnhub API key for the stock-data half — Coinbase's
  crypto side needs no key). See
  [`docs/requirements/live-market-data-streaming.md`](docs/requirements/live-market-data-streaming.md).
- **v1.2 — Multi-Broker Kafka Cluster & Monitor** (GitHub milestone #15) — sub-story (a), the
  multi-broker topology, sub-story (b), the CLI-shellout observability data layer, and sub-story
  (c), a baked Prometheus JMX exporter agent in the broker image (loopback-only, no new host port)
  feeding real per-broker heap%/produce-fetch p99/idle% into the same snapshot, are done and signed
  off (issue #58, acceptance-validated live against a real cluster, all 5 acceptance criteria
  PASS — `docs/qa/jmx-exporter-acceptance.md`). Sub-story (d), a Kafka Cluster Monitor panel (a 4th
  tab — Overview / Job Detail / Node Detail / **Kafka** — in the existing Cluster Monitor slide-in
  panel, showing a broker health strip, diagnostics/incident cards, a per-broker card grid,
  ISR-shrink events, and topics/consumer-groups tables with per-partition lag drill-down) is also
  done and signed off (issue #59, live-verified including a real broker-kill scenario —
  `docs/qa/kafka-cluster-monitor-panel-ui-acceptance.md`). A broker-kill fault-tolerance demo
  (issue #60) remains. See
  [`docs/requirements/multi-broker-kafka-cluster.md`](docs/requirements/multi-broker-kafka-cluster.md).

**Not yet started:** the Structured Streaming curriculum topic itself (content + notebook, v1.1's
remaining sub-stories), the broker-kill fault-tolerance demo (v1.2's last remaining sub-story), and
the remaining 9 topics of the new Kafka curriculum track (`docs/requirements/kafka-curriculum.md`).

For the full story-by-story history, acceptance evidence, and prioritized backlog, see
[`docs/backlog.md`](docs/backlog.md). For the phased roadmap and architecture in detail, see
[`PLAN.md`](PLAN.md).

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
security model, or [Deploy (LAN-only, home server)](#deploy-lan-only-home-server) for a
no-domain/no-TLS variant meant for a homelab box on your own network.

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

## Deploy (LAN-only, home server)

A third way to run Spark Playbook: containerized like the remote-deploy path above, but reachable
only on your local network at a bare IP (e.g. `http://192.168.0.131:8000`) — no domain, no TLS, no
login. This is for a homelab box that never needs to be reachable from the internet, so the domain +
Let's Encrypt machinery `deploy.sh` needs doesn't apply (Let's Encrypt's HTTP-01 challenge requires a
real, publicly-resolvable domain — it cannot issue a certificate for a private IP at all). There's no
packaged one-command script for this mode yet, unlike `./deploy.sh` — the steps below are the full
manual procedure.

**Trust model:** identical to local dev's single-user design, just widened from `localhost` to your
whole LAN — no password, no TLS. Anyone on your network can reach the app and spawn/tear down Spark
clusters using the homelab's resources. Don't use this mode on a LAN you don't trust.

1. **Get the code onto the server.** If the server has no GitHub deploy key for a private repo, ship
   the current commit straight from a clone that already has push/pull access — no GitHub
   credentials needed on the server:
   ```bash
   git archive --format=tar HEAD | ssh user@server 'mkdir -p ~/Spark-Playbook && tar -x -C ~/Spark-Playbook'
   ```
   If that source checkout is on Windows with `core.autocrlf` on, normalize line endings on the
   server afterward — `git archive` can carry CRLF through, which breaks `bash` scripts (`set -euo
   pipefail` fails with `invalid option name` if the line has a trailing `\r`):
   ```bash
   find ~/Spark-Playbook -type f \( -name "*.sh" -o -name "*.py" -o -name "*.yml" -o -name "*.j2" \
     -o -name "*.template" -o -name "*.conf" -o -name "Dockerfile*" \) -print0 \
     | xargs -0 sed -i 's/\r$//'
   ```

2. **Build the images** (same as any other path):
   ```bash
   bash compose/build.sh                             # Spark cluster image
   docker build -t sparkpb-app -f Dockerfile.app .    # app image
   ```

3. **Run the app container directly** — no nginx/certbot, which is the whole difference from
   `deploy.sh` — bound to the host network so it's reachable on the LAN, with `PUBLIC_ORIGIN` set to
   the server's LAN URL (needed so the embedded JupyterLab's CSP `frame-ancestors` allows this
   origin) and `JUPYTER_URL`/`MASTER_UI_URL` pointed at the LAN IP too (their `localhost` defaults
   resolve to the *browser's* machine, not the server, once you're off `localhost`):
   ```bash
   docker run -d --name sparkpb-app \
     --restart unless-stopped \
     --network host \
     -v /var/run/docker.sock:/var/run/docker.sock \
     -v <repo-path-on-server>:<repo-path-on-server> \
     -w <repo-path-on-server> \
     -e PUBLIC_ORIGIN=http://<server-lan-ip>:8000 \
     -e JUPYTER_URL=http://<server-lan-ip>:8888/jupyter \
     -e MASTER_UI_URL=http://<server-lan-ip>:8080 \
     sparkpb-app \
     uvicorn app.main:app --host 0.0.0.0 --port 8000
   ```
   `JUPYTER_URL` needs the `/jupyter` suffix: setting `PUBLIC_ORIGIN` also flips the driver's Jupyter
   to `base_url=/jupyter/` (`driver/jupyter_config.py`), so the bare host:port 404s.

4. **Forward the loopback-only ports onto the LAN interface.** The compose template publishes the
   Spark Master UI (`:8080`), Jupyter (`:8888`), and the driver UI (`:4040-4042`) to `127.0.0.1`
   only — correct for the `deploy.sh` topology (nginx sits on the same host loopback), but nothing is
   listening on the LAN-facing interface in a nginx-less setup like this one. A small nginx sidecar,
   bound only to the server's LAN IP (not `0.0.0.0`, which collides with Docker's own
   `127.0.0.1:PORT` publish on the same port), forwards each port through. Two easy-to-miss details:
   - **`proxy_set_header Host $http_host;`, not `$host`.** nginx's `$host` strips the port from the
     forwarded `Host` header; Jupyter compares it against the browser's `Origin` header (which
     includes the port) and blocks the mismatch as cross-origin — surfaces as a confusing "File Load
     Error: Not Found" when opening a notebook, not an auth error.
   - **Split the `/spark-master/static/` path out.** Setting `PUBLIC_ORIGIN` also turns on Spark's
     `spark.ui.reverseProxyUrl=/spark-master` (`compose/templates/spark-defaults.conf.j2`), baking a
     `/spark-master` prefix into every link Spark's UI generates. Its static-asset handler is *not*
     prefix-aware and only serves real CSS/JS at the unprefixed path (prefixed requests fall back to
     an HTML page — an unstyled dashboard) — but the per-app UI proxy *is* prefix-aware and 403s if
     the prefix is stripped. Strip it only for `/spark-master/static/`; pass everything else through
     unchanged.
   ```nginx
   server {
       listen <server-lan-ip>:8080;
       location /spark-master/static/ {
           proxy_pass http://127.0.0.1:8080/static/;
           proxy_set_header Host $http_host;
       }
       location / {
           proxy_pass http://127.0.0.1:8080;
           proxy_set_header Host $http_host;
       }
   }
   server {
       listen <server-lan-ip>:8888;
       location / {
           proxy_pass http://127.0.0.1:8888;
           proxy_http_version 1.1;
           proxy_set_header Upgrade $http_upgrade;
           proxy_set_header Connection "upgrade";
           proxy_set_header Host $http_host;
           proxy_read_timeout 86400;
       }
   }
   server {
       listen <server-lan-ip>:4040;
       location / { proxy_pass http://127.0.0.1:4040; proxy_set_header Host $http_host; }
   }
   # repeat for :4041 and :4042 (driver UI's fallback ports, PLAN.md/app/config.py DRIVER_APP_UI_PORTS)
   ```
   ```bash
   docker run -d --name sparkpb-lan-proxy --restart unless-stopped --network host \
     -v ./lan-proxy.conf:/etc/nginx/conf.d/default.conf:ro nginx:1.27-alpine
   ```

5. **Open the firewall** for the four ports above (`ufw allow 8000/tcp`, `8080/tcp`, `8888/tcp`,
   `4040:4042/tcp`), plus whatever's already needed for SSH.

6. **Redeploying after a code change:** re-run step 1's transfer, rebuild `sparkpb-app` (step 2),
   then `docker rm -f sparkpb-app` and re-run step 3's `docker run`. Restarting the app container
   resets its in-memory "is a cluster spawned?" state even though the underlying cluster keeps
   running (same benign redeploy desync as the remote-deploy path, `deploy/README.md` §7) — tear down
   and respawn from the UI afterward to resync.

### Automated (CI/CD)

The manual steps above are also packaged into a non-interactive script (`deploy-lan.sh` at the repo
root, plus the `deploy-lan/` directory: a `docker-compose.yml` for the app + LAN-forwarding nginx
sidecar, and a templated `nginx/default.conf.template`) and wired up to run automatically on every
push to `main` — a direct commit or a merged PR — via
[`.github/workflows/deploy-lan.yml`](.github/workflows/deploy-lan.yml), on a **self-hosted GitHub
Actions runner physically on the homelab box itself**. Because the runner already lives on the
target machine, `actions/checkout` is the entire "get the code onto the server" step — no
SSH/`git archive` workaround needed for this path.

Every automated run tears down and respawns the Spark cluster itself too (not just the app+proxy
containers) via `compose/cli.py`, for a full clean slate on each deploy — a deliberate trade-off:
**any push to `main`, including a docs-only change, destroys whatever cluster work is in progress on
the LAN.** `compose/cli.py`'s `render` subcommand needed a small `--public-origin` flag added for
this (default `""`, unchanged behavior for existing manual/dev usage) — without it, the standalone
CLI has no way to thread `PUBLIC_ORIGIN` through to the spawned cluster the way the app's own
`renderer.py` already does, which would silently reintroduce the Jupyter CSP/`base_url` bugs
described above on every automated deploy.

One-time prerequisites, done once before the first push-triggered run (none of these are
automatable from inside the workflow itself):

1. **A self-hosted runner registered to this repo**, with Docker access (the runner's OS user needs
   `docker` group membership) and `python3` + `jinja2` importable (`compose/cli.py`'s only
   dependency). Settings → Actions → Runners → New self-hosted runner.
2. **The `HOMELAB_LAN_IP` repository variable** (Settings → Secrets and variables → Actions →
   Variables), e.g. `192.168.0.131` — passed into the workflow as `${{ vars.HOMELAB_LAN_IP }}`,
   deliberately not hardcoded in any committed file or auto-detected at runtime (unreliable on a
   multi-NIC box).
3. **The firewall rules from step 5 above**, opened once on the box — they don't change per deploy,
   so the workflow doesn't (and structurally can't, without passwordless `sudo`) manage them.

Known, accepted gaps: there's no rollback if the post-deploy health check fails (the previous app
container is already gone by then — `docker compose ... --force-recreate` has no undo); and if the
runner or the homelab box itself is offline, a push just queues forever with no failure signal
(GitHub doesn't fail a job that's never picked up).

## Project structure

```
compose/      Phase 0 cluster harness — Dockerfile, Jinja2 compose templates, standalone CLI
app/          Phase 1+ FastAPI web app — cluster lifecycle, topic pages, (later) annotation engine
content/      Curriculum topics — one folder per topic (concept.md, notebook.ipynb, manifest.yaml)
deploy/       Public-deploy base stack — nginx config, compose file, gitignored secrets/certs
deploy-lan/   LAN-only deploy base stack — app + LAN-forwarding nginx sidecar, no TLS/secrets
Dockerfile.app  Containerizes the app itself for the public-deploy base stack (deploy/)
deploy.sh     One-command public deploy — see "Deploy (single-user, remote)" above
deploy-lan.sh Non-interactive LAN-only deploy — see "Deploy (LAN-only, home server)" above
docs/         Requirements, backlog, acceptance reports, architecture notes
PLAN.md       Full technical design: architecture, key decisions, phased roadmap, named risks
```

## Further reading

- [`PLAN.md`](PLAN.md) — full architecture and design (component diagram, cluster lifecycle,
  annotation engine design, phased roadmap, named risks and mitigations).
- [`CLAUDE.md`](CLAUDE.md) — how this repo's own development process works (the SDLC pipeline,
  sprint cadence, definition of done). The mechanics of the underlying agent team are documented
  separately in [`docs/AGENT_TEAM.md`](docs/AGENT_TEAM.md).
