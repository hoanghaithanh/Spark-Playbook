# Spark Playbook — public deploy: operator prerequisites

Design: `docs/architecture/public-deploy.md`. This file is the concrete, actionable
checklist for standing the stack up on a real VM; the ADR is the source of truth for
*why* each piece exists.

## 1. VM shape (ADR OQ4)

- **8 vCPU / 48 GB RAM / 80 GB SSD**, Linux (Debian/Ubuntu assumed — package names below
  are apt-based).
- The 48 GB figure covers the app's own `RESOURCE_CEILING_GB = 32` worst-case spawn plus
  ~6 GB for host OS / Docker daemon / base stack / JVM overhead. **Invariant:** VM RAM
  must stay ≥ `RESOURCE_CEILING_GB` (`app/config.py`) + ~6 GB — if you resize the VM down,
  lower the ceiling to match, or a max-size cluster spawn can OOM-kill containers.

## 2. OS packages

- **Docker Engine** (`docker` CLI) + **Compose v2 plugin** (`docker compose`, not the
  standalone `docker-compose` v1 binary). Debian's own apt archive does not carry a
  Compose v2 package (verified during this review) — install from Docker's official apt
  repo: https://docs.docker.com/engine/install/debian/ (or your distro's equivalent).
- The user running `./deploy.sh` needs Docker daemon access (member of the `docker`
  group, or root) — `deploy.sh` checks this up front and fails loud if not.
- No other packages required on the host; nginx, certbot, and the app all run
  containerized.

## 3. DNS

- An **A record** for your chosen domain must already point at this VM's public IP
  **before** running `./deploy.sh` — the TLS bootstrap step (Let's Encrypt HTTP-01
  challenge) fails if it doesn't resolve, or resolves to the wrong host. `deploy.sh`
  fails loud with this hint if cert issuance fails.

## 4. Firewall / security group

Allow **inbound 22 (SSH), 80 (HTTP/ACME challenge + redirect), 443 (HTTPS)** only.
Everything else (the app's `:8000`, Jupyter `:8888`, Spark Master UI `:8080`, driver UI
`:4040-4042`) is published loopback-only (`127.0.0.1:...`) by the containers themselves
— the firewall is the primary enforcement, loopback binding is defense-in-depth on top
(ADR D2).

Example (ufw):

```bash
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
```

Cloud security groups: same three ports, source `0.0.0.0/0` (or narrower if you know
your own egress IP).

Nothing else on the host should already be bound to 80/443 — nginx runs with
`network_mode: host` and will fail to start (visible in `docker compose ... logs nginx`)
if another process (a distro-default nginx/apache, another stack) holds those ports.
Stop/disable any pre-installed web server first.

## 5. Run it

```bash
git clone <this-repo> && cd Spark-Playbook
./deploy.sh
```

Prompts for domain, contact email (Let's Encrypt), and a basic-auth username/password on
first run only; re-running is idempotent (see the ADR's OQ7 for exactly what a redeploy
does and doesn't disturb, and `./deploy.sh --reset-auth` to rotate the password).

## 6. Windows (Docker Desktop)

This checklist assumes a Linux VM. On Windows, run `deploy.sh` from **WSL2** with the repo cloned
*inside* the WSL2 filesystem (not `/mnt/c/...`/`/mnt/d/...`) — Git Bash's MSYS path rewriting
breaks the DooD path alignment (ADR D1) for the sibling containers the app spawns. You also need
Docker Desktop's "Enable host networking" turned on (Settings → Features in development, restart
Docker Desktop; 4.34+) for `network_mode: host` to publish 80/443/8000. See the README's public/
remote deploy section for the full walkthrough and the home-network port-forwarding caveat.

## 7. Known operational notes

- **App container networking:** the app service runs `network_mode: host` (not the
  `host.docker.internal` + `extra_hosts` scheme the ADR text originally named) — this
  was corrected during devops review because the spawned cluster's ports are published
  `127.0.0.1:PORT:PORT` (loopback-scoped), which is unreachable from another container
  via the `host-gateway` bridge IP on plain Linux Docker (only via the actual host
  loopback interface). `network_mode: host` gives the app container that same loopback,
  matching the pattern nginx already uses for the identical reason. Flagged for an ADR
  addendum; functionally this is what makes D1/D3 actually work on a Linux VM.
- **Redeploy desync (ADR R-PD6):** restarting the app container resets its in-memory
  cluster state to "idle" even if a `sparkpb` cluster is still running underneath — the
  UI shows "no cluster" until the next spawn, which self-heals it (`compose_ops.down()`
  runs first). Benign; tear the cluster down from the UI before redeploying if you want
  a clean slate.
