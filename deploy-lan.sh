#!/usr/bin/env bash
# Spark Playbook — non-interactive LAN-only deploy (README "Deploy (LAN-only,
# home server)"). Unlike ./deploy.sh (domain+TLS+basic-auth, interactive),
# this reads all input from env vars and never prompts -- built to run
# unattended from .github/workflows/deploy-lan.yml on a self-hosted runner
# physically on the homelab box, but is a normal standalone script too:
#   LAN_IP=192.168.0.131 bash deploy-lan.sh
#
# Every run tears down and respawns the Spark cluster itself ('sparkpb'
# project, via compose/cli.py), not just the app+nginx containers -- a full
# clean slate on every deploy (deliberate; any in-progress cluster work on
# the LAN is destroyed by the next push to main).
#
# Ordering: cluster respawn happens BEFORE the app+nginx recreate below, on
# purpose. A cluster failure aborts here (set -euo pipefail, no swallowed
# exit code), leaving the previous app+nginx deploy untouched and still
# serving -- recreating app+nginx first would risk a fresh app pointed at a
# broken/absent cluster instead.
#
# Required env:
#   LAN_IP    the homelab box's LAN IP (e.g. 192.168.0.131). Passed by the
#             workflow from the vars.HOMELAB_LAN_IP repo variable -- not
#             auto-detected (unreliable on a multi-NIC box) and not hardcoded.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

# --- Preflight: fail loud, not deep in a confusing docker/apt error --------
if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: 'docker' not found. Install Docker Engine first." >&2
    exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
    echo "ERROR: 'docker compose' (v2 plugin) not found." >&2
    exit 1
fi
if ! docker info >/dev/null 2>&1; then
    echo "ERROR: cannot talk to the Docker daemon (permission denied, or it isn't" >&2
    echo "running). Run as a user in the 'docker' group, or as root." >&2
    exit 1
fi
if ! python3 -c "import jinja2" >/dev/null 2>&1; then
    echo "ERROR: python3 + the 'jinja2' package are required (compose/cli.py)." >&2
    echo "Install once on this box (e.g. 'pip install jinja2') -- this script" >&2
    echo "deliberately does not attempt an unattended install itself." >&2
    exit 1
fi
if [ -z "${LAN_IP:-}" ]; then
    echo "ERROR: LAN_IP env var is required (e.g. LAN_IP=192.168.0.131 bash deploy-lan.sh)." >&2
    exit 1
fi

# DooD path alignment (docs/architecture/public-deploy.md D1, same as
# deploy.sh): the app container mounts the repo at this SAME absolute path,
# so the spawned cluster's relative bind mounts (../../:/workspace) land on
# the real repo instead of an empty directory.
REPO_HOST_PATH="$(pwd)"
export REPO_HOST_PATH LAN_IP

echo "==> [1/2] Tearing down and respawning the Spark cluster ('sparkpb' project)"
bash compose/build.sh
python3 compose/cli.py render --public-origin "http://${LAN_IP}:8000"
python3 compose/cli.py up
if ! python3 compose/cli.py wait-for-ready; then
    echo "ERROR: Spark cluster did not become ready -- aborting BEFORE touching" >&2
    echo "the app/nginx containers. The broken 'sparkpb' stack is left running" >&2
    echo "for inspection (docker ps / docker logs spark-master), per" >&2
    echo "compose/cli.py's own message above. Any previous app/nginx deploy is" >&2
    echo "untouched and still serving." >&2
    exit 1
fi

echo "==> [2/2] Recreating the app + LAN-forwarding nginx sidecar ('sparkpb-lan' project)"
# --force-recreate: Dockerfile.app never bakes in app/ source (only
# requirements.txt is COPY'd -- code is bind-mounted at runtime), so a
# code-only change does not change the image digest and `up -d --build`
# alone would leave the existing container (and its already-running, stale
# uvicorn process) untouched. --force-recreate guarantees uvicorn re-execs
# against the fresh bind-mounted code every run.
docker compose -p sparkpb-lan -f deploy-lan/docker-compose.yml up -d --build --force-recreate

echo
echo "Deployed. http://${LAN_IP}:8000/ (open LAN access, no login)."
echo "Run 'LAN_IP=${LAN_IP} bash deploy-lan/health-check.sh' to verify."
