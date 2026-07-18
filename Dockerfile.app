# Spark Playbook — FastAPI app image (docs/architecture/public-deploy.md D1).
#
# Containerizes the app itself for the public-deploy `deploy/` base stack
# (project `sparkpb-deploy`). Distinct from `compose/Dockerfile.spark`, which
# builds the *cluster* image (master/worker/driver) the app spawns via
# Docker-out-of-Docker against the mounted host socket -- this image never
# runs Spark, it only drives `docker compose` against it, so it needs the
# Docker CLI + Compose v2 plugin on PATH in addition to the app's own Python
# dependencies.
FROM python:3.11-slim

# Docker CLI + Compose v2 plugin (D1: "the app shells out to `docker compose`,
# see compose_ops.py") -- verified live (devops review) that Debian's own apt
# archive does NOT carry a Compose v2 package on either bookworm or trixie
# (only the deprecated Python `docker-compose` v1); `docker-compose-v2` 404s
# on `apt-get install` and breaks the build outright. Docker's official apt
# repo is the standard source for `docker-ce-cli` + `docker-compose-plugin`
# (no dockerd is installed or run in this image; it only ever talks to the
# host daemon over the bind-mounted socket) -- and as a bonus keeps the
# client close to current Docker releases rather than Debian's frozen distro
# version, reducing client/server API-version skew against the host daemon.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && . /etc/os-release \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $VERSION_CODENAME stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update && apt-get install -y --no-install-recommends \
        docker-ce-cli \
        docker-compose-plugin \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY app/requirements.txt ./app/requirements.txt
RUN pip install --no-cache-dir -r app/requirements.txt

# The repo is bind-mounted at ${REPO_HOST_PATH}:${REPO_HOST_PATH} at runtime
# (D1's DooD path-alignment requirement), not baked into the image here --
# see deploy/docker-compose.yml. WORKDIR above is only where `pip install`
# runs during the build; `working_dir` at runtime is overridden by the
# compose service to the mounted repo path.

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8000"]
