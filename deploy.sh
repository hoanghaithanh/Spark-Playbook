#!/usr/bin/env bash
# Spark Playbook — one-command public deploy (docs/architecture/public-deploy.md D1, OQ2/OQ3/OQ7).
#
# Usage:
#   ./deploy.sh               # first run: provisions everything; safe to re-run (idempotent)
#   ./deploy.sh --reset-auth  # rotate the basic-auth credential, then deploy as usual
#
# Run from the repo root, on a VM with Docker + Docker Compose installed, a
# domain A-record already pointed at this VM's public IP, and inbound
# firewall/security-group restricted to 22/80/443 (US-PD5 -- see
# deploy/README.md for concrete VM sizing / package / firewall steps).
#
# Redeploy path (OQ7): `git pull && ./deploy.sh` rebuilds and recreates only
# the sparkpb-deploy project's app/nginx/certbot containers -- it never
# touches a running spawned `sparkpb` cluster (separate compose project).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

# --- Preflight: fail loud, not deep in a confusing docker/apt error --------
if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: 'docker' not found. Install Docker Engine first (see deploy/README.md)." >&2
    exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
    echo "ERROR: 'docker compose' (v2 plugin) not found. Install the compose plugin" >&2
    echo "(see deploy/README.md)." >&2
    exit 1
fi
if ! docker info >/dev/null 2>&1; then
    echo "ERROR: cannot talk to the Docker daemon (permission denied, or it isn't" >&2
    echo "running). Run as a user in the 'docker' group, or as root." >&2
    exit 1
fi

# DooD path alignment (D1, the load-bearing detail): the app container mounts
# the repo at this SAME absolute path, so the spawned cluster's relative bind
# mounts (../../:/workspace, resolved by the host daemon) land on the real
# repo instead of an empty directory.
REPO_HOST_PATH="$(pwd)"

RESET_AUTH=0
for arg in "$@"; do
    case "$arg" in
        --reset-auth) RESET_AUTH=1 ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

# --- Domain / contact email (OQ3 needs both for certbot) ------------------
# Persisted (gitignored) so re-running ./deploy.sh never re-prompts.
SECRETS_DIR="./deploy/secrets"
DEPLOY_ENV_FILE="$SECRETS_DIR/deploy.env"
mkdir -p "$SECRETS_DIR" ./deploy/certs/letsencrypt ./deploy/certs/webroot

if [ -f "$DEPLOY_ENV_FILE" ]; then
    # shellcheck disable=SC1090
    source "$DEPLOY_ENV_FILE"
fi
if [ -z "${DOMAIN:-}" ]; then
    read -rp "Domain (must already A-record to this VM's public IP): " DOMAIN
fi
if [ -z "${EMAIL:-}" ]; then
    read -rp "Contact email for Let's Encrypt expiry notices: " EMAIL
fi
{
    echo "DOMAIN=$DOMAIN"
    echo "EMAIL=$EMAIL"
} > "$DEPLOY_ENV_FILE"
chmod 600 "$DEPLOY_ENV_FILE"

PUBLIC_ORIGIN="https://$DOMAIN"

# --- Spark cluster image (idempotent: only build if absent) ---------------
if ! docker image inspect sparkpb/spark:4.0.3 >/dev/null 2>&1; then
    echo "Building sparkpb/spark:4.0.3 (not found locally) ..."
    ./compose/build.sh
else
    echo "sparkpb/spark:4.0.3 already present, skipping build."
fi

# --- Basic-auth credential (OQ2) -------------------------------------------
HTPASSWD_FILE="$SECRETS_DIR/htpasswd"
if [ "$RESET_AUTH" = "1" ] || [ ! -f "$HTPASSWD_FILE" ]; then
    read -rp  "Basic-auth username: " BA_USER
    while true; do
        read -rsp "Basic-auth password (min 16 chars): " BA_PASS; echo
        if [ -z "$BA_PASS" ]; then
            echo "Password cannot be empty. Try again." >&2
            continue
        fi
        if [ "${#BA_PASS}" -lt 16 ]; then
            echo "Password must be at least 16 characters. Try again." >&2
            unset BA_PASS
            continue
        fi
        read -rsp "Confirm password: " BA_PASS_CONFIRM; echo
        if [ "$BA_PASS" != "$BA_PASS_CONFIRM" ]; then
            echo "Passwords did not match. Try again." >&2
            unset BA_PASS BA_PASS_CONFIRM
            continue
        fi
        unset BA_PASS_CONFIRM
        break
    done
    # bcrypt (-B) at cost 12 (-C 12), not the htpasswd default (cost 5) or
    # apr1/MD5; piped via stdin so the password never hits the process argv
    # (`ps`) or a `.env` file, and `read -s` keeps it off the
    # terminal/shell history.
    docker run --rm -i httpd:2.4-alpine htpasswd -niB -C 12 "$BA_USER" <<<"$BA_PASS" > "$HTPASSWD_FILE"
    chmod 600 "$HTPASSWD_FILE"
    unset BA_PASS
else
    echo "$HTPASSWD_FILE already exists, skipping (use --reset-auth to rotate)."
fi

# --- TLS bootstrap (OQ3) ----------------------------------------------------
# nginx's server block references the cert path unconditionally, so it
# refuses to start with no cert on disk at all -- seed a throwaway
# self-signed cert first (not certbot-tracked, so the real `certonly` call
# below cleanly overwrites it) purely so nginx can come up and serve the
# ACME HTTP-01 challenge for the real cert.
CERT_LIVE_DIR="./deploy/certs/letsencrypt/live/$DOMAIN"
NEED_REAL_CERT=0
if [ ! -f "$CERT_LIVE_DIR/fullchain.pem" ]; then
    echo "No certificate on disk yet -- seeding a temporary self-signed one so nginx can start ..."
    mkdir -p "$CERT_LIVE_DIR"
    docker run --rm -v "$REPO_HOST_PATH/deploy/certs/letsencrypt:/etc/letsencrypt" \
        --entrypoint openssl certbot/certbot req -x509 -nodes -newkey rsa:2048 -days 1 \
        -keyout "/etc/letsencrypt/live/$DOMAIN/privkey.pem" \
        -out "/etc/letsencrypt/live/$DOMAIN/fullchain.pem" \
        -subj "/CN=$DOMAIN"
    NEED_REAL_CERT=1
fi

# --- Bring up the base stack -------------------------------------------
export REPO_HOST_PATH DOMAIN PUBLIC_ORIGIN
docker compose -p sparkpb-deploy -f deploy/docker-compose.yml up -d --build

# --- Obtain the real certificate now that nginx is serving the challenge --
if [ "$NEED_REAL_CERT" = "1" ]; then
    echo "Obtaining a Let's Encrypt certificate for $DOMAIN ..."
    if ! docker run --rm \
        -v "$REPO_HOST_PATH/deploy/certs/letsencrypt:/etc/letsencrypt" \
        -v "$REPO_HOST_PATH/deploy/certs/webroot:/var/www/certbot" \
        certbot/certbot certonly --webroot -w /var/www/certbot \
        -d "$DOMAIN" --cert-name "$DOMAIN" --email "$EMAIL" --agree-tos -n; then
        echo "ERROR: could not obtain a TLS certificate for '$DOMAIN'." >&2
        echo "Check that its DNS A-record points at this VM's public IP and that port 80" >&2
        echo "is reachable from the internet (see README's deploy prerequisites)." >&2
        exit 1
    fi
    # --cert-name pins the lineage to live/$DOMAIN, but assert it actually
    # landed there and is Let's Encrypt-issued -- a non-interactive certonly
    # that somehow still diverged (e.g. a stale lineage) would otherwise
    # leave nginx silently serving the 1-day self-signed bootstrap cert.
    CERT_ISSUER="$(docker run --rm -v "$REPO_HOST_PATH/deploy/certs/letsencrypt:/etc/letsencrypt" \
        --entrypoint openssl certbot/certbot x509 -issuer -noout \
        -in "/etc/letsencrypt/live/$DOMAIN/fullchain.pem" 2>/dev/null || true)"
    if ! echo "$CERT_ISSUER" | grep -qi "let's encrypt"; then
        echo "ERROR: '$CERT_LIVE_DIR/fullchain.pem' is not a Let's Encrypt certificate" >&2
        echo "after certbot reported success -- refusing to reload nginx with the" >&2
        echo "self-signed bootstrap cert still live. Check for a diverged lineage under" >&2
        echo "./deploy/certs/letsencrypt/live/." >&2
        exit 1
    fi
    docker compose -p sparkpb-deploy -f deploy/docker-compose.yml exec nginx nginx -s reload
fi

echo
echo "Deployed. https://$DOMAIN/ (basic auth required)."
echo "Status:   docker compose -p sparkpb-deploy -f deploy/docker-compose.yml ps"
