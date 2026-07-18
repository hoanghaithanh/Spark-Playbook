#!/usr/bin/env bash
# Spark Playbook — post-deploy health check for the LAN-only stack.
# Standalone and independently re-runnable, not just a tail-end of
# deploy-lan.sh: LAN_IP=192.168.0.131 bash deploy-lan/health-check.sh
#
# Not a full browser-equivalent check (no cookies/CORS preflight nuance),
# but it specifically targets two failure modes that were confirmed live to
# occur SILENTLY otherwise (a 200 that isn't really the right content):
#   - Spark's reverseProxyUrl static-asset split (deploy-lan/nginx/default.conf.template)
#   - nginx's $host vs $http_host breaking Jupyter's origin check
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [ -z "${LAN_IP:-}" ]; then
    echo "ERROR: LAN_IP env var is required (e.g. LAN_IP=192.168.0.131 bash deploy-lan/health-check.sh)." >&2
    exit 1
fi

# `docker compose ... --force-recreate` (deploy-lan.sh) returns once
# containers are STARTING, not once uvicorn/nginx are actually listening --
# confirmed live that curling immediately after can hit a brief
# connection-refused window before the fresh containers bind their ports.
# Retry each curl for up to ~10s rather than failing on that startup race.
curl_retry() {  # curl_retry <curl args...>
    local attempt
    for attempt in $(seq 1 10); do
        if curl -fsSL --max-time 5 "$@"; then
            return 0
        fi
        sleep 1
    done
    return 1
}

fail=0
check() {  # check <description> <url>
    if curl_retry -o /dev/null "$2"; then
        echo "  OK   $1 ($2)"
    else
        echo "  FAIL $1 ($2)" >&2
        fail=1
    fi
}

check "app (topics-index landing page)" "http://${LAN_IP}:8000/"
check "Spark Master UI (bare)"          "http://${LAN_IP}:8080/"
check "Spark Master JSON API"           "http://${LAN_IP}:8080/json/"
check "Spark Master UI (/spark-master/ reverseProxy path)" "http://${LAN_IP}:8080/spark-master/"

# Targets the specific, previously-live-confirmed-broken bug: Spark's
# reverseProxy static-asset handler is NOT prefix-aware, so
# /spark-master/static/<file> falls back to an HTML page instead of real CSS
# unless nginx strips the prefix. Discover the actual asset URL Spark itself
# renders (no hardcoded, Spark-version-specific filename) and confirm it's
# really text/css, not the HTML fallback.
css_path="$(curl_retry "http://${LAN_IP}:8080/spark-master/" \
    | grep -o 'href="/spark-master/static/[^"]*\.css"' | head -1 | cut -d'"' -f2 || true)"
if [ -z "$css_path" ]; then
    echo "  FAIL Spark Master UI static asset: no stylesheet link found" >&2
    fail=1
else
    css_type="$(curl_retry -o /dev/null -w '%{content_type}' "http://${LAN_IP}:8080${css_path}")"
    case "$css_type" in
        text/css*) echo "  OK   Spark Master UI static asset ($css_path -> $css_type)" ;;
        *) echo "  FAIL Spark Master UI static asset ($css_path -> '$css_type', expected text/css -- /spark-master/static/ prefix-strip likely broken" >&2
           fail=1 ;;
    esac
fi

# Targets the other previously-live-confirmed-broken bug: nginx's $host
# strips the port, so Jupyter compares a portless Host against the browser's
# Origin header (which includes the port) and false-blocks it. Send a real
# Origin header with a port against the exact endpoint that silently 404'd.
if curl_retry -H "Origin: http://${LAN_IP}:8888" -o /dev/null \
    "http://${LAN_IP}:8888/jupyter/api/contents/"; then
    echo "  OK   Jupyter /api/contents/ (with a port-bearing Origin header)"
else
    echo "  FAIL Jupyter /api/contents/ (with a port-bearing Origin header) -- \$http_host fix likely broken" >&2
    fail=1
fi

if [ "$fail" = "1" ]; then
    echo "ERROR: one or more health checks failed." >&2
    exit 1
fi
echo "All health checks passed. http://${LAN_IP}:8000/"
