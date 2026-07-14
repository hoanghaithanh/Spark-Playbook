"""Spark Playbook — Jupyter server config for the `spark-driver` container.

PLAN.md §4 (`driver/jupyter_config.py`) and §6/R3: JupyterLab's default
`X-Frame-Options: SAMEORIGIN` / CSP `frame-ancestors 'self'` blocks embedding
it in an iframe on a *different* origin. The FastAPI app (`localhost:8000`,
per PLAN.md §1's architecture diagram and `app/config.APP_ORIGIN`) and
JupyterLab (`localhost:8888`) are different origins, so without this file the
embedded-JupyterLab iframe (US-1.3) renders blank and the browser console
shows a CSP `frame-ancestors` refusal.

This is a plain, standalone Python file (loaded via `jupyter lab
--config=/workspace/driver/jupyter_config.py`, see
`compose/templates/docker-compose.yml.j2`), not part of the `app` package —
it runs inside the `spark-driver` container's own Python environment, which
has no access to `app/config.py`. The app's origin below is therefore
hardcoded to match `app/config.APP_ORIGIN` (`http://localhost:8000`) rather
than imported; if that port ever changes, this constant must be updated to
match (see `app/config.py::APP_PORT` for the canonical value).

Per this project's locked non-goals (no auth/security hardening — single-user,
localhost-only tool), the settings below intentionally relax XSRF checking and
run tokenless, matching how `docker-compose.yml.j2` already launches Jupyter
(`--ServerApp.token=''`).
"""

# The FastAPI app's origin — must match app/config.py::APP_ORIGIN.
APP_ORIGIN = "http://localhost:8000"

c = get_config()  # noqa: F821 - `get_config()` is injected by the Jupyter config loader

# Allow this app's origin (and Jupyter's own origin, via 'self') to frame this
# server. Setting `frame-ancestors` in the CSP is what actually controls
# framing in modern browsers; Jupyter Server emits no separate
# `X-Frame-Options` header once a CSP with `frame-ancestors` is configured
# this way (PLAN.md §6/R3) -- there is no separate trait to toggle for that,
# the CSP setting below *is* the mechanism.
c.ServerApp.tornado_settings = {
    "headers": {
        "Content-Security-Policy": f"frame-ancestors 'self' {APP_ORIGIN}",
    },
}

c.ServerApp.allow_origin = APP_ORIGIN
c.ServerApp.disable_check_xsrf = True
