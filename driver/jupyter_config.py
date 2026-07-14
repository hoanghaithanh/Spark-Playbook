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

**CSP allowlist covers both `localhost` and `127.0.0.1` (test-engineer
re-validation of issue #7).** Browsers treat `http://localhost:8000` and
`http://127.0.0.1:8000` as different origins for `frame-ancestors` purposes
even though they resolve to the same host/port, so a learner who happens to
open the app via `127.0.0.1:8000` instead of `localhost:8000` would still hit
a blank iframe if only one were allowed. `app/config.APP_ORIGIN` stays
`localhost:8000` (that's what the app itself binds/is documented as, per
PLAN.md §1) -- only the CSP allowlist here is widened defensively to cover
both, since it costs nothing on a single-user localhost tool with no auth to
protect.
"""

# The FastAPI app's origin — must match app/config.py::APP_ORIGIN.
APP_ORIGIN = "http://localhost:8000"
# Same app, same port, the other loopback spelling browsers treat as a
# distinct origin for CSP purposes -- see module docstring.
APP_ORIGIN_127 = "http://127.0.0.1:8000"

c = get_config()  # noqa: F821 - `get_config()` is injected by the Jupyter config loader

# Allow this app's origin -- both loopback spellings -- (and Jupyter's own
# origin, via 'self') to frame this server. Setting `frame-ancestors` in the
# CSP is what actually controls framing in modern browsers; Jupyter Server
# emits no separate `X-Frame-Options` header once a CSP with
# `frame-ancestors` is configured this way (PLAN.md §6/R3) -- there is no
# separate trait to toggle for that, the CSP setting below *is* the
# mechanism.
c.ServerApp.tornado_settings = {
    "headers": {
        "Content-Security-Policy": f"frame-ancestors 'self' {APP_ORIGIN} {APP_ORIGIN_127}",
    },
}

c.ServerApp.allow_origin = APP_ORIGIN
c.ServerApp.disable_check_xsrf = True
