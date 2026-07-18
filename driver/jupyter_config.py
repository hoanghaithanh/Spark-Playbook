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
localhost-only tool), Jupyter runs tokenless, matching how
`docker-compose.yml.j2` already launches Jupyter (`--ServerApp.token=''`).
XSRF checking is relaxed only in dev (cross-origin embed, see below); a
public deploy is same-origin and keeps XSRF enabled (security audit MED-2).

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

**Public deploy (docs/architecture/public-deploy.md D4).** A deployed
instance is reached at a public HTTPS origin (e.g.
`https://spark.example.com`), a third origin distinct from both loopback
spellings above -- without it in the allowlist too, the iframe renders blank
on the deployed site. Read from the `SPARKPB_PUBLIC_ORIGIN` env var (set on
the spawned driver container by `compose/templates/docker-compose.yml.j2`,
sourced from `app/config.py::PUBLIC_ORIGIN`) rather than imported, same
can't-import-config rationale as the rest of this file. Empty in dev (no env
var set) reproduces the exact localhost-only allowlist above.
"""

import os

# The FastAPI app's origin — must match app/config.py::APP_ORIGIN.
APP_ORIGIN = "http://localhost:8000"
# Same app, same port, the other loopback spelling browsers treat as a
# distinct origin for CSP purposes -- see module docstring.
APP_ORIGIN_127 = "http://127.0.0.1:8000"
# Public deploy's HTTPS origin, if any -- see module docstring.
PUBLIC_ORIGIN = os.environ.get("SPARKPB_PUBLIC_ORIGIN", "").strip()

c = get_config()  # noqa: F821 - `get_config()` is injected by the Jupyter config loader

# Allow this app's origin -- both loopback spellings, plus the deployed
# public origin when set -- (and Jupyter's own origin, via 'self') to frame
# this server. Setting `frame-ancestors` in the CSP is what actually
# controls framing in modern browsers; Jupyter Server emits no separate
# `X-Frame-Options` header once a CSP with `frame-ancestors` is configured
# this way (PLAN.md §6/R3) -- there is no separate trait to toggle for that,
# the CSP setting below *is* the mechanism.
_frame_ancestors = f"'self' {APP_ORIGIN} {APP_ORIGIN_127}"
if PUBLIC_ORIGIN:
    _frame_ancestors += f" {PUBLIC_ORIGIN}"

c.ServerApp.tornado_settings = {
    "headers": {
        "Content-Security-Policy": f"frame-ancestors {_frame_ancestors}",
    },
}

c.ServerApp.allow_origin = PUBLIC_ORIGIN or APP_ORIGIN
# XSRF was only disabled for dev, where the embedding app (localhost:8000)
# and Jupyter (localhost:8888) are cross-origin and XSRF cookies can't be
# read across origins by the embedding page. A deployed instance is
# same-origin (the app and Jupyter are both reached via https://<domain>/,
# proxied under one origin -- see PUBLIC_ORIGIN above), so XSRF checking can
# and should stay enabled there.
c.ServerApp.disable_check_xsrf = not PUBLIC_ORIGIN
