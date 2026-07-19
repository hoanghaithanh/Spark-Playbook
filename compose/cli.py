#!/usr/bin/env python3
"""Spark Playbook — Phase 0 cluster lifecycle CLI (PLAN.md §2, §5 Phase 0).

Manually-operated equivalent of the lifecycle module Phase 1's FastAPI app will
own later (app/lifecycle/*). This script is deliberately self-contained so the
cluster harness is buildable/testable standalone before app/ exists (US-0.5).

Subcommands:
    render          Render docker-compose.yml.j2 + spark-defaults.conf.j2 into
                     compose/rendered/, from CLI flags (or defaults).
    up              down any previous stack (awaited), then `docker compose up -d`.
    down            `docker compose -p sparkpb down --remove-orphans`.
    wait-for-ready  Poll http://localhost:8080/json/ until aliveworkers ==
                     worker_count, or fail with a clear message on timeout.
    status          Print master /json/ once, unformatted-poll.

Typical flow:
    python compose/cli.py render
    python compose/cli.py up
    python compose/cli.py wait-for-ready
    python compose/cli.py down

Requires: Python 3.9+, the `jinja2` package (`pip install jinja2`), and the
`docker` CLI with the Compose v2 plugin (`docker compose ...`) on PATH.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

try:
    from jinja2 import Environment, FileSystemLoader, StrictUndefined
except ImportError:  # pragma: no cover
    print(
        "ERROR: the 'jinja2' package is required. Install it with:\n"
        "    pip install jinja2\n",
        file=sys.stderr,
    )
    sys.exit(1)

COMPOSE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = COMPOSE_DIR / "templates"
RENDERED_DIR = COMPOSE_DIR / "rendered"
COMPOSE_FILE = RENDERED_DIR / "docker-compose.yml"
SPARK_DEFAULTS_FILE = RENDERED_DIR / "spark-defaults.conf"

PROJECT_NAME = "sparkpb"
IMAGE_NAME = "sparkpb/spark:4.0.3"

# Defaults per PLAN.md §2's template-variable table / resource budget.
DEFAULTS = {
    "worker_count": 3,
    "worker_cores": 2,
    "worker_memory_gb": 4,
    "driver_memory_gb": 2,
    "shuffle_partitions": 200,
    "aqe_enabled": False,
}

MASTER_JSON_URL = "http://localhost:8080/json/"


# --------------------------------------------------------------------------- #
# render
# --------------------------------------------------------------------------- #

def cmd_render(args: argparse.Namespace) -> int:
    _validate_ranges(args)

    RENDERED_DIR.mkdir(parents=True, exist_ok=True)
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )

    context = {
        "image_name": IMAGE_NAME,
        "worker_count": args.worker_count,
        "worker_cores": args.worker_cores,
        "worker_memory_gb": args.worker_memory_gb,
        "driver_memory_gb": args.driver_memory_gb,
        "shuffle_partitions": args.shuffle_partitions,
        "aqe_enabled": args.aqe_enabled,
        # Standalone Phase 0 CLI has no deployed-instance concept by default --
        # dev/empty (docs/architecture/public-deploy.md D4's public_origin
        # template var). --public-origin is an explicit opt-in override for
        # non-interactive deploy scripts (deploy-lan.sh) that need this CLI to
        # respawn a cluster with the same base_url/CSP/reverseProxyUrl
        # behavior app/lifecycle/renderer.py already applies for app-driven
        # spawns -- unset, this stays "" and behavior is unchanged.
        "public_origin": args.public_origin,
    }

    compose_tpl = env.get_template("docker-compose.yml.j2")
    COMPOSE_FILE.write_text(compose_tpl.render(**context), encoding="utf-8")

    conf_tpl = env.get_template("spark-defaults.conf.j2")
    SPARK_DEFAULTS_FILE.write_text(conf_tpl.render(**context), encoding="utf-8")

    print(f"Rendered {COMPOSE_FILE}")
    print(f"Rendered {SPARK_DEFAULTS_FILE}")
    print(
        f"Config: workers={args.worker_count} cores={args.worker_cores} "
        f"mem={args.worker_memory_gb}GB driver_mem={args.driver_memory_gb}GB "
        f"shuffle_partitions={args.shuffle_partitions} aqe={args.aqe_enabled}"
    )
    return 0


def _validate_ranges(args: argparse.Namespace) -> None:
    # PLAN.md §2 ranges (US-1.2); enforced here too since Phase 0 has no web UI
    # to guard against typos.
    errors = []
    if not (1 <= args.worker_count <= 5):
        errors.append("worker_count must be 1-5")
    if not (1 <= args.worker_cores <= 4):
        errors.append("worker_cores must be 1-4")
    if not (1 <= args.worker_memory_gb <= 8):
        errors.append("worker_memory_gb must be 1-8")
    if args.shuffle_partitions <= 0:
        errors.append("shuffle_partitions must be a positive integer")

    total_gb = 1 + args.worker_count * args.worker_memory_gb + args.driver_memory_gb
    if total_gb > 48:
        errors.append(
            f"requested config totals ~{total_gb}GB, exceeding the 48GB sanity "
            "ceiling (PLAN.md §2 resource-ceiling check)"
        )

    if errors:
        print("ERROR: invalid configuration:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        sys.exit(1)


# --------------------------------------------------------------------------- #
# up / down
# --------------------------------------------------------------------------- #

def _run_compose(*args: str) -> int:
    cmd = ["docker", "compose", "-p", PROJECT_NAME, "-f", str(COMPOSE_FILE), *args]
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def _norm_path(path) -> str:
    """Mirrors `app/config.py::norm_path` -- kept as a local copy since this
    CLI deliberately does not import `app/` (see module docstring)."""
    return os.path.normcase(os.path.normpath(str(path)))


def _running_owner() -> str | None:
    """Sync mirror of `app/lifecycle/compose_ops.py::running_owner()` (issue
    #38 ownership guard, docs/architecture/worktree-cluster-isolation.md).
    Normalized `project.working_dir` of the worktree owning the currently
    running `sparkpb` Compose project, or None if nothing is running. Never
    raises -- degrades to None on any docker error (fail-open by design)."""
    try:
        ps = subprocess.run(
            ["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project={PROJECT_NAME}"],
            capture_output=True, text=True,
        )
        if ps.returncode != 0:
            return None
        ids = [line.strip() for line in ps.stdout.splitlines() if line.strip()]
        if not ids:
            return None

        inspect = subprocess.run(
            ["docker", "inspect", ids[0], "--format",
             '{{ index .Config.Labels "com.docker.compose.project.working_dir" }}'],
            capture_output=True, text=True,
        )
        if inspect.returncode != 0:
            return None
        label = inspect.stdout.strip()
        if not label:
            return None
        return _norm_path(label)
    except OSError:
        return None


def _refuse_if_foreign_owner() -> bool:
    """Returns True (and prints a clear refusal) if a 'sparkpb' cluster is
    already running, owned by a different worktree. Guard only, not a lock
    -- see docs/architecture/worktree-cluster-isolation.md."""
    owner = _running_owner()
    self_owner = _norm_path(RENDERED_DIR)
    if owner is None or owner == self_owner:
        return False
    print(
        f"ERROR: a '{PROJECT_NAME}' cluster is already running, owned by another "
        f"worktree ({owner}). Refusing -- it would tear down that worktree's live "
        f"cluster. Tear it down there first, or wait.",
        file=sys.stderr,
    )
    return True


def cmd_down(_args: argparse.Namespace) -> int:
    if _refuse_if_foreign_owner():
        return 1

    if not COMPOSE_FILE.exists():
        # Nothing rendered yet in this checkout; still attempt a project-scoped
        # down so stale containers from a previous render are cleaned up
        # (PLAN.md R4 — fixed project name makes this safe/idempotent).
        cmd = ["docker", "compose", "-p", PROJECT_NAME, "down", "--remove-orphans"]
        print(f"$ {' '.join(cmd)} (no rendered compose file found; using project name only)")
        return subprocess.run(cmd).returncode
    return _run_compose("down", "--remove-orphans")


def cmd_up(args: argparse.Namespace) -> int:
    if not COMPOSE_FILE.exists():
        print(
            "ERROR: no rendered compose file found. Run `python compose/cli.py render` first.",
            file=sys.stderr,
        )
        return 1

    if _refuse_if_foreign_owner():
        return 1

    # PLAN.md §2 step 3 / R4: always tear down any prior stack first, and await
    # it to completion, before starting the new one. Idempotent if nothing is running.
    print("Tearing down any previous 'sparkpb' stack (awaited)...")
    rc = cmd_down(args)
    if rc != 0:
        print(f"WARNING: teardown exited {rc}; continuing to bring up the new stack anyway.")

    print("Starting stack...")
    rc = _run_compose("up", "-d")
    if rc != 0:
        print(f"ERROR: `docker compose up -d` failed (exit {rc}).", file=sys.stderr)
        return rc

    print("Stack started. Run `python compose/cli.py wait-for-ready` to confirm workers registered.")
    return 0


# --------------------------------------------------------------------------- #
# wait-for-ready
# --------------------------------------------------------------------------- #

def _fetch_master_json() -> dict | None:
    try:
        with urllib.request.urlopen(MASTER_JSON_URL, timeout=3) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, ConnectionError, TimeoutError, ValueError):
        return None


def cmd_wait_for_ready(args: argparse.Namespace) -> int:
    expected = args.worker_count
    timeout_s = args.timeout
    interval_s = 2
    deadline = time.monotonic() + timeout_s

    print(
        f"Waiting for {expected} worker(s) to register at {MASTER_JSON_URL} "
        f"(timeout {timeout_s}s)..."
    )

    last_seen = None
    while time.monotonic() < deadline:
        data = _fetch_master_json()
        if data is not None:
            alive = data.get("aliveworkers")
            last_seen = alive
            if alive == expected:
                print(f"READY: {alive}/{expected} workers alive.")
                print("Master UI:  http://localhost:8080")
                print("Driver UI:  http://localhost:4040 (once a Spark application is running)")
                print("JupyterLab: http://localhost:8888")
                return 0
            print(f"  ... {alive if alive is not None else 0}/{expected} workers alive, waiting")
        else:
            print("  ... master not reachable yet, waiting")
        time.sleep(interval_s)

    print(
        f"TIMEOUT: after {timeout_s}s, master reports "
        f"{last_seen if last_seen is not None else 'unreachable'}/{expected} workers alive.\n"
        f"The stack is left running for inspection (docker ps / docker logs spark-master).\n"
        f"Run `python compose/cli.py down` before retrying.",
        file=sys.stderr,
    )
    return 1


def cmd_status(_args: argparse.Namespace) -> int:
    data = _fetch_master_json()
    if data is None:
        print("Master not reachable at", MASTER_JSON_URL, file=sys.stderr)
        return 1
    print(json.dumps(data, indent=2))
    return 0


# --------------------------------------------------------------------------- #
# argparse wiring
# --------------------------------------------------------------------------- #

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Spark Playbook Phase 0 cluster lifecycle CLI")
    sub = p.add_subparsers(dest="command", required=True)

    render_p = sub.add_parser("render", help="Render docker-compose.yml + spark-defaults.conf")
    render_p.add_argument("--worker-count", dest="worker_count", type=int, default=DEFAULTS["worker_count"])
    render_p.add_argument("--worker-cores", dest="worker_cores", type=int, default=DEFAULTS["worker_cores"])
    render_p.add_argument("--worker-memory-gb", dest="worker_memory_gb", type=int, default=DEFAULTS["worker_memory_gb"])
    render_p.add_argument("--driver-memory-gb", dest="driver_memory_gb", type=int, default=DEFAULTS["driver_memory_gb"])
    render_p.add_argument("--shuffle-partitions", dest="shuffle_partitions", type=int, default=DEFAULTS["shuffle_partitions"])
    render_p.add_argument("--aqe", dest="aqe_enabled", action="store_true", default=DEFAULTS["aqe_enabled"])
    render_p.add_argument(
        "--public-origin",
        dest="public_origin",
        default="",
        help=(
            "Deployed-instance origin (e.g. http://192.168.0.131:8000), for "
            "non-interactive deploy scripts. Default '' reproduces today's "
            "dev-only behavior unchanged."
        ),
    )
    render_p.set_defaults(func=cmd_render)

    up_p = sub.add_parser("up", help="Tear down any previous stack (awaited), then bring the rendered stack up")
    up_p.set_defaults(func=cmd_up)

    down_p = sub.add_parser("down", help="Tear down the current stack")
    down_p.set_defaults(func=cmd_down)

    wait_p = sub.add_parser("wait-for-ready", help="Poll :8080/json/ until all workers register")
    wait_p.add_argument("--worker-count", dest="worker_count", type=int, default=DEFAULTS["worker_count"])
    wait_p.add_argument("--timeout", dest="timeout", type=int, default=60)
    wait_p.set_defaults(func=cmd_wait_for_ready)

    status_p = sub.add_parser("status", help="Print the current master /json/ payload")
    status_p.set_defaults(func=cmd_status)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
