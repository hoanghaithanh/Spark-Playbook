"""Tests for the public-deploy config split (docs/architecture/public-deploy.md
D3): CLUSTER_HOST / JUPYTER_URL / MASTER_UI_URL / PUBLIC_ORIGIN must all
default to today's literal localhost values with no env vars set (constraint:
`uvicorn app.main:app` run directly for local dev must behave exactly as
before), and must pick up the deploy stack's overrides when set.

`app/config.py` reads these via `os.environ.get(...)` at *import* time, so
these tests reload the module under a patched environment rather than
monkeypatching module attributes directly -- that's the only way to actually
exercise the default-vs-override branch the real process takes at startup.
"""
from __future__ import annotations

import importlib
import os
import sys
from unittest.mock import patch

import pytest
import yaml

import app.config as config_module


def _reload_config():
    return importlib.reload(config_module)


@pytest.fixture(autouse=True)
def _restore_reloaded_modules():
    """Every test below reloads `app.config` (and sometimes
    `app.spark_api.app_client` / `app.lifecycle.renderer`) under a patched
    `os.environ` to exercise the real import-time default-vs-override branch
    -- `importlib.reload` mutates the shared module object in place, so if a
    test left it reloaded under a non-default env (e.g. an assertion failed
    before its own `finally` ran, or a future test forgets one), every other
    test importing `app.config`/`app_client` afterwards -- most notably
    `tests/unit/test_app_client.py`'s CLUSTER_HOST-derived base_url
    assertions -- would silently see the wrong module state depending on
    collection order. Belt-and-suspenders on top of each test's own
    try/finally: unconditionally reload all three back to today's real,
    unpatched environment after every test here, pass or fail."""
    yield
    importlib.reload(config_module)
    for name in ("app.spark_api.app_client", "app.lifecycle.renderer"):
        mod = sys.modules.get(name)
        if mod is not None:
            importlib.reload(mod)


class TestDevDefaultsPreserved:
    """No env vars set -- must reproduce dev behavior. `CLUSTER_HOST` defaults
    to the literal IP `127.0.0.1`, not the hostname `localhost`: verified live
    on Windows that resolving `localhost` in Python's urllib pays a ~2s IPv6
    (::1)-then-IPv4 fallback tax per call, multiplied across every
    `DRIVER_APP_UI_PORTS` probe each ~2s dashboard-collector cycle -- made the
    Cluster Monitor panel look hung. `JUPYTER_URL`/`MASTER_UI_URL` are
    browser-facing (not this app's own urllib calls) and keep the `localhost`
    default."""

    def test_defaults_match_original_localhost_urls(self):
        with patch.dict(os.environ, {}, clear=False):
            for key in ("CLUSTER_HOST", "JUPYTER_URL", "MASTER_UI_URL", "PUBLIC_ORIGIN"):
                os.environ.pop(key, None)
            cfg = _reload_config()
            try:
                assert cfg.CLUSTER_HOST == "127.0.0.1"
                assert cfg.MASTER_JSON_URL == "http://127.0.0.1:8080/json/"
                assert cfg.DRIVER_APP_UI_URL == "http://127.0.0.1:4040"
                assert cfg.JUPYTER_URL == "http://localhost:8888"
                assert cfg.MASTER_UI_URL == "http://localhost:8080"
                assert cfg.PUBLIC_ORIGIN == ""
            finally:
                _reload_config()  # restore a clean module state for later tests


class TestDeployOverrides:
    def test_cluster_host_rewrites_server_side_urls_only(self):
        with patch.dict(
            os.environ,
            {"CLUSTER_HOST": "host.docker.internal", "JUPYTER_URL": "/jupyter", "MASTER_UI_URL": "/spark-master"},
        ):
            cfg = _reload_config()
            try:
                assert cfg.MASTER_JSON_URL == "http://host.docker.internal:8080/json/"
                assert cfg.DRIVER_APP_UI_URL == "http://host.docker.internal:4040"
                # Browser-facing URLs are proxy subpaths, not host:port --
                # they don't inherit CLUSTER_HOST.
                assert cfg.JUPYTER_URL == "/jupyter"
                assert cfg.MASTER_UI_URL == "/spark-master"
            finally:
                for key in ("CLUSTER_HOST", "JUPYTER_URL", "MASTER_UI_URL"):
                    os.environ.pop(key, None)
                _reload_config()

    def test_public_origin_defaults_empty_and_is_settable(self):
        with patch.dict(os.environ, {"PUBLIC_ORIGIN": "https://spark.example.com"}):
            cfg = _reload_config()
            try:
                assert cfg.PUBLIC_ORIGIN == "https://spark.example.com"
            finally:
                os.environ.pop("PUBLIC_ORIGIN", None)
                _reload_config()


def test_app_client_probes_use_cluster_host():
    """app/spark_api/app_client.py:85 (ADR-flagged mandatory fix) must build
    its probe URLs from config.CLUSTER_HOST, not a hardcoded 'localhost' --
    otherwise the containerized app's in-container REST reads break."""
    import app.spark_api.app_client as app_client_module

    with patch.dict(os.environ, {"CLUSTER_HOST": "host.docker.internal"}):
        importlib.reload(config_module)
        importlib.reload(app_client_module)
        try:
            with patch("urllib.request.urlopen", side_effect=OSError("unreachable")):
                app_client_module.resolve_current_app()
            # No exception means the port loop ran; assert the URL shape
            # directly via the private probe helper.
            probed_urls = []

            def _fake_urlopen(url, timeout=None):
                probed_urls.append(url)
                raise OSError("unreachable")

            with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
                app_client_module._probe_ports(timeout_s=0.1)
            assert probed_urls, "expected at least one probed URL"
            assert all(url.startswith("http://host.docker.internal:") for url in probed_urls)
        finally:
            os.environ.pop("CLUSTER_HOST", None)
            importlib.reload(config_module)
            importlib.reload(app_client_module)


def test_renderer_forwards_public_origin_into_compose_template():
    """docs/architecture/public-deploy.md D4: PUBLIC_ORIGIN must reach the
    spawned driver container as SPARKPB_PUBLIC_ORIGIN so
    driver/jupyter_config.py's CSP allowlist can widen to it."""
    from app.lifecycle import renderer

    with patch.dict(os.environ, {"PUBLIC_ORIGIN": "https://spark.example.com"}):
        importlib.reload(config_module)
        try:
            importlib.reload(renderer)
            renderer.render(renderer.ClusterParams())
            rendered = config_module.COMPOSE_FILE.read_text(encoding="utf-8")
            assert "SPARKPB_PUBLIC_ORIGIN" in rendered
            assert "https://spark.example.com" in rendered
        finally:
            os.environ.pop("PUBLIC_ORIGIN", None)
            importlib.reload(config_module)
            importlib.reload(renderer)


def test_rendered_compose_is_valid_yaml_in_deploy_and_dev_modes():
    """BLOCKER regression (live acceptance validation of public-deploy work):
    the spark-driver `command: >-` folded block scalar in
    docker-compose.yml.j2 had a `{% if public_origin %}` line that, under the
    renderer's trim_blocks=True/lstrip_blocks=True Jinja config, rendered its
    flags at column 0 -- breaking out of the folded scalar and producing
    invalid YAML (`could not find expected ':'`) whenever public_origin was
    set, i.e. every real deploy via deploy.sh/PUBLIC_ORIGIN. Dev-mode
    (public_origin empty) never exercised this branch, which is why the
    existing render-only tests + compose/cli.py (dev-mode only) missed it.
    """
    from app.lifecycle import renderer

    # Deploy mode: public_origin set -> must still be valid YAML, and must
    # actually contain the base_url/allow_remote_access flags.
    with patch.dict(os.environ, {"PUBLIC_ORIGIN": "https://demo.spark.test"}):
        importlib.reload(config_module)
        try:
            importlib.reload(renderer)
            renderer.render(renderer.ClusterParams())
            rendered = config_module.COMPOSE_FILE.read_text(encoding="utf-8")
            parsed = yaml.safe_load(rendered)  # raises yaml.YAMLError if invalid
            driver_command = parsed["services"]["spark-driver"]["command"]
            assert "--ServerApp.base_url=/jupyter/" in driver_command
            assert "--ServerApp.allow_remote_access=True" in driver_command
        finally:
            os.environ.pop("PUBLIC_ORIGIN", None)
            importlib.reload(config_module)
            importlib.reload(renderer)

    # Dev mode: public_origin empty -> valid YAML, deploy-only flags absent.
    importlib.reload(config_module)
    try:
        importlib.reload(renderer)
        renderer.render(renderer.ClusterParams())
        rendered = config_module.COMPOSE_FILE.read_text(encoding="utf-8")
        parsed = yaml.safe_load(rendered)
        driver_command = parsed["services"]["spark-driver"]["command"]
        assert "--ServerApp.base_url=/jupyter/" not in driver_command
        assert "--ServerApp.allow_remote_access=True" not in driver_command
    finally:
        importlib.reload(config_module)
        importlib.reload(renderer)
