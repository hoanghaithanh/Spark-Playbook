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
from unittest.mock import patch

import app.config as config_module


def _reload_config():
    return importlib.reload(config_module)


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
