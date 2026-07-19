"""Spark Playbook — Jinja2 render step (PLAN.md §2 step 1-2, §4 renderer.py).

Adapted from `compose/cli.py`'s `cmd_render`/`_validate_ranges` — same
templates, same defaults, same validation rules, same resource-ceiling check —
just split into a pure `validate()` + `render()` pair the FastAPI app can call
synchronously from the lifecycle manager.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from app import config


@dataclass
class ClusterParams:
    worker_count: int = config.DEFAULTS["worker_count"]
    worker_cores: int = config.DEFAULTS["worker_cores"]
    worker_memory_gb: int = config.DEFAULTS["worker_memory_gb"]
    driver_memory_gb: int = config.DEFAULTS["driver_memory_gb"]
    shuffle_partitions: int = config.DEFAULTS["shuffle_partitions"]
    aqe_enabled: bool = config.DEFAULTS["aqe_enabled"]
    # Kafka ADR D1: render-time flag driven by Topic.requires_kafka, not a
    # user-facing toggle. Defaults false so every existing spawn is unaffected.
    include_kafka: bool = False


@dataclass
class ValidationResult:
    errors: List[str] = field(default_factory=list)
    total_gb: int = 0

    @property
    def ok(self) -> bool:
        return not self.errors


def validate(params: ClusterParams) -> ValidationResult:
    """Same rules as `compose/cli.py::_validate_ranges` (PLAN.md §2 table + ceiling)."""
    errors: List[str] = []

    lo, hi = config.WORKER_COUNT_RANGE
    if not (lo <= params.worker_count <= hi):
        errors.append(f"worker_count must be {lo}-{hi}")

    lo, hi = config.WORKER_CORES_RANGE
    if not (lo <= params.worker_cores <= hi):
        errors.append(f"worker_cores must be {lo}-{hi}")

    lo, hi = config.WORKER_MEMORY_GB_RANGE
    if not (lo <= params.worker_memory_gb <= hi):
        errors.append(f"worker_memory_gb must be {lo}-{hi}")

    if params.shuffle_partitions <= 0:
        errors.append("shuffle_partitions must be a positive integer")

    if params.driver_memory_gb <= 0:
        errors.append("driver_memory_gb must be a positive integer")

    total_gb = (
        config.MASTER_MEMORY_GB
        + params.worker_count * params.worker_memory_gb
        + params.driver_memory_gb
    )
    if params.include_kafka:
        # Kafka ADR (docs/architecture/kafka-streaming-infra.md) resource-ceiling
        # accounting: +2GB conservative reservation when the broker is included.
        total_gb += config.KAFKA_MEMORY_GB
    if total_gb > config.RESOURCE_CEILING_GB:
        errors.append(
            f"requested config totals ~{total_gb}GB, exceeding the "
            f"{config.RESOURCE_CEILING_GB}GB sanity ceiling (PLAN.md §2 resource-ceiling check)"
        )

    return ValidationResult(errors=errors, total_gb=total_gb)


def render(params: ClusterParams) -> None:
    """Render docker-compose.yml + spark-defaults.conf into compose/rendered/.

    Caller is responsible for calling `validate()` first (the lifecycle manager
    does this before starting the state machine so failures surface pre-spawn,
    per US-1.2).
    """
    config.RENDERED_DIR.mkdir(parents=True, exist_ok=True)
    env = Environment(
        loader=FileSystemLoader(str(config.TEMPLATES_DIR)),
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )

    context = {
        "image_name": config.IMAGE_NAME,
        "worker_count": params.worker_count,
        "worker_cores": params.worker_cores,
        "worker_memory_gb": params.worker_memory_gb,
        "driver_memory_gb": params.driver_memory_gb,
        "shuffle_partitions": params.shuffle_partitions,
        "aqe_enabled": params.aqe_enabled,
        "include_kafka": params.include_kafka,
        "public_origin": config.PUBLIC_ORIGIN,
    }

    compose_tpl = env.get_template("docker-compose.yml.j2")
    config.COMPOSE_FILE.write_text(compose_tpl.render(**context), encoding="utf-8")

    conf_tpl = env.get_template("spark-defaults.conf.j2")
    config.SPARK_DEFAULTS_FILE.write_text(conf_tpl.render(**context), encoding="utf-8")
