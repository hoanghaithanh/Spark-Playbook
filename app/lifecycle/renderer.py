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
    # Multi-broker Kafka ADR (docs/architecture/multi-broker-kafka-cluster.md
    # D-MBK1, supersedes #50's D1): now a genuine drawer-driven toggle read
    # from the spawn form, not solely from Topic.requires_kafka -- the topic
    # manifest only pre-checks the form's default. Defaults false so every
    # existing non-Kafka spawn is unaffected.
    include_kafka: bool = False
    kafka_broker_count: int = config.DEFAULTS["kafka_broker_count"]


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
        lo, hi = config.KAFKA_BROKER_COUNT_RANGE
        if not (lo <= params.kafka_broker_count <= hi):
            errors.append(f"kafka_broker_count must be {lo}-{hi}")
        # Multi-broker Kafka ADR D-MBK4: Kafka's contribution scales with
        # broker count, not a flat reservation (supersedes #50's flat +2GB).
        total_gb += config.KAFKA_MEMORY_GB * params.kafka_broker_count
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
        "kafka_broker_count": params.kafka_broker_count,
        "public_origin": config.PUBLIC_ORIGIN,
    }

    compose_tpl = env.get_template("docker-compose.yml.j2")
    config.COMPOSE_FILE.write_text(compose_tpl.render(**context), encoding="utf-8")

    conf_tpl = env.get_template("spark-defaults.conf.j2")
    config.SPARK_DEFAULTS_FILE.write_text(conf_tpl.render(**context), encoding="utf-8")
