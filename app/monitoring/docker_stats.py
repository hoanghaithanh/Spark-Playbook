"""Spark Playbook — Docker container stats source for the monitoring dashboard
(ADR D-C).

CPU/RAM/disk/net for the master, workers, and driver containers have no Spark
REST source at all (requirements doc's "measurability" section) -- they come
from Docker itself, invoked the same way `app/lifecycle/compose_ops.py`
already talks to Docker: shelling out to the CLI via
`asyncio.create_subprocess_exec`, no new pip dependency (ADR D-C, "Docker
Python SDK" rejected as an alternative).

Two subprocess calls per cycle, scoped to the `sparkpb` compose project:
  1. `docker ps -q --filter label=com.docker.compose.project=sparkpb` --
     container IDs currently in this project (a container that stopped simply
     doesn't appear -- this is how US-5.1's "reflect that stats are no longer
     available" is satisfied upstream of this module).
  2. `docker stats --no-stream --format {{json .}} <ids...>` -- one call for
     the whole (<=6 container) stack, never per-container (ADR D-C's overhead
     mitigation).

CPU% caveat (ADR D-C): `docker stats`' CPUPerc is computed relative to ONE
full host core (so it can exceed 100% on a multi-core container). The color
thresholds need to mean "saturating its *allotted* cores", so `sample()`
normalizes CPUPerc against each container's configured cpu limit
(`cpu_limits`, supplied by the caller -- `collector.py`, derived from
`ClusterParams`/the compose template's `deploy.resources.limits.cpus`), i.e.
100% here means "fully using its allocation."
"""
from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from typing import Dict, List, Optional

from app import config

_SIZE_RE = re.compile(r"^([\d.]+)\s*([a-zA-Z]*)$")

# go-units size suffixes docker's CLI formatter can emit. MemUsage uses
# binary (Ki/Mi/Gi) units; NetIO/BlockIO use decimal (k/M/G) units -- both
# handled here since we don't want to guess which field we're parsing.
_UNIT_MULTIPLIERS = {
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
}


def _parse_size(text: str) -> Optional[int]:
    """`"15.5MiB"` / `"1.2kB"` / `"0B"` -> bytes. Returns None if unparsable
    (degrade gracefully -- a malformed/unexpected docker CLI field must not
    take down the whole collector cycle)."""
    text = text.strip()
    m = _SIZE_RE.match(text)
    if not m:
        return None
    number, unit = m.groups()
    multiplier = _UNIT_MULTIPLIERS.get(unit.lower(), None)
    if multiplier is None:
        return None
    try:
        return round(float(number) * multiplier)
    except ValueError:
        return None


def _parse_pair(text: str) -> tuple[Optional[int], Optional[int]]:
    """`"15.5MiB / 1.9GiB"` -> (15_5..., 1_9...) in bytes each."""
    parts = text.split("/")
    if len(parts) != 2:
        return None, None
    return _parse_size(parts[0]), _parse_size(parts[1])


@dataclass
class ContainerStat:
    name: str  # container_name, e.g. "spark-master", "spark-worker-1", "spark-driver"
    cpu_pct: Optional[float]  # normalized against cpu_limits (None if unparsable)
    mem_used_bytes: Optional[int]
    mem_limit_bytes: Optional[int]
    net_rx_bytes: Optional[int]
    net_tx_bytes: Optional[int]
    block_read_bytes: Optional[int]
    block_write_bytes: Optional[int]
    container_id: Optional[str] = None  # short ID, from `docker stats`' own "ID" field


async def _run(*args: str, timeout_s: float) -> Optional[str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, _stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
        except asyncio.TimeoutError:
            if proc.returncode is None:
                proc.kill()
                await proc.wait()
            return None
    except (OSError, asyncio.CancelledError):
        raise
    if proc.returncode != 0:
        return None
    return stdout.decode("utf-8", errors="replace")


async def list_container_ids(timeout_s: float = 5.0) -> List[str]:
    """Container IDs currently in the `sparkpb` compose project. Empty list if
    Docker isn't reachable or nothing is running -- never raises (callers
    treat "no containers" the same as "no cluster" rather than an error)."""
    out = await _run(
        "docker", "ps", "-q",
        "--filter", f"label=com.docker.compose.project={config.PROJECT_NAME}",
        timeout_s=timeout_s,
    )
    if not out:
        return []
    return [line.strip() for line in out.splitlines() if line.strip()]


async def sample(cpu_limits: Dict[str, float], timeout_s: float = 5.0) -> List[ContainerStat]:
    """One `docker stats --no-stream` call for every container in the
    project, normalized CPU% against `cpu_limits` (keyed by container name,
    e.g. `{"spark-master": 1.0, "spark-worker-1": 2.0, "spark-driver": 2.0}`).

    A container with no entry in `cpu_limits` is returned with `cpu_pct=None`
    rather than guessed at -- normalization needs a real limit to mean
    anything (ADR D-C caveat).
    """
    ids = await list_container_ids(timeout_s=timeout_s)
    if not ids:
        return []

    out = await _run(
        "docker", "stats", "--no-stream", "--format", "{{json .}}", *ids,
        timeout_s=timeout_s,
    )
    if not out:
        return []

    results: List[ContainerStat] = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(raw, dict):
            continue

        name = raw.get("Name", "")
        cpu_raw_text = str(raw.get("CPUPerc", "")).rstrip("%")
        cpu_raw = None
        try:
            cpu_raw = float(cpu_raw_text)
        except ValueError:
            pass

        limit_cores = cpu_limits.get(name)
        cpu_pct = (cpu_raw / limit_cores) if (cpu_raw is not None and limit_cores) else None

        mem_used, mem_limit = _parse_pair(str(raw.get("MemUsage", "")))
        net_rx, net_tx = _parse_pair(str(raw.get("NetIO", "")))
        block_read, block_write = _parse_pair(str(raw.get("BlockIO", "")))

        results.append(
            ContainerStat(
                name=name,
                cpu_pct=cpu_pct,
                mem_used_bytes=mem_used,
                mem_limit_bytes=mem_limit,
                net_rx_bytes=net_rx,
                net_tx_bytes=net_tx,
                block_read_bytes=block_read,
                block_write_bytes=block_write,
                container_id=raw.get("ID"),
            )
        )
    return results


async def container_ip_map(container_ids: List[str], timeout_s: float = 5.0) -> Dict[str, str]:
    """`{container_ip: container_name}` for the given (short) container ids.

    Needed because, verified against a real spawn (not just code review):
    standalone Spark's Worker registers each executor using the container's
    raw bridge-network IP as its advertised host, NOT the container
    hostname -- only the driver's own hostPort happens to report a hostname.
    So the ADR's assumed join key ("service name == hostname == executor
    host") holds for the driver but not for workers, and
    `collector._executor_host_map()` needs this IP fallback to actually
    attach GC time / partition counts to worker node cards (ADR R-Dash-1's
    anticipated mitigation: "join defensively... never mis-attach")."""
    if not container_ids:
        return {}
    out = await _run(
        "docker", "inspect",
        "--format", "{{.Name}}|{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
        *container_ids,
        timeout_s=timeout_s,
    )
    if not out:
        return {}
    mapping: Dict[str, str] = {}
    for line in out.splitlines():
        line = line.strip()
        if not line or "|" not in line:
            continue
        name, ip = line.split("|", 1)
        name = name.lstrip("/")
        if name and ip:
            mapping[ip] = name
    return mapping
