"""Spark Playbook — Kafka observability data layer (ADR D-MBK5).

All admin-plane reads are `docker exec` shellouts into a *live* broker
container, mirroring `docker_stats.py`'s `asyncio.create_subprocess_exec`
idiom exactly -- no `KafkaAdminClient`, no new Python Kafka dependency
(`docs/architecture/kafka-streaming-infra.md` already proved
`KafkaAdminClient` raises `NodeNotReadyError` against this broker; the CLI
shellout idiom is this project's established Kafka-tooling discipline).

Broker-fallback (US-MBK2): any one live broker's CLI can describe the whole
cluster via `--bootstrap-server localhost:9092` (its own listener), so
`describe_cluster()` tries `spark-kafka-1`, then `-2`, then `-3`, ... in
order, falling to the next broker on a non-zero exec -- the broker-kill demo
(#60) means broker 1 itself may be the one that's down exactly when the
monitor matters most.

Each parser below ships a `demo()` self-check (ponytail: "lazy code without
its check is unfinished") asserting against a captured/representative sample
string, run via `python -m app.monitoring.kafka_stats`.
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

_KAFKA_BIN = "/opt/kafka/bin"


# --------------------------------------------------------------------- #
# subprocess plumbing (mirrors docker_stats._run exactly)
# --------------------------------------------------------------------- #


async def _run(*args: str, timeout_s: float) -> Optional[str]:
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
    except asyncio.CancelledError:
        # Same fix as docker_stats._run's CancelledError branch (issue #18):
        # kill/reap the child before propagating, so a collector cycle
        # cancelled mid-shellout doesn't orphan a `docker exec` process.
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
        raise
    if proc.returncode != 0:
        return None
    return stdout.decode("utf-8", errors="replace")


async def _exec_in(container: str, *args: str, timeout_s: float) -> Optional[str]:
    return await _run("docker", "exec", container, *args, timeout_s=timeout_s)


async def find_live_broker(brokers: List[str], timeout_s: float = 5.0) -> Optional[str]:
    """Broker-fallback (US-MBK2, D-MBK5): try each broker container in order,
    returning the first one that answers a cheap `kafka-broker-api-versions.sh`
    probe against its own listener. Returns None if every broker is
    unreachable."""
    for container in brokers:
        out = await _exec_in(
            container,
            f"{_KAFKA_BIN}/kafka-broker-api-versions.sh",
            "--bootstrap-server", "localhost:9092",
            timeout_s=timeout_s,
        )
        if out is not None:
            return container
    return None


# --------------------------------------------------------------------- #
# kafka-topics.sh --describe  (High confidence)
# --------------------------------------------------------------------- #


@dataclass
class PartitionInfo:
    topic: str
    partition: int
    leader: Optional[int]
    replicas: List[int] = field(default_factory=list)
    isr: List[int] = field(default_factory=list)


@dataclass
class TopicInfo:
    name: str
    partition_count: Optional[int] = None
    replication_factor: Optional[int] = None
    partitions: List[PartitionInfo] = field(default_factory=list)


def _split_kv_line(line: str) -> Dict[str, str]:
    """Tab-delimited `Key: value\tKey2: value2` -> {"Key": "value", ...},
    each field split on its FIRST colon (values like replica lists never
    contain a colon, but future keys like `Elr:`/`LastKnownElr:` might carry
    padding -- first-colon split handles both)."""
    fields: Dict[str, str] = {}
    for chunk in line.split("\t"):
        chunk = chunk.strip()
        if not chunk or ":" not in chunk:
            continue
        key, _, value = chunk.partition(":")
        fields[key.strip()] = value.strip()
    return fields


def _int_list(text: str) -> List[int]:
    if not text:
        return []
    result = []
    for part in text.split(","):
        part = part.strip()
        if part:
            try:
                result.append(int(part))
            except ValueError:
                pass
    return result


def parse_topics_describe(output: str) -> List[TopicInfo]:
    """`kafka-topics.sh --describe` output -> per-topic rows with
    per-partition leader/replicas/ISR. A line is a partition line iff it has
    a `Partition:` field; otherwise (and it has a `Topic:` field) it's the
    topic header line."""
    topics: Dict[str, TopicInfo] = {}
    order: List[str] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = _split_kv_line(line)
        name = fields.get("Topic")
        if not name:
            continue
        if name not in topics:
            topics[name] = TopicInfo(name=name)
            order.append(name)
        topic = topics[name]

        if "Partition" in fields:
            leader_text = fields.get("Leader", "")
            leader = None
            try:
                leader = int(leader_text)
            except ValueError:
                pass
            try:
                partition_num = int(fields["Partition"])
            except (KeyError, ValueError):
                continue
            topic.partitions.append(
                PartitionInfo(
                    topic=name,
                    partition=partition_num,
                    leader=leader,
                    replicas=_int_list(fields.get("Replicas", "")),
                    isr=_int_list(fields.get("Isr", "")),
                )
            )
        else:
            try:
                topic.partition_count = int(fields["PartitionCount"])
            except (KeyError, ValueError):
                pass
            try:
                topic.replication_factor = int(fields["ReplicationFactor"])
            except (KeyError, ValueError):
                pass

    return [topics[n] for n in order]


def demo_parse_topics_describe() -> None:
    sample = (
        "Topic: prices\tTopicId: abc123\tPartitionCount: 2\tReplicationFactor: 3\tConfigs: \n"
        "\tTopic: prices\tPartition: 0\tLeader: 1\tReplicas: 1,2,3\tIsr: 1,2,3\n"
        "\tTopic: prices\tPartition: 1\tLeader: 2\tReplicas: 2,3,1\tIsr: 2,3\n"
    )
    topics = parse_topics_describe(sample)
    assert len(topics) == 1
    t = topics[0]
    assert t.name == "prices"
    assert t.partition_count == 2
    assert t.replication_factor == 3
    assert len(t.partitions) == 2
    assert t.partitions[0].leader == 1
    assert t.partitions[0].replicas == [1, 2, 3]
    assert t.partitions[1].isr == [2, 3]


# --------------------------------------------------------------------- #
# kafka-topics.sh --describe --under-replicated-partitions (High confidence)
# --------------------------------------------------------------------- #


def parse_urp(output: str) -> List[PartitionInfo]:
    """Same per-partition line shape as `parse_topics_describe`, URP lines
    only, no topic header lines. Empty output = no under-replicated
    partitions."""
    result: List[PartitionInfo] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = _split_kv_line(line)
        name = fields.get("Topic")
        if not name or "Partition" not in fields:
            continue
        leader = None
        try:
            leader = int(fields.get("Leader", ""))
        except ValueError:
            pass
        try:
            partition_num = int(fields["Partition"])
        except (KeyError, ValueError):
            continue
        result.append(
            PartitionInfo(
                topic=name,
                partition=partition_num,
                leader=leader,
                replicas=_int_list(fields.get("Replicas", "")),
                isr=_int_list(fields.get("Isr", "")),
            )
        )
    return result


def demo_parse_urp() -> None:
    assert parse_urp("") == []
    sample = "\tTopic: prices\tPartition: 1\tLeader: 2\tReplicas: 2,3,1\tIsr: 2,3\n"
    result = parse_urp(sample)
    assert len(result) == 1
    assert result[0].topic == "prices"
    assert result[0].partition == 1
    assert result[0].isr == [2, 3]


# --------------------------------------------------------------------- #
# kafka-consumer-groups.sh --describe --all-groups (High confidence)
# --------------------------------------------------------------------- #


@dataclass
class GroupOffsetRow:
    group: str
    topic: str
    partition: Optional[int]
    current_offset: Optional[int]
    log_end_offset: Optional[int]
    lag: Optional[int]


def _int_or_none(text: str) -> Optional[int]:
    if not text or text == "-":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def parse_consumer_groups_offsets(output: str) -> List[GroupOffsetRow]:
    """Fixed-width header `GROUP TOPIC PARTITION CURRENT-OFFSET
    LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID`; `-` for absent values;
    groups separated by blank lines. GROUP/TOPIC never contain spaces, so
    whitespace-split + index 0-5 is safe here (unlike the --state variant
    below)."""
    rows: List[GroupOffsetRow] = []
    for line in output.splitlines():
        line = line.strip()
        if not line or line.startswith("GROUP"):
            continue
        parts = line.split()
        if len(parts) < 6:
            continue
        rows.append(
            GroupOffsetRow(
                group=parts[0],
                topic=parts[1],
                partition=_int_or_none(parts[2]),
                current_offset=_int_or_none(parts[3]),
                log_end_offset=_int_or_none(parts[4]),
                lag=_int_or_none(parts[5]),
            )
        )
    return rows


def demo_parse_consumer_groups_offsets() -> None:
    sample = (
        "\n"
        "GROUP           TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID   HOST  CLIENT-ID\n"
        "price-consumers prices  0          100             120             20   consumer-1-a  /172.19.0.5  consumer-1\n"
        "price-consumers prices  1          50              50              0    -             -     -\n"
        "\n"
    )
    rows = parse_consumer_groups_offsets(sample)
    assert len(rows) == 2
    assert rows[0].group == "price-consumers"
    assert rows[0].lag == 20
    assert rows[1].current_offset == 50
    assert rows[1].lag == 0


# --------------------------------------------------------------------- #
# kafka-consumer-groups.sh --describe --all-groups --state (Medium confidence
# -- the COORDINATOR (ID) space gotcha)
# --------------------------------------------------------------------- #


@dataclass
class GroupStateRow:
    group: str
    state: str
    members: Optional[int]


def parse_consumer_groups_state(output: str) -> List[GroupStateRow]:
    """Header `GROUP COORDINATOR (ID) ASSIGNMENT-STRATEGY STATE #MEMBERS` --
    `COORDINATOR (ID)` contains a space, so naive left-to-right index
    parsing breaks (index 1 would read "(ID)" as if it were its own field).
    Do NOT index from the left: GROUP is the first token, #MEMBERS is the
    last token, STATE is the second-to-last token."""
    rows: List[GroupStateRow] = []
    for line in output.splitlines():
        line = line.strip()
        if not line or line.startswith("GROUP"):
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        group = parts[0]
        members = _int_or_none(parts[-1])
        state = parts[-2]
        rows.append(GroupStateRow(group=group, state=state, members=members))
    return rows


def demo_parse_consumer_groups_state() -> None:
    sample = (
        "\n"
        "GROUP            COORDINATOR (ID)        ASSIGNMENT-STRATEGY  STATE          #MEMBERS\n"
        "price-consumers  spark-kafka-2 (2)        range                 Stable         1\n"
    )
    rows = parse_consumer_groups_state(sample)
    assert len(rows) == 1
    assert rows[0].group == "price-consumers"
    assert rows[0].state == "Stable"
    assert rows[0].members == 1


# --------------------------------------------------------------------- #
# kafka-metadata-quorum.sh describe --status (High confidence for LeaderId)
# --------------------------------------------------------------------- #


def parse_quorum_status(output: str) -> Dict[str, str]:
    """`key:<pad>value` per line -> {key: value}, split on the first colon."""
    result: Dict[str, str] = {}
    for line in output.splitlines():
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        result[key.strip()] = value.strip()
    return result


def active_controller_id(quorum_status: Dict[str, str]) -> Optional[int]:
    try:
        return int(quorum_status["LeaderId"])
    except (KeyError, ValueError):
        return None


def demo_parse_quorum_status() -> None:
    sample = (
        "ClusterId:              abc123\n"
        "LeaderId:                2\n"
        "LeaderEpoch:             5\n"
        "HighWatermark:           100\n"
        "CurrentVoters:           [1,2,3]\n"
        "CurrentObservers:        []\n"
    )
    status = parse_quorum_status(sample)
    assert status["ClusterId"] == "abc123"
    assert active_controller_id(status) == 2


# --------------------------------------------------------------------- #
# kafka-log-dirs.sh --describe (High confidence)
# --------------------------------------------------------------------- #


def parse_log_dirs(output: str) -> Dict[Tuple[str, int], int]:
    """Two human-readable lines then one JSON line -- find the line starting
    with `{`, json.loads it, walk brokers -> logDirs -> partitions -> size.
    Returns {(topic, partition): total_size_bytes} summed across every
    broker/logDir that reports that partition (a replica's local size, not
    a cluster-wide sum, but summing all reporting brokers is the closest
    proxy without per-broker attribution the collector needs)."""
    json_line = None
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("{"):
            json_line = stripped
            break
    if json_line is None:
        return {}
    try:
        data = json.loads(json_line)
    except json.JSONDecodeError:
        return {}

    sizes: Dict[Tuple[str, int], int] = {}
    for broker in data.get("brokers", []):
        for log_dir in broker.get("logDirs", []):
            for partition in log_dir.get("partitions", []):
                name = partition.get("partition", "")
                size = partition.get("size", 0) or 0
                if "-" not in name:
                    continue
                topic, _, part_str = name.rpartition("-")
                try:
                    part_num = int(part_str)
                except ValueError:
                    continue
                key = (topic, part_num)
                sizes[key] = sizes.get(key, 0) + size
    return sizes


def demo_parse_log_dirs() -> None:
    sample = (
        "Querying brokers for log directories information\n"
        "Received log directory information from brokers 1,2,3\n"
        '{"version":1,"brokers":[{"broker":1,"logDirs":[{"logDir":"/tmp/kraft-broker-logs",'
        '"partitions":[{"partition":"prices-0","size":1024,"offsetLag":0,"isFuture":false}]}]}]}\n'
    )
    sizes = parse_log_dirs(sample)
    assert sizes == {("prices", 0): 1024}
    assert parse_log_dirs("no json here") == {}


# --------------------------------------------------------------------- #
# kafka-run-class.sh kafka.tools.GetOffsetShell (High confidence)
# --------------------------------------------------------------------- #


def parse_offsets(output: str) -> Dict[Tuple[str, int], int]:
    """`topic:partition:latestOffset` per line -> {(topic, partition): offset}."""
    result: Dict[Tuple[str, int], int] = {}
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(":")
        if len(parts) != 3:
            continue
        topic, part_str, offset_str = parts
        try:
            result[(topic, int(part_str))] = int(offset_str)
        except ValueError:
            continue
    return result


def demo_parse_offsets() -> None:
    sample = "prices:0:120\nprices:1:80\n"
    result = parse_offsets(sample)
    assert result == {("prices", 0): 120, ("prices", 1): 80}


# --------------------------------------------------------------------- #
# high-level shellout wrappers (each targets the given live broker)
# --------------------------------------------------------------------- #


async def fetch_topics_describe(container: str, timeout_s: float = 15.0) -> List[TopicInfo]:
    out = await _exec_in(
        container, f"{_KAFKA_BIN}/kafka-topics.sh",
        "--bootstrap-server", "localhost:9092", "--describe",
        timeout_s=timeout_s,
    )
    return parse_topics_describe(out) if out else []


async def fetch_urp(container: str, timeout_s: float = 15.0) -> List[PartitionInfo]:
    out = await _exec_in(
        container, f"{_KAFKA_BIN}/kafka-topics.sh",
        "--bootstrap-server", "localhost:9092", "--describe",
        "--under-replicated-partitions",
        timeout_s=timeout_s,
    )
    return parse_urp(out) if out else []


async def fetch_consumer_groups_offsets(container: str, timeout_s: float = 15.0) -> List[GroupOffsetRow]:
    out = await _exec_in(
        container, f"{_KAFKA_BIN}/kafka-consumer-groups.sh",
        "--bootstrap-server", "localhost:9092", "--describe", "--all-groups",
        timeout_s=timeout_s,
    )
    return parse_consumer_groups_offsets(out) if out else []


async def fetch_consumer_groups_state(container: str, timeout_s: float = 15.0) -> List[GroupStateRow]:
    out = await _exec_in(
        container, f"{_KAFKA_BIN}/kafka-consumer-groups.sh",
        "--bootstrap-server", "localhost:9092", "--describe", "--all-groups", "--state",
        timeout_s=timeout_s,
    )
    return parse_consumer_groups_state(out) if out else []


async def fetch_quorum_status(container: str, timeout_s: float = 15.0) -> Dict[str, str]:
    out = await _exec_in(
        container, f"{_KAFKA_BIN}/kafka-metadata-quorum.sh",
        "--bootstrap-server", "localhost:9092", "describe", "--status",
        timeout_s=timeout_s,
    )
    return parse_quorum_status(out) if out else {}


async def fetch_log_dirs(container: str, timeout_s: float = 15.0) -> Dict[Tuple[str, int], int]:
    out = await _exec_in(
        container, f"{_KAFKA_BIN}/kafka-log-dirs.sh",
        "--bootstrap-server", "localhost:9092", "--describe",
        timeout_s=timeout_s,
    )
    return parse_log_dirs(out) if out else {}


async def fetch_offsets(container: str, topic: str, timeout_s: float = 15.0) -> Dict[Tuple[str, int], int]:
    out = await _exec_in(
        container, f"{_KAFKA_BIN}/kafka-run-class.sh", "kafka.tools.GetOffsetShell",
        "--broker-list", "localhost:9092", "--topic", topic,
        timeout_s=timeout_s,
    )
    return parse_offsets(out) if out else {}


def demo() -> None:
    demo_parse_topics_describe()
    demo_parse_urp()
    demo_parse_consumer_groups_offsets()
    demo_parse_consumer_groups_state()
    demo_parse_quorum_status()
    demo_parse_log_dirs()
    demo_parse_offsets()
    print("kafka_stats: all parser self-checks passed")


if __name__ == "__main__":
    demo()
