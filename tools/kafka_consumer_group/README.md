# tools/kafka_consumer_group — consumer-group member CLI

One consumer-group member per OS process, for the `kafka-consumers-groups`
notebook (US-KC4, issue #65) to scale a group up/down and simulate a real
crash (SIGKILL). Design rationale is in `member.py`'s module docstring: a
notebook-thread heartbeat can't be killed cleanly enough to fake a crash, so
every member is a genuinely independent process, same idiom
`tools/kafka_producer/produce.py` / `driver/playbook/producer.py` already
established for this repo's Kafka tooling.

## Prerequisites

- `kafka-python` — already baked into the `sparkpb/spark` image
  (`compose/Dockerfile.spark`) for in-cluster runs.

## Usage

**In-cluster** (inside the `spark-driver` container):

```bash
docker exec spark-driver python /workspace/tools/kafka_consumer_group/member.py \
    --group cg-demo --label m1 --topic consumer-groups-demo
```

Runs until `Ctrl-C`/`SIGTERM` (graceful: commits/closes normally), a
`SIGKILL` (a real, uncatchable crash -- no commit, no clean group leave), or
for exactly `--max-messages` messages if given.

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--bootstrap` | `kafka-1:9092,kafka-2:9092,kafka-3:9092` | Comma-separated Kafka bootstrap servers. |
| `--topic` | `consumer-groups-demo` | Topic to consume. |
| `--group` | (required) | Consumer group id -- shared across every member in the group. |
| `--label` | `member` | Short id printed on every stdout line. |
| `--commit-mode` | `manual` | `manual` commits once per processed message; `auto` relies on kafka-python's own background auto-commit. |
| `--auto-commit-interval-ms` | `5000` | Only used in `--commit-mode auto`. |
| `--process-delay` | `0.05` | Seconds of simulated work per message. |
| `--batch-size` | `10` | Max records per `poll()` call. |
| `--max-messages` | (unset) | Stop cleanly after exactly this many messages instead of running until killed. |
| `--self-check` | off | Publish a small batch, consume+commit it, and assert the committed offset matches — then exit. |

### Stdout format

```
[m1] started topic='consumer-groups-demo' group='cg-demo' commit_mode=manual
[m1] ASSIGNED [0, 1, 2]
[m1] PROCESSED key=None offset=0 partition=0 total=1
...
[m1] REVOKED [0, 1, 2]
[m1] ASSIGNED [1]
```

The notebook drains a spawned process's stdout in a background thread and
parses these `ASSIGNED`/`REVOKED`/`PROCESSED` lines instead of needing its
own thread-safe way to inspect another process's consumer state.

## Self-check

```bash
python tools/kafka_consumer_group/member.py --bootstrap 127.0.0.1:9092 --self-check
```

Publishes a small batch under one fixed key (so it lands on a single
partition, deterministic), consumes and manually commits every message, then
opens a fresh consumer in the same group and asserts the committed offset
equals the message count — the smallest thing that fails if the
poll/process/commit loop or the manual-commit offset math breaks. Requires a
live broker.

## Notebook usage (`driver/playbook/consumer_group.py`)

A thin wrapper that launches `member.py` as its own background OS process:

```python
import sys
sys.path.insert(0, "/workspace")
from driver.playbook import consumer_group

m1 = consumer_group.start(group="cg-demo", label="m1")
...
consumer_group.stop(m1)    # graceful (SIGTERM)
consumer_group.crash(m1)   # SIGKILL -- no commit, no clean group leave
```
