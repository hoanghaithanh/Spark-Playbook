# Multi-Broker Kafka Cluster + Drawer Config — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-20, against `main` at commit `86a3708` (`feat(kafka): multi-broker topology + drawer
      config (#56)`) — issue #56, US-MBK1 (sub-story a of 5), milestone #15 (`v1.2 — Multi-Broker
      Kafka Cluster & Monitor`).
Scope: `docs/architecture/multi-broker-kafka-cluster.md` D-MBK1–D-MBK4,
      `docs/requirements/multi-broker-kafka-cluster.md` US-MBK1's given/thens — verified against
      real Docker clusters spawned through the app's own routes (`POST /topics/{id}/spawn`), not
      `compose/cli.py` or `docker compose` directly, and not a re-read of the diff or the unit
      suite alone. This is independent re-confirmation of the developer's own live-cluster claims
      in the commit message, per this repo's "trust but verify" discipline — every scenario below
      was re-run from scratch by this pass, not taken on the developer's word.

## Method

**Unit suite**, re-run clean before and after this pass: `py -3.9 -m pytest tests/unit -q` →
**409 passed, 0 failed** both times. See "Discrepancy from the developer's report" below for a
skip-count mismatch worth flagging — not a regression.

**Live cluster.** `docker ps -a` was empty before starting and empty at the end. Two real FastAPI
app instances were used:
- **Primary** (`d:\Workplace\repos\Spark-Playbook`), `py -3.9 -m uvicorn app.main:app --host
  127.0.0.1 --port 8010` — drove every spawn/teardown/RF/ISR/kill scenario below, always via
  `POST /topics/{id}/spawn` / `/teardown`, never `compose/cli.py` or raw `docker compose`.
- **Foreign worktree** (`.claude/worktrees/51-udf-pandas-udf`, a real pre-existing git worktree
  already in this repo, not created for this pass), `py -3.9 -m uvicorn app.main:app --host
  127.0.0.1 --port 8011` — used solely to drive step 7's cross-worktree guard check with a genuine
  second `RENDERED_DIR`/working directory, the same technique this repo's prior acceptance passes
  reference (a second real app instance pointed at a different worktree root, not a mock).

**Scratch manifest.** No shipped topic currently sets `requires_kafka: true` (confirmed: all 14
existing `content/*/manifest.yaml` files say `requires_kafka: false`), so a temporary,
not-committed-to-`content/` scratch topic (`content/_qa-scratch-kafka-mbk/`, `requires_kafka:
true`, minimal `manifest.yaml` + `concept.md` + empty `notebook.ipynb`) was created solely to
drive the D1-reversal's "manifest pre-checks but doesn't override an uncheck" half through the real
spawn route — same technique `kafka-streaming-infra-acceptance.md`'s `_qa-scratch-kafka` used.
Deleted at the end of this pass (see Cleanup).

## Given/then 1 — drawer has a Kafka section (Include Kafka checkbox + broker-count 1-5 default 3)

**PASS, by inspection + live spawn.** `app/web/templates/fragments/_cluster_form.html` renders a
`<fieldset><legend>Kafka</legend>` section with `name="include_kafka"` (checkbox) and
`name="kafka_broker_count"` (`min`/`max` from `config.KAFKA_BROKER_COUNT_RANGE = (1, 5)`,
`value="{{ defaults.kafka_broker_count }}"`, default 3 per `config.DEFAULTS["kafka_broker_count"]`)
— mirrors the `worker_count` field's min/max/default pattern exactly, as required. Live-exercised
implicitly by every spawn below (the same form fields are what every `POST .../spawn` call sends).

## Given/then 2 — D1 reversal, both directions, through the real app

### 2a — `requires_kafka: true` manifest, "Include Kafka" unchecked → honored, not overridden

Spawned the scratch topic (`requires_kafka: true`) via `POST /topics/_qa-scratch-kafka-mbk/spawn`
with `include_kafka` **omitted** from the submitted form (the real-world equivalent of an unchecked
HTML checkbox, which browsers never submit). Result:

```
NAMES            IMAGE
spark-driver     sparkpb/spark:4.0.3
spark-worker-1   sparkpb/spark:4.0.3
spark-master     sparkpb/spark:4.0.3
```

No `spark-kafka-*` container, despite the manifest's `requires_kafka: true`. **PASS** — the
manifest only pre-checks the box in the rendered form; it does not gate the actual spawn once the
form is submitted unchecked. Torn down cleanly.

### 2b — `requires_kafka: false` (or omitted) manifest, "Include Kafka" manually checked → Kafka spawns anyway

Spawned the real, existing `aqe` topic (`requires_kafka: false`, one of the 14 shipped topics,
picked arbitrarily — not a streaming topic) via `POST /topics/aqe/spawn` with
`include_kafka=true&kafka_broker_count=1`. Result:

```
NAMES            IMAGE                 PORTS
spark-driver     sparkpb/spark:4.0.3   127.0.0.1:4040-4042->4040-4042/tcp, 127.0.0.1:8888->8888/tcp
spark-kafka-1    apache/kafka:3.9.0    127.0.0.1:9092->29092/tcp
spark-worker-1   sparkpb/spark:4.0.3
spark-master     sparkpb/spark:4.0.3
```

Kafka spawned on a non-streaming topic purely from the manual checkbox — confirming Kafka is
available on any topic, not gated to streaming topics only. **PASS**, both directions of the D1
reversal confirmed live.

## Given/then 3 — one Spawn/Teardown action brings up N brokers, no second lifecycle control

**PASS**, confirmed by every spawn in this pass: a single `POST /topics/{id}/spawn` call with
`include_kafka=true&kafka_broker_count=N` brought up the whole `sparkpb` project including N
broker containers in one `docker compose up`, and a single `POST .../teardown` tore all of it down
together — no separate Kafka route exists (`app/web/routes/topics.py` has exactly `/spawn` and
`/teardown`, unchanged in shape from before this feature).

## Given/then 4 — combined broker+controller mode, shared quorum voters, single fixed `CLUSTER_ID`

Spawned `partitioning-shuffle` with `include_kafka=true&kafka_broker_count=3`:

```
NAMES            IMAGE                 PORTS
spark-driver     sparkpb/spark:4.0.3   127.0.0.1:4040-4042->4040-4042/tcp, 127.0.0.1:8888->8888/tcp
spark-worker-1   sparkpb/spark:4.0.3
spark-kafka-3    apache/kafka:3.9.0    127.0.0.1:9292->29092/tcp
spark-kafka-1    apache/kafka:3.9.0    127.0.0.1:9092->29092/tcp
spark-kafka-2    apache/kafka:3.9.0    127.0.0.1:9192->29092/tcp
spark-master     sparkpb/spark:4.0.3
```

Exactly 3 `spark-kafka-N` containers, each a distinct host-published loopback port (9092/9192/9292)
— **PASS** on the port surface.

`docker exec spark-kafka-{1,2,3} printenv` confirmed `KAFKA_PROCESS_ROLES=broker,controller` on
every broker (combined mode, no separate controller quorum), `CLUSTER_ID=sparkpb-kafka-kraft-0001`
identical on all three, and `KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093`
identical on all three (verified via the same `printenv` pull used for the RF/min-isr check below).
**PASS.**

## Given/then 5 — per-broker reachability (`kafka-{i}:9092` in-cluster, own loopback host port)

Already evidenced by given/then 4's `docker ps` output (distinct 9092/9192/9292 host ports, broker
1 keeping the pre-existing 9092). Load-bearing correctness confirmed further by given/then 8 below
(leaders spread across all 3 brokers at RF=3, and producing successfully via broker 1's bootstrap
against a partition led by broker 3 after broker 2's kill) — a host client bootstrapping against
9092 genuinely reaches whichever broker leads a partition, not just broker 1. **PASS.**

## Given/then 6 — RF/min-isr formulas scale with broker count (`min(3,N)` / `2 if N>=2 else 1`)

**At N=3** (same spawn as given/then 4): created a topic explicitly at RF=3 and described it:

```
$ docker exec spark-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic mbk-qa-topic
Topic: mbk-qa-topic  TopicId: bo42YG9YRzuflzfR46vedA  PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,retention.bytes=536870912
        Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
        Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
        Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

`ReplicationFactor: 3`, full 3-member ISR per partition, `min.insync.replicas=2` on the topic
config — matches `min(3,3)=3` / `2 (N>=2)`. Leaders are naturally spread across all 3 brokers
(1, 2, 3), independently corroborating given/then 5's per-broker reachability. **PASS.**

**At N=1** (boundary, separate `aqe` spawn from given/then 2b): `docker exec spark-kafka-1
printenv` showed `KAFKA_DEFAULT_REPLICATION_FACTOR=1`, `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`,
`KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1`, `KAFKA_MIN_INSYNC_REPLICAS=1`,
`KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1` — matches `min(3,1)=1` / `1 (N<2)`. A topic created at
RF=1 confirmed: `ReplicationFactor: 1  Configs: min.insync.replicas=1 ... Leader: 1 Replicas: 1
Isr: 1`. **PASS**, both the N=3 default and the N=1 lower boundary verified live.

## Given/then 7 — resource ceiling: `KAFKA_MEMORY_GB * kafka_broker_count`

Submitted `worker_count=4, worker_cores=2, worker_memory_gb=6` (Spark-only total: `1 + 4*6 + 2 =
27GB`, under the 32GB ceiling alone) with `include_kafka=true&kafka_broker_count=3` (Kafka
contribution: `2 * 3 = 6GB`, total `33GB`):

```
HTTP:200
"...requested config totals ~33GB, exceeding the 32GB sanity ceiling (PLAN.md §2 resource-ceiling check)"
```

Rejected with the exact predicted total (`27 + 6 = 33`), and `docker ps -a` stayed empty — no
containers spawned for the rejected attempt. This directly demonstrates the per-broker-scaled
formula, not a flat reservation: the same Spark config (27GB) alone is under budget, and it's
specifically the `2GB * 3 brokers` Kafka contribution that pushes it over. **PASS.**

## Given/then 8 — #38 cross-worktree guard unaffected by broker count

With a real 3-broker Kafka-included cluster running (owned by the primary worktree, `spawn`ed via
`aqe` topic on port 8010), a second, genuinely separate app instance was started from this repo's
existing git worktree `.claude/worktrees/51-udf-pandas-udf` (port 8011) and attempted its own spawn:

```
POST http://127.0.0.1:8011/topics/aqe/spawn (include_kafka=true, kafka_broker_count=2) -> HTTP 200
"...already running, owned by another worktree (d:\workplace\repos\spark-playbook\compose\rendered).
Refusing to spawn/teardown -- it would tear down that worktree's live cluster. Tear it down there
first, or wait."
```

`docker ps` immediately after the foreign attempt showed the original 6 containers unchanged (still
exactly `spark-master`, `spark-worker-1`, `spark-driver`, `spark-kafka-1/2/3`) — the foreign spawn
was refused outright, no teardown or interference occurred, and the guard fired identically to the
non-Kafka case despite the running cluster having 3 broker containers rather than 0. **PASS** —
confirms `running_owner()`/`list_container_ids()`'s project-label scoping is genuinely
broker-count-agnostic, not just "unaffected by construction" per the code read.

## Given/then 9 — live 3-broker spawn, `kafka-topics.sh --describe` reports RF=3 / 3-member ISR

Covered by given/then 6's N=3 evidence above. **PASS.**

## Given/then 10 — `docker stop spark-kafka-2` → leader re-election, ISR shrink, `acks=all` still succeeds

Against the same 3-broker cluster (given/then 4/6/9), broker 2 was killed (`docker kill
spark-kafka-2`, the harsher case per the task brief) and `kafka-topics.sh --describe` re-run from
broker 1 (still live):

```
$ docker kill spark-kafka-2
spark-kafka-2
$ docker exec spark-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic mbk-qa-topic
Topic: mbk-qa-topic  PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,...
        Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,3
        Partition: 1  Leader: 3  Replicas: 2,3,1  Isr: 3,1
        Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1
```

Partition 1's leader re-elected from broker 2 (now dead) to broker 3 (a surviving broker), and
every partition's ISR shrank from `{1,2,3}` to a 2-member set excluding 2 — exactly the given/then's
claim. Then produced with `acks=all` while broker 2 was still down:

```
$ docker exec -i spark-kafka-1 kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic mbk-qa-topic --producer-property acks=all <<< "qa-message-1/2/3-acks-all"
(producer exit: 0)
$ docker exec spark-kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic mbk-qa-topic --from-beginning --max-messages 3 --timeout-ms 10000
qa-message-1-acks-all
qa-message-2-acks-all
qa-message-3-acks-all
Processed a total of 3 messages
```

All 3 messages accepted and consumed back — `acks=all` writes against `min.insync.replicas=2`
genuinely succeed with one broker down, the concrete demonstration this given/then asks for.
**PASS**, all three sub-claims (leader re-election, ISR shrink, surviving writes) independently
verified live, not assumed from the ISR math alone.

## Discrepancy from the developer's report

The developer's commit message claims **"409 passed, 0 failed, 2 skipped."** This pass's own
clean re-run of the exact same suite, both before and after live testing, produced:

```
$ py -3.9 -m pytest tests/unit -q
409 passed in 8.86s
```

**0 skipped**, not 2 — confirmed with `-rs` (no skip reasons reported) and a repo-wide grep for
`skipif`/`importorskip`/`pytest.skip` across `tests/unit`, which returned no matches at all. There
is no skip marker anywhere in this test directory on this host, so "2 skipped" does not reproduce.
This is the same class of discrepancy `kafka-streaming-infra-acceptance.md` (#50) already flagged
for its own "393 passed, 2 skipped" claim (a host-dependent skip that didn't reproduce on that pass
either) — **not treated as a defect**, since the count that matters (409 passed, 0 failed) matches
exactly, and a lower skip count than claimed cannot hide a failure. Flagging for the human's
awareness since it's a small, second instance of the same reporting pattern, not investigated
further here (out of scope for a topology/drawer-config acceptance pass to chase a test-runner
environment difference).

No other discrepancy found — every other live-cluster claim in the developer's commit message (RF=3
with 3-member ISR, leader re-election + ISR shrink on kill, `acks=all` writes surviving one broker
down, the #38 guard unaffected by broker count) was independently reproduced from scratch above,
not taken on the developer's word.

## Coverage review

`tests/unit/test_renderer.py`, `tests/unit/test_compose_cli.py`, and `tests/unit/test_routes.py`
(per the commit's diff stat) already cover the D1-reversal threading, broker-count range
validation, and the RF/min-isr formulas at the unit level — this pass's job was live behavior
against real Docker clusters, and no gap was found there that a unit test would catch better than
the live evidence above. No new unit tests added; none needed for this sub-story.

## Cleanup confirmation

- `docker ps -a` returned empty before starting, empty after every teardown in this pass (7
  spawn/teardown cycles total), and empty at the very end.
- Both `uvicorn` processes started for this pass (primary port 8010, foreign-worktree port 8011)
  were killed (`taskkill /F`); both ports confirmed free (`netstat` showed no `LISTENING` entry on
  either afterward).
- The scratch topic folder `content/_qa-scratch-kafka-mbk/` (created solely for this pass) was
  deleted (`rm -rf`).
- No changes were made inside the foreign worktree (`.claude/worktrees/51-udf-pandas-udf`) — it was
  only used to run a second, unmodified app instance; `git status --short` there shows only its own
  pre-existing, unrelated in-progress work (issue #51), nothing added by this pass.
- `git status --short` in the primary worktree shows only the pre-existing untracked
  `.claude/worktrees/` (a real git-worktree artifact predating this pass) — no stray scratch files,
  no modified tracked files from this pass.
- No notebook was executed during this pass (the scratch topic's notebook was an empty stub, never
  opened/run through Jupyter), so the notebook-cleanliness convention (CLAUDE.md) doesn't apply
  here.
- Unit suite re-confirmed clean after cleanup: `py -3.9 -m pytest tests/unit -q` → **409 passed, 0
  failed** (matches the pre-test baseline exactly, confirming the scratch topic's removal restored
  the expected 14-shipped-topic count the suite asserts on).

## Recommendation

This is a **recommendation, not final sign-off** — the human should review and give final sign-off
before issue #56 (US-MBK1) is considered done.

- Given/thens 1-10 (drawer section, both directions of the D1 reversal, single spawn/teardown
  action, combined broker+controller mode with shared quorum/CLUSTER_ID, per-broker reachability,
  RF/min-isr scaling at N=3 and the N=1 boundary, the resource-ceiling formula's live rejection, the
  #38 guard's broker-count-agnostic refusal, and the full RF=3/broker-kill/ISR-shrink/`acks=all`
  demonstration) — **all PASS**, live-verified against real Docker clusters spawned through the
  app's own routes, not `compose/cli.py` and not a code read-through.
- One discrepancy flagged for awareness (developer's claimed "2 skipped" doesn't reproduce; not a
  defect, doesn't affect the pass/fail count).
- No GitHub issues filed — no defects found in US-MBK1's actual scope.
