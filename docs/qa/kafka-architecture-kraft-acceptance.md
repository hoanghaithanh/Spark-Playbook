# Kafka Architecture: Brokers, Controllers & KRaft — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-20, against worktree `.claude/worktrees/issue-62-kafka-architecture-kraft`
      (branch `worktree-issue-62-kafka-architecture-kraft`, 3 commits ahead of `main`: `ffc96f8` the
      topic + `track:` grouping feature, `94de620` a route test, `204d1fc` a `track=None` bugfix) —
      issue #62, US-KC1 (`docs/requirements/kafka-curriculum.md`, lines ~166-182), plus a live check
      of the bundled D-KC1 topics-index grouping change (`docs/architecture/kafka-curriculum.md`).
Scope: US-KC1's three given/when/then acceptance criteria, verified against a real 3-broker KRaft
      cluster spawned through the app's own route (`POST /topics/kafka-architecture-kraft/spawn`),
      and D-KC1's grouping verified via a live browser screenshot of the running topics-index page —
      not by re-reading the diff or the developer's own claims alone.

## Method

**Unit suite**, re-run clean before and after this pass: `py -3 -m pytest tests/unit -q` →
**429 passed**, both times.

**Live cluster.** `docker ps -a` was empty immediately before starting. The FastAPI app was started
fresh (`py -3 -m uvicorn app.main:app --host 127.0.0.1 --port 8020`), and the cluster was spawned
through the app's own route (`POST /topics/kafka-architecture-kraft/spawn`, form body matching this
topic's own `manifest.yaml` `cluster_defaults`: 1 worker/1 core/1GB Spark footprint,
`kafka_broker_count=3`) — never `compose/cli.py` or raw `docker compose`.

**Cross-worktree collision, observed live (twice).** This pass hit the exact platform risk
`docs/architecture/worktree-cluster-isolation.md` (ADR for issue #38, still `Status: Proposed`)
documents, matching the pattern already recorded in `docs/qa/udf-pandas-udf-acceptance.md` and
`docs/qa/multi-broker-kafka-cluster-acceptance.md`:

1. Shortly after this pass's first successful spawn, `docker exec spark-kafka-1
   kafka-metadata-quorum.sh ...` returned exit 137 with no output, and the very next `docker ps -a`
   showed all 5 of this pass's containers gone with no `teardown` call in this session's own uvicorn
   log — an unattributed mid-session wipe, the same silent-teardown symptom the ADR names.
2. On the immediate respawn, `docker ps -a` came back with a **mixed** container set (one broker on
   a differently-tagged image, `sparkpb/kafka:3.9.0`, alongside two on this pass's own
   `apache/kafka:3.9.0`), and `kafka-metadata-quorum.sh` inside the mismatched broker crashed with a
   Raft `IllegalStateException` (`leader OptionalInt[3] ... inconsistent with current leader
   OptionalInt[1]`) — i.e. a genuine simultaneous cold-start race (ADR risk R-WT-3) briefly
   intermingled two different sessions' containers under the shared `sparkpb` project/container
   names. `docker inspect spark-master`'s `com.docker.compose.project.working_dir` label confirmed
   the cluster at that moment was actually owned by `.claude/worktrees/issue-58-jmx-exporter`, a
   sibling session (JMX exporter, issue #58) sharing this same Docker daemon.

Per this task's instructions and established precedent, this was treated as "wait and retry," never
forced: a background poll watched `spark-master`'s ownership label until it either freed or matched
this worktree, the human/coordinator separately confirmed `docker ps -a` had gone fully empty, and
the cluster was then respawned cleanly (confirmed via the same ownership-label check pointing at
`.claude\worktrees\issue-62-kafka-architecture-kraft\compose\rendered` before any verification
below). **This is a pre-existing platform/tooling risk already tracked under issue #38, not a defect
in the `kafka-architecture-kraft` topic** — flagged here as a second live data point (a genuine
cross-worktree collision, not merely theoretical), consistent with how the two prior acceptance
reports referenced above handled the same event.

**Notebook execution.** `content/kafka-architecture-kraft/notebook.ipynb` has 4 code cells and runs
no Spark job (Kafka-only topic; the driver container has no Docker socket access, so its CLI steps
are explicitly written as "run this in your own host terminal," not notebook cells). To exercise the
one real Python step faithfully, this pass drove the same JupyterLab kernel REST/WebSocket API
JupyterLab's own UI uses (`POST /api/kernels` + `/api/kernels/<id>/channels` websocket) against the
live cluster's `:8888`, executing all 4 code cells in file order. All 4 executed cleanly with no
errors; `notebook.ipynb` on disk was never opened/saved through the Jupyter UI, so it was never
written to during this pass.

## US-KC1, criterion 1 — KRaft quorum voters + active controller, no ZooKeeper

**PASS**, verified live. With the 3-broker cluster up:

```
$ docker exec spark-kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
ClusterId:              sparkpb-kafka-kraft-0001
LeaderId:               2
LeaderEpoch:            1
HighWatermark:          52
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   407
CurrentVoters:          [{"id": 1, ... "CONTROLLER://kafka-1:9093"}, {"id": 2, ... "CONTROLLER://kafka-2:9093"}, {"id": 3, ... "CONTROLLER://kafka-3:9093"}]
CurrentObservers:       []
```

All 3 broker node IDs (1, 2, 3) appear in `CurrentVoters`, and `LeaderId: 2` identifies broker 2 as
the active controller. `docker exec <n> printenv` on all three brokers confirmed
`KAFKA_PROCESS_ROLES=broker,controller` and an identical `CLUSTER_ID=sparkpb-kafka-kraft-0001` on
every broker — combined mode, one shared quorum. `docker ps -a --format "{{.Names}}\t{{.Image}}" |
grep -i zook` returned no matches — no ZooKeeper container anywhere in the stack.

**Criterion 1: PASS.**

## US-KC1, criterion 2 — topic creation + `--describe` shows partitions/RF/leader/ISR, explained in `concept.md`

**PASS**, verified live. Notebook cell 3 (`producer.send(TOPIC, ...)`, `TOPIC = "kraft-demo-topic"`)
executed cleanly via the kernel API, auto-creating the topic and sending 6 keyed messages across 3
distinct keys:

```
sent event-0 -> partition 1, offset 0
sent event-1 -> partition 0, offset 0
sent event-2 -> partition 2, offset 0
sent event-3 -> partition 1, offset 1
sent event-4 -> partition 0, offset 1
sent event-5 -> partition 2, offset 1
```

`kafka-topics.sh --describe` against the live cluster:

```
$ docker exec spark-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic kraft-demo-topic
Topic: kraft-demo-topic  TopicId: xyockrLCQmmKCPbC6BwYYg  PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,retention.bytes=536870912
        Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
        Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
        Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
```

`PartitionCount`, `ReplicationFactor`, and per-partition `Leader`/`Isr` are all present, matching the
criterion. `content/kafka-architecture-kraft/concept.md`'s "What to look for in this exercise"
section explicitly walks through what to read off this exact command's output (`PartitionCount`,
`ReplicationFactor`, and each partition's `Leader`/`Isr` columns, with a plain-language description
of each), and its "Why it matters" section separately defines both RF and ISR in prose — this is a
real explanation of the columns, not just a claim that the CLI output exists.

**Criterion 2: PASS.**

## US-KC1, criterion 3 — `concept.md`'s "Why it matters" contrasts KRaft against legacy ZooKeeper

**PASS**, verified by reading `content/kafka-architecture-kraft/concept.md`. The "Why it matters"
section does more than name-drop ZooKeeper — it states concretely what ZooKeeper *used to do*
("held the metadata (topic configs, partition assignments, ACLs), ran its own leader-election
protocol among *its own* nodes, and brokers watched it for changes... a second distributed system... 
with its own quorum, its own failure modes, and its own operational burden") and *why KRaft removes
it* ("The metadata that used to live in ZooKeeper's tree now lives in Kafka's own internal
`__cluster_metadata` log, replicated via Raft directly among the brokers that already exist... fewer
moving parts, one consensus mechanism instead of two, and one less thing to operate"). It closes by
telling the learner to still recognize ZooKeeper-era vocabulary for when they meet it in an older
codebase. This is a genuine before/after contrast, not a passing mention.

**Criterion 3: PASS.**

## D-KC1 — topics-index grouping (bundled infra change)

**PASS**, verified live via a real browser screenshot, not just the passing unit/route tests
(`tests/unit/test_topics_loader.py::TestTrackGrouping`, `tests/unit/test_routes.py`'s new two-track
index-rendering case, both already green in the 429-passed run above). With the app running at
`http://127.0.0.1:8020/`, `GET /` was captured full-page via `npx playwright screenshot`:

`docs/qa/screenshots/kafka-architecture-kraft/desktop-01-topics-index-kafka-grouping.png`

The rendered page shows a **"Spark"** heading followed by all 15 existing topic cards (Topics 00-14,
DAG & Lazy Evaluation through Fault Tolerance & Lineage, plus UDF vs pandas UDF), then a separate
**"Kafka"** heading below it with exactly one card — "Kafka Architecture: Brokers, Controllers &
KRaft" (labeled "Topic 01," per-track `order: 1`, independent of the Spark track's own 00-14
numbering, exactly as `docs/architecture/kafka-curriculum.md`'s D-KC1 design specifies: "Each track
is sorted independently by its own `order`"). No existing Spark topic card was altered, reordered,
or dropped by the change.

**D-KC1 grouping: PASS**, confirmed live, matching design.

## Cleanup

```
DELETE /api/kernels/f361fbe0-938a-4787-ba59-0d1fe1e1b83b  (issued; cluster teardown below
                                                            superseded it — kernel died with the
                                                            container it ran in either way)
POST /topics/kafka-architecture-kraft/teardown            -> 200, "Cluster torn down"
docker ps -a                                               -> (empty)
uvicorn process (PID 28796, port 8020)                      -> killed via taskkill, confirmed no
                                                               LISTENING entry on :8020 afterward
py -3 -m pytest tests/unit -q                               -> 429 passed (matches pre-pass baseline)
```

**Notebook cleanliness check** (all 4 code cells were executed directly against a JupyterLab kernel
via the REST/WebSocket API, never by opening/saving `notebook.ipynb` itself through the Jupyter UI —
the file on disk was never written to during this pass):

```
grep -c '"execution_count"' content/kafka-architecture-kraft/notebook.ipynb   -> 4
grep -o '"execution_count": [^,}]*' ...                                       -> "execution_count": null  (x4)
grep -c '"outputs": \[\]' content/kafka-architecture-kraft/notebook.ipynb     -> 4
git status --short                                                            -> only
                                                                                  docs/qa/screenshots/kafka-architecture-kraft/
                                                                                  (new, this pass's own
                                                                                  screenshot) added;
                                                                                  no diff inside
                                                                                  notebook.ipynb
```

All 4 code cells confirmed at `execution_count: null` with empty `outputs: []`; the only working-tree
changes this pass produced are this report and its screenshot.

## Overall recommendation

**All 3 of US-KC1's acceptance criteria PASS, live-verified against a real 3-broker KRaft cluster and
a real JupyterLab kernel**, and **D-KC1's topics-index grouping PASS**, live-verified via a real
browser screenshot of the running app — not re-derived from the diff or the developer's own claims.
The KRaft quorum-voters/active-controller evidence (all 3 broker IDs as voters, a named `LeaderId`,
no ZooKeeper anywhere in `docker ps`), the topic-describe evidence (partition count, RF, leader, ISR
per partition, matched by `concept.md`'s own column-by-column explanation), `concept.md`'s explicit
ZooKeeper-vs-KRaft contrast, and the live two-section (Spark/Kafka) topics-index rendering were all
independently reproduced this pass.

One platform-level event is worth the human's attention even though it isn't a defect in this topic:
this pass hit the exact cross-worktree collision risk `docs/architecture/worktree-cluster-isolation.md`
(issue #38) already tracks — twice in quick succession, including a genuine simultaneous cold-start
race (ADR risk R-WT-3) that briefly intermingled containers from this worktree and the sibling
issue #58 (JMX exporter) session under the same `sparkpb` project/container names. No new issue is
being filed for this, since it isn't a `kafka-architecture-kraft`-topic defect and the risk is already
tracked under #38 — but it's flagged here as a second live data point (two collisions in one pass) in
case it should move that ADR's implementation priority up.

No defects found in the `kafka-architecture-kraft` topic or the D-KC1 grouping change themselves;
nothing filed against either. This is a recommendation, not an approval — per this project's
Definition of Done, please review this report and give explicit sign-off (or flag anything that needs
a second look) before issue #62 is considered done.

## Human sign-off

**Given, 2026-07-20.** All 3 of US-KC1's acceptance criteria and the bundled D-KC1 topics-index
grouping change approved as PASS; issue #62 considered done pending remaining pipeline steps
(tech-writer, project-manager close-out).
