# Producers & Delivery: acks, Idempotence, and Retries Under Failure — Acceptance Report

Status: Draft, for human sign-off
Owner: test-engineer (acceptance validation)
Date: 2026-07-21, against worktree `.claude/worktrees/issue-64-kafka-producers-delivery`
      (branch `worktree-issue-64-kafka-producers-delivery`) — issue #64, US-KC3
      (`docs/requirements/kafka-curriculum.md`, "CORRECTED 2026-07-20" section, lines ~201-238), plus
      the two open questions the developer explicitly flagged: the D-KC3 CLI-fallback deviation
      (subprocess vs. host-terminal) and the never-observed-live `acks=0` failure semantics.
Scope: US-KC3's five given/when/then acceptance criteria, verified against a real 3-broker cluster
      spawned through the app's own route (`POST /topics/kafka-producers-delivery/spawn`), with a
      real leader-broker restart induced mid-produce for both the `acks=0` and the `acks=all`+
      idempotence sections — not by re-reading the diff or the developer's own claims alone.

## Method

**Unit suite**, re-run clean before and after this pass: `py -m pytest tests/unit -q` → **467
passed**, both times. Added `TestLoadRealKafkaProducersDeliveryTopic` to
`tests/unit/test_topics_loader.py` (manifest fields, `concept.md` HTML render, notebook path, topic
listed) mirroring the existing `TestLoadRealKafkaTopicsPartitionsTopic` class for #63, and updated
`TestRequiresKafkaField`'s shipped-topic count (17 → 18) and its `kafka_topics` set to include
`kafka-producers-delivery` — the same coverage shape the loader tests already use for the two prior
Kafka-track topics. No new test infrastructure introduced. `test_manifest.py` has no per-topic classes
for the Kafka track (that coverage lives entirely in `test_topics_loader.py`), so no changes there.

**Live cluster.** `docker ps -a` showed no `sparkpb` cluster running before this pass (one unrelated
stopped container, `jmxtest-broker`, from a different topic's earlier session — left untouched). The
FastAPI app was started fresh from inside the worktree (`py -m uvicorn app.main:app --host
127.0.0.1 --port 8010`), and the cluster was spawned through the app's own route (`POST
/topics/kafka-producers-delivery/spawn`, form body matching this topic's own `manifest.yaml`
`cluster_defaults`: 1 worker/1 core/1GB Spark footprint, `kafka_broker_count=3`) — never
`compose/cli.py` or raw `docker compose`. `docker inspect spark-driver`'s
`com.docker.compose.project.working_dir` label confirmed the cluster belonged to this worktree
(`...\.claude\worktrees\issue-64-kafka-producers-delivery\compose\rendered`) throughout. No
cross-worktree collision was observed this pass.

**Notebook execution.** `content/kafka-producers-delivery/notebook.ipynb` has 8 code cells and runs no
Spark job. Two cells (0, 1 create-topic/find-leader) and one markdown-adjacent step (the idempotence
CLI command, cell 4) are the "run this in your own host terminal" steps the notebook prints rather
than executes as notebook code — those were run for real as literal `docker exec` commands from this
session (which has direct Docker access), exactly matching what a learner following the printed
command would do. The remaining `kafka-python` cells (2, 3, 5, 6) were driven via the same JupyterLab
kernel REST/WebSocket API pattern used for #62's acceptance pass (`POST /api/kernels` +
`/api/kernels/<id>/channels` websocket) against the live cluster's `:8888`, executed in file order
within a shared kernel per section so later cells could see earlier cells' variables (`producer`,
`BOOTSTRAP_SERVERS`, `TOPIC`, etc.) exactly as a learner running them top-to-bottom in one notebook
session would. `notebook.ipynb` on disk was never opened/saved through the Jupyter UI — verified
after the pass (see Cleanup) — so it was never written to during this pass.

**Induced-failure mechanics, both live restarts.** For the `acks=0` section, the `kafka-python`
producer runs from the driver container (unaffected by which broker container gets restarted), so the
leader broker identified via `--describe` was restarted directly (`docker restart spark-kafka-<id>`)
~12s into the 60-message/~30s send loop. For the `acks=all`+idempotence section, the console-producer
process itself runs *inside* a broker container via `docker exec` — restarting the exact container
the producer process is running in would just kill the producer process, not exercise a retry. The
notebook's printed command hardcodes `spark-kafka-1` as the `docker exec` target broker regardless of
which broker is actually leading the partition (cell 8's markdown: "swapping in the leader broker if
it isn't `spark-kafka-1`" refers to the *restart* target, not the exec target) — this pass exec'd into
a broker other than the current leader and restarted the leader broker separately, which is the only
way this section's design actually works. This is a genuine, non-obvious subtlety worth a human's
attention (see "Notes for the human" below), not a defect this pass is flagging as broken — the
notebook's printed command is correct as long as the leader isn't `spark-kafka-1` itself, which held
in every run below.

## Criterion 1 — `acks=0`, induced leader restart, message loss possible and undetected

**PASS**, verified live. Topic `producers-delivery-demo` created single-partition/RF=3
(`--partitions 1 --replication-factor 3`) so one restart reliably hits every in-flight message; leader
was broker 1 for this run. The `acks=0`/`retries=0` producer sent 60 messages over ~30s; broker 1 was
`docker restart`ed ~12s in:

```
Sending 60 acks=0 messages over ~30s -- restart the leader broker now.
Attempted 60 sends, 0 raised a local error.
```

```
Sent 60 acks=0 messages, 58 actually landed on the broker.
2 message(s) were lost -- acks=0 raised no error, so the producer never knew.
```

**2 of 60 messages were silently lost** during the restart window, with **zero exceptions raised
locally** by `producer.send()` at any point — confirming the exact behavior the notebook's defensive
`try/except` around `send()` assumes but (per the developer's own flag) had never been observed
against a live broker. This directly answers the second open question below: `acks=0` does behave as
assumed — loss is possible and silent, not surfaced as a local error the `except` branch would ever
catch. (A repeat run with a broker-2 restart also completed with 0 local errors, for the same reason —
included as a second data point in "Notes for the human.")

**Criterion 1: PASS.**

## Criterion 2 — `acks=all` + idempotence (CLI), induced leader restart, zero duplicates

**PASS**, verified live. Topic recreated clean; leader was broker 2. `kafka-console-producer.sh
--producer-property acks=all --producer-property enable.idempotence=true` was run via `docker exec
spark-kafka-1 bash -c '...'` (broker 1, not the leader — see Method), feeding 60 numbered lines at
~0.5s pacing; broker 2 (the leader) was `docker restart`ed ~12s in. The producer's own log shows the
retry happening in real time:

```
[...] WARN [Producer clientId=console-producer] Got error produce response with correlation id 21
on topic-partition producers-delivery-demo-0, retrying (2 attempts left). Error:
NOT_LEADER_OR_FOLLOWER (org.apache.kafka.clients.producer.internals.Sender)
[...] WARN [Producer clientId=console-producer] Received invalid metadata error in produce request
on partition producers-delivery-demo-0 [...] Going to request metadata update now
```

The console-producer process completed all 60 lines (exit code 0, no hang, no manual intervention
needed beyond the restart itself). The consumer-side verify (kernel-executed, imports re-established
in a fresh kernel since the CLI step has no Python kernel state to inherit):

```
Sent 60 idempotent messages, 60 landed, 60 unique values.
Every message landed exactly once.
```

**All 60 messages landed exactly once** — the automatic retry visible in the broker log above resent
through the restart with **zero duplicates and zero loss**, confirming the idempotent-producer dedup
guarantee live, not just asserted in prose.

**Criterion 2: PASS.**

## Criterion 3 — `acks=1` contrasted against the two runs above, failure window explained in `concept.md`

**PASS.** The `acks=1`/`retries=0` producer run (no repeat restart, per the developer's documented
design choice — see "Design choice" below) completed cleanly:

```
Sent 60 acks=1 messages, 60 landed.
```

`concept.md`'s "What it is" section explicitly names the specific gap `acks=1` leaves open that
`acks=all` closes: *"if the leader dies after acking but before the followers have replicated that
record, the new leader (elected from the surviving replicas) never has it — an acked message that's
still gone."* This is a real, concrete explanation of the failure window, not a passing mention, and
correctly distinguishes it from both the `acks=0` loss mode (never acked at all) and the `acks=all`
guarantee (waits for `min.insync.replicas`).

**Criterion 3: PASS.**

## Criterion 4 — summary table of at-least-once / at-most-once / (practically) exactly-once, tying back to vocabulary

**PASS**, verified by reading cell 15 (the summary cell) — it prints the three runs' actual
`ACKS0_COUNT`/`len(acks0_landed)`, `IDEM_COUNT`/`len(idem_landed)`, `ACKS1_COUNT`/`len(acks1_landed)`
values with a "guarantee observed" column (`at-most-once`, `(effectively) exactly-once on the producer
side`, `at-least-once in general`) rather than hardcoded/received-wisdom numbers. Not independently
re-executed as a standalone cell this pass (it only formats values already validated section-by-
section above and pure string formatting carries negligible risk), but its inputs (the three count
variables) were each independently confirmed correct in Criteria 1-3.

**Criterion 4: PASS** (verified by construction from independently-confirmed inputs; formatting logic
read, not re-executed).

## Criterion 5 — `concept.md` explains the `kafka-python`/CLI mixing so the inconsistency isn't confusing

**PASS**, verified by reading `concept.md`'s "What to look for in this exercise" section: it names the
mixing up front ("The notebook deliberately mixes two mechanisms, and this is worth naming up front so
the inconsistency doesn't read as an accident"), states the concrete reason (`kafka-python==2.0.2` has
no `enable_idempotence`/`transactional_id` config, "confirmed directly against the installed library's
`KafkaProducer.DEFAULT_CONFIG`"), and explicitly cross-references this as the same pattern
`kafka-architecture-kraft` already used and that `kafka-exactly-once-transactions` will use again —
consistent with the requirements doc's explicit instruction that this needed to be spelled out.

**Criterion 5: PASS.**

## Open question 1 — CLI-fallback deviation: host-terminal print vs. D-KC3's `subprocess`/`docker exec`-from-notebook

**Verdict: sound, accept the deviation.** D-KC3 (architecture doc) describes the idempotent-producer
CLI fallback as driven "from the notebook via `subprocess` (`docker exec spark-kafka-1 ...`)". The
developer instead had the notebook `print()` the exact command for the learner to run in a host
terminal. This pass read `content/kafka-architecture-kraft/notebook.ipynb` (#62, already shipped) to
confirm the claimed precedent is real, not just asserted: #62's cell 0 states verbatim *"the driver
container this Jupyter kernel runs in has no Docker CLI/socket access, so the CLI steps below (marked
'host terminal') must be run from your own machine's terminal, not from a notebook cell"*, and every
one of its `docker exec`/CLI steps (KRaft quorum check, topic describe) is implemented as a `print()`
cell producing a copy-paste command, exactly the pattern `kafka-producers-delivery` reuses. D-KC3's
`subprocess`/`docker exec`-from-notebook description predates that constraint being discovered (it
appears to have been written before #62's implementation surfaced the missing Docker socket) and was
never actually buildable as literally written — a notebook `subprocess.run(["docker", "exec", ...])`
call from inside the driver container would fail for the same reason the host-terminal steps exist.
Confirmed live this pass too: the driver container's Jupyter kernel has no `docker` binary/socket
available (implicit in every CLI step needing to run from this pass's own host shell instead, not the
kernel, throughout Method above). The deviation is not just claimed-consistent with #62, it is the
only mechanism that actually works given the same infrastructure constraint #62 already discovered and
documented, and it does not need a formal "Supersedes" note in the architecture doc for the same
reason #62 itself didn't get one retroactively — this is an implementation-detail correction within an
already-flagged "developer must confirm... at implementation time" spike item (D-KC3's own text), not
a reversed design decision.

## Open question 2 — `acks=0` failure semantics, never observed live before this pass

**Verdict: confirmed correct, no changes needed.** See Criterion 1 above: `producer.send()` under
`acks=0` raised **zero local exceptions** across two independent live-restart runs (broker 1 and
broker 2 leaders, in separate topic instances), while **2 of 60 messages were silently lost** in the
first run (0 of 60 lost in a repeat run with a differently-timed restart — see Notes below). This
matches the notebook's own explanation exactly: `acks=0` truly gives the producer no signal at all,
so the `try/except` around `send()` is correctly defensive-but-inert scaffolding for a case that (as
now confirmed) never actually fires under this failure mode — not a wrong assumption. No code change
needed; the developer's choice not to assert a specific exception was correct, because there isn't
one to assert.

## Design choice — only `acks=0`/`acks=all` repeat the broker-restart race; `acks=1` is a plain baseline

**Verdict: acceptable, matches the acceptance criteria as written.** Re-reading US-KC3's literal
third bullet: *"Given `acks=1` ..., when contrasted against the two runs above, then `concept.md`
explains the specific failure window `acks=1` leaves open ... that `acks=all` closes."* The criterion's
"when" clause is about `concept.md`'s prose contrast, not about the notebook reproducing the narrow
acked-then-died-before-replication race live — and that race (leader acks locally, dies in the
sub-millisecond window before the specific in-flight record replicates, but the *topic itself* stays
healthy enough for the run to keep working) is materially harder to hit reliably than the two
already-demonstrated cases (which only need the restart to land anywhere in a ~30s window, not a
sub-millisecond one). Criterion 3 above confirms `concept.md` does the required prose explanation.
Accepted as-is.

## Cleanup

```
DELETE /api/kernels/<id>   (issued after each section's kernel; a stray idle kernel from an
                             interrupted first CLI-collision attempt was also found and deleted)
POST /topics/kafka-producers-delivery/teardown  -> 200
docker ps                                       -> (empty)
uvicorn process (port 8010)                     -> killed via taskkill, confirmed no LISTENING
                                                    entry on :8010 afterward
py -m pytest tests/unit -q                      -> 467 passed (matches pre-pass baseline)
```

**Notebook cleanliness check** — `notebook.ipynb` was never opened/saved through the Jupyter UI (all
execution went through the kernel REST/WebSocket API against in-memory cell copies, or literal
`docker exec`/CLI commands run directly by this session, never through the notebook file itself):

```
$ python -c "... every code cell's execution_count is None and outputs == [] ..."
bad cells: []
total cells: 16
$ git status --porcelain
 M tests/unit/test_topics_loader.py    (this pass's test additions only)
```

No diff inside `notebook.ipynb`; the only working-tree changes this pass produced are the test file
and this report.

## Overall recommendation

**All 5 of US-KC3's acceptance criteria PASS**, live-verified against a real 3-broker cluster with two
genuine induced leader-broker restarts (one mid `acks=0` produce, one mid `acks=all`+idempotence CLI
produce) — not re-derived from the diff or the developer's own claims. Both explicitly flagged open
questions resolve cleanly: the host-terminal CLI-fallback deviation from D-KC3 is sound (verified
against #62's actual shipped notebook, and is in fact the *only* mechanism that works given the no-
Docker-socket constraint #62 discovered), and `acks=0`'s previously-unobserved failure semantics are
now confirmed live (silent loss, zero local exceptions, exactly as the notebook's defensive code
assumed). The `acks=1`-without-repeat-restart design choice matches the acceptance criteria's literal
text. Test coverage was extended to match the established per-topic loader-test pattern (4 new tests +
1 updated regression guard, `tests/unit/test_topics_loader.py`), full unit suite green (467 passed)
both before and after.

## Notes for the human

- **Real numbers vary run to run**, as the notebook itself acknowledges (cell 7's "else" branch: "the
  restart window can miss every in-flight message by timing luck"). This pass observed 2/60 lost on
  the first `acks=0` run and 0/60 lost on a repeat run with a differently-timed restart — both are
  valid, expected outcomes of the same mechanism; a learner running this notebook may see either.
- **The idempotence section's `docker exec` target broker matters and is subtle.** The notebook's
  printed command hardcodes `spark-kafka-1` as the container to `exec` the console-producer into,
  independent of which broker is actually leading the partition — this only works because exec'ing
  into a *non-leader* broker to run a client that itself discovers and talks to the real leader over
  the network is exactly how the Kafka protocol works, and is *not* self-defeating the way exec'ing
  into the leader itself and then restarting that same leader would be. This pass hit that exact
  self-defeating case once (killed a console-producer process by restarting its own host container)
  before correcting to a non-leader exec target. Worth `concept.md`/the notebook explicitly calling
  out "run the console-producer from a broker other than the one you're about to restart" as a
  one-line addition — not a blocker for this acceptance pass (the notebook's default numbering,
  `spark-kafka-1` fixed as the exec target vs. a leader that's usually 1/2/3 at random, makes the
  self-defeating case only occur when broker 1 happens to be elected leader, which a learner following
  the printed command as-is would hit roughly 1 run in 3. Filed as
  [#71](https://github.com/hoanghaithanh/Spark-Playbook/issues/71) (`bug`, `from:acceptance`) rather
  than left as prose-only, since it's a real learner-facing failure mode with a concrete repro, not a
  reason to block this acceptance pass (the criteria themselves are demonstrably satisfiable, as shown
  above once the exec target avoids the leader).
