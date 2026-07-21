# Consumer Groups: Rebalancing & Offset Commits

## What it is

A **consumer group** is a named set of consumer instances that cooperatively
read a topic: the broker's group coordinator assigns each of the topic's
partitions to exactly one member of the group at a time, so the group as a
whole reads every partition exactly once while individual members each see
only their own slice. When membership changes -- a consumer joins, leaves,
or is declared dead (missed too many heartbeats) -- the coordinator triggers
a **rebalance**: it revokes every member's current assignment and hands out
a fresh one across whoever is left. This is the mechanism `kafka-topics-
partitions` named but didn't demonstrate: "a consumer group can have at most
one consumer per partition actively reading it at a time." This topic makes
that ceiling observable directly.

The other half of group membership is **offset commits** -- a consumer group
tracks, per partition, the offset of the next record it hasn't yet
"finished" (committed to Kafka's internal `__consumer_offsets` topic).
Committing is *how* the group remembers where it left off across restarts
and rebalances; **when** a consumer commits relative to when it actually
finishes processing a message is the whole game:

- **Manual commit** (`enable.auto.commit=False`, application calls
  `commit()` itself): the application controls exactly when the committed
  offset advances -- normally, only after a message's work is verifiably
  done. A crash before that commit means the message gets redelivered on
  restart (possible reprocessing), but nothing is ever silently skipped.
- **Auto commit** (`enable.auto.commit=True`, the client library commits on
  a timer in the background): the client commits the consumer's current
  *position* -- which already advances the instant `poll()` hands records to
  the application, **before** the application has necessarily finished
  acting on all of them. If the timer fires in that gap and the process then
  crashes, the committed offset has moved past work that was never actually
  finished -- and that work is never redelivered, because Kafka only ever
  redelivers *from* the last committed offset forward.

## Why it matters

**Partition count is a hard ceiling on a consumer group's parallelism**, not
a soft guideline. A group with fewer members than partitions has members
each covering more than one partition; scaled to exactly N members (N =
partition count), each member covers exactly one; scaled *beyond* N, the
extra members get nothing assigned at all -- they sit idle, burning a
connection and a heartbeat for zero throughput. Adding consumers past the
partition count is a common but wasted scaling instinct: it looks like
"more workers," but the coordinator has nothing left to hand them. The only
way to add real parallelism past that ceiling is to add partitions to the
topic itself (a separate, heavier operation, out of scope here) or to shard
the group's work by a different key entirely.

**The manual-vs-auto commit choice is a real reliability trade-off, not a
style preference.** Auto-commit is simpler to write and fine when occasional
silent message loss on a rare crash is acceptable (e.g. best-effort metrics).
Manual commit is more code (an explicit `commit()` call, usually after each
message or each small batch) but gives an at-least-once guarantee: a crash
can cause reprocessing of whatever was in flight, but never a silent gap.
Systems where losing a message actually costs something (billing events,
inventory changes, anything downstream trusts as complete) need manual
commit's stronger guarantee and must be built to tolerate the reprocessing
it implies (typically via idempotent downstream writes).

## What to look for in this exercise

This topic runs no Spark job -- it's consumer-group mechanics observed
directly via `kafka-python` and `kafka-consumer-groups.sh --describe`
against the live 3-broker cluster. Because rebalancing and a genuine crash
both need real, independent OS-level consumer processes (an in-notebook
thread's background heartbeat can't be killed cleanly enough to simulate a
crash -- see `driver/playbook/consumer_group.py`'s docstring), each "member"
in this notebook is its own process, started/stopped/killed from notebook
cells via that helper.

- **Fewer members than partitions (1 member, 3 partitions).** One consumer
  owns all 3 partitions. `kafka-consumer-groups.sh --describe --group
  <g>` shows one `CONSUMER-ID` against all 3 partition rows, plus the
  group's total lag (nonzero, since messages were produced before the
  consumer started draining them).
- **Members == partitions (3 members, 3 partitions).** Scaling the same
  group up to 3 members and re-describing shows each consumer owning
  *exactly* one partition -- the 1:1 mapping is the direct, observable
  consequence of the ceiling.
- **Members > partitions (4 members, 3 partitions).** Adding a 4th member
  and re-describing shows 3 members still at one partition each and the 4th
  with **zero** partitions assigned -- sitting idle, exactly as "Why it
  matters" describes. This is the concrete answer to "why can't I just add
  more consumers to go faster."
- **Manual commit, killed mid-batch, restarted.** A single manual-commit
  consumer processes several messages (committing after each), then is
  SIGKILLed mid-message -- no commit, no clean shutdown, a real crash. A
  fresh consumer in the same group picks up from the last *committed*
  offset: the message that was in flight when it crashed is redelivered and
  reprocessed; nothing earlier is repeated, nothing is missing.
- **Auto commit, same crash timing, contrasted.** An auto-commit consumer
  fetches a batch, and the auto-commit timer fires (committing the whole
  batch's end position) before the application finishes acting on every
  message in it. Killed at that point, a fresh consumer resumes *past* the
  unfinished messages -- they are never seen again. Same crash, same
  timing, worse outcome: this is auto-commit's "weaker at-most-once-on-crash
  behavior" the requirements doc calls out, made visible side by side with
  manual commit's stronger guarantee.

Form a hypothesis before each `--describe` call: with 1 / 3 / 4 members,
how many partitions will each own? And before the crash cells: which
messages, if any, do you expect never to be seen again by either consumer?
Same habit this whole tool is built around.
