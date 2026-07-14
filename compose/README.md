# Spark Playbook — Phase 0 cluster harness

Manually-operated Spark Standalone cluster: master + N workers + a driver/Jupyter
container, all on a dedicated Docker bridge network. No web app required (US-0.5).
See `PLAN.md` §1-§2 and §5 (Phase 0) for the design this implements.

## Prerequisites

- Docker Desktop with the WSL2 backend, Compose v2 (`docker compose ...`, not the
  old `docker-compose`) on PATH — see PLAN.md D1.
- Python 3.9+ on the host (WSL2), with the `jinja2` package:
  ```bash
  pip install jinja2
  ```
- Run everything from the repo root, or `cd compose` and adjust paths accordingly.
  Commands below assume the repo root.

## 1. Build the image (once, and after editing Dockerfile.spark)

```bash
bash compose/build.sh
```

Builds `sparkpb/spark:4.0.3` — a thin layer of JupyterLab + PyArrow + pandas on
top of the official `apache/spark:4.0.3-scala2.13-java17-python3` image. The same
image is reused for master, worker, and driver roles; only the container command
differs.

## 2. Render the compose stack

```bash
python compose/cli.py render
```

Uses the defaults from PLAN.md §2 (3 workers x 2 cores/4GB, driver 2GB, shuffle
partitions 200, AQE off). Override any of them, e.g.:

```bash
python compose/cli.py render --worker-count 5 --worker-memory-gb 8 --aqe
```

Writes `compose/rendered/docker-compose.yml` and
`compose/rendered/spark-defaults.conf` (gitignored — see PLAN.md §4).

## 3. Bring the cluster up

```bash
python compose/cli.py up
```

Tears down any previous `sparkpb` stack first (awaited to completion — see
PLAN.md §2/R4), then runs `docker compose -p sparkpb up -d`.

## 4. Wait for the workers to register

```bash
python compose/cli.py wait-for-ready
```

Polls `http://localhost:8080/json/` every ~2s until `aliveworkers` equals the
worker count (default 3), with a 60s timeout (US-0.1). If you rendered a
non-default worker count, pass it again:

```bash
python compose/cli.py wait-for-ready --worker-count 5 --timeout 90
```

**What you should see at this point, opening `http://localhost:8080` in a
browser:** the Spark Master UI, "Workers" table listing exactly your configured
worker count, each `ALIVE`, with the cores/memory you set.

## 5. Run the smoke test (US-0.3)

From the driver container:

```bash
docker exec -it spark-driver /opt/spark/bin/spark-submit /workspace/compose/smoke_test.py
```

**Git Bash on Windows note:** Git Bash rewrites leading-`/`-style arguments as
Windows paths, which breaks the container-side paths above. If you see an error
like `stat C:/Program Files/.../opt/spark/bin/spark-submit: no such file or
directory`, prefix the command with `MSYS_NO_PATHCONV=1`:

```bash
MSYS_NO_PATHCONV=1 docker exec spark-driver /opt/spark/bin/spark-submit /workspace/compose/smoke_test.py
```

(or open `http://localhost:8888` — JupyterLab, no token — and paste the body of
`compose/smoke_test.py` into a notebook cell; the whole repo is mounted at
`/workspace` inside every container.)

**What you should see:**

- Console output ending in `PASS: at least one stage reports nonzero shuffle
  read/write bytes.`
- `http://localhost:4040` — the driver's application UI, while (or right after)
  the job runs: **Jobs** tab shows the `spark-playbook-smoke-test` job, **Stages**
  tab shows a stage with nonzero **Shuffle Read** / **Shuffle Write** columns,
  **SQL** tab shows the `groupBy`/`agg` query plan, **Executors** tab shows tasks
  completed across more than one executor (one per worker in the default config).
- `http://localhost:4040/api/v1/applications/<app-id>/stages` — raw JSON with
  `shuffleReadBytes` / `shuffleWriteBytes` / `numTasks` per stage (this is what
  the script checks automatically).

## 6. Tear down

```bash
python compose/cli.py down
```

Runs `docker compose -p sparkpb down --remove-orphans`. Safe to run even if
nothing is up.

## Unguided practice (US-0.5)

Once step 4 succeeds, open `http://localhost:8888` directly and start a new
notebook. No connection boilerplate is needed:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("scratch").getOrCreate()
spark.range(1000).groupBy((spark_range_col := "id")).count()  # or any PySpark code
```

`spark.master`, `spark.driver.host/bindAddress/port`, `spark.sql.shuffle.partitions`,
and `spark.sql.adaptive.enabled` are all pre-set via the rendered
`spark-defaults.conf` mounted into the driver — see `compose/templates/spark-defaults.conf.j2`.

## Notes / deviations from a literal reading of PLAN.md

- **Worker resource knobs** (`SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY`) are set
  as compose `environment:` entries on each `spark-worker-N` service, not baked
  into `spark-defaults.conf` — this is the mechanism the official `apache/spark`
  image's standalone `Worker` class actually reads for per-worker resource limits.
  `spark-defaults.conf.j2` carries the query/networking settings only.
- **Kafka connector jars, `numpy`/`faker`, and the `driver/playbook` package** are
  *not* in `Dockerfile.spark` yet, even though PLAN.md's D2 narrative describes
  the eventual image including them. Per this pass's explicit scope (Kafka is
  Phase 3; `tools/datagen`/`driver/playbook` are separate backlog items), the
  image here is Phase-0-minimal: JupyterLab + PyArrow + pandas only. Extending the
  Dockerfile later is a one-line `RUN pip install` / `COPY` addition, not a
  redesign.
- **The whole repo is bind-mounted at `/workspace`** in every container (not just
  a `content/`/`driver/` subset, since those don't exist yet). This keeps the
  compose template forward-compatible — later phases add `content/` and
  `driver/playbook/` as real directories, and they'll simply appear under
  `/workspace` with no template changes needed.
- **`include_kafka`/`requires_kafka` templating is omitted** from
  `docker-compose.yml.j2` for this pass (Phase 3 concern per PLAN.md §5), matching
  the task's explicit scope.
- **Host port 7077 is intentionally not published.** Only in-network containers
  dial `spark://spark-master:7077` (resolved via Docker embedded DNS); the host
  never needs to reach it. Found necessary in practice when another, unrelated
  Spark stack on the same machine already had host port 7077 bound.
- **Two relative bind-mount path bugs were found and fixed by actually running
  the stack (not just static template validation):** docker compose resolves
  relative volume source paths against the *compose file's own directory*
  (`compose/rendered/`), not the invoker's cwd or the template's directory.
  - The repo bind mount needed `../../:/workspace` (two levels up from
    `compose/rendered/`), not `../:/workspace` — the latter only exposed
    `compose/` itself inside the containers.
  - The `spark-defaults.conf` mount needed `./spark-defaults.conf` (it renders
    into the same `compose/rendered/` directory as the compose file), not
    `./rendered/spark-defaults.conf` — the latter resolved to a nonexistent
    `compose/rendered/rendered/...` path, which Docker silently satisfied by
    creating an **empty directory** at the container mount point instead of
    mounting a file. The practical symptom was that `spark.master` was never
    applied and `spark-submit` silently fell back to `local[*]` (single-JVM,
    no cluster involved) instead of erroring — worth knowing if you ever see a
    "successful" run that doesn't actually touch the workers.
