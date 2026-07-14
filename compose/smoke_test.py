"""Spark Playbook — Phase 0 smoke test (PLAN.md §5 Phase 0, US-0.3).

Confirms the cluster harness produces real, distributed execution rather than
local-mode behavior:
  1. the job completes successfully,
  2. tasks run on more than one worker/executor (check the Spark UI by hand —
     see the printed instructions below),
  3. at least one stage reports nonzero shuffleReadBytes/shuffleWriteBytes via
     the driver's REST API (checked automatically by this script).

Deliberately self-contained: generates its own small synthetic dataset inline.
tools/datagen/ (US-0.4, tunable skew/volume) is a separate, later backlog item —
not a dependency of this smoke test.

Run from inside the spark-driver container (matches how a learner would run
real work, per US-0.5's client-mode-from-the-driver-container design):

    docker exec -it spark-driver /opt/spark/bin/spark-submit /workspace/compose/smoke_test.py

or paste its body into a JupyterLab notebook cell at http://localhost:8888.

What to look for while/after it runs:
  - http://localhost:8080  -> master UI lists the expected number of ALIVE workers.
  - http://localhost:4040  -> driver application UI; Jobs tab shows the job,
    Stages tab shows a stage whose "Shuffle Read"/"Shuffle Write" columns are
    nonzero, and the Executors tab shows tasks completed on more than one
    executor (one executor per worker in this default config).
  - This script additionally queries :4040/api/v1/applications/<id>/stages
    directly and asserts nonzero shuffle bytes, printing PASS/FAIL.
"""
from __future__ import annotations

import json
import sys
import time
import urllib.request

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

APP_NAME = "spark-playbook-smoke-test"
DRIVER_REST_BASE = "http://localhost:4040/api/v1"

# Small, self-generated dataset: enough rows/partitions to guarantee a shuffle
# and to spread tasks across all workers, without needing tools/datagen.
NUM_ROWS = 200_000
NUM_KEYS = 500
NUM_PARTITIONS = 12  # > default worker_count so tasks fan out across workers


def build_dataset(spark: SparkSession):
    # Deterministic, dependency-free synthetic rows: key + value, spread across
    # NUM_PARTITIONS partitions so the initial stage's tasks are distributed.
    rdd = spark.sparkContext.parallelize(range(NUM_ROWS), NUM_PARTITIONS).map(
        lambda i: Row(key=f"key-{i % NUM_KEYS}", value=float(i % 997))
    )
    return spark.createDataFrame(rdd)


def run_shuffle_job(spark: SparkSession):
    df = build_dataset(spark)
    # groupBy().agg() forces an Exchange (shuffle) per PLAN.md §3 / US-0.3.
    result = df.groupBy("key").agg(
        F.count("*").alias("cnt"),
        F.avg("value").alias("avg_value"),
    )
    total = result.count()  # materialize the job
    print(f"groupBy().agg() produced {total} distinct keys.")
    return result


def find_current_app_id() -> str | None:
    url = f"{DRIVER_REST_BASE}/applications"
    with urllib.request.urlopen(url, timeout=5) as resp:
        apps = json.loads(resp.read().decode("utf-8"))
    for app in apps:
        attempts = app.get("attempts", [])
        if attempts and not attempts[-1].get("endTime") or attempts[-1].get("endTime") == "1969-12-31T23:59:59.999GMT":
            return app["id"]
    # Fallback: most recent application in the list.
    return apps[0]["id"] if apps else None


def check_shuffle_metrics(app_id: str) -> bool:
    url = f"{DRIVER_REST_BASE}/applications/{app_id}/stages"
    with urllib.request.urlopen(url, timeout=5) as resp:
        stages = json.loads(resp.read().decode("utf-8"))

    found = False
    for stage in stages:
        read_bytes = stage.get("shuffleReadBytes", 0)
        write_bytes = stage.get("shuffleWriteBytes", 0)
        if read_bytes > 0 or write_bytes > 0:
            found = True
            print(
                f"  stage {stage.get('stageId')} (attempt {stage.get('attemptId')}): "
                f"shuffleReadBytes={read_bytes} shuffleWriteBytes={write_bytes} "
                f"numTasks={stage.get('numTasks')}"
            )
    return found


def main() -> int:
    spark = (
        SparkSession.builder
        # No .master(...) / .config(...) calls needed — spark-defaults.conf
        # (rendered from spark-defaults.conf.j2) already points this at
        # spark://spark-master:7077 with driver networking pre-baked (US-0.5).
        .appName(APP_NAME)
        .getOrCreate()
    )

    try:
        print(f"Spark version: {spark.version}")
        print(f"Master: {spark.sparkContext.master}")

        run_shuffle_job(spark)

        # Give the REST API a moment to reflect the completed stages.
        time.sleep(2)

        app_id = spark.sparkContext.applicationId
        print(f"Application id: {app_id}")

        ok = check_shuffle_metrics(app_id)
        if ok:
            print("PASS: at least one stage reports nonzero shuffle read/write bytes.")
        else:
            print(
                "FAIL: no stage reported nonzero shuffle bytes. Check "
                f"{DRIVER_REST_BASE}/applications/{app_id}/stages manually.",
                file=sys.stderr,
            )
            return 1

        print(
            "\nManual checks:\n"
            "  - http://localhost:8080 -> confirm ALIVE worker count matches your config.\n"
            f"  - http://localhost:4040/api/v1/applications/{app_id}/stages -> raw JSON used above.\n"
            "  - http://localhost:4040 Executors tab -> confirm tasks ran on more than one executor.\n"
        )
        return 0
    finally:
        # Leave the session open if run interactively in a notebook; only stop
        # when executed as a standalone spark-submit script.
        if __name__ == "__main__":
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
