"""Spark Playbook — plan-node tokenizer (PLAN.md §3, §4 plan_parser.py).

Tokenizes `df.explain(mode="formatted")` output into an ordered list of
plan-node operator names (e.g. `Exchange`, `BroadcastExchange`,
`BroadcastHashJoin`, `SortMergeJoin`, `Window`, `Sort`). Keeps tree order,
assigns no meaning -- concept mapping is entirely `engine.py`'s job, driven
by the topic manifest (G7).

`explain(mode="formatted")` output shape (Spark 3.x/4.x):

    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- == Initial Plan ==
       * BroadcastHashJoin Inner BuildRight (7)
       :- * Filter (2)
       :  +- Scan parquet default.small (1)
       +- BroadcastExchange HashedRelationBroadcastMode (6)
          +- * Filter (5)
             +- Scan parquet default.large (4)

    (1) Scan parquet default.small
    Output: [id#0, name#1]
    ...

Under AQE (`spark.sql.adaptive.enabled=true`), the tree additionally nests
`== Initial Plan ==` / `== Final Plan ==` sub-trees under an `AdaptiveSparkPlan`
root (used by the AQE topic, US-2.5) -- both are tokenized, in the order
printed; this module does not care which sub-tree a node came from.

The numbered "detail" blocks below the tree (`(1) Scan parquet ...`,
`Output:`, `Arguments:`, ...) are not plan nodes and are excluded.
"""
from __future__ import annotations

import re
from typing import List

# Leading ASCII tree-drawing characters (space, ':', '+', '-', '|') plus an
# optional whole-stage-codegen '*' marker, stripped from the front of a tree
# line before the operator name is read off.
_TREE_PREFIX_RE = re.compile(r"^[\s:+\-|]*\*?\s*")
_OPERATOR_NAME_RE = re.compile(r"([A-Za-z][A-Za-z0-9]*)")

# The numbered detail blocks below the tree always start, at column 0 (after
# tree-prefix stripping, i.e. no tree-drawing characters precede them), with
# "(<digits>)" -- e.g. "(1) Scan parquet ...". No real tree-node line matches
# this shape: a tree line's node id (if present) is a trailing "(<id>)"
# *after* the operator name, never a leading one.
_DETAIL_BLOCK_START_RE = re.compile(r"^\(\d+\)")


def parse_operators(explain_text: str) -> List[str]:
    """Returns the ordered list of plan-node operator names found in the
    `== Physical Plan ==` tree section(s) of `explain_text`."""
    operators: List[str] = []
    in_physical_plan = False

    for raw_line in explain_text.splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            continue

        stripped = _TREE_PREFIX_RE.sub("", line, count=1)

        if stripped.startswith("=="):
            if "Physical Plan" in stripped:
                in_physical_plan = True
            # Any other "== ... ==" header (Initial Plan / Final Plan /
            # Subqueries) is skipped without leaving the physical-plan
            # section once entered.
            continue

        if not in_physical_plan:
            continue

        if _DETAIL_BLOCK_START_RE.match(stripped):
            # Reached the numbered detail blocks below the tree -- stop.
            break

        match = _OPERATOR_NAME_RE.match(stripped)
        if match:
            operators.append(match.group(1))

    return operators
