"""Tests for app/annotation/plan_parser.py (US-2.1: static plan tokenization)."""
from __future__ import annotations

from app.annotation import plan_parser

BROADCAST_JOIN_PLAN = """== Physical Plan ==
* BroadcastHashJoin Inner BuildRight (7)
:- * Filter (2)
:  +- * ColumnarToRow (1)
:     +- Scan parquet default.small (0)
+- BroadcastExchange HashedRelationBroadcastMode (6)
   +- * Filter (5)
      +- * ColumnarToRow (4)
         +- Scan parquet default.large (3)

(0) Scan parquet default.small
Output: [id#0, name#1]

(1) ColumnarToRow
Input: [id#0, name#1]

(2) Filter
Input: [id#0, name#1]
Condition: isnotnull(id#0)
"""

SORT_MERGE_JOIN_PLAN = """== Physical Plan ==
* SortMergeJoin [id#0], [id#10], Inner (9)
:- * Sort [id#0 ASC NULLS FIRST], false, 0 (4)
:  +- Exchange hashpartitioning(id#0, 200) (3)
:     +- * Filter (2)
:        +- Scan parquet default.large_a (1)
+- * Sort [id#10 ASC NULLS FIRST], false, 0 (8)
   +- Exchange hashpartitioning(id#10, 200) (7)
      +- * Filter (6)
         +- Scan parquet default.large_b (5)

(1) Scan parquet default.large_a
"""

BUCKETED_JOIN_NO_EXCHANGE_PLAN = """== Physical Plan ==
* SortMergeJoin [id#0], [id#10], Inner (7)
:- * Sort [id#0 ASC NULLS FIRST], false, 0 (3)
:  +- * Filter (2)
:     +- Scan parquet default.bucketed_a (1)
+- * Sort [id#10 ASC NULLS FIRST], false, 0 (6)
   +- * Filter (5)
      +- Scan parquet default.bucketed_b (4)

(1) Scan parquet default.bucketed_a
"""

AQE_PLAN = """== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   * BroadcastHashJoin Inner BuildRight (7)
   :- * Filter (2)
   :  +- Scan parquet default.small (1)
   +- BroadcastExchange HashedRelationBroadcastMode (6)
      +- * Filter (5)
         +- Scan parquet default.large (4)
+- == Initial Plan ==
   SortMergeJoin [id#0], [id#10], Inner (9)
   :- Exchange hashpartitioning(id#0, 200) (3)
   :  +- Scan parquet default.small (1)
   +- Exchange hashpartitioning(id#10, 200) (8)
      +- Scan parquet default.large (4)

(1) Scan parquet default.small
"""


class TestParseOperators:
    def test_broadcast_join_plan_order(self):
        ops = plan_parser.parse_operators(BROADCAST_JOIN_PLAN)
        assert ops[:2] == ["BroadcastHashJoin", "Filter"]
        assert "BroadcastExchange" in ops
        assert "ColumnarToRow" in ops
        assert "Scan" in ops  # "Scan parquet ..." -> first word token

    def test_detail_blocks_excluded(self):
        ops = plan_parser.parse_operators(BROADCAST_JOIN_PLAN)
        # "Output:", "Input:", "Condition:" lines and the "(0) Scan parquet"
        # detail-block headers must not be tokenized a second time.
        assert ops.count("Scan") == 2  # once per tree leaf, not from detail blocks too
        assert "Output" not in ops
        assert "Input" not in ops
        assert "Condition" not in ops

    def test_sort_merge_join_includes_exchange(self):
        ops = plan_parser.parse_operators(SORT_MERGE_JOIN_PLAN)
        assert "SortMergeJoin" in ops
        assert "Exchange" in ops
        assert ops.index("SortMergeJoin") < ops.index("Exchange")  # tree order preserved

    def test_bucketed_join_has_no_exchange(self):
        ops = plan_parser.parse_operators(BUCKETED_JOIN_NO_EXCHANGE_PLAN)
        assert "SortMergeJoin" in ops
        assert "Exchange" not in ops

    def test_aqe_plan_captures_both_subtrees(self):
        ops = plan_parser.parse_operators(AQE_PLAN)
        assert "BroadcastHashJoin" in ops
        assert "BroadcastExchange" in ops
        assert "SortMergeJoin" in ops
        assert "Exchange" in ops

    def test_empty_text_returns_empty_list(self):
        assert plan_parser.parse_operators("") == []

    def test_text_without_physical_plan_header_returns_empty_list(self):
        assert plan_parser.parse_operators("some unrelated text\nwith no plan header\n") == []
