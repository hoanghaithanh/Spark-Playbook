"""Tests for the JMX exporter scrape/parse layer (ADR D-MBK6, US-MBK3) in
`app/monitoring/kafka_stats.py`: `parse_prometheus_text`, `parse_jmx_metrics`,
and `fetch_jmx_metrics`.

The module ships `demo()`-style self-checks for its own happy-path samples;
this file adds the pytest coverage this repo's convention otherwise expects
(see `tests/unit/test_manifest.py`, `test_collector_kafka.py`, etc.), with a
focus on edge cases the demo doesn't exercise: malformed lines, missing
MBeans, the >100% idle clamp, and the FetchConsumer vs Fetch/FetchFollower
metric-name disambiguation.
"""
from __future__ import annotations

import pytest

from app.monitoring import kafka_stats


class TestParsePrometheusText:
    def test_parses_name_labels_value(self):
        text = 'kafka_network_requestmetrics_99thpercentile{name="TotalTimeMs",request="Produce"} 12.5\n'
        result = kafka_stats.parse_prometheus_text(text)
        assert result[
            ("kafka_network_requestmetrics_99thpercentile", (("name", "TotalTimeMs"), ("request", "Produce")))
        ] == 12.5

    def test_parses_name_without_labels(self):
        result = kafka_stats.parse_prometheus_text("java_lang_memory_heapmemoryusage_used 3.65922848E8\n")
        assert result[("java_lang_memory_heapmemoryusage_used", ())] == 365922848.0

    def test_skips_comment_and_blank_lines(self):
        text = "# HELP some_metric a metric\n# TYPE some_metric gauge\n\nsome_metric 1.0\n"
        result = kafka_stats.parse_prometheus_text(text)
        assert result == {("some_metric", ()): 1.0}

    def test_skips_malformed_lines_without_raising(self):
        text = (
            "not_a_valid_line_at_all\n"
            "metric_with_no_value\n"
            "metric_with_bad_value abc\n"
            "good_metric 5.0\n"
        )
        result = kafka_stats.parse_prometheus_text(text)
        assert result == {("good_metric", ()): 5.0}

    def test_empty_text_returns_empty_dict(self):
        assert kafka_stats.parse_prometheus_text("") == {}


class TestParseJmxMetrics:
    def _sample(self, **overrides) -> str:
        lines = {
            "heap_used": 'java_lang_memory_heapmemoryusage_used 3.65922848E8',
            "heap_max": 'java_lang_memory_heapmemoryusage_max 1.073741824E9',
            "produce": 'kafka_network_requestmetrics_99thpercentile{name="TotalTimeMs",request="Produce"} 12.5',
            "fetch_consumer": 'kafka_network_requestmetrics_99thpercentile{name="TotalTimeMs",request="FetchConsumer"} 4.0',
            "idle": 'kafka_server_kafkarequesthandlerpool_oneminuterate{name="RequestHandlerAvgIdlePercent"} 0.5',
        }
        lines.update(overrides)
        return "\n".join(v for v in lines.values() if v is not None) + "\n"

    def test_happy_path_all_fields_populated(self):
        m = kafka_stats.parse_jmx_metrics(self._sample())
        assert m.heap_pct == 34
        assert m.produce_p99_ms == 12.5
        assert m.fetch_p99_ms == 4.0
        assert m.rh_idle_pct == 50.0

    def test_missing_mbeans_entirely_returns_all_none(self):
        m = kafka_stats.parse_jmx_metrics("")
        assert m.heap_pct is None
        assert m.produce_p99_ms is None
        assert m.fetch_p99_ms is None
        assert m.rh_idle_pct is None

    def test_missing_heap_max_leaves_heap_pct_none_not_zero_division(self):
        m = kafka_stats.parse_jmx_metrics(self._sample(heap_max=None))
        assert m.heap_pct is None

    def test_fetch_consumer_disambiguated_from_fetch_and_fetch_follower(self):
        """Verified-live gotcha: `request=` label carries `Fetch`,
        `FetchFollower`, and `FetchConsumer` as distinct series -- only the
        consumer-facing one should land in fetch_p99_ms."""
        sample = self._sample(
            fetch_consumer='kafka_network_requestmetrics_99thpercentile{name="TotalTimeMs",request="FetchConsumer"} 4.0',
            fetch_plain='kafka_network_requestmetrics_99thpercentile{name="TotalTimeMs",request="Fetch"} 999.0',
            fetch_follower='kafka_network_requestmetrics_99thpercentile{name="TotalTimeMs",request="FetchFollower"} 888.0',
        )
        m = kafka_stats.parse_jmx_metrics(sample)
        assert m.fetch_p99_ms == 4.0

    def test_one_minute_rate_missing_falls_back_to_mean_rate(self):
        sample = self._sample(
            idle=None,
            mean_idle='kafka_server_kafkarequesthandlerpool_meanrate{name="RequestHandlerAvgIdlePercent"} 0.25',
        )
        m = kafka_stats.parse_jmx_metrics(sample)
        assert m.rh_idle_pct == 25.0

    def test_idle_rate_over_100_percent_is_clamped(self):
        """Verified live: the raw meter sums idle time across every I/O
        handler thread and routinely exceeds 1.0 (measured ~188% on a real
        3-broker spawn) -- clamped to 100%, never surfaced raw, so the
        dashboard never shows a fabricated/misleading >100% "idle" figure."""
        sample = self._sample(
            idle='kafka_server_kafkarequesthandlerpool_oneminuterate{name="RequestHandlerAvgIdlePercent"} 1.8835'
        )
        m = kafka_stats.parse_jmx_metrics(sample)
        assert m.rh_idle_pct == 100.0

    def test_idle_rate_exactly_at_100_percent_not_clamped_below(self):
        sample = self._sample(
            idle='kafka_server_kafkarequesthandlerpool_oneminuterate{name="RequestHandlerAvgIdlePercent"} 1.0'
        )
        m = kafka_stats.parse_jmx_metrics(sample)
        assert m.rh_idle_pct == 100.0


class TestFetchJmxMetrics:
    @pytest.mark.asyncio
    async def test_returns_none_when_scrape_fails(self, monkeypatch):
        async def _fake_exec_in(container, *args, timeout_s):
            return None

        monkeypatch.setattr(kafka_stats, "_exec_in", _fake_exec_in)
        result = await kafka_stats.fetch_jmx_metrics("spark-kafka-1")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_parsed_metrics_when_scrape_succeeds(self, monkeypatch):
        sample = 'java_lang_memory_heapmemoryusage_used 500\njava_lang_memory_heapmemoryusage_max 1000\n'

        async def _fake_exec_in(container, *args, timeout_s):
            assert container == "spark-kafka-2"
            return sample

        monkeypatch.setattr(kafka_stats, "_exec_in", _fake_exec_in)
        result = await kafka_stats.fetch_jmx_metrics("spark-kafka-2")
        assert result is not None
        assert result.heap_pct == 50

    @pytest.mark.asyncio
    async def test_malformed_scrape_output_returns_metrics_with_none_fields_not_a_raise(self, monkeypatch):
        async def _fake_exec_in(container, *args, timeout_s):
            return "garbage not prometheus format at all !!!\n"

        monkeypatch.setattr(kafka_stats, "_exec_in", _fake_exec_in)
        result = await kafka_stats.fetch_jmx_metrics("spark-kafka-1")
        assert result is not None
        assert result.heap_pct is None
