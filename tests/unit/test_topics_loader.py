"""Tests for app/topics/loader.py (US-1.1: content-as-data)."""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest
import yaml

from app import config
from app.topics import loader


class TestLoadRealPartitioningShuffleTopic:
    """Sanity check against the actual shipped content/partitioning-shuffle/."""

    def test_manifest_fields(self):
        topic = loader.load_topic("partitioning-shuffle")
        assert topic.id == "partitioning-shuffle"
        assert topic.title == "Partitioning & Shuffle Mechanics"
        assert topic.order == 1
        assert topic.requires_kafka is False
        assert topic.cluster_defaults.worker_count == 3
        assert topic.cluster_defaults.worker_cores == 2
        assert topic.cluster_defaults.worker_memory_gb == 4
        assert topic.cluster_defaults.shuffle_partitions == 200
        assert topic.cluster_defaults.aqe_enabled is False

    def test_concept_markdown_renders_to_html(self):
        topic = loader.load_topic("partitioning-shuffle")
        html = topic.concept_html()
        assert "<h1>Partitioning" in html or "Partitioning &amp; Shuffle Mechanics" in html
        assert "shuffle" in html.lower()
        assert "Exchange" in html  # a specific concept term from the real content

    def test_notebook_path_resolves(self):
        topic = loader.load_topic("partitioning-shuffle")
        assert topic.notebook_path.name == "notebook.ipynb"
        assert topic.notebook_path.exists()

    def test_notebook_relpath_used_for_jupyter_deep_link(self):
        topic = loader.load_topic("partitioning-shuffle")
        assert topic.notebook_relpath == "content/partitioning-shuffle/notebook.ipynb"

    def test_list_topics_includes_partitioning_shuffle(self):
        topics = loader.list_topics()
        ids = [t.id for t in topics]
        assert "partitioning-shuffle" in ids


class TestLoadRealDagLazyEvaluationTopic:
    """Sanity check against the actual shipped content/dag-lazy-evaluation/
    (US-C1, issue #27) -- same coverage shape as
    TestLoadRealPartitioningShuffleTopic above."""

    def test_manifest_fields(self):
        topic = loader.load_topic("dag-lazy-evaluation")
        assert topic.id == "dag-lazy-evaluation"
        assert topic.title == "DAG & Lazy Evaluation"
        assert topic.requires_kafka is False
        assert topic.cluster_defaults.worker_count == 3
        assert topic.cluster_defaults.shuffle_partitions == 200

    def test_concept_markdown_renders_to_html(self):
        topic = loader.load_topic("dag-lazy-evaluation")
        html = topic.concept_html()
        assert "lazy" in html.lower()
        assert "Exchange" in html  # the shuffle-boundary concept term

    def test_notebook_path_resolves(self):
        topic = loader.load_topic("dag-lazy-evaluation")
        assert topic.notebook_path.name == "notebook.ipynb"
        assert topic.notebook_path.exists()

    def test_list_topics_includes_dag_lazy_evaluation(self):
        topics = loader.list_topics()
        ids = [t.id for t in topics]
        assert "dag-lazy-evaluation" in ids


class TestLoadRealCachingPersistenceTopic:
    """Sanity check against the actual shipped content/caching-persistence/
    (US-C5, issue #28) -- same coverage shape as
    TestLoadRealDagLazyEvaluationTopic above."""

    def test_manifest_fields(self):
        topic = loader.load_topic("caching-persistence")
        assert topic.id == "caching-persistence"
        assert topic.title == "Caching & Persistence"
        assert topic.requires_kafka is False
        assert topic.cluster_defaults.worker_count == 3
        assert topic.cluster_defaults.shuffle_partitions == 200

    def test_concept_markdown_renders_to_html(self):
        topic = loader.load_topic("caching-persistence")
        html = topic.concept_html()
        assert "cach" in html.lower()
        assert "Storage tab" in html

    def test_notebook_path_resolves(self):
        topic = loader.load_topic("caching-persistence")
        assert topic.notebook_path.name == "notebook.ipynb"
        assert topic.notebook_path.exists()

    def test_list_topics_includes_caching_persistence(self):
        topics = loader.list_topics()
        ids = [t.id for t in topics]
        assert "caching-persistence" in ids


class TestLoadRealWindowFunctionsTopic:
    """Sanity check against the actual shipped content/window-functions/
    (US-C6, issue #29) -- same coverage shape as
    TestLoadRealCachingPersistenceTopic above."""

    def test_manifest_fields(self):
        topic = loader.load_topic("window-functions")
        assert topic.id == "window-functions"
        assert topic.title == "Window Functions"
        assert topic.requires_kafka is False
        assert topic.cluster_defaults.worker_count == 3
        assert topic.cluster_defaults.shuffle_partitions == 200

    def test_concept_markdown_renders_to_html(self):
        topic = loader.load_topic("window-functions")
        html = topic.concept_html()
        assert "window" in html.lower()
        assert "partitionBy" in html

    def test_notebook_path_resolves(self):
        topic = loader.load_topic("window-functions")
        assert topic.notebook_path.name == "notebook.ipynb"
        assert topic.notebook_path.exists()

    def test_list_topics_includes_window_functions(self):
        topics = loader.list_topics()
        ids = [t.id for t in topics]
        assert "window-functions" in ids


class TestLoadRealSerializationFormatsTopic:
    """Sanity check against the actual shipped content/serialization-formats/
    (US-C8, issue #30) -- same coverage shape as
    TestLoadRealWindowFunctionsTopic above."""

    def test_manifest_fields(self):
        topic = loader.load_topic("serialization-formats")
        assert topic.id == "serialization-formats"
        assert topic.title == "Serialization Formats"
        assert topic.order == 8
        assert topic.requires_kafka is False
        assert topic.cluster_defaults.worker_count == 3
        assert topic.cluster_defaults.shuffle_partitions == 200

    def test_concept_markdown_renders_to_html(self):
        topic = loader.load_topic("serialization-formats")
        html = topic.concept_html()
        assert "columnar" in html.lower()
        assert "inputBytes" in html

    def test_notebook_path_resolves(self):
        topic = loader.load_topic("serialization-formats")
        assert topic.notebook_path.name == "notebook.ipynb"
        assert topic.notebook_path.exists()

    def test_list_topics_includes_serialization_formats(self):
        topics = loader.list_topics()
        ids = [t.id for t in topics]
        assert "serialization-formats" in ids


class TestLoadRealUdfPandasUdfTopic:
    """Sanity check against the actual shipped content/udf-pandas-udf/
    (US-4.3, issue #51) -- same coverage shape as
    TestLoadRealSerializationFormatsTopic above."""

    def test_manifest_fields(self):
        topic = loader.load_topic("udf-pandas-udf")
        assert topic.id == "udf-pandas-udf"
        assert topic.title == "UDF vs pandas UDF: Serialization Cost"
        assert topic.order == 14
        assert topic.requires_kafka is False
        assert topic.cluster_defaults.worker_count == 3
        assert topic.cluster_defaults.worker_cores == 2
        assert topic.cluster_defaults.worker_memory_gb == 4
        assert topic.cluster_defaults.shuffle_partitions == 200
        assert topic.cluster_defaults.aqe_enabled is False

    def test_concept_markdown_renders_to_html(self):
        topic = loader.load_topic("udf-pandas-udf")
        html = topic.concept_html()
        assert "vectorized" in html.lower()
        assert "BatchEvalPython" in html
        assert "ArrowEvalPython" in html
        assert "executorRunTime" in html

    def test_notebook_path_resolves(self):
        topic = loader.load_topic("udf-pandas-udf")
        assert topic.notebook_path.name == "notebook.ipynb"
        assert topic.notebook_path.exists()

    def test_list_topics_includes_udf_pandas_udf(self):
        topics = loader.list_topics()
        ids = [t.id for t in topics]
        assert "udf-pandas-udf" in ids


class TestLoadRealExecutorTuningTopic:
    """Sanity check against the actual shipped content/executor-tuning/
    (US-C3, issue #34) -- same coverage shape as
    TestLoadRealSerializationFormatsTopic above. cluster_defaults uses this
    platform's own max per-worker cores/memory (4 cores / 8GB, `app/
    config.py` WORKER_CORES_RANGE/WORKER_MEMORY_GB_RANGE) so the notebook's
    two executor configs both fit within a real worker's advertised budget
    -- see manifest.yaml's own deviation-from-the-mockup comment."""

    def test_manifest_fields(self):
        topic = loader.load_topic("executor-tuning")
        assert topic.id == "executor-tuning"
        assert topic.title == "Executor Tuning"
        assert topic.order == 10
        assert topic.requires_kafka is False
        assert topic.cluster_defaults.worker_count == 3
        assert topic.cluster_defaults.worker_cores == 4
        assert topic.cluster_defaults.worker_memory_gb == 8

    def test_concept_markdown_renders_to_html(self):
        topic = loader.load_topic("executor-tuning")
        html = topic.concept_html()
        assert "executor" in html.lower()
        assert "totalGCTime" in html

    def test_notebook_path_resolves(self):
        topic = loader.load_topic("executor-tuning")
        assert topic.notebook_path.name == "notebook.ipynb"
        assert topic.notebook_path.exists()

    def test_list_topics_includes_executor_tuning(self):
        topics = loader.list_topics()
        ids = [t.id for t in topics]
        assert "executor-tuning" in ids


class TestLoadRealSkewSaltingTopic:
    """Sanity check against the actual shipped content/skew-salting/
    (US-C2, issue #35) -- same coverage shape as
    TestLoadRealExecutorTuningTopic above."""

    def test_manifest_fields(self):
        topic = loader.load_topic("skew-salting")
        assert topic.id == "skew-salting"
        assert topic.title == "Skew & Salting"
        assert topic.order == 11
        assert topic.requires_kafka is False
        assert topic.cluster_defaults.worker_count == 3
        assert topic.cluster_defaults.shuffle_partitions == 200
        assert topic.cluster_defaults.aqe_enabled is False

    def test_concept_markdown_renders_to_html(self):
        topic = loader.load_topic("skew-salting")
        html = topic.concept_html()
        assert "salt" in html.lower()
        assert "skewJoin" in html

    def test_notebook_path_resolves(self):
        topic = loader.load_topic("skew-salting")
        assert topic.notebook_path.name == "notebook.ipynb"
        assert topic.notebook_path.exists()

    def test_list_topics_includes_skew_salting(self):
        topics = loader.list_topics()
        ids = [t.id for t in topics]
        assert "skew-salting" in ids


class TestBlurb:
    """Topic.blurb() (topics-index landing page, issue #26/US-SH5) derives a
    card blurb from concept.md's "## What it is" section instead of a new
    manifest field -- must work unchanged for every currently shipped topic
    (no special-casing), and degrade to a synthetic fixture's exact expected
    text so a regression in the extraction rule itself is caught."""

    def test_first_paragraph_extracted_from_synthetic_fixture(self, tmp_path):
        topic_dir = tmp_path / "blurb-topic"
        topic_dir.mkdir()
        (topic_dir / "manifest.yaml").write_text(
            yaml.dump({"id": "blurb-topic", "title": "Blurb Topic", "content": "concept.md",
                       "notebook": "notebook.ipynb"}),
            encoding="utf-8",
        )
        (topic_dir / "concept.md").write_text(
            "# Blurb Topic\n"
            "\n"
            "## What it is\n"
            "\n"
            "This is the first paragraph, spanning\n"
            "two lines of prose.\n"
            "\n"
            "This second paragraph must not be included.\n"
            "\n"
            "## Another section\n"
            "\n"
            "Also not included.\n",
            encoding="utf-8",
        )
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        with patch.object(config, "CONTENT_DIR", tmp_path):
            topic = loader.load_topic("blurb-topic")
            blurb = topic.blurb()

        assert blurb == "This is the first paragraph, spanning two lines of prose."

    def test_missing_what_it_is_section_yields_empty_blurb_not_a_raise(self, tmp_path):
        topic_dir = tmp_path / "no-section-topic"
        topic_dir.mkdir()
        (topic_dir / "manifest.yaml").write_text(
            yaml.dump({"id": "no-section-topic", "title": "No Section", "content": "concept.md",
                       "notebook": "notebook.ipynb"}),
            encoding="utf-8",
        )
        (topic_dir / "concept.md").write_text("# No Section\n\nJust some text, no heading.\n", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        with patch.object(config, "CONTENT_DIR", tmp_path):
            topic = loader.load_topic("no-section-topic")
            assert topic.blurb() == ""

    def test_every_real_topic_yields_a_non_empty_blurb(self):
        """Regression guard for issue #26: every one of the 5 shipped
        content/*/concept.md files follows the "# Title" / "## What it is"
        convention -- if any drifted, this fails loudly instead of the
        topics-index page silently shipping a blank card."""
        for topic in loader.list_topics():
            blurb = topic.blurb()
            assert blurb, f"{topic.id} yielded an empty blurb"
            assert not blurb.startswith("#")
            # Coordinator-reported bug: every one of the 5 real "What it is"
            # paragraphs uses inline **bold**/`code`/*italic* markdown, and
            # blurb() renders as auto-escaped plain text (not through the
            # `markdown` library), so raw markers must be stripped -- not
            # leaked as literal `*`/`` ` `` characters on the card.
            assert "*" not in blurb, f"{topic.id} blurb leaked a raw '*' marker: {blurb!r}"
            assert "`" not in blurb, f"{topic.id} blurb leaked a raw '`' marker: {blurb!r}"

    def test_inline_markdown_markers_are_stripped_not_rendered(self, tmp_path):
        """Coordinator-reported bug: blurb() previously returned raw markdown
        source, so `**bold**` and `` `code` `` showed up as literal asterisks/
        backticks on the topics-index card (blurb() is auto-escaped plain
        text, unlike concept_html() which runs through the `markdown`
        library). The fix strips the marker characters, keeping the inner
        words."""
        topic_dir = tmp_path / "markdown-blurb-topic"
        topic_dir.mkdir()
        (topic_dir / "manifest.yaml").write_text(
            yaml.dump({"id": "markdown-blurb-topic", "title": "Markdown Blurb", "content": "concept.md",
                       "notebook": "notebook.ipynb"}),
            encoding="utf-8",
        )
        (topic_dir / "concept.md").write_text(
            "# Markdown Blurb\n"
            "\n"
            "## What it is\n"
            "\n"
            "A **bold** word, some `inline code`, and an *italic* word.\n",
            encoding="utf-8",
        )
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        with patch.object(config, "CONTENT_DIR", tmp_path):
            topic = loader.load_topic("markdown-blurb-topic")
            blurb = topic.blurb()

        assert "*" not in blurb
        assert "`" not in blurb
        assert blurb == "A bold word, some inline code, and an italic word."


class TestLoadRealKafkaTopicsPartitionsTopic:
    """Sanity check against the actual shipped content/kafka-topics-partitions/
    (US-KC2, issue #63) -- second Kafka-curriculum topic, same coverage shape
    as the loader tests for kafka-architecture-kraft's manifest fields."""

    def test_manifest_fields(self):
        topic = loader.load_topic("kafka-topics-partitions")
        assert topic.id == "kafka-topics-partitions"
        assert topic.title == "Topics & Partitions: Ordering and Distribution"
        assert topic.order == 2
        assert topic.track == "kafka"
        assert topic.requires_kafka is True
        assert topic.cluster_defaults.worker_count == 1
        assert topic.cluster_defaults.kafka_broker_count == 3

    def test_concept_markdown_renders_to_html(self):
        topic = loader.load_topic("kafka-topics-partitions")
        html = topic.concept_html()
        assert "partition" in html.lower()
        assert "murmur2" in html
        assert "DefaultPartitioner" in html

    def test_notebook_path_resolves(self):
        topic = loader.load_topic("kafka-topics-partitions")
        assert topic.notebook_path.name == "notebook.ipynb"
        assert topic.notebook_path.exists()

    def test_list_topics_includes_kafka_topics_partitions(self):
        topics = loader.list_topics()
        ids = [t.id for t in topics]
        assert "kafka-topics-partitions" in ids


class TestLoadRealKafkaConsumersGroupsTopic:
    """Sanity check against the actual shipped content/kafka-consumers-groups/
    (US-KC4, issue #65) -- fourth Kafka-curriculum topic, same coverage shape
    as the loader tests for the prior Kafka topics' manifest fields."""

    def test_manifest_fields(self):
        topic = loader.load_topic("kafka-consumers-groups")
        assert topic.id == "kafka-consumers-groups"
        assert topic.title == "Consumer Groups: Rebalancing & Offset Commits"
        assert topic.order == 4
        assert topic.track == "kafka"
        assert topic.requires_kafka is True
        assert topic.cluster_defaults.worker_count == 1
        assert topic.cluster_defaults.kafka_broker_count == 3

    def test_concept_markdown_renders_to_html(self):
        topic = loader.load_topic("kafka-consumers-groups")
        html = topic.concept_html()
        assert "consumer group" in html.lower()
        assert "rebalance" in html.lower()
        assert "commit" in html.lower()

    def test_notebook_path_resolves(self):
        topic = loader.load_topic("kafka-consumers-groups")
        assert topic.notebook_path.name == "notebook.ipynb"
        assert topic.notebook_path.exists()

    def test_list_topics_includes_kafka_consumers_groups(self):
        topics = loader.list_topics()
        ids = [t.id for t in topics]
        assert "kafka-consumers-groups" in ids


class TestRequiresKafkaField:
    """docs/architecture/kafka-streaming-infra.md's claim: `requires_kafka:
    bool` already existed pre-#50 (`app/topics/loader.py:233`) and all
    shipped manifests set/default it to false. Individual topic classes
    above already assert this piecemeal for a handful of topics -- this is
    the one comprehensive regression guard across every shipped manifest,
    the R-K1 mitigation's precondition (the streaming topic must be the
    *only* one with requires_kafka: true)."""

    def test_every_shipped_topic_defaults_requires_kafka_false(self):
        topics = loader.list_topics()
        # 15 Spark topics as of #51 (UDF vs pandas UDF), +1 for
        # kafka-architecture-kraft (#62), +1 for kafka-topics-partitions (#63),
        # +1 for kafka-consumers-groups (#65).
        assert len(topics) == 18, "expected 18 shipped topics as of #65 (kafka-consumers-groups)"
        kafka_topics = {"kafka-architecture-kraft", "kafka-topics-partitions", "kafka-consumers-groups"}
        for topic in topics:
            if topic.id in kafka_topics:
                assert topic.requires_kafka is True
                continue
            assert topic.requires_kafka is False, f"{topic.id} unexpectedly requires_kafka=True"

    def test_a_manifest_declaring_requires_kafka_true_is_honored(self, tmp_path):
        """The flag isn't hardcoded false in the loader -- a manifest that
        opts in is actually threaded through (the streaming topic #18
        eventually ships depends on this)."""
        topic_dir = tmp_path / "streaming-topic"
        topic_dir.mkdir()
        (topic_dir / "manifest.yaml").write_text(
            yaml.dump({"id": "streaming-topic", "title": "Streaming Topic", "content": "concept.md",
                       "notebook": "notebook.ipynb", "requires_kafka": True}),
            encoding="utf-8",
        )
        (topic_dir / "concept.md").write_text("# ok", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        with patch.object(config, "CONTENT_DIR", tmp_path):
            topic = loader.load_topic("streaming-topic")

        assert topic.requires_kafka is True


class TestTrackGrouping:
    """Kafka-curriculum ADR (docs/architecture/kafka-curriculum.md D-KC1):
    a manifest without `track` defaults to "spark" (so the 15 pre-existing
    manifests need no edits), and `list_topics_by_track()` renders Spark
    before Kafka with each group internally `order`-sorted, omitting any
    track with no topics."""

    def test_manifest_without_track_field_defaults_to_spark(self):
        topic = loader.load_topic("partitioning-shuffle")
        assert topic.track == "spark"

    def test_manifest_with_explicit_track_kafka_is_honored(self):
        topic = loader.load_topic("kafka-architecture-kraft")
        assert topic.track == "kafka"

    def test_list_topics_by_track_groups_spark_before_kafka_order_sorted(self):
        groups = loader.list_topics_by_track()
        labels = [label for label, _ in groups]
        assert labels == ["Spark", "Kafka"]

        spark_label, spark_topics = groups[0]
        assert [t.order for t in spark_topics] == sorted(t.order for t in spark_topics)

        kafka_label, kafka_topics = groups[1]
        assert all(t.track == "kafka" for t in kafka_topics)
        assert [t.order for t in kafka_topics] == sorted(t.order for t in kafka_topics)

    def test_empty_track_is_omitted_not_rendered_with_an_empty_heading(self, tmp_path):
        topic_dir = tmp_path / "spark-only-topic"
        topic_dir.mkdir()
        (topic_dir / "manifest.yaml").write_text(
            yaml.dump({"id": "spark-only-topic", "title": "Spark Only", "content": "concept.md",
                       "notebook": "notebook.ipynb"}),
            encoding="utf-8",
        )
        (topic_dir / "concept.md").write_text("# ok", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        with patch.object(config, "CONTENT_DIR", tmp_path):
            groups = loader.list_topics_by_track()

        assert [label for label, _ in groups] == ["Spark"]

    def test_manifest_with_null_track_defaults_to_spark_not_none(self, tmp_path):
        """`track:` with no value (or explicit `track: null`) parses to `None`
        via YAML, which is a *present* key -- `data.get("track", "spark")`
        would not catch this (only an absent key falls back), leaving
        `Topic.track = None`. That then hits list_topics_by_track()'s
        unknown-track branch and crashes on `None.title()`, taking down the
        whole topics-index page. Guards the `or "spark"` fallback instead of
        a bare `.get(..., "spark")`."""
        topic_dir = tmp_path / "null-track-topic"
        topic_dir.mkdir()
        (topic_dir / "manifest.yaml").write_text(
            yaml.dump({"id": "null-track-topic", "title": "Null Track", "content": "concept.md",
                       "notebook": "notebook.ipynb", "track": None}),
            encoding="utf-8",
        )
        (topic_dir / "concept.md").write_text("# ok", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        with patch.object(config, "CONTENT_DIR", tmp_path):
            topic = loader.load_topic("null-track-topic")
            groups = loader.list_topics_by_track()

        assert topic.track == "spark"
        assert [label for label, _ in groups] == ["Spark"]


class TestMissingTopicFailsClearly:
    def test_nonexistent_topic_id_raises(self):
        with pytest.raises(loader.TopicNotFoundError):
            loader.load_topic("does-not-exist")

    def test_directory_without_manifest_raises(self, tmp_path):
        (tmp_path / "no-manifest-topic").mkdir()
        with patch.object(config, "CONTENT_DIR", tmp_path):
            with pytest.raises(loader.TopicNotFoundError):
                loader.load_topic("no-manifest-topic")

    def test_missing_concept_file_fails_on_access_not_silently(self, tmp_path):
        """load_topic() requires manifest.yaml *and* (since issue #5's fix)
        notebook_path to exist at load time, but concept.md is intentionally
        NOT eagerly validated the same way -- it fails the moment the content
        is actually read (concept_html()), not silently returning empty/wrong
        content and not at load_topic() time. notebook.ipynb is created here
        so this test isolates concept.md's lazy-read behavior specifically,
        independent of issue #5's now-eager notebook check."""
        topic_dir = tmp_path / "broken-topic"
        topic_dir.mkdir()
        (topic_dir / "manifest.yaml").write_text(
            yaml.dump({"id": "broken-topic", "title": "Broken", "content": "concept.md",
                       "notebook": "notebook.ipynb"}),
            encoding="utf-8",
        )
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")
        # Deliberately do NOT create concept.md.

        with patch.object(config, "CONTENT_DIR", tmp_path):
            topic = loader.load_topic("broken-topic")  # does not raise yet
            with pytest.raises(FileNotFoundError):
                topic.concept_html()

    def test_malformed_yaml_syntax_fails_clearly_not_with_raw_yaml_error(self, tmp_path):
        topic_dir = tmp_path / "malformed-yaml-topic"
        topic_dir.mkdir()
        # Unterminated flow sequence -- a realistic hand-edit typo, not a
        # contrived pathological input.
        (topic_dir / "manifest.yaml").write_text(
            "id: malformed-yaml-topic\nannotation: [unterminated\n", encoding="utf-8"
        )
        (topic_dir / "concept.md").write_text("# ok", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")

        with patch.object(config, "CONTENT_DIR", tmp_path):
            with pytest.raises(loader.TopicNotFoundError):
                loader.load_topic("malformed-yaml-topic")

    def test_missing_notebook_file_raises_clearly_at_load_time(self, tmp_path):
        """Issue #5 fix: load_topic() now validates notebook_path.exists(),
        matching how a missing manifest.yaml is already handled -- a manifest
        with a typo'd/missing `notebook:` path fails loudly at load_topic()
        time instead of loading silently and only surfacing later as a 404
        inside the Jupyter iframe."""
        topic_dir = tmp_path / "no-notebook-topic"
        topic_dir.mkdir()
        (topic_dir / "manifest.yaml").write_text(
            yaml.dump({"id": "no-notebook-topic", "title": "No Notebook", "content": "concept.md",
                       "notebook": "notebook.ipynb"}),
            encoding="utf-8",
        )
        (topic_dir / "concept.md").write_text("# ok", encoding="utf-8")
        # notebook.ipynb deliberately absent.

        with patch.object(config, "CONTENT_DIR", tmp_path):
            with pytest.raises(loader.TopicNotFoundError, match="notebook.ipynb"):
                loader.load_topic("no-notebook-topic")


class TestContentAsDataNoCaching:
    """US-1.1: 'when the content is edited, the change is reflected on next
    page load without a code change.' Points the loader at a temp fixture
    directory, edits it, and confirms the loader picks up the change with no
    caching surprise -- load_topic() does no module-level/process caching."""

    @pytest.fixture
    def fixture_topic_dir(self, tmp_path):
        topic_dir = tmp_path / "editable-topic"
        topic_dir.mkdir()
        manifest = {
            "id": "editable-topic",
            "title": "Editable Topic",
            "order": 5,
            "content": "concept.md",
            "notebook": "notebook.ipynb",
            "cluster_defaults": {"worker_count": 2},
            "requires_kafka": False,
        }
        (topic_dir / "manifest.yaml").write_text(yaml.dump(manifest), encoding="utf-8")
        (topic_dir / "concept.md").write_text("# Version 1\n\noriginal content", encoding="utf-8")
        (topic_dir / "notebook.ipynb").write_text("{}", encoding="utf-8")
        return tmp_path, topic_dir

    def test_edited_concept_md_is_reflected_without_code_change(self, fixture_topic_dir):
        content_dir, topic_dir = fixture_topic_dir

        with patch.object(config, "CONTENT_DIR", content_dir):
            topic1 = loader.load_topic("editable-topic")
            html1 = topic1.concept_html()
            assert "original content" in html1

            (topic_dir / "concept.md").write_text("# Version 2\n\nupdated content", encoding="utf-8")

            topic2 = loader.load_topic("editable-topic")
            html2 = topic2.concept_html()
            assert "updated content" in html2
            assert "original content" not in html2

    def test_edited_manifest_field_is_reflected(self, fixture_topic_dir):
        content_dir, topic_dir = fixture_topic_dir

        with patch.object(config, "CONTENT_DIR", content_dir):
            topic1 = loader.load_topic("editable-topic")
            assert topic1.cluster_defaults.worker_count == 2

            manifest_path = topic_dir / "manifest.yaml"
            data = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
            data["cluster_defaults"]["worker_count"] = 4
            manifest_path.write_text(yaml.dump(data), encoding="utf-8")

            topic2 = loader.load_topic("editable-topic")
            assert topic2.cluster_defaults.worker_count == 4


class TestWalkthroughSteps:
    """Topic.walkthrough_steps() (topic-shell redesign, US-SH7) parses the
    Notebook tab's step list straight out of notebook.ipynb's "## N. Title"
    markdown cells (content-as-data, G-SH1/G7) -- no manifest field, no new
    schema. Uses synthetic fixture notebooks to pin down the parsing rules
    precisely, plus a check against the real shipped partitioning-shuffle
    notebook so the two don't drift apart."""

    def _write_notebook(self, path, cells):
        path.write_text(json.dumps({"cells": cells}), encoding="utf-8")

    def _md_cell(self, source_lines):
        return {"cell_type": "markdown", "source": source_lines}

    def _code_cell(self, source_lines):
        return {"cell_type": "code", "source": source_lines}

    def test_real_partitioning_shuffle_notebook_parses_five_numbered_steps(self):
        topic = loader.load_topic("partitioning-shuffle")
        steps = topic.walkthrough_steps()

        assert [s.number for s in steps] == ["1", "2", "3", "4", "5"]
        assert steps[0].title == "Build a keyed dataset spread across input partitions"
        # idx=2 (0-indexed) markdown cell -> the following code cell sits at
        # 1-indexed position idx+2 = 4 in the real shipped notebook.
        assert steps[0].cell == 4
        assert steps[1].cell == 6
        assert "Exchange" in steps[2].detail  # step 3's real body text, not a stub

    def test_only_numbered_markdown_headings_become_steps(self, tmp_path):
        """A title cell with no '## N.' heading (like every notebook's first
        cell) and a plain (non-numbered) '##' heading are both skipped --
        only cells matching the numbered convention become steps."""
        notebook = tmp_path / "notebook.ipynb"
        self._write_notebook(
            notebook,
            [
                self._md_cell(["# Some Topic\n", "\n", "intro text"]),
                self._code_cell(["import pyspark"]),
                self._md_cell(["## Not numbered\n", "\n", "should be skipped"]),
                self._code_cell(["x = 1"]),
                self._md_cell(["## 1. First real step\n", "\n", "body text"]),
                self._code_cell(["y = 2"]),
            ],
        )
        topic = loader.load_topic("partitioning-shuffle")
        topic.notebook_path = notebook

        steps = topic.walkthrough_steps()

        assert len(steps) == 1
        assert steps[0].number == "1"
        assert steps[0].title == "First real step"
        assert steps[0].detail == "body text"
        assert steps[0].cell == 6  # idx=4 -> idx+2

    def test_markdown_step_as_final_cell_uses_its_own_1_indexed_position(self, tmp_path):
        """If the numbered heading is the very last cell (no following code
        cell), 'cell' falls back to the step's own 1-indexed position rather
        than pointing past the end of the notebook."""
        notebook = tmp_path / "notebook.ipynb"
        self._write_notebook(
            notebook,
            [
                self._code_cell(["setup = True"]),
                self._md_cell(["## 1. Trailing step with no code after it"]),
            ],
        )
        topic = loader.load_topic("partitioning-shuffle")
        topic.notebook_path = notebook

        steps = topic.walkthrough_steps()

        assert len(steps) == 1
        assert steps[0].cell == 2  # idx=1 -> idx+1 (last cell, no next cell)

    def test_skips_intervening_markdown_to_find_the_next_code_cell(self, tmp_path):
        """Code-review finding: the original cell_number = idx + 2 assumed
        the cell immediately after a numbered heading is always code -- true
        for all 4 shipped notebooks today (they strictly alternate
        markdown/code) but not guaranteed. An explanatory aside markdown
        cell between the heading and its code must be skipped over, not
        mistaken for the step's cell."""
        notebook = tmp_path / "notebook.ipynb"
        self._write_notebook(
            notebook,
            [
                self._md_cell(["## 1. Step with an aside before its code\n", "\n", "body text"]),
                self._md_cell(["Just an explanatory aside, not a numbered step."]),
                self._code_cell(["z = 1"]),
            ],
        )
        topic = loader.load_topic("partitioning-shuffle")
        topic.notebook_path = notebook

        steps = topic.walkthrough_steps()

        assert len(steps) == 1
        assert steps[0].cell == 3  # idx=0 -> skips idx=1 (markdown aside) -> idx=2 (code) -> idx+1

    def test_markdown_step_with_only_markdown_after_it_falls_back_to_own_position(self, tmp_path):
        """If every remaining cell after the heading is markdown (no code
        cell before the notebook ends), 'cell' falls back to the heading's
        own 1-indexed position -- same graceful degradation as the
        last-cell case, not a crash or an out-of-range reference."""
        notebook = tmp_path / "notebook.ipynb"
        self._write_notebook(
            notebook,
            [
                self._md_cell(["## 1. Step with no code anywhere after it\n", "\n", "body text"]),
                self._md_cell(["Trailing markdown aside, still no code."]),
            ],
        )
        topic = loader.load_topic("partitioning-shuffle")
        topic.notebook_path = notebook

        steps = topic.walkthrough_steps()

        assert len(steps) == 1
        assert steps[0].cell == 1  # idx=0 -> no code cell follows -> falls back to idx+1

    def test_source_as_plain_string_is_handled_same_as_list_of_lines(self, tmp_path):
        """nbformat's `source` field may be a single string instead of a list
        of lines -- both are valid per the notebook format spec, and the
        parser must handle both the same way."""
        notebook = tmp_path / "notebook.ipynb"
        self._write_notebook(
            notebook,
            [
                {"cell_type": "markdown", "source": "## 1. String-source step\nsome detail"},
                self._code_cell(["z = 3"]),
            ],
        )
        topic = loader.load_topic("partitioning-shuffle")
        topic.notebook_path = notebook

        steps = topic.walkthrough_steps()

        assert len(steps) == 1
        assert steps[0].title == "String-source step"
        assert steps[0].detail == "some detail"

    def test_malformed_notebook_json_degrades_to_empty_list_not_a_raise(self, tmp_path):
        """Per the method's own docstring: a broken/unparseable notebook.ipynb
        must not blow up the Notebook tab -- it just shows no steps. This is
        distinct from a *missing* notebook file, which load_topic() already
        rejects eagerly at load time (issue #5) before walkthrough_steps() is
        ever called."""
        notebook = tmp_path / "notebook.ipynb"
        notebook.write_text("{not valid json", encoding="utf-8")
        topic = loader.load_topic("partitioning-shuffle")
        topic.notebook_path = notebook

        assert topic.walkthrough_steps() == []

    def test_notebook_with_no_cells_key_degrades_to_empty_list(self, tmp_path):
        notebook = tmp_path / "notebook.ipynb"
        notebook.write_text(json.dumps({}), encoding="utf-8")
        topic = loader.load_topic("partitioning-shuffle")
        topic.notebook_path = notebook

        assert topic.walkthrough_steps() == []

    def test_every_built_topic_notebook_yields_no_parse_errors(self):
        """R-Shell-5-style smoke check: every currently shipped topic's
        notebook must parse to *some* list (possibly empty) without raising,
        since walkthrough_steps() backs a tab every topic renders through the
        one shared shell."""
        for topic in loader.list_topics():
            steps = topic.walkthrough_steps()
            assert isinstance(steps, list)
