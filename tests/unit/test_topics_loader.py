"""Tests for app/topics/loader.py (US-1.1: content-as-data)."""
from __future__ import annotations

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
        """load_topic() itself only requires manifest.yaml to exist (matching
        `_topic_dir`/`load_topic`'s actual checks); a manifest pointing at a
        concept.md that doesn't exist on disk fails the moment the content is
        actually read (concept_html()), not silently returning empty/wrong
        content. This documents current behavior -- see report for whether a
        louder failure at load_topic() time would be preferable."""
        topic_dir = tmp_path / "broken-topic"
        topic_dir.mkdir()
        (topic_dir / "manifest.yaml").write_text(
            yaml.dump({"id": "broken-topic", "title": "Broken", "content": "concept.md",
                       "notebook": "notebook.ipynb"}),
            encoding="utf-8",
        )
        # Deliberately do NOT create concept.md or notebook.ipynb.

        with patch.object(config, "CONTENT_DIR", tmp_path):
            topic = loader.load_topic("broken-topic")  # does not raise yet
            with pytest.raises(FileNotFoundError):
                topic.concept_html()

    def test_missing_notebook_file_not_validated_at_load_time(self, tmp_path):
        """Gap found during test-writing: load_topic() never checks that
        notebook_path actually exists on disk -- unlike concept.md (which
        fails loudly the moment concept_html() is called), a topic with a
        missing notebook.ipynb loads without error and only surfaces as a
        problem later, when JupyterLab tries to open a 404'd path inside the
        iframe. This is a real, currently-untested failure mode; flagged in
        the report rather than treated as a hard test failure since it's a
        judgment call on where/how loudly it should fail."""
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
            topic = loader.load_topic("no-notebook-topic")  # currently succeeds
            assert not topic.notebook_path.exists()
            # notebook_relpath is still produced (used for the Jupyter iframe
            # src URL) even though the file it points at doesn't exist.
            assert topic.notebook_relpath == "content/no-notebook-topic/notebook.ipynb"


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
