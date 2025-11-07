"""Tests for metadata includes/excludes functionality."""

from langgraph_checkpoint_cassandra.cassandra_saver import (
    _flatten_metadata,
    _should_include_field,
)


class TestShouldIncludeField:
    """Test the _should_include_field helper function."""

    def test_no_filters_includes_all(self):
        """When both includes and excludes are None, all fields should be included."""
        assert _should_include_field("user.name", None, None) is True
        assert _should_include_field("config.db", None, None) is True
        assert _should_include_field("any.field.path", None, None) is True

    def test_empty_lists_behave_correctly(self):
        """Empty includes excludes everything, empty excludes excludes nothing."""
        # Empty includes means include nothing
        assert _should_include_field("user.name", [], None) is True  # No restrictions
        assert _should_include_field("user.name", [], []) is True  # No restrictions

        # Empty excludes means exclude nothing (no-op)
        assert _should_include_field("user.name", None, []) is True

    def test_includes_pattern_matching(self):
        """Test that includes patterns work correctly."""
        # Include only user fields
        assert _should_include_field("user.name", ["user.*"], None) is True
        assert _should_include_field("user.age", ["user.*"], None) is True
        assert _should_include_field("config.db", ["user.*"], None) is False
        assert _should_include_field("step", ["user.*"], None) is False

        # Include fields ending with .id
        assert _should_include_field("user.id", ["*.id"], None) is True
        assert _should_include_field("session.id", ["*.id"], None) is True
        assert _should_include_field("user.name", ["*.id"], None) is False

        # Multiple include patterns
        assert _should_include_field("user.name", ["user.*", "config.*"], None) is True
        assert _should_include_field("config.db", ["user.*", "config.*"], None) is True
        assert _should_include_field("step", ["user.*", "config.*"], None) is False

    def test_excludes_pattern_matching(self):
        """Test that excludes patterns work correctly."""
        # Exclude password fields
        assert _should_include_field("user.password", None, ["*.password"]) is False
        assert _should_include_field("admin.password", None, ["*.password"]) is False
        assert _should_include_field("user.name", None, ["*.password"]) is True

        # Exclude debug fields
        assert _should_include_field("debug.info", None, ["debug.*"]) is False
        assert _should_include_field("debug.trace", None, ["debug.*"]) is False
        assert _should_include_field("user.debug", None, ["debug.*"]) is True

        # Multiple exclude patterns
        assert (
            _should_include_field("user.password", None, ["*.password", "*.secret"])
            is False
        )
        assert (
            _should_include_field("api.secret", None, ["*.password", "*.secret"])
            is False
        )
        assert (
            _should_include_field("user.name", None, ["*.password", "*.secret"]) is True
        )

    def test_includes_and_excludes_together(self):
        """Test that includes and excludes work together correctly."""
        # Include user fields but exclude passwords
        includes = ["user.*"]
        excludes = ["*.password"]

        assert _should_include_field("user.name", includes, excludes) is True
        assert _should_include_field("user.age", includes, excludes) is True
        assert (
            _should_include_field("user.password", includes, excludes) is False
        )  # Exclude wins
        assert (
            _should_include_field("config.db", includes, excludes) is False
        )  # Not in includes

        # Include everything except sensitive fields
        includes = None
        excludes = ["*.password", "*.secret", "*.token"]

        assert _should_include_field("user.name", includes, excludes) is True
        assert _should_include_field("user.password", includes, excludes) is False
        assert _should_include_field("api.secret", includes, excludes) is False
        assert _should_include_field("auth.token", includes, excludes) is False
        assert _should_include_field("config.db", includes, excludes) is True

    def test_exact_matches(self):
        """Test exact field name matching without wildcards."""
        # Exact include
        assert _should_include_field("step", ["step"], None) is True
        assert _should_include_field("source", ["step"], None) is False

        # Exact exclude
        assert _should_include_field("step", None, ["step"]) is False
        assert _should_include_field("source", None, ["step"]) is True

    def test_nested_paths(self):
        """Test deeply nested field paths."""
        includes = ["config.database.*"]
        excludes = ["*.password"]

        assert _should_include_field("config.database.host", includes, excludes) is True
        assert _should_include_field("config.database.port", includes, excludes) is True
        assert (
            _should_include_field("config.database.password", includes, excludes)
            is False
        )
        assert _should_include_field("config.cache.ttl", includes, excludes) is False


class TestFlattenMetadataWithFilters:
    """Test _flatten_metadata with includes/excludes parameters."""

    def test_no_filters(self):
        """Test that no filters includes all fields."""
        metadata = {
            "user": {"name": "alice", "age": 30},
            "config": {"db": "postgres"},
            "step": 5,
        }

        result = _flatten_metadata(metadata)

        assert result["metadata_text"]["user.name"] == "alice"
        assert result["metadata_int"]["user.age"] == 30
        assert result["metadata_text"]["config.db"] == "postgres"
        assert result["metadata_int"]["step"] == 5

    def test_include_specific_fields(self):
        """Test that includes filter works."""
        metadata = {
            "user": {"name": "alice", "age": 30, "password": "secret"},
            "config": {"db": "postgres"},
            "step": 5,
        }

        # Only include user fields
        result = _flatten_metadata(metadata, includes=["user.*"])

        assert result["metadata_text"]["user.name"] == "alice"
        assert result["metadata_int"]["user.age"] == 30
        assert result["metadata_text"]["user.password"] == "secret"
        # config and step should not be included
        assert "config.db" not in result["metadata_text"]
        assert "step" not in result["metadata_int"]

    def test_exclude_specific_fields(self):
        """Test that excludes filter works."""
        metadata = {
            "user": {"name": "alice", "password": "secret"},
            "api": {"key": "public", "secret": "private"},
        }

        # Exclude all password and secret fields
        result = _flatten_metadata(metadata, excludes=["*.password", "*.secret"])

        assert result["metadata_text"]["user.name"] == "alice"
        assert result["metadata_text"]["api.key"] == "public"
        # Sensitive fields should be excluded
        assert "user.password" not in result["metadata_text"]
        assert "api.secret" not in result["metadata_text"]

    def test_includes_and_excludes_together(self):
        """Test that includes and excludes work together."""
        metadata = {
            "user": {"name": "alice", "age": 30, "password": "secret"},
            "config": {"db": "postgres", "cache": "redis"},
            "step": 5,
        }

        # Include only user fields, but exclude passwords
        result = _flatten_metadata(
            metadata, includes=["user.*"], excludes=["*.password"]
        )

        assert result["metadata_text"]["user.name"] == "alice"
        assert result["metadata_int"]["user.age"] == 30
        # Password should be excluded even though it matches user.*
        assert "user.password" not in result["metadata_text"]
        # config and step should not be included
        assert "config.db" not in result["metadata_text"]
        assert "step" not in result["metadata_int"]

    def test_multiple_include_patterns(self):
        """Test multiple include patterns."""
        metadata = {
            "user": {"name": "alice"},
            "session": {"id": "123"},
            "config": {"db": "postgres"},
            "step": 5,
        }

        # Include user and session fields only
        result = _flatten_metadata(metadata, includes=["user.*", "session.*"])

        assert result["metadata_text"]["user.name"] == "alice"
        assert result["metadata_text"]["session.id"] == "123"
        assert "config.db" not in result["metadata_text"]
        assert "step" not in result["metadata_int"]

    def test_all_types_with_filters(self):
        """Test that filters work with all data types."""
        metadata = {
            "string_field": "value",
            "int_field": 42,
            "float_field": 3.14,
            "bool_field": True,
            "null_field": None,
            "excluded_string": "hidden",
        }

        result = _flatten_metadata(metadata, excludes=["excluded_*"])

        assert result["metadata_text"]["string_field"] == "value"
        assert result["metadata_int"]["int_field"] == 42
        assert result["metadata_double"]["float_field"] == 3.14
        assert result["metadata_bool"]["bool_field"] is True
        assert "null_field" in result["metadata_null"]
        # Excluded field should not appear
        assert "excluded_string" not in result["metadata_text"]

    def test_nested_dict_filtering(self):
        """Test filtering on nested dictionaries."""
        metadata = {
            "config": {
                "database": {
                    "host": "localhost",
                    "password": "secret",
                },
                "cache": {
                    "ttl": 3600,
                },
            }
        }

        result = _flatten_metadata(
            metadata, includes=["config.*"], excludes=["*.password"]
        )

        # All config fields should be included except passwords
        assert result["metadata_text"]["config.database.host"] == "localhost"
        assert result["metadata_int"]["config.cache.ttl"] == 3600
        assert "config.database.password" not in result["metadata_text"]

    def test_wildcard_patterns(self):
        """Test various wildcard patterns."""
        metadata = {
            "user_id": "123",
            "session_id": "456",
            "product_name": "widget",
            "step": 5,
        }

        # Include only *_id fields
        result = _flatten_metadata(metadata, includes=["*_id"])

        assert result["metadata_text"]["user_id"] == "123"
        assert result["metadata_text"]["session_id"] == "456"
        assert "product_name" not in result["metadata_text"]
        assert "step" not in result["metadata_int"]

    def test_empty_includes_list(self):
        """Test that empty includes list includes everything (no restrictions)."""
        metadata = {
            "user": {"name": "alice"},
            "step": 5,
        }

        result = _flatten_metadata(metadata, includes=[])

        # Empty includes means no restrictions, so include all
        assert result["metadata_text"]["user.name"] == "alice"
        assert result["metadata_int"]["step"] == 5

    def test_complex_scenario(self):
        """Test a complex real-world scenario."""
        metadata = {
            "user": {
                "id": "user123",
                "name": "Alice",
                "email": "alice@example.com",
                "password_hash": "hashed",
                "api_token": "secret",
            },
            "request": {
                "id": "req456",
                "method": "POST",
                "path": "/api/users",
            },
            "debug": {
                "trace_id": "trace789",
                "timing_ms": 150,
            },
            "step": 1,
        }

        # Include user and request fields, but exclude:
        # - all passwords, tokens, hashes
        # - debug info
        result = _flatten_metadata(
            metadata,
            includes=["user.*", "request.*", "step"],
            excludes=["*.password_hash", "*.api_token", "debug.*"],
        )

        # Should include
        assert result["metadata_text"]["user.id"] == "user123"
        assert result["metadata_text"]["user.name"] == "Alice"
        assert result["metadata_text"]["user.email"] == "alice@example.com"
        assert result["metadata_text"]["request.id"] == "req456"
        assert result["metadata_text"]["request.method"] == "POST"
        assert result["metadata_int"]["step"] == 1

        # Should exclude
        assert "user.password_hash" not in result["metadata_text"]
        assert "user.api_token" not in result["metadata_text"]
        assert "debug.trace_id" not in result["metadata_text"]
        assert "debug.timing_ms" not in result["metadata_int"]
