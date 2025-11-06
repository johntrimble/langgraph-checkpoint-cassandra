"""
Tests for metadata filtering with flattened dot notation.
"""

import pytest
from cassandra.cluster import Cluster
from langgraph.checkpoint.base import Checkpoint, CheckpointMetadata
from langgraph_checkpoint_cassandra import CassandraSaver, drop_schema

TEST_KEYSPACE = "test_metadata_filtering"
CASSANDRA_HOST = "cassandra"


@pytest.fixture(scope="module")
def cassandra_session():
    """Provide a Cassandra session for tests."""
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()

    # Ensure clean state
    drop_schema(session, keyspace=TEST_KEYSPACE)

    yield session

    # Teardown: Drop schema
    drop_schema(session, keyspace=TEST_KEYSPACE)
    cluster.shutdown()


@pytest.fixture(scope="function")
def saver(cassandra_session):
    """Create a CassandraSaver instance for testing."""
    saver = CassandraSaver(
        cassandra_session,
        keyspace=TEST_KEYSPACE,
        checkpoint_id_type="text",
    )
    saver.setup(replication_factor=1)
    return saver


def test_flatten_simple_metadata(saver):
    """Test filtering on simple top-level metadata fields."""
    thread_id = "test_thread_simple"

    # Create checkpoints with different metadata
    checkpoints = [
        {
            "config": {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
            "checkpoint": {"v": 1, "id": "cp1", "ts": "2024-01-01T00:00:00Z"},
            "metadata": {"source": "input", "step": 1, "score": 0.5},
        },
        {
            "config": {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
            "checkpoint": {"v": 1, "id": "cp2", "ts": "2024-01-01T00:01:00Z"},
            "metadata": {"source": "loop", "step": 2, "score": 0.8},
        },
        {
            "config": {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
            "checkpoint": {"v": 1, "id": "cp3", "ts": "2024-01-01T00:02:00Z"},
            "metadata": {"source": "loop", "step": 3, "score": 0.9},
        },
    ]

    # Save checkpoints
    for cp_data in checkpoints:
        saver.put(
            cp_data["config"],
            cp_data["checkpoint"],
            cp_data["metadata"],
            {},
        )

    # Test filter by string field
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"source": "loop"}
    ))
    assert len(results) == 2
    assert all(r.metadata["source"] == "loop" for r in results)

    # Test filter by int field
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"step": 2}
    ))
    assert len(results) == 1
    assert results[0].metadata["step"] == 2

    # Test filter by float field
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"score": 0.9}
    ))
    assert len(results) == 1
    assert results[0].metadata["score"] == 0.9

    # Test multiple filters (AND logic)
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"source": "loop", "step": 3}
    ))
    assert len(results) == 1
    assert results[0].metadata["source"] == "loop"
    assert results[0].metadata["step"] == 3


def test_flatten_nested_metadata(saver):
    """Test filtering on nested metadata fields using dot notation."""
    thread_id = "test_thread_nested"

    # Create checkpoints with nested metadata
    checkpoints = [
        {
            "config": {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
            "checkpoint": {"v": 1, "id": "cp1", "ts": "2024-01-01T00:00:00Z"},
            "metadata": {
                "source": "input",
                "user": {"name": "alice", "age": 30},
                "config": {"env": "prod", "debug": False},
            },
        },
        {
            "config": {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
            "checkpoint": {"v": 1, "id": "cp2", "ts": "2024-01-01T00:01:00Z"},
            "metadata": {
                "source": "loop",
                "user": {"name": "bob", "age": 25},
                "config": {"env": "dev", "debug": True},
            },
        },
        {
            "config": {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
            "checkpoint": {"v": 1, "id": "cp3", "ts": "2024-01-01T00:02:00Z"},
            "metadata": {
                "source": "loop",
                "user": {"name": "alice", "age": 31},
                "config": {"env": "prod", "debug": False},
            },
        },
    ]

    # Save checkpoints
    for cp_data in checkpoints:
        saver.put(
            cp_data["config"],
            cp_data["checkpoint"],
            cp_data["metadata"],
            {},
        )

    # Test filter by nested string field
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"user.name": "alice"}
    ))
    assert len(results) == 2
    assert all(r.metadata["user"]["name"] == "alice" for r in results)

    # Test filter by nested int field
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"user.age": 25}
    ))
    assert len(results) == 1
    assert results[0].metadata["user"]["age"] == 25

    # Test filter by nested bool field
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"config.debug": True}
    ))
    assert len(results) == 1
    assert results[0].metadata["config"]["debug"] is True

    # Test multiple nested filters
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"user.name": "alice", "config.env": "prod"}
    ))
    assert len(results) == 2
    for r in results:
        assert r.metadata["user"]["name"] == "alice"
        assert r.metadata["config"]["env"] == "prod"


def test_flatten_null_values(saver):
    """Test filtering on NULL metadata values."""
    thread_id = "test_thread_null"

    # Create checkpoints with NULL values
    checkpoints = [
        {
            "config": {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
            "checkpoint": {"v": 1, "id": "cp1", "ts": "2024-01-01T00:00:00Z"},
            "metadata": {"source": "input", "score": None, "active": True},
        },
        {
            "config": {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
            "checkpoint": {"v": 1, "id": "cp2", "ts": "2024-01-01T00:01:00Z"},
            "metadata": {"source": "loop", "score": 0.8, "active": True},
        },
        {
            "config": {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
            "checkpoint": {"v": 1, "id": "cp3", "ts": "2024-01-01T00:02:00Z"},
            "metadata": {"source": "loop", "score": None, "active": False},
        },
    ]

    # Save checkpoints
    for cp_data in checkpoints:
        saver.put(
            cp_data["config"],
            cp_data["checkpoint"],
            cp_data["metadata"],
            {},
        )

    # Test filter by NULL value
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"score": None}
    ))
    assert len(results) == 2
    assert all(r.metadata["score"] is None for r in results)

    # Test combined filter with NULL and non-NULL
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"score": None, "active": False}
    ))
    assert len(results) == 1
    assert results[0].metadata["score"] is None
    assert results[0].metadata["active"] is False


def test_flatten_keys_with_dots(saver):
    """Test that keys containing actual dots are properly escaped.

    Keys with literal dots must be escaped with backslash (\\.) in filter queries.
    This allows distinguishing between:
    - "file.txt" (navigation: file -> txt)
    - "file\\.txt" (literal key named "file.txt")
    """
    thread_id = "test_thread_dots"

    # Create checkpoint with keys containing dots
    metadata = {
        "source": "input",
        "file.txt": "content1",  # Key with literal dot
        "config.json": "content2",  # Key with literal dot
        "nested": {
            "file.py": "content3",  # Nested key with literal dot
        },
    }

    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    checkpoint = {"v": 1, "id": "cp1", "ts": "2024-01-01T00:00:00Z"}

    saver.put(config, checkpoint, metadata, {})

    # Filter by top-level key with literal dot (must escape the dot)
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"file\\.txt": "content1"}
    ))
    assert len(results) == 1
    assert results[0].metadata["file.txt"] == "content1"

    # Filter by another top-level key with literal dot
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"config\\.json": "content2"}
    ))
    assert len(results) == 1
    assert results[0].metadata["config.json"] == "content2"

    # Filter by nested key with literal dot (navigation dot is unescaped, literal dot is escaped)
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"nested.file\\.py": "content3"}
    ))
    assert len(results) == 1
    assert results[0].metadata["nested"]["file.py"] == "content3"


def test_flatten_parents_field(saver):
    """Test that the parents field (dict[str, str]) is properly flattened."""
    thread_id = "test_thread_parents"

    # Create checkpoints with parents field
    checkpoints = [
        {
            "config": {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
            "checkpoint": {"v": 1, "id": "cp1", "ts": "2024-01-01T00:00:00Z"},
            "metadata": {
                "source": "loop",
                "step": 0,
                "parents": {},  # Empty parents (root graph)
            },
        },
        {
            "config": {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
            "checkpoint": {"v": 1, "id": "cp2", "ts": "2024-01-01T00:01:00Z"},
            "metadata": {
                "source": "loop",
                "step": 0,
                "parents": {"": "checkpoint_root_123"},  # Single parent
            },
        },
        {
            "config": {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
            "checkpoint": {"v": 1, "id": "cp3", "ts": "2024-01-01T00:02:00Z"},
            "metadata": {
                "source": "loop",
                "step": 0,
                "parents": {
                    "": "checkpoint_root_123",
                    ":task_xyz": "checkpoint_task_456",
                },
            },
        },
    ]

    # Save checkpoints
    for cp_data in checkpoints:
        saver.put(
            cp_data["config"],
            cp_data["checkpoint"],
            cp_data["metadata"],
            {},
        )

    # Filter by root parent (empty string key becomes "parents.")
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"parents.": "checkpoint_root_123"}
    ))
    assert len(results) == 2
    assert all(r.metadata["parents"].get("") == "checkpoint_root_123" for r in results)

    # Filter by subgraph parent
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"parents.:task_xyz": "checkpoint_task_456"}
    ))
    assert len(results) == 1
    assert results[0].metadata["parents"][":task_xyz"] == "checkpoint_task_456"


def test_no_filter_returns_all(saver):
    """Test that no filter returns all checkpoints."""
    thread_id = "test_thread_all"

    # Create multiple checkpoints
    for i in range(5):
        config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
        checkpoint = {"v": 1, "id": f"cp{i}", "ts": f"2024-01-01T00:0{i}:00Z"}
        metadata = {"source": "loop", "step": i}
        saver.put(config, checkpoint, metadata, {})

    # List without filter should return all
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
    ))
    assert len(results) == 5


def test_filter_with_limit(saver):
    """Test that filtering works correctly with limit parameter."""
    thread_id = "test_thread_limit"

    # Create checkpoints with same filter value
    for i in range(10):
        config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
        checkpoint = {"v": 1, "id": f"cp{i}", "ts": f"2024-01-01T00:{i:02d}:00Z"}
        metadata = {"source": "loop", "step": i}
        saver.put(config, checkpoint, metadata, {})

    # Filter with limit
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"source": "loop"},
        limit=5
    ))
    assert len(results) == 5


def test_deeply_nested_metadata(saver):
    """Test filtering on deeply nested metadata structures."""
    thread_id = "test_thread_deep"

    metadata = {
        "source": "input",
        "level1": {
            "level2": {
                "level3": {
                    "value": "deep",
                    "count": 42,
                },
            },
        },
    }

    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    checkpoint = {"v": 1, "id": "cp1", "ts": "2024-01-01T00:00:00Z"}

    saver.put(config, checkpoint, metadata, {})

    # Filter by deeply nested string
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"level1.level2.level3.value": "deep"}
    ))
    assert len(results) == 1
    assert results[0].metadata["level1"]["level2"]["level3"]["value"] == "deep"

    # Filter by deeply nested int
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"level1.level2.level3.count": 42}
    ))
    assert len(results) == 1
    assert results[0].metadata["level1"]["level2"]["level3"]["count"] == 42


def test_metadata_includes_integration(cassandra_session):
    """Test that metadata_includes parameter works end-to-end."""
    # Create saver with includes filter
    saver = CassandraSaver(
        cassandra_session,
        keyspace=TEST_KEYSPACE,
        checkpoint_id_type="text",
        metadata_includes=["user.*", "step"],  # Only include user fields and step
    )
    saver.setup(replication_factor=1)

    thread_id = "test_thread_includes"
    metadata = {
        "user": {"name": "alice", "age": 30},
        "config": {"db": "postgres"},  # Should not be indexed
        "debug": {"trace": "xyz"},  # Should not be indexed
        "step": 5,  # Should be indexed (exact match)
    }

    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    checkpoint = {"v": 1, "id": "cp1", "ts": "2024-01-01T00:00:00Z"}

    saver.put(config, checkpoint, metadata, {})

    # Should be able to filter on user fields (they were included)
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"user.name": "alice"}
    ))
    assert len(results) == 1
    assert results[0].metadata["user"]["name"] == "alice"

    # Should be able to filter on step (it was included)
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"step": 5}
    ))
    assert len(results) == 1

    # CAN still filter on config.db (not indexed, but client-side filtering works)
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"config.db": "postgres"}
    ))
    assert len(results) == 1  # Client-side filtering found the match
    assert results[0].metadata["config"]["db"] == "postgres"

    # Client-side filtering also works for debug fields
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"debug.trace": "xyz"}
    ))
    assert len(results) == 1
    assert results[0].metadata["debug"]["trace"] == "xyz"

    # Metadata blob contains all fields
    all_results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    ))
    assert len(all_results) == 1
    assert all_results[0].metadata["config"]["db"] == "postgres"
    assert all_results[0].metadata["debug"]["trace"] == "xyz"


def test_metadata_excludes_integration(cassandra_session):
    """Test that metadata_excludes parameter works end-to-end."""
    # Create saver with excludes filter
    saver = CassandraSaver(
        cassandra_session,
        keyspace=TEST_KEYSPACE,
        checkpoint_id_type="text",
        metadata_excludes=["*.password", "*.secret"],  # Exclude sensitive fields
    )
    saver.setup(replication_factor=1)

    thread_id = "test_thread_excludes"
    metadata = {
        "user": {"name": "alice", "password": "secret123"},
        "api": {"key": "public", "secret": "private"},
        "step": 5,
    }

    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    checkpoint = {"v": 1, "id": "cp1", "ts": "2024-01-01T00:00:00Z"}

    saver.put(config, checkpoint, metadata, {})

    # Should be able to filter on non-excluded fields
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"user.name": "alice"}
    ))
    assert len(results) == 1

    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"api.key": "public"}
    ))
    assert len(results) == 1

    # CAN still filter on excluded fields (client-side filtering works)
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"user.password": "secret123"}
    ))
    assert len(results) == 1  # Client-side filtering found the match

    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"api.secret": "private"}
    ))
    assert len(results) == 1  # Client-side filtering found the match

    # Metadata blob contains all fields
    all_results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    ))
    assert len(all_results) == 1
    assert all_results[0].metadata["user"]["password"] == "secret123"  # Still in blob
    assert all_results[0].metadata["api"]["secret"] == "private"  # Still in blob


def test_metadata_includes_and_excludes_integration(cassandra_session):
    """Test that metadata_includes and excludes work together end-to-end."""
    # Include user fields but exclude passwords
    saver = CassandraSaver(
        cassandra_session,
        keyspace=TEST_KEYSPACE,
        checkpoint_id_type="text",
        metadata_includes=["user.*"],
        metadata_excludes=["*.password", "*.token"],
    )
    saver.setup(replication_factor=1)

    thread_id = "test_thread_includes_excludes"
    metadata = {
        "user": {
            "id": "user123",
            "name": "alice",
            "email": "alice@example.com",
            "password": "secret",
            "token": "token123",
        },
        "config": {"db": "postgres"},  # Not in includes
        "step": 5,  # Not in includes
    }

    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    checkpoint = {"v": 1, "id": "cp1", "ts": "2024-01-01T00:00:00Z"}

    saver.put(config, checkpoint, metadata, {})

    # Should be able to filter on included, non-excluded user fields
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"user.name": "alice"}
    ))
    assert len(results) == 1

    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"user.email": "alice@example.com"}
    ))
    assert len(results) == 1

    # CAN still filter on excluded fields (client-side filtering)
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"user.password": "secret"}
    ))
    assert len(results) == 1  # Client-side filtering found the match

    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"user.token": "token123"}
    ))
    assert len(results) == 1  # Client-side filtering found the match

    # CAN still filter on fields not in includes (client-side filtering)
    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"config.db": "postgres"}
    ))
    assert len(results) == 1  # Client-side filtering found the match

    results = list(saver.list(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}},
        filter={"step": 5}
    ))
    assert len(results) == 1  # Client-side filtering found the match
