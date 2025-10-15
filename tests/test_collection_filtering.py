"""Tests for collection-based metadata filtering with CONTAINS operator."""

import time

import pytest
from cassandra.cluster import Cluster

from langgraph_checkpoint_cassandra import CassandraSaver


@pytest.fixture
def cluster():
    """Create a Cassandra cluster connection."""
    cluster = Cluster(["cassandra"])
    yield cluster
    cluster.shutdown()


@pytest.fixture
def saver_with_collections(cluster):
    """Create a CassandraSaver with collection-type metadata fields."""
    session = cluster.connect()
    keyspace = f"test_collections_{int(time.time())}"

    # Create saver with list, set, and map metadata fields
    saver = CassandraSaver(
        session,
        keyspace=keyspace,
        queryable_metadata={"tags": list, "categories": set, "attributes": dict},
    )

    # Setup schema
    saver.setup(replication_factor=1)

    yield saver

    # Cleanup
    session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
    session.shutdown()


@pytest.fixture
def sample_checkpoint():
    """Sample checkpoint for testing."""
    return {
        "v": 1,
        "id": "test-checkpoint-id",
        "ts": "2024-01-01T00:00:00Z",
        "channel_values": {"messages": ["hello"]},
        "channel_versions": {"messages": "0000000001.abc"},
        "versions_seen": {},
        "pending_sends": [],
    }


def test_list_filtering_with_contains(saver_with_collections, sample_checkpoint):
    """Test CONTAINS operator on list metadata field."""
    saver = saver_with_collections
    base_config = {"configurable": {"thread_id": "test-thread-1", "checkpoint_ns": ""}}

    # Create checkpoints with different tags
    checkpoints_data = [
        ("checkpoint-1", {"tags": ["python", "cassandra", "langgraph"]}),
        ("checkpoint-2", {"tags": ["python", "postgres"]}),
        ("checkpoint-3", {"tags": ["javascript", "mongodb"]}),
    ]

    for checkpoint_id, metadata in checkpoints_data:
        cp = {**sample_checkpoint, "id": checkpoint_id}
        saver.put(base_config, cp, metadata, {})

    # Filter by tag that should match checkpoint-1 only
    cassandra_checkpoints = list(saver.list(base_config, filter={"tags": "cassandra"}))
    assert len(cassandra_checkpoints) == 1
    assert cassandra_checkpoints[0].checkpoint["id"] == "checkpoint-1"

    # Filter by tag that should match checkpoints 1 and 2
    python_checkpoints = list(saver.list(base_config, filter={"tags": "python"}))
    assert len(python_checkpoints) == 2
    assert set(cp.checkpoint["id"] for cp in python_checkpoints) == {
        "checkpoint-1",
        "checkpoint-2",
    }


def test_set_filtering_with_contains(saver_with_collections, sample_checkpoint):
    """Test CONTAINS operator on set metadata field."""
    saver = saver_with_collections
    base_config = {"configurable": {"thread_id": "test-thread-2", "checkpoint_ns": ""}}

    # Create checkpoints with different categories (as sets)
    checkpoints_data = [
        ("checkpoint-1", {"categories": {"backend", "database", "distributed"}}),
        ("checkpoint-2", {"categories": {"backend", "api"}}),
        ("checkpoint-3", {"categories": {"frontend", "ui"}}),
    ]

    for checkpoint_id, metadata in checkpoints_data:
        cp = {**sample_checkpoint, "id": checkpoint_id}
        saver.put(base_config, cp, metadata, {})

    # Filter by category
    backend_checkpoints = list(
        saver.list(base_config, filter={"categories": "backend"})
    )
    assert len(backend_checkpoints) == 2
    assert set(cp.checkpoint["id"] for cp in backend_checkpoints) == {
        "checkpoint-1",
        "checkpoint-2",
    }

    # Filter by category that only matches one
    database_checkpoints = list(
        saver.list(base_config, filter={"categories": "database"})
    )
    assert len(database_checkpoints) == 1
    assert database_checkpoints[0].checkpoint["id"] == "checkpoint-1"


def test_map_value_filtering_with_contains(saver_with_collections, sample_checkpoint):
    """Test CONTAINS operator on map metadata field (value search)."""
    saver = saver_with_collections
    base_config = {"configurable": {"thread_id": "test-thread-3", "checkpoint_ns": ""}}

    # Create checkpoints with different attributes (as maps)
    checkpoints_data = [
        ("checkpoint-1", {"attributes": {"env": "prod", "region": "us-east"}}),
        ("checkpoint-2", {"attributes": {"env": "staging", "region": "us-west"}}),
        ("checkpoint-3", {"attributes": {"env": "prod", "region": "eu-central"}}),
    ]

    for checkpoint_id, metadata in checkpoints_data:
        cp = {**sample_checkpoint, "id": checkpoint_id}
        saver.put(base_config, cp, metadata, {})

    # Filter by map containing a specific value
    prod_checkpoints = list(saver.list(base_config, filter={"attributes": "prod"}))
    assert len(prod_checkpoints) == 2
    assert set(cp.checkpoint["id"] for cp in prod_checkpoints) == {
        "checkpoint-1",
        "checkpoint-3",
    }


def test_map_key_value_filtering(saver_with_collections, sample_checkpoint):
    """Test filtering on map with specific key-value pairs (PostgreSQL @> behavior)."""
    saver = saver_with_collections
    base_config = {"configurable": {"thread_id": "test-thread-4", "checkpoint_ns": ""}}

    # Create checkpoints with different attributes
    checkpoints_data = [
        ("checkpoint-1", {"attributes": {"env": "prod", "region": "us-east"}}),
        ("checkpoint-2", {"attributes": {"env": "staging", "region": "us-west"}}),
        ("checkpoint-3", {"attributes": {"env": "prod", "region": "eu-central"}}),
    ]

    for checkpoint_id, metadata in checkpoints_data:
        cp = {**sample_checkpoint, "id": checkpoint_id}
        saver.put(base_config, cp, metadata, {})

    # Filter by specific key-value pair
    prod_us_east = list(
        saver.list(
            base_config, filter={"attributes": {"env": "prod", "region": "us-east"}}
        )
    )
    assert len(prod_us_east) == 1
    assert prod_us_east[0].checkpoint["id"] == "checkpoint-1"


def test_indexed_metadata_parameter(cluster, sample_checkpoint):
    """Test that indexed_metadata controls which fields get SAI indexes."""
    session = cluster.connect()
    keyspace = f"test_indexed_{int(time.time())}"

    # Create saver with queryable metadata but only index some fields
    saver = CassandraSaver(
        session,
        keyspace=keyspace,
        queryable_metadata={
            "user_id": str,
            "step": int,
            "tags": list,
        },
        indexed_metadata=["user_id"],  # Only index user_id
    )

    try:
        saver.setup(replication_factor=1)

        # Check that only user_id has an index
        # Query system tables to verify indexes
        index_result = session.execute(
            f"SELECT index_name FROM system_schema.indexes "
            f"WHERE keyspace_name = '{keyspace}' AND table_name = 'checkpoints'"
        )

        index_names = {row.index_name for row in index_result}

        # Should have index for user_id
        assert "idx_metadata__user_id" in index_names

        # Should NOT have indexes for step or tags
        assert "idx_metadata__step" not in index_names
        assert "idx_metadata__tags" not in index_names

        # But filtering should still work (with ALLOW FILTERING for non-indexed)
        base_config = {
            "configurable": {"thread_id": "test-thread-5", "checkpoint_ns": ""}
        }

        # Create checkpoint with metadata
        metadata = {"user_id": "user1", "step": 5, "tags": ["test"]}
        saver.put(base_config, sample_checkpoint, metadata, {})

        # Filter by indexed field (fast, uses SAI)
        by_user = list(saver.list(base_config, filter={"user_id": "user1"}))
        assert len(by_user) == 1

        # Filter by non-indexed field (slower, uses ALLOW FILTERING)
        by_step = list(saver.list(base_config, filter={"step": 5}))
        assert len(by_step) == 1

        # Filter by non-indexed list field with CONTAINS
        by_tag = list(saver.list(base_config, filter={"tags": "test"}))
        assert len(by_tag) == 1

    finally:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        session.shutdown()


def test_empty_indexed_metadata(cluster, sample_checkpoint):
    """Test that queryable_metadata without any indexes still works."""
    session = cluster.connect()
    keyspace = f"test_no_index_{int(time.time())}"

    # Create saver with queryable metadata but NO indexes
    saver = CassandraSaver(
        session,
        keyspace=keyspace,
        queryable_metadata={"user_id": str, "step": int},
        indexed_metadata=[],  # No indexes at all
    )

    try:
        saver.setup(replication_factor=1)

        # Verify no indexes were created
        index_result = session.execute(
            f"SELECT index_name FROM system_schema.indexes "
            f"WHERE keyspace_name = '{keyspace}' AND table_name = 'checkpoints'"
        )

        index_names = list(index_result)
        assert len(index_names) == 0

        # But filtering should still work with ALLOW FILTERING
        base_config = {
            "configurable": {"thread_id": "test-thread-6", "checkpoint_ns": ""}
        }

        metadata = {"user_id": "user1", "step": 5}
        saver.put(base_config, sample_checkpoint, metadata, {})

        # Both filters should work (both use ALLOW FILTERING)
        by_user = list(saver.list(base_config, filter={"user_id": "user1"}))
        assert len(by_user) == 1

        by_step = list(saver.list(base_config, filter={"step": 5}))
        assert len(by_step) == 1

    finally:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        session.shutdown()


if __name__ == "__main__":
    """Run tests directly for debugging."""
    pytest.main([__file__, "-v"])
