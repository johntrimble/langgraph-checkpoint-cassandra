"""Tests for queryable metadata functionality."""

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
def saver_with_queryable_metadata(cluster):
    """Create a CassandraSaver with queryable metadata configured."""
    session = cluster.connect()
    keyspace = f"test_queryable_{int(time.time())}"

    # Create saver with queryable metadata fields
    saver = CassandraSaver(
        session,
        keyspace=keyspace,
        queryable_metadata={"user_id": str, "step": int, "source": str},
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


def test_queryable_metadata_setup(saver_with_queryable_metadata):
    """Test that queryable metadata columns and indexes are created."""
    saver = saver_with_queryable_metadata

    # Check that columns exist by querying the table
    result = saver.session.execute(
        f"SELECT column_name FROM system_schema.columns "
        f"WHERE keyspace_name = '{saver.keyspace}' AND table_name = 'checkpoints'"
    )

    column_names = {row.column_name for row in result}

    # Verify metadata columns exist
    assert "metadata__user_id" in column_names
    assert "metadata__step" in column_names
    assert "metadata__source" in column_names


def test_put_with_queryable_metadata(saver_with_queryable_metadata, sample_checkpoint):
    """Test that metadata fields are stored in queryable columns."""
    saver = saver_with_queryable_metadata

    config = {
        "configurable": {
            "thread_id": "test-thread-1",
            "checkpoint_ns": "",
        }
    }

    metadata = {
        "user_id": "user123",
        "step": 5,
        "source": "input",
        "other_field": "not_queryable",
    }

    # Put checkpoint with metadata
    saver.put(config, sample_checkpoint, metadata, {})

    # Retrieve the checkpoint directly from Cassandra to verify columns
    result = saver.session.execute(
        f"""
        SELECT metadata__user_id, metadata__step, metadata__source
        FROM {saver.keyspace}.checkpoints
        WHERE thread_id = 'test-thread-1' AND checkpoint_ns = ''
        """
    )

    row = result.one()
    assert row is not None
    assert row.metadata__user_id == "user123"
    assert row.metadata__step == 5
    assert row.metadata__source == "input"


def test_list_with_server_side_filter(saver_with_queryable_metadata, sample_checkpoint):
    """Test server-side filtering on queryable metadata fields."""
    saver = saver_with_queryable_metadata

    # Create checkpoints with different metadata
    base_config = {"configurable": {"thread_id": "test-thread-2", "checkpoint_ns": ""}}

    checkpoints_data = [
        ("checkpoint-1", {"user_id": "user1", "step": 1, "source": "input"}),
        ("checkpoint-2", {"user_id": "user2", "step": 2, "source": "loop"}),
        ("checkpoint-3", {"user_id": "user1", "step": 3, "source": "loop"}),
        ("checkpoint-4", {"user_id": "user2", "step": 4, "source": "input"}),
    ]

    for checkpoint_id, metadata in checkpoints_data:
        cp = {**sample_checkpoint, "id": checkpoint_id}
        saver.put(base_config, cp, metadata, {})

    # Filter by user_id (server-side)
    user1_checkpoints = list(saver.list(base_config, filter={"user_id": "user1"}))
    assert len(user1_checkpoints) == 2
    assert all(cp.metadata["user_id"] == "user1" for cp in user1_checkpoints)

    # Filter by source (server-side)
    input_checkpoints = list(saver.list(base_config, filter={"source": "input"}))
    assert len(input_checkpoints) == 2
    assert all(cp.metadata["source"] == "input" for cp in input_checkpoints)

    # Filter by multiple fields (server-side)
    filtered = list(
        saver.list(base_config, filter={"user_id": "user1", "source": "loop"})
    )
    assert len(filtered) == 1
    assert filtered[0].checkpoint["id"] == "checkpoint-3"


def test_list_with_mixed_filters(saver_with_queryable_metadata, sample_checkpoint):
    """Test filtering with both server-side and client-side filters."""
    saver = saver_with_queryable_metadata

    base_config = {"configurable": {"thread_id": "test-thread-3", "checkpoint_ns": ""}}

    # Create checkpoints
    checkpoints_data = [
        ("checkpoint-1", {"user_id": "user1", "step": 1, "custom": "value1"}),
        ("checkpoint-2", {"user_id": "user1", "step": 2, "custom": "value2"}),
        ("checkpoint-3", {"user_id": "user2", "step": 3, "custom": "value1"}),
    ]

    for checkpoint_id, metadata in checkpoints_data:
        cp = {**sample_checkpoint, "id": checkpoint_id}
        saver.put(base_config, cp, metadata, {})

    # Filter with both server-side (user_id) and client-side (custom) filters
    filtered = list(
        saver.list(base_config, filter={"user_id": "user1", "custom": "value2"})
    )

    assert len(filtered) == 1
    assert filtered[0].checkpoint["id"] == "checkpoint-2"
    assert filtered[0].metadata["user_id"] == "user1"
    assert filtered[0].metadata["custom"] == "value2"


def test_list_with_limit_and_filter(saver_with_queryable_metadata, sample_checkpoint):
    """Test that limit works correctly with filtered results."""
    saver = saver_with_queryable_metadata

    base_config = {"configurable": {"thread_id": "test-thread-4", "checkpoint_ns": ""}}

    # Create 5 checkpoints for same user
    for i in range(5):
        checkpoint_id = f"{i:032d}"
        cp = {**sample_checkpoint, "id": checkpoint_id}
        metadata = {"user_id": "user1", "step": i}
        saver.put(base_config, cp, metadata, {})

    # List with filter and limit
    filtered = list(saver.list(base_config, filter={"user_id": "user1"}, limit=2))

    assert len(filtered) == 2
    assert all(cp.metadata["user_id"] == "user1" for cp in filtered)


def test_queryable_metadata_with_none_values(
    saver_with_queryable_metadata, sample_checkpoint
):
    """Test that None values in queryable metadata are handled correctly."""
    saver = saver_with_queryable_metadata

    config = {
        "configurable": {
            "thread_id": "test-thread-5",
            "checkpoint_ns": "",
        }
    }

    # Metadata without some queryable fields
    metadata = {
        "user_id": "user1",
        # step and source are missing
    }

    # Should not raise an error
    saver.put(config, sample_checkpoint, metadata, {})

    # Retrieve checkpoint
    checkpoints = list(saver.list(config))
    assert len(checkpoints) == 1
    assert checkpoints[0].metadata["user_id"] == "user1"


def test_saver_without_queryable_metadata(cluster, sample_checkpoint):
    """Test that saver works normally without queryable metadata."""
    session = cluster.connect()
    keyspace = f"test_no_queryable_{int(time.time())}"

    # Create saver WITHOUT queryable metadata
    saver = CassandraSaver(session, keyspace=keyspace)
    saver.setup(replication_factor=1)

    try:
        config = {
            "configurable": {
                "thread_id": "test-thread-6",
                "checkpoint_ns": "",
            }
        }

        metadata = {"user_id": "user1", "step": 1}

        # Should work normally
        saver.put(config, sample_checkpoint, metadata, {})

        # List should work with client-side filtering
        checkpoints = list(saver.list(config, filter={"user_id": "user1"}))
        assert len(checkpoints) == 1

    finally:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        session.shutdown()


if __name__ == "__main__":
    """Run tests directly for debugging."""
    pytest.main([__file__, "-v"])
