"""
Integration tests for CassandraSaver.

These tests verify all synchronous operations of the CassandraSaver class.
"""

import pytest
from cassandra.cluster import Cluster
from langgraph.checkpoint.base import Checkpoint, CheckpointMetadata

from langgraph_checkpoint_cassandra import CassandraSaver
from tests.utils import drop_schema

TEST_KEYSPACE = "test_cassandra_saver"
CASSANDRA_HOST = "cassandra"


@pytest.fixture(scope="module")
def cassandra_session():
    """Provide a Cassandra session for tests."""
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()

    # Ensure clean state
    drop_schema(session, keyspace=TEST_KEYSPACE)

    # Schema will be created by the saver

    yield session

    # Teardown: Drop schema
    drop_schema(session, keyspace=TEST_KEYSPACE)
    cluster.shutdown()


@pytest.fixture(scope="function")
def saver(cassandra_session):
    """Provide a fresh CassandraSaver for each test."""
    # Use text type for checkpoint_id to maintain compatibility with test data
    saver = CassandraSaver(
        cassandra_session, keyspace=TEST_KEYSPACE, checkpoint_id_type="text"
    )
    saver.setup(replication_factor=1)  # Create schema
    return saver


@pytest.fixture
def sample_checkpoint():
    """Provide a sample checkpoint for testing."""
    return Checkpoint(
        v=1,
        id="01234567890123456789012345678901",
        ts="2024-01-01T00:00:00Z",
        channel_values={"messages": ["hello", "world"], "count": 42},
        channel_versions={"messages": "0000000001.abc", "count": "0000000002.def"},
        versions_seen={"node1": {"messages": "0000000001.abc"}},
    )


@pytest.fixture
def sample_metadata():
    """Provide sample metadata for testing."""
    return CheckpointMetadata(
        source="input",
        step=-1,
        parents={},
    )


def test_put_and_get_checkpoint(saver, sample_checkpoint, sample_metadata):
    """Test saving and retrieving a checkpoint."""
    config = {
        "configurable": {
            "thread_id": "test-thread-1",
            "checkpoint_ns": "",
        }
    }

    # Put checkpoint
    returned_config = saver.put(config, sample_checkpoint, sample_metadata, {})

    assert returned_config["configurable"]["thread_id"] == "test-thread-1"
    assert returned_config["configurable"]["checkpoint_id"] == sample_checkpoint["id"]

    # Get checkpoint
    result = saver.get_tuple(returned_config)

    assert result is not None
    assert result.checkpoint["id"] == sample_checkpoint["id"]
    assert result.checkpoint["channel_values"] == sample_checkpoint["channel_values"]
    assert result.metadata["source"] == "input"
    assert result.metadata["step"] == -1


def test_get_latest_checkpoint(saver, sample_checkpoint, sample_metadata):
    """Test retrieving the latest checkpoint."""
    config = {
        "configurable": {
            "thread_id": "test-thread-2",
            "checkpoint_ns": "",
        }
    }

    # Put first checkpoint
    checkpoint1 = {**sample_checkpoint, "id": "00000000000000000000000000000001"}
    saver.put(config, checkpoint1, sample_metadata, {})

    # Put second checkpoint
    checkpoint2 = {**sample_checkpoint, "id": "00000000000000000000000000000002"}
    saver.put(config, checkpoint2, sample_metadata, {})

    # Get latest (should be checkpoint2)
    result = saver.get_tuple(config)

    assert result is not None
    assert result.checkpoint["id"] == "00000000000000000000000000000002"


def test_get_nonexistent_checkpoint(saver):
    """Test retrieving a checkpoint that doesn't exist."""
    config = {
        "configurable": {
            "thread_id": "nonexistent-thread",
            "checkpoint_ns": "",
        }
    }

    result = saver.get_tuple(config)
    assert result is None


def test_list_checkpoints(saver, sample_checkpoint, sample_metadata):
    """Test listing checkpoints."""
    config = {
        "configurable": {
            "thread_id": "test-thread-3",
            "checkpoint_ns": "",
        }
    }

    # Create 3 checkpoints
    for i in range(1, 4):
        checkpoint = {**sample_checkpoint, "id": f"0000000000000000000000000000000{i}"}
        saver.put(config, checkpoint, sample_metadata, {})

    # List all checkpoints
    checkpoints = list(saver.list(config))

    assert len(checkpoints) == 3
    # Should be in descending order (newest first)
    assert checkpoints[0].checkpoint["id"] == "00000000000000000000000000000003"
    assert checkpoints[1].checkpoint["id"] == "00000000000000000000000000000002"
    assert checkpoints[2].checkpoint["id"] == "00000000000000000000000000000001"


def test_list_with_limit(saver, sample_checkpoint, sample_metadata):
    """Test listing checkpoints with a limit."""
    config = {
        "configurable": {
            "thread_id": "test-thread-4",
            "checkpoint_ns": "",
        }
    }

    # Create 5 checkpoints
    for i in range(1, 6):
        checkpoint = {**sample_checkpoint, "id": f"0000000000000000000000000000000{i}"}
        saver.put(config, checkpoint, sample_metadata, {})

    # List with limit=2
    checkpoints = list(saver.list(config, limit=2))

    assert len(checkpoints) == 2
    assert checkpoints[0].checkpoint["id"] == "00000000000000000000000000000005"
    assert checkpoints[1].checkpoint["id"] == "00000000000000000000000000000004"


def test_put_writes(saver, sample_checkpoint, sample_metadata):
    """Test storing pending writes."""
    config = {
        "configurable": {
            "thread_id": "test-thread-5",
            "checkpoint_ns": "",
            "checkpoint_id": sample_checkpoint["id"],
        }
    }

    # Put checkpoint first
    saver.put(config, sample_checkpoint, sample_metadata, {})

    # Put writes
    writes = [
        ("messages", "new message"),
        ("count", 100),
    ]
    saver.put_writes(config, writes, task_id="task-1")

    # Get checkpoint with writes
    result = saver.get_tuple(config)

    assert result is not None
    assert result.pending_writes is not None
    assert len(result.pending_writes) == 2
    assert result.pending_writes[0] == ("task-1", "messages", "new message")
    assert result.pending_writes[1] == ("task-1", "count", 100)


def test_parent_checkpoint(saver, sample_checkpoint, sample_metadata):
    """Test checkpoint with parent reference."""
    config1 = {
        "configurable": {
            "thread_id": "test-thread-6",
            "checkpoint_ns": "",
        }
    }

    # Create parent checkpoint
    parent_checkpoint = {**sample_checkpoint, "id": "parent-checkpoint-id"}
    saver.put(config1, parent_checkpoint, sample_metadata, {})

    # Create child checkpoint
    config2 = {
        "configurable": {
            "thread_id": "test-thread-6",
            "checkpoint_ns": "",
            "checkpoint_id": "parent-checkpoint-id",
        }
    }
    child_checkpoint = {**sample_checkpoint, "id": "child-checkpoint-id"}
    child_config = saver.put(config2, child_checkpoint, sample_metadata, {})

    # Get child checkpoint
    result = saver.get_tuple(child_config)

    assert result is not None
    assert result.parent_config is not None
    assert (
        result.parent_config["configurable"]["checkpoint_id"] == "parent-checkpoint-id"
    )


def test_delete_thread(saver, sample_checkpoint, sample_metadata):
    """Test deleting all checkpoints for a thread."""
    thread_id = "test-thread-delete"
    config = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": "",
        }
    }

    # Create checkpoints
    for i in range(1, 4):
        checkpoint = {**sample_checkpoint, "id": f"checkpoint-{i}"}
        saver.put(config, checkpoint, sample_metadata, {})

    # Verify they exist
    checkpoints = list(saver.list(config))
    assert len(checkpoints) == 3

    # Delete thread
    saver.delete_thread(thread_id)

    # Verify they're gone
    checkpoints = list(saver.list(config))
    assert len(checkpoints) == 0


def test_multiple_threads(saver, sample_checkpoint, sample_metadata):
    """Test that different threads are isolated."""
    # Create checkpoints for thread 1
    config1 = {
        "configurable": {
            "thread_id": "thread-1",
            "checkpoint_ns": "",
        }
    }
    checkpoint1 = {**sample_checkpoint, "id": "thread-1-checkpoint"}
    saver.put(config1, checkpoint1, sample_metadata, {})

    # Create checkpoints for thread 2
    config2 = {
        "configurable": {
            "thread_id": "thread-2",
            "checkpoint_ns": "",
        }
    }
    checkpoint2 = {**sample_checkpoint, "id": "thread-2-checkpoint"}
    saver.put(config2, checkpoint2, sample_metadata, {})

    # Get thread 1 checkpoints
    result1 = saver.get_tuple(config1)
    assert result1.checkpoint["id"] == "thread-1-checkpoint"

    # Get thread 2 checkpoints
    result2 = saver.get_tuple(config2)
    assert result2.checkpoint["id"] == "thread-2-checkpoint"


def test_list_many_checkpoints_with_writes(saver, sample_checkpoint, sample_metadata):
    """Test listing many checkpoints with writes to verify batching works."""
    config = {
        "configurable": {
            "thread_id": "test-thread-many",
            "checkpoint_ns": "",
        }
    }

    # Create 300 checkpoints (more than batch size of 250) with writes
    num_checkpoints = 300
    for i in range(num_checkpoints):
        checkpoint_id = f"{i:032d}"
        checkpoint = {**sample_checkpoint, "id": checkpoint_id}
        saver.put(config, checkpoint, sample_metadata, {})

        # Add writes to each checkpoint
        config_with_id = {
            "configurable": {
                "thread_id": "test-thread-many",
                "checkpoint_ns": "",
                "checkpoint_id": checkpoint_id,
            }
        }
        writes = [(f"channel_{i}", f"value_{i}")]
        saver.put_writes(config_with_id, writes, f"task_{i}")

    # List all checkpoints
    checkpoints = list(saver.list(config))

    # Verify count
    assert len(checkpoints) == num_checkpoints

    # Verify all checkpoints have their writes
    for i, cp in enumerate(checkpoints):
        # Checkpoints are ordered by ID descending, so reverse index
        expected_idx = num_checkpoints - 1 - i
        assert cp.checkpoint["id"] == f"{expected_idx:032d}"
        assert cp.pending_writes is not None
        assert len(cp.pending_writes) == 1
        assert cp.pending_writes[0][1] == f"channel_{expected_idx}"
        assert cp.pending_writes[0][2] == f"value_{expected_idx}"


if __name__ == "__main__":
    """Run tests directly for debugging."""
    pytest.main([__file__, "-v"])
