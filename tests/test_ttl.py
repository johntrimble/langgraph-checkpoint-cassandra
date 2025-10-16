"""
Tests for TTL (Time To Live) functionality in CassandraSaver.
"""

import time

import pytest
from cassandra.cluster import Cluster

from langgraph_checkpoint_cassandra import CassandraSaver
from langgraph_checkpoint_cassandra.schema import drop_schema

TEST_KEYSPACE = "test_ttl"


@pytest.fixture(scope="module")
def cassandra_session():
    """Create a Cassandra session for testing."""
    cluster = Cluster(["cassandra"])
    session = cluster.connect()

    # Ensure clean state by dropping the keyspace
    drop_schema(session, keyspace=TEST_KEYSPACE)

    # We'll create the schema through the saver's setup method in each test

    yield session

    # Cleanup
    drop_schema(session, keyspace=TEST_KEYSPACE)
    cluster.shutdown()


@pytest.fixture
def saver_with_ttl(cassandra_session):
    """Create a CassandraSaver with TTL enabled (5 seconds for testing)."""
    saver = CassandraSaver(
        cassandra_session,
        keyspace=TEST_KEYSPACE,
        ttl_seconds=5,
        checkpoint_id_type="text",
    )
    saver.setup(replication_factor=1)  # Create schema
    return saver


@pytest.fixture
def saver_without_ttl(cassandra_session):
    """Create a CassandraSaver without TTL."""
    saver = CassandraSaver(
        cassandra_session, keyspace=TEST_KEYSPACE, checkpoint_id_type="text"
    )
    saver.setup(replication_factor=1)  # Create schema
    return saver


def test_checkpoint_without_ttl_persists(saver_without_ttl):
    """Test that checkpoints without TTL persist indefinitely."""
    import uuid

    from langgraph.checkpoint.base import Checkpoint

    # Create a checkpoint
    config = {"configurable": {"thread_id": "test-thread-no-ttl", "checkpoint_ns": ""}}
    checkpoint_id = str(uuid.uuid4())
    checkpoint = Checkpoint(
        v=1,
        id=checkpoint_id,
        ts="2024-01-01T00:00:00Z",
        channel_values={"messages": ["Hello"]},
        channel_versions={},
        versions_seen={},
        pending_sends=[],
    )
    metadata = {"source": "test", "step": 1}

    # Save checkpoint
    saver_without_ttl.put(config, checkpoint, metadata, {})

    # Retrieve immediately
    result = saver_without_ttl.get_tuple(config)
    assert result is not None
    assert result.checkpoint["id"] == checkpoint_id

    # Wait 6 seconds (longer than TTL would be if it were set)
    time.sleep(6)

    # Retrieve again - should still exist
    result = saver_without_ttl.get_tuple(config)
    assert result is not None
    assert result.checkpoint["id"] == checkpoint_id


def test_checkpoint_with_ttl_expires(saver_with_ttl):
    """Test that checkpoints with TTL expire after the specified time."""
    import uuid

    from langgraph.checkpoint.base import Checkpoint

    # Create a checkpoint
    config = {
        "configurable": {"thread_id": "test-thread-with-ttl", "checkpoint_ns": ""}
    }
    checkpoint_id = str(uuid.uuid4())
    checkpoint = Checkpoint(
        v=1,
        id=checkpoint_id,
        ts="2024-01-01T00:00:00Z",
        channel_values={"messages": ["Hello"]},
        channel_versions={},
        versions_seen={},
        pending_sends=[],
    )
    metadata = {"source": "test", "step": 1}

    # Save checkpoint
    saver_with_ttl.put(config, checkpoint, metadata, {})

    # Retrieve immediately - should exist
    result = saver_with_ttl.get_tuple(config)
    assert result is not None
    assert result.checkpoint["id"] == checkpoint_id

    # Wait for TTL to expire (5 seconds + 2 second buffer)
    print("Waiting for TTL to expire (7 seconds)...")
    time.sleep(7)

    # Retrieve again - should be gone
    result = saver_with_ttl.get_tuple(config)
    assert result is None


def test_checkpoint_writes_with_ttl_expire(saver_with_ttl):
    """Test that checkpoint writes with TTL also expire."""
    from langgraph.checkpoint.base import Checkpoint

    # Create a checkpoint first
    config = {
        "configurable": {"thread_id": "test-thread-writes-ttl", "checkpoint_ns": ""}
    }
    import uuid

    checkpoint_id = str(uuid.uuid4())
    checkpoint = Checkpoint(
        v=1,
        id=checkpoint_id,
        ts="2024-01-01T00:00:00Z",
        channel_values={"messages": []},
        channel_versions={},
        versions_seen={},
        pending_sends=[],
    )
    metadata = {"source": "test", "step": 1}

    # Save checkpoint
    result_config = saver_with_ttl.put(config, checkpoint, metadata, {})

    # Add writes - need to use result_config which has checkpoint_id
    writes = [("channel1", {"data": "value1"}), ("channel2", {"data": "value2"})]
    saver_with_ttl.put_writes(result_config, writes, "task-1")

    # Verify writes exist immediately
    # Note: get_tuple includes pending_writes, so we check that
    result = saver_with_ttl.get_tuple(config)
    assert result is not None

    # Wait for TTL to expire
    print("Waiting for TTL to expire (7 seconds)...")
    time.sleep(7)

    # Both checkpoint and writes should be gone
    result = saver_with_ttl.get_tuple(config)
    assert result is None


def test_multiple_checkpoints_ttl(saver_with_ttl):
    """Test that multiple checkpoints with TTL all expire independently."""
    from langgraph.checkpoint.base import Checkpoint

    thread_id = "test-thread-multiple-ttl"

    # Create first checkpoint
    config1 = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    checkpoint1 = Checkpoint(
        v=1,
        id="checkpoint-multi-1",
        ts="2024-01-01T00:00:00Z",
        channel_values={"messages": ["First"]},
        channel_versions={},
        versions_seen={},
        pending_sends=[],
    )
    saver_with_ttl.put(config1, checkpoint1, {"step": 1}, {})

    # Wait 3 seconds
    time.sleep(3)

    # Create second checkpoint
    config2 = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    checkpoint2 = Checkpoint(
        v=1,
        id="checkpoint-multi-2",
        ts="2024-01-01T00:00:03Z",
        channel_values={"messages": ["Second"]},
        channel_versions={},
        versions_seen={},
        pending_sends=[],
    )
    saver_with_ttl.put(config2, checkpoint2, {"step": 2}, {})

    # At this point:
    # - checkpoint1 has ~2 seconds left
    # - checkpoint2 has ~5 seconds left

    # Wait 3 more seconds (total 6 seconds from checkpoint1)
    time.sleep(3)

    # checkpoint1 should be expired, checkpoint2 should still exist
    result1 = saver_with_ttl.get_tuple(
        {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
                "checkpoint_id": "checkpoint-multi-1",
            }
        }
    )
    assert result1 is None  # First checkpoint expired

    result2 = saver_with_ttl.get_tuple(
        {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
                "checkpoint_id": "checkpoint-multi-2",
            }
        }
    )
    assert result2 is not None  # Second checkpoint still exists
    assert result2.checkpoint["id"] == "checkpoint-multi-2"

    # Wait another 3 seconds (total 9 seconds from checkpoint1, 6 from checkpoint2)
    time.sleep(3)

    # Now both should be expired
    result2 = saver_with_ttl.get_tuple(
        {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
                "checkpoint_id": "checkpoint-multi-2",
            }
        }
    )
    assert result2 is None


def test_ttl_parameter_validation(cassandra_session):
    """Test that TTL parameter is properly stored."""
    # Create saver with TTL
    saver = CassandraSaver(cassandra_session, keyspace=TEST_KEYSPACE, ttl_seconds=3600)
    assert saver.ttl_seconds == 3600

    # Create saver without TTL
    saver_no_ttl = CassandraSaver(cassandra_session, keyspace=TEST_KEYSPACE)
    assert saver_no_ttl.ttl_seconds is None


if __name__ == "__main__":
    # Run with: python -m pytest tests/test_ttl.py -v
    pytest.main([__file__, "-v", "-s"])
