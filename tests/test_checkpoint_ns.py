"""
Test to demonstrate how checkpoint_ns is used for subgraph management in LangGraph.

This test shows how a parent graph and subgraph can maintain separate checkpoint histories
using the checkpoint_ns parameter.
"""

import pytest
from cassandra.cluster import Cluster
from langgraph.checkpoint.base import Checkpoint, CheckpointMetadata

from langgraph_checkpoint_cassandra import CassandraSaver
from tests.utils import drop_schema

TEST_KEYSPACE = "test_checkpoint_ns"
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


@pytest.fixture
def saver(cassandra_session):
    """Provide a fresh CassandraSaver for each test."""
    saver = CassandraSaver(cassandra_session, keyspace=TEST_KEYSPACE)
    saver.setup(replication_factor=1)  # Create schema
    return saver


@pytest.fixture
def sample_checkpoint():
    """Provide a sample checkpoint for testing."""
    from langgraph.checkpoint.base.id import uuid6

    return Checkpoint(
        v=1,
        id=str(uuid6(-1)),  # Generate valid time-sortable UUID
        ts="2024-01-01T00:00:00Z",
        channel_values={"messages": ["hello"], "count": 1},
        channel_versions={"messages": "0000000001.abc", "count": "0000000001.def"},
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


def test_checkpoints_with_different_namespaces(
    saver, sample_checkpoint, sample_metadata
):
    """
    Test that checkpoint_ns isolates checkpoints for different graph components.

    This test simulates:
    1. A main graph with default namespace (empty string)
    2. A subgraph with namespace "subgraph_1"
    3. Another subgraph with namespace "subgraph_2"

    All sharing the same thread_id but with separate checkpoint histories.
    """
    thread_id = "user-conversation-123"

    # Create config for main graph (root level)
    root_config = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": "",  # Default/root namespace
        }
    }

    # Create config for first subgraph
    subgraph1_config = {
        "configurable": {
            "thread_id": thread_id,  # Same thread ID
            "checkpoint_ns": "subgraph_1",  # Different namespace
        }
    }

    # Create config for second subgraph
    subgraph2_config = {
        "configurable": {
            "thread_id": thread_id,  # Same thread ID
            "checkpoint_ns": "subgraph_2",  # Different namespace
        }
    }

    from langgraph.checkpoint.base.id import uuid6

    # Create checkpoints for main graph
    main_id1 = str(uuid6(-1))
    main_checkpoint = {**sample_checkpoint, "id": main_id1}
    main_checkpoint["channel_values"]["messages"] = ["Hello from main graph"]
    saver.put(root_config, main_checkpoint, sample_metadata, {})

    # Create checkpoints for subgraph1
    subgraph1_id1 = str(uuid6(-1))
    subgraph1_checkpoint = {**sample_checkpoint, "id": subgraph1_id1}
    subgraph1_checkpoint["channel_values"]["messages"] = ["Hello from subgraph 1"]
    saver.put(subgraph1_config, subgraph1_checkpoint, sample_metadata, {})

    # Create checkpoints for subgraph2
    subgraph2_id1 = str(uuid6(-1))
    subgraph2_checkpoint = {**sample_checkpoint, "id": subgraph2_id1}
    subgraph2_checkpoint["channel_values"]["messages"] = ["Hello from subgraph 2"]
    saver.put(subgraph2_config, subgraph2_checkpoint, sample_metadata, {})

    # Retrieve the checkpoints
    main_result = saver.get_tuple(root_config)
    subgraph1_result = saver.get_tuple(subgraph1_config)
    subgraph2_result = saver.get_tuple(subgraph2_config)

    # Check that each namespace has its own checkpoint
    assert main_result is not None
    assert main_result.checkpoint["id"] == main_id1
    assert main_result.checkpoint["channel_values"]["messages"] == [
        "Hello from main graph"
    ]

    assert subgraph1_result is not None
    assert subgraph1_result.checkpoint["id"] == subgraph1_id1
    assert subgraph1_result.checkpoint["channel_values"]["messages"] == [
        "Hello from subgraph 1"
    ]

    assert subgraph2_result is not None
    assert subgraph2_result.checkpoint["id"] == subgraph2_id1
    assert subgraph2_result.checkpoint["channel_values"]["messages"] == [
        "Hello from subgraph 2"
    ]

    # Now add more checkpoints to each namespace
    main_id2 = str(uuid6(-1))
    main_checkpoint2 = {**sample_checkpoint, "id": main_id2}
    main_checkpoint2["channel_values"]["messages"] = ["Hello again from main graph"]
    saver.put(root_config, main_checkpoint2, sample_metadata, {})

    subgraph1_id2 = str(uuid6(-1))
    subgraph1_checkpoint2 = {**sample_checkpoint, "id": subgraph1_id2}
    subgraph1_checkpoint2["channel_values"]["messages"] = [
        "Hello again from subgraph 1"
    ]
    saver.put(subgraph1_config, subgraph1_checkpoint2, sample_metadata, {})

    # List checkpoints for each namespace
    main_checkpoints = list(saver.list(root_config))
    subgraph1_checkpoints = list(saver.list(subgraph1_config))
    subgraph2_checkpoints = list(saver.list(subgraph2_config))

    # Check that each namespace has the correct number of checkpoints
    assert len(main_checkpoints) == 2
    assert main_checkpoints[0].checkpoint["id"] == main_id2
    assert main_checkpoints[1].checkpoint["id"] == main_id1

    assert len(subgraph1_checkpoints) == 2
    assert subgraph1_checkpoints[0].checkpoint["id"] == subgraph1_id2
    assert subgraph1_checkpoints[1].checkpoint["id"] == subgraph1_id1

    assert len(subgraph2_checkpoints) == 1
    assert subgraph2_checkpoints[0].checkpoint["id"] == subgraph2_id1

    # This demonstrates that all three namespaces maintain separate checkpoint histories
    # despite sharing the same thread_id

    # Now test that delete_thread removes checkpoints from all namespaces
    saver.delete_thread(thread_id)

    # Verify all checkpoints are gone from all namespaces
    main_checkpoints_after = list(saver.list(root_config))
    subgraph1_checkpoints_after = list(saver.list(subgraph1_config))
    subgraph2_checkpoints_after = list(saver.list(subgraph2_config))

    # Check that all namespaces have no checkpoints
    assert len(main_checkpoints_after) == 0, (
        "Main namespace should have no checkpoints after deletion"
    )
    assert len(subgraph1_checkpoints_after) == 0, (
        "Subgraph1 namespace should have no checkpoints after deletion"
    )
    assert len(subgraph2_checkpoints_after) == 0, (
        "Subgraph2 namespace should have no checkpoints after deletion"
    )
