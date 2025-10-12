#!/usr/bin/env python3
"""Test that TIMEUUID schema changes work correctly."""

import pytest
import uuid

from cassandra.cluster import Cluster
from langgraph.checkpoint.base import Checkpoint
from langgraph.checkpoint.base.id import uuid6

from langgraph_checkpoint_cassandra import CassandraSaver
from langgraph_checkpoint_cassandra.schema import drop_schema

TEST_KEYSPACE = "test_timeuuid"

@pytest.fixture
def cassandra_session():
    """Create a Cassandra session for testing."""
    # Connect to Cassandra
    cluster = Cluster(["cassandra"])
    session = cluster.connect()
    
    # Clean up before test
    drop_schema(session, TEST_KEYSPACE)
    
    # Create the keyspace
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {TEST_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    
    yield session
    
    # Clean up after test
    drop_schema(session, TEST_KEYSPACE)
    cluster.shutdown()

@pytest.fixture
def uuid_saver(cassandra_session):
    # Create saver with timeuuid type
    saver = CassandraSaver(
        cassandra_session, 
        keyspace=TEST_KEYSPACE, 
        checkpoint_id_type="uuid"
    )

    saver.setup(replication_factor=1)

    return saver

def test_uuid_checkpoint(cassandra_session, uuid_saver):
    """Test using a UUID for thread_id."""
    thread_id = str(uuid.uuid4())
    
    # Create a checkpoint
    checkpoint_id = str(uuid6())
    checkpoint = Checkpoint(
        v=1,
        id=checkpoint_id,
        ts="2024-03-15T10:30:00.000Z",
        channel_values={"messages": ["Hello, World!"]},
        channel_versions={"messages": 1},
        versions_seen={},
        updated_channels=["messages"],
    )
    
    # Config with UUID thread_id
    config = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": "",
        }
    }
    
    # Save the checkpoint
    metadata = {"source": "test", "step": 1}
    result_config = uuid_saver.put(config, checkpoint, metadata, {})
    
    # Verify it was saved and can be retrieved
    assert result_config["configurable"]["checkpoint_id"] == checkpoint_id
    
    # Retrieve the checkpoint
    retrieved = uuid_saver.get_tuple(config)
    assert retrieved is not None
    assert retrieved.checkpoint["id"] == checkpoint_id
    assert retrieved.checkpoint["channel_values"]["messages"] == ["Hello, World!"]
