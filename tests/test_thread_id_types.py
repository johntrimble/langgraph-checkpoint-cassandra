#!/usr/bin/env python3
"""Test different thread_id_type configurations."""

import uuid
import pytest

from cassandra.cluster import Cluster
from langgraph.checkpoint.base import Checkpoint

from langgraph_checkpoint_cassandra import CassandraSaver


@pytest.mark.parametrize("thread_id_type,thread_id_value,keyspace_suffix", [
    ("text", "my-custom-thread-id", "text"),
    pytest.param("uuid", None, "uuid", id="uuid"),  # Will be generated in test
    pytest.param("timeuuid", None, "timeuuid", id="timeuuid"),  # Will be generated in test
])
def test_thread_id_type(thread_id_type, thread_id_value, keyspace_suffix):
    """Test a specific thread_id_type configuration."""
    # Generate UUID values if needed
    if thread_id_type == "uuid" and thread_id_value is None:
        thread_id_value = str(uuid.uuid4())
    elif thread_id_type == "timeuuid" and thread_id_value is None:
        thread_id_value = str(uuid.uuid1())

    print(f"\n{'=' * 70}")
    print(f"Testing thread_id_type='{thread_id_type}'")
    print(f"{'=' * 70}")

    # Connect
    cluster = Cluster(["cassandra"])
    session = cluster.connect()

    keyspace = f"test_thread_type_{keyspace_suffix}"

    try:
        # Drop keyspace if exists
        print(f"Dropping keyspace '{keyspace}' if it exists...")
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")

        # Create saver with specific thread_id_type
        print(f"Creating CassandraSaver with thread_id_type='{thread_id_type}'...")
        saver = CassandraSaver(session, keyspace, thread_id_type=thread_id_type)

        # Setup schema
        print("Setting up schema...")
        saver.setup(replication_factor=1)

        # Verify table structure
        print("\nVerifying schema...")
        result = session.execute(f"""
            SELECT column_name, type
            FROM system_schema.columns
            WHERE keyspace_name = '{keyspace}'
              AND table_name = 'checkpoints'
              AND column_name = 'thread_id'
        """)
        row = result.one()
        assert row is not None, "thread_id column not found"
        actual_type = row.type
        print(f"✓ thread_id column type: {actual_type}")
        expected_type = thread_id_type
        assert actual_type == expected_type, f"Type mismatch! Expected: {expected_type}, Got: {actual_type}"
        print(f"✓ Type matches expected: {expected_type}")

        # Create a test checkpoint
        print(f"\nCreating checkpoint with thread_id='{thread_id_value}'...")
        checkpoint_id = str(uuid.uuid1())

        checkpoint = Checkpoint(
            v=1,
            id=checkpoint_id,
            ts="2024-03-15T10:30:00.000Z",
            channel_values={
                "messages": [f"Test with {thread_id_type}"],
                "user": "test",
            },
            channel_versions={"messages": 1, "user": 1},
            versions_seen={},
            updated_channels=["messages", "user"],
        )

        config = {
            "configurable": {
                "thread_id": thread_id_value,
                "checkpoint_ns": "",
            }
        }

        metadata = {
            "source": "input",
            "step": 0,
        }

        result_config = saver.put(config, checkpoint, metadata, {})
        print(f"✓ Checkpoint saved: {result_config['configurable']['checkpoint_id']}")

        # Retrieve the checkpoint
        print("\nRetrieving checkpoint...")
        retrieved = saver.get_tuple(
            {
                "configurable": {
                    "thread_id": thread_id_value,
                    "checkpoint_ns": "",
                    "checkpoint_id": checkpoint_id,
                }
            }
        )

        assert retrieved is not None, "Failed to retrieve checkpoint"
        print(f"✓ Retrieved checkpoint: {retrieved.config['configurable']['checkpoint_id']}")
        print(f"  Channel values: {retrieved.checkpoint['channel_values']}")

        # List checkpoints
        print("\nListing checkpoints...")
        checkpoints = list(saver.list({"configurable": {"thread_id": thread_id_value}}))
        print(f"✓ Found {len(checkpoints)} checkpoint(s)")

        assert len(checkpoints) == 1, f"Expected 1 checkpoint, got {len(checkpoints)}"

        print(f"\n✅ All tests passed for thread_id_type='{thread_id_type}'!")
    finally:
        # Cleanup
        print(f"\nCleaning up keyspace '{keyspace}'...")
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        cluster.shutdown()
