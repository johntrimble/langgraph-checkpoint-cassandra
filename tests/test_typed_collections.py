"""Tests for typed collection metadata (list[int], dict[str, int], set[str], etc.)."""

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


def test_list_int_metadata(cluster, sample_checkpoint):
    """Test list[int] metadata type."""
    session = cluster.connect()
    keyspace = f"test_list_int_{int(time.time())}"

    saver = CassandraSaver(
        session,
        keyspace=keyspace,
        queryable_metadata={"scores": list[int]},
    )

    try:
        saver.setup(replication_factor=1)

        # Verify column type is LIST<BIGINT>
        result = session.execute(
            f"""
            SELECT type FROM system_schema.columns
            WHERE keyspace_name = '{keyspace}'
            AND table_name = 'checkpoints'
            AND column_name = 'metadata__scores'
            """
        )
        row = result.one()
        assert row is not None
        # The type will be shown as frozen<list<bigint>> or list<bigint>
        assert "list<bigint>" in row.type.lower()

        # Test storing and retrieving
        config = {"configurable": {"thread_id": "test-thread", "checkpoint_ns": ""}}
        metadata = {"scores": [100, 200, 300]}

        saver.put(config, sample_checkpoint, metadata, {})

        # Retrieve and verify
        result_tuple = saver.get_tuple(config)
        assert result_tuple is not None
        assert result_tuple.metadata["scores"] == [100, 200, 300]

    finally:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        session.shutdown()


def test_set_str_metadata(cluster, sample_checkpoint):
    """Test set[str] metadata type."""
    session = cluster.connect()
    keyspace = f"test_set_str_{int(time.time())}"

    saver = CassandraSaver(
        session,
        keyspace=keyspace,
        queryable_metadata={"tags": set[str]},
    )

    try:
        saver.setup(replication_factor=1)

        # Verify column type is SET<TEXT>
        result = session.execute(
            f"""
            SELECT type FROM system_schema.columns
            WHERE keyspace_name = '{keyspace}'
            AND table_name = 'checkpoints'
            AND column_name = 'metadata__tags'
            """
        )
        row = result.one()
        assert row is not None
        assert "set<text>" in row.type.lower() or "set<varchar>" in row.type.lower()

        # Test storing and retrieving
        config = {"configurable": {"thread_id": "test-thread", "checkpoint_ns": ""}}
        metadata = {"tags": {"python", "cassandra", "langgraph"}}

        saver.put(config, sample_checkpoint, metadata, {})

        # Retrieve and verify (sets can come back in different order)
        result_tuple = saver.get_tuple(config)
        assert result_tuple is not None
        assert result_tuple.metadata["tags"] == {"python", "cassandra", "langgraph"}

    finally:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        session.shutdown()


def test_dict_str_int_metadata(cluster, sample_checkpoint):
    """Test dict[str, int] metadata type."""
    session = cluster.connect()
    keyspace = f"test_dict_str_int_{int(time.time())}"

    saver = CassandraSaver(
        session,
        keyspace=keyspace,
        queryable_metadata={"counters": dict[str, int]},
    )

    try:
        saver.setup(replication_factor=1)

        # Verify column type is MAP<TEXT, BIGINT>
        result = session.execute(
            f"""
            SELECT type FROM system_schema.columns
            WHERE keyspace_name = '{keyspace}'
            AND table_name = 'checkpoints'
            AND column_name = 'metadata__counters'
            """
        )
        row = result.one()
        assert row is not None
        # The type might be shown as map<text, bigint> or map<varchar, bigint>
        type_str = row.type.lower()
        assert "map<" in type_str
        assert "bigint" in type_str

        # Test storing and retrieving
        config = {"configurable": {"thread_id": "test-thread", "checkpoint_ns": ""}}
        metadata = {"counters": {"views": 42, "likes": 15, "shares": 3}}

        saver.put(config, sample_checkpoint, metadata, {})

        # Retrieve and verify
        result_tuple = saver.get_tuple(config)
        assert result_tuple is not None
        assert result_tuple.metadata["counters"] == {
            "views": 42,
            "likes": 15,
            "shares": 3,
        }

    finally:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        session.shutdown()


def test_dict_str_str_metadata(cluster, sample_checkpoint):
    """Test dict[str, str] metadata type."""
    session = cluster.connect()
    keyspace = f"test_dict_str_str_{int(time.time())}"

    saver = CassandraSaver(
        session,
        keyspace=keyspace,
        queryable_metadata={"labels": dict[str, str]},
    )

    try:
        saver.setup(replication_factor=1)

        # Verify column type is MAP<TEXT, TEXT>
        result = session.execute(
            f"""
            SELECT type FROM system_schema.columns
            WHERE keyspace_name = '{keyspace}'
            AND table_name = 'checkpoints'
            AND column_name = 'metadata__labels'
            """
        )
        row = result.one()
        assert row is not None
        type_str = row.type.lower()
        assert "map<" in type_str
        assert "text" in type_str or "varchar" in type_str

        # Test storing and retrieving
        config = {"configurable": {"thread_id": "test-thread", "checkpoint_ns": ""}}
        metadata = {
            "labels": {"env": "production", "region": "us-east", "version": "v1.2.3"}
        }

        saver.put(config, sample_checkpoint, metadata, {})

        # Retrieve and verify
        result_tuple = saver.get_tuple(config)
        assert result_tuple is not None
        assert result_tuple.metadata["labels"] == {
            "env": "production",
            "region": "us-east",
            "version": "v1.2.3",
        }

    finally:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        session.shutdown()


def test_list_float_metadata(cluster, sample_checkpoint):
    """Test list[float] metadata type."""
    session = cluster.connect()
    keyspace = f"test_list_float_{int(time.time())}"

    saver = CassandraSaver(
        session,
        keyspace=keyspace,
        queryable_metadata={"measurements": list[float]},
    )

    try:
        saver.setup(replication_factor=1)

        # Verify column type is LIST<DOUBLE>
        result = session.execute(
            f"""
            SELECT type FROM system_schema.columns
            WHERE keyspace_name = '{keyspace}'
            AND table_name = 'checkpoints'
            AND column_name = 'metadata__measurements'
            """
        )
        row = result.one()
        assert row is not None
        assert "list<double>" in row.type.lower()

        # Test storing and retrieving
        config = {"configurable": {"thread_id": "test-thread", "checkpoint_ns": ""}}
        metadata = {"measurements": [1.5, 2.7, 3.14159]}

        saver.put(config, sample_checkpoint, metadata, {})

        # Retrieve and verify
        result_tuple = saver.get_tuple(config)
        assert result_tuple is not None
        assert result_tuple.metadata["measurements"] == [1.5, 2.7, 3.14159]

    finally:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        session.shutdown()


def test_mixed_typed_collections(cluster, sample_checkpoint):
    """Test multiple different typed collections in one saver."""
    session = cluster.connect()
    keyspace = f"test_mixed_{int(time.time())}"

    saver = CassandraSaver(
        session,
        keyspace=keyspace,
        queryable_metadata={
            "scores": list[int],
            "tags": set[str],
            "attributes": dict[str, int],
            "priorities": list[float],
        },
    )

    try:
        saver.setup(replication_factor=1)

        config = {"configurable": {"thread_id": "test-thread", "checkpoint_ns": ""}}
        metadata = {
            "scores": [100, 200, 300],
            "tags": {"python", "test"},
            "attributes": {"retries": 3, "timeout": 30},
            "priorities": [1.0, 0.5, 0.25],
        }

        saver.put(config, sample_checkpoint, metadata, {})

        # Retrieve and verify all types
        result_tuple = saver.get_tuple(config)
        assert result_tuple is not None
        assert result_tuple.metadata["scores"] == [100, 200, 300]
        assert result_tuple.metadata["tags"] == {"python", "test"}
        assert result_tuple.metadata["attributes"] == {"retries": 3, "timeout": 30}
        assert result_tuple.metadata["priorities"] == [1.0, 0.5, 0.25]

    finally:
        session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        session.shutdown()


if __name__ == "__main__":
    """Run tests directly for debugging."""
    pytest.main([__file__, "-v"])
