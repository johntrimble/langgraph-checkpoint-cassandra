"""
Property-based tests using Hypothesis to verify async and sync methods produce
identical results as the InMemorySaver.
"""

import asyncio
import copy
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, overload

import pytest
import uuid6
from cassandra.cluster import Cluster
from cassandra_asyncio.cluster import Cluster as AsyncCluster
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)
from langgraph.checkpoint.memory import InMemorySaver

from langgraph_checkpoint_cassandra import CassandraSaver


# Operation types for our stateful tests
@dataclass
class PutOperation:
    """Put a checkpoint."""

    thread_id: str
    checkpoint_ns: str
    checkpoint: Checkpoint
    metadata: CheckpointMetadata


@dataclass
class GetOperation:
    """Get a checkpoint."""

    thread_id: str
    checkpoint_ns: str
    checkpoint_id: str | None  # None means get latest
    checkpoint_index: (
        int | None
    )  # If checkpoint_id is "__will_replace__", use this index


@dataclass
class PutWritesOperation:
    """Put writes for a checkpoint."""

    thread_id: str
    checkpoint_ns: str
    checkpoint_id: str
    writes: list[tuple[str, Any]]
    task_id: str


@dataclass
class ListOperation:
    """List checkpoints."""

    thread_id: str
    checkpoint_ns: str
    limit: int | None
    filter: dict[str, Any] | None


@dataclass
class DeleteThreadOperation:
    """Delete all data for a thread."""

    thread_id: str


# Hypothesis strategies for generating test data
@st.composite
def thread_ids(draw):
    """Generate valid thread IDs from a small pool to encourage collisions."""
    return draw(st.sampled_from(["thread_1", "thread_2", "thread_3"]))


@st.composite
def checkpoint_namespaces(draw):
    """Generate checkpoint namespaces from a small pool."""
    return draw(st.sampled_from(["", "ns_a", "ns_b"]))


@st.composite
def channel_names(draw):
    """Generate channel names from a small pool."""
    return draw(st.sampled_from(["channel_a", "channel_b", "channel_c"]))


@st.composite
def channel_value(draw):
    """Generate a single channel value with ASCII-safe strings."""
    return draw(
        st.one_of(
            st.text(
                alphabet=st.characters(min_codepoint=32, max_codepoint=126), max_size=20
            ),
            st.integers(min_value=-100, max_value=100),
        )
    )


@st.composite
def checkpoints(draw):
    """Generate valid Checkpoint objects with uuid6 IDs.

    Note: channel_values and channel_versions have matching keys.
    Version strings are formatted to be sortable/comparable.
    The test must track version immutability (same version = same value).
    """
    # Pick 1-3 channels for this checkpoint
    num_channels = draw(st.integers(min_value=1, max_value=3))
    channels = draw(
        st.lists(
            channel_names(), min_size=num_channels, max_size=num_channels, unique=True
        )
    )

    channel_values = {}
    channel_versions = {}

    for channel in channels:
        # Generate a version string (will be materialized in the test based on version store)
        # Use format v<number> for easy comparison
        version = f"v{draw(st.integers(min_value=1, max_value=100))}"
        value = draw(channel_value())

        channel_values[channel] = value
        channel_versions[channel] = version

    return Checkpoint(
        v=1,
        id=str(uuid6.uuid6()),
        ts=draw(st.datetimes().map(lambda d: d.isoformat())),
        channel_values=channel_values,
        channel_versions=channel_versions,
        versions_seen={},
    )


@st.composite
def metadata_dicts(draw):
    """Generate metadata dictionaries with ASCII-safe characters.

    Includes both standard CheckpointMetadata fields (source, step, parents)
    and custom queryable fields (user_id, step_num, score, tags) drawn from
    small value pools to encourage overlap with filter expressions.
    """
    metadata = CheckpointMetadata(
        source=draw(st.sampled_from(["input", "loop", "update"])),
        step=draw(st.integers(min_value=0, max_value=3)),
        parents={},
    )

    # Add custom queryable fields with small value pools for overlap
    # user_id: string from small pool
    metadata["user_id"] = draw(st.sampled_from(["user1", "user2"]))

    # step_num: integer from small range
    metadata["step_num"] = draw(st.integers(min_value=0, max_value=2))

    # score: float from small range (with some None values)
    metadata["score"] = draw(
        st.one_of(
            st.none(),
            st.floats(min_value=0.0, max_value=10.0, allow_nan=False, allow_infinity=False)
        )
    )

    # tags: list of strings from small pool
    metadata["tags"] = draw(
        st.lists(
            st.sampled_from(["test", "prod"]),
            min_size=0,
            max_size=3,
            unique=True
        )
    )

    return metadata


@st.composite
def metadata_filters(draw):
    """Generate filter dictionaries that match metadata structure.

    Generates filters using the same value pools as metadata_dicts() to ensure
    high probability of matches. Returns None ~50% of the time (no filter),
    otherwise returns a filter dict with 1-3 fields.
    """
    # 50% chance of no filter (to ensure list operations return more results)
    if draw(st.booleans()):
        return None

    filter_dict = {}

    # Decide how many filter fields to include (1-3)
    num_fields = draw(st.integers(min_value=1, max_value=3))

    # Available filter fields (matching queryable metadata fields)
    # Note: Excluding 'tags' because InMemorySaver doesn't support CONTAINS semantics
    # for lists - it does exact equality matching only
    available_fields = ["user_id", "step_num", "score", "source", "step"]

    # Randomly select fields to filter on
    selected_fields = draw(
        st.lists(
            st.sampled_from(available_fields),
            min_size=num_fields,
            max_size=num_fields,
            unique=True
        )
    )

    for field in selected_fields:
        if field == "user_id":
            filter_dict["user_id"] = draw(st.sampled_from(["user1", "user2", "user3"]))
        elif field == "step_num":
            filter_dict["step_num"] = draw(st.integers(min_value=0, max_value=5))
        elif field == "score":
            # Note: Avoid None in filters for queryable fields as Cassandra doesn't support
            # NULL filtering with = operator (would need IS NULL syntax)
            # Only use non-None float values for now
            filter_dict["score"] = draw(
                st.floats(min_value=0.0, max_value=10.0, allow_nan=False, allow_infinity=False)
            )
        elif field == "source":
            filter_dict["source"] = draw(st.sampled_from(["input", "loop", "update"]))
        elif field == "step":
            filter_dict["step"] = draw(st.integers(min_value=0, max_value=10))

    return filter_dict


@st.composite
def write_sequences(draw):
    """Generate sequences of writes (channel, value pairs) with ASCII-safe strings."""
    num_writes = draw(st.integers(min_value=1, max_value=5))
    writes = []

    for _ in range(num_writes):
        # 50% chance of using a regular channel, 50% chance of using a special write channel
        if draw(st.booleans()):
            channel = draw(channel_names())
        else:
            channel = draw(st.sampled_from(list(WRITES_IDX_MAP.keys())))

        value = draw(
            st.one_of(
                st.text(
                    alphabet=st.characters(min_codepoint=32, max_codepoint=126),
                    max_size=20,
                ),
                st.integers(min_value=-100, max_value=100),
            )
        )

        writes.append((channel, value))

    return writes


@st.composite
def operation_sequences(draw, min_size=None, max_size=None):
    """
    Generate sequences of operations with explicit size control.

    Unlike st.lists() which has exponential bias toward min_size, this strategy
    draws the size explicitly first, giving better distribution across the range.

    This allows:
    - Large examples during generation (well-distributed 100-1000 ops)
    - Small minimal examples when shrinking (down to min_size on failure)

    The size is drawn as an integer, which Hypothesis can shrink independently
    of the operations themselves.
    """
    # Draw the number of operations
    # Simple approach: just use the full range, Hypothesis will handle it
    num_ops = draw(st.integers(min_value=min_size, max_value=max_size))

    # Define weighted operation types (same weights as before)
    # Total: 100 items for easy percentage calculation
    operation_pool = (
        ["put"] * 70 +           # 70% - PutOperation
        ["list"] * 10 +          # 10% - ListOperation
        ["get"] * 7 +            # 7%  - GetOperation
        ["put_writes"] * 10 +    # 10% - PutWritesOperation
        ["delete"] * 3           # 3%  - DeleteThreadOperation
    )

    operations = []
    for _ in range(num_ops):
        # Draw operation type from weighted pool
        op_type = draw(st.sampled_from(operation_pool))

        # Build the appropriate operation based on type
        if op_type == "put":
            op = draw(st.builds(
                PutOperation,
                thread_id=thread_ids(),
                checkpoint_ns=checkpoint_namespaces(),
                checkpoint=checkpoints(),
                metadata=metadata_dicts(),
            ))
        elif op_type == "list":
            op = draw(st.builds(
                ListOperation,
                thread_id=thread_ids(),
                checkpoint_ns=checkpoint_namespaces(),
                limit=st.one_of(st.none(), st.integers(min_value=1, max_value=5)),
                filter=metadata_filters(),
            ))
        elif op_type == "get":
            op = draw(st.builds(
                GetOperation,
                thread_id=thread_ids(),
                checkpoint_ns=checkpoint_namespaces(),
                checkpoint_id=st.one_of(st.none(), st.just("__will_replace__")),
                checkpoint_index=st.integers(min_value=0, max_value=100),
            ))
        elif op_type == "put_writes":
            op = draw(st.builds(
                PutWritesOperation,
                thread_id=thread_ids(),
                checkpoint_ns=checkpoint_namespaces(),
                checkpoint_id=st.just("__will_replace__"),
                writes=write_sequences(),
                task_id=st.text(min_size=1, max_size=10, alphabet="abcdef"),
            ))
        else:  # delete
            op = draw(st.builds(
                DeleteThreadOperation,
                thread_id=thread_ids(),
            ))

        operations.append(op)

    return operations


# Fixtures
@pytest.fixture(scope="module")
def clusters():
    """Create both sync and async Cassandra clusters."""
    sync_cluster = Cluster(["cassandra"])
    async_cluster = AsyncCluster(["cassandra"])
    yield sync_cluster, async_cluster
    sync_cluster.shutdown()
    async_cluster.shutdown()


def cleanup_keyspaces(session, keyspaces):
    """Drop specified keyspaces if they exist."""
    for ks in keyspaces:
        try:
            session.execute(f"DROP KEYSPACE IF EXISTS {ks}")
        except Exception:
            pass


@pytest.fixture
def savers(clusters):
    """Create both sync and async CassandraSaver instances with separate keyspaces."""
    sync_cluster, async_cluster = clusters

    sync_session = sync_cluster.connect()
    async_session = async_cluster.connect()

    # Use separate keyspaces for sync and async
    keyspace_base = "test_hypothesis"
    sync_keyspace = f"{keyspace_base}_sync"
    async_keyspace = f"{keyspace_base}_async"

    # Cleanup any existing keyspaces from previous runs FIRST
    cleanup_keyspaces(sync_session, [sync_keyspace, async_keyspace])

    # Configure queryable metadata fields to match our filter generation strategy
    # Note: tags is excluded because InMemorySaver doesn't support CONTAINS semantics
    queryable_metadata = {
        "user_id": str,
        "step_num": int,
        "score": float,
    }

    # Create sync saver and setup schema
    sync_saver = CassandraSaver(
        sync_session,
        keyspace=sync_keyspace,
        queryable_metadata=queryable_metadata
    )
    sync_saver.setup(replication_factor=1)

    # Create async saver (with async session) and setup schema
    async_saver = CassandraSaver(
        async_session,
        keyspace=async_keyspace,
        queryable_metadata=queryable_metadata
    )
    async_saver.setup(replication_factor=1)

    yield sync_saver, async_saver

    # Cleanup after test
    cleanup_keyspaces(sync_session, [sync_keyspace, async_keyspace])
    sync_session.shutdown()
    async_session.shutdown()


def _normalize_pending_writes(pending_writes):
    """Sort pending writes by (task_id, channel, value_str) for consistent comparison.

    Different implementations may return pending_writes in different orders:
    - InMemorySaver: Returns in insertion order (dict iteration order)
    - CassandraSaver: Returns sorted by (task_id, idx) due to clustering order
    - PostgresSaver: Returns sorted by (task_id, idx) with ORDER BY clause

    Where does the order matter? Order matters for a given (task_id, channel) pair,
    as it defines the sequence of writes to apply to that channel. Aside from that,
    the order of different (task_id, channel) pairs does not matter. Consequently,
    we sort by (task_id, channel) to enable comparison across implementations.
    """
    if not pending_writes:
        return []

    return sorted(pending_writes, key=lambda w: (w[0], w[1]))


@overload
def normalize_checkpoint_tuple(
    checkpoint_tuple: CheckpointTuple,
) -> CheckpointTuple: ...


@overload
def normalize_checkpoint_tuple(
    checkpoint_tuple: Iterable[CheckpointTuple],
) -> Iterable[CheckpointTuple]: ...


def normalize_checkpoint_tuple(
    checkpoint_tuple: Iterable[CheckpointTuple] | CheckpointTuple,
) -> Iterable[CheckpointTuple] | CheckpointTuple:
    if checkpoint_tuple is None:
        return checkpoint_tuple

    if not isinstance(checkpoint_tuple, CheckpointTuple):
        return [normalize_checkpoint_tuple(t) for t in checkpoint_tuple]

    if checkpoint_tuple.pending_writes:
        # _replace returns a new namedtuple, it doesn't modify in place
        checkpoint_tuple = checkpoint_tuple._replace(
            pending_writes=_normalize_pending_writes(checkpoint_tuple.pending_writes)
        )

    return checkpoint_tuple


class TestSyncAsyncEquivalence:
    """Test that sync and async implementations produce identical results for operation sequences."""

    @settings(
        max_examples=10,
        deadline=10000,
        suppress_health_check=[
            HealthCheck.function_scoped_fixture,
            HealthCheck.too_slow,
            HealthCheck.large_base_example,
            HealthCheck.data_too_large,
        ],
    )
    @given(
        operations=operation_sequences(min_size=100)
    )
    def test_operation_sequences_produce_identical_results(self, savers, operations):
        """Property: Executing the same sequence of operations should produce identical results."""
        sync_saver, async_saver = savers

        # Clear all data from Cassandra tables for each Hypothesis example to ensure clean state
        # Use synchronous execute for both since cassandra-asyncio session also supports it
        sync_saver.session.execute(f"TRUNCATE {sync_saver.keyspace}.checkpoints")
        sync_saver.session.execute(f"TRUNCATE {sync_saver.keyspace}.checkpoint_writes")
        async_saver.session.execute(f"TRUNCATE {async_saver.keyspace}.checkpoints")
        async_saver.session.execute(
            f"TRUNCATE {async_saver.keyspace}.checkpoint_writes"
        )

        # Create a fresh InMemorySaver for each Hypothesis example to avoid state persistence
        in_memory_saver = InMemorySaver()

        # Track checkpoints we've created so we can reference them later
        # Key: (thread_id, checkpoint_ns) -> list of checkpoint_ids
        created_checkpoints = {}

        # Track version immutability: (thread_id, checkpoint_ns, channel, version) -> value
        # Once a version is set for a channel, it must always have the same value
        version_store = {}

        for op in operations:
            if isinstance(op, PutOperation):
                # Enforce version immutability: ensure same version always has same value
                # Use deepcopy to avoid modifying the original checkpoint dict
                checkpoint = copy.deepcopy(op.checkpoint)
                key_prefix = (op.thread_id, op.checkpoint_ns)

                for channel, version in checkpoint["channel_versions"].items():
                    version_key = (*key_prefix, channel, version)
                    if version_key in version_store:
                        # Reuse the existing value for this version
                        checkpoint["channel_values"][channel] = version_store[
                            version_key
                        ]
                    else:
                        # Store the value for this version
                        version_store[version_key] = checkpoint["channel_values"][
                            channel
                        ]

                # Put checkpoint in both sync and async
                config = {
                    "configurable": {
                        "thread_id": op.thread_id,
                        "checkpoint_ns": op.checkpoint_ns,
                    }
                }

                # Pass channel_versions as new_versions parameter
                result_sync = sync_saver.put(
                    config, checkpoint, op.metadata, checkpoint["channel_versions"]
                )
                result_async = asyncio.run(
                    async_saver.aput(
                        config, checkpoint, op.metadata, checkpoint["channel_versions"]
                    )
                )
                result_memory = in_memory_saver.put(
                    config, checkpoint, op.metadata, checkpoint["channel_versions"]
                )

                # Results should be identical
                assert result_sync == result_async
                assert result_sync == result_memory
                assert result_sync["configurable"]["checkpoint_id"] == checkpoint["id"]

                # Track this checkpoint
                key = (op.thread_id, op.checkpoint_ns)
                if key not in created_checkpoints:
                    created_checkpoints[key] = []
                created_checkpoints[key].append(checkpoint["id"])

            elif isinstance(op, PutWritesOperation):
                # Try to find a valid checkpoint for this thread/ns
                key = (op.thread_id, op.checkpoint_ns)
                if key not in created_checkpoints or not created_checkpoints[key]:
                    # Skip this operation if no checkpoint exists
                    continue

                checkpoint_id = created_checkpoints[key][-1]

                # Put writes in both sync and async
                config = {
                    "configurable": {
                        "thread_id": op.thread_id,
                        "checkpoint_ns": op.checkpoint_ns,
                        "checkpoint_id": checkpoint_id,
                    }
                }

                sync_saver.put_writes(config, op.writes, op.task_id)
                asyncio.run(async_saver.aput_writes(config, op.writes, op.task_id))
                in_memory_saver.put_writes(config, op.writes, op.task_id)

            elif isinstance(op, GetOperation):
                # Determine which checkpoint_id to get
                checkpoint_id = op.checkpoint_id
                if checkpoint_id == "__will_replace__":
                    # Try to get a real checkpoint ID from our created list
                    key = (op.thread_id, op.checkpoint_ns)
                    if key in created_checkpoints and created_checkpoints[key]:
                        # Pick a checkpoint using the deterministic index (modulo to handle overflow)
                        idx = op.checkpoint_index % len(created_checkpoints[key])
                        checkpoint_id = created_checkpoints[key][idx]
                    else:
                        checkpoint_id = None

                # Get checkpoint from both sync and async
                config = {
                    "configurable": {
                        "thread_id": op.thread_id,
                        "checkpoint_ns": op.checkpoint_ns,
                    }
                }
                if checkpoint_id:
                    config["configurable"]["checkpoint_id"] = checkpoint_id

                result_sync = normalize_checkpoint_tuple(sync_saver.get_tuple(config))
                result_async = normalize_checkpoint_tuple(
                    asyncio.run(async_saver.aget_tuple(config))
                )
                result_memory = normalize_checkpoint_tuple(
                    in_memory_saver.get_tuple(config)
                )

                # All three should return the same result
                assert result_sync == result_async
                assert result_sync == result_memory

            elif isinstance(op, ListOperation):
                # List checkpoints from both
                config = {
                    "configurable": {
                        "thread_id": op.thread_id,
                        "checkpoint_ns": op.checkpoint_ns,
                    }
                }

                kwargs = {}
                if op.limit is not None:
                    kwargs["limit"] = op.limit
                if op.filter is not None:
                    kwargs["filter"] = op.filter

                list_sync = normalize_checkpoint_tuple(
                    sync_saver.list(config, **kwargs)
                )
                list_async = normalize_checkpoint_tuple(
                    asyncio.run(
                        self._async_list_to_list(async_saver.alist(config, **kwargs))
                    )
                )
                list_memory = normalize_checkpoint_tuple(
                    in_memory_saver.list(config, **kwargs)
                )

                # Should have same number of results
                assert len(list_sync) == len(list_async)
                assert len(list_sync) == len(list_memory)

                # Compare checkpoints (normalize pending_writes first)
                for sync_tuple, async_tuple, memory_tuple in zip(
                    list_sync, list_async, list_memory, strict=False
                ):
                    assert sync_tuple == async_tuple
                    assert sync_tuple == memory_tuple

            elif isinstance(op, DeleteThreadOperation):
                # Delete thread in all three
                sync_saver.delete_thread(op.thread_id)
                asyncio.run(async_saver.adelete_thread(op.thread_id))
                in_memory_saver.delete_thread(op.thread_id)

                # Remove from our tracking
                keys_to_remove = [
                    k for k in created_checkpoints.keys() if k[0] == op.thread_id
                ]
                for key in keys_to_remove:
                    del created_checkpoints[key]

    @staticmethod
    async def _async_list_to_list(async_gen):
        """Convert an async generator to a list."""
        result = []
        async for item in async_gen:
            result.append(item)
        return result
