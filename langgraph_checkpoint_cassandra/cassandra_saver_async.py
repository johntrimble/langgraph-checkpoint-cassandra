"""
Async Cassandra-based checkpoint saver implementation for LangGraph.

This module provides an async CheckpointSaver implementation using cassandra-asyncio-driver
for true async I/O operations.
"""

import logging
from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal

from cassandra.query import BatchStatement, BatchType, ConsistencyLevel
from cassandra_asyncio.cluster import Cluster
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
)

from .cassandra_base import BaseCassandraSaver, DEFAULT_KEYSPACE, _python_type_to_cql_type
from .migrations import MigrationManager

logger = logging.getLogger(__name__)


class AsyncCassandraSaver(BaseCassandraSaver):
    """
    Async Cassandra-based checkpoint saver implementation.

    Uses cassandra-asyncio-driver for true async I/O operations, enabling
    high concurrency (1000s of concurrent operations) without thread pool limitations.

    Features:
    - True async/await I/O (no thread pool blocking)
    - Native BLOB storage
    - Optional TTL support
    - Tunable consistency levels
    - Server-side metadata filtering with SAI indexes

    Example:
        ```python
        from cassandra_asyncio.cluster import Cluster
        from langgraph_checkpoint_cassandra import AsyncCassandraSaver

        cluster = Cluster(['localhost'])
        session = cluster.connect()

        checkpointer = AsyncCassandraSaver(
            session,
            keyspace='my_checkpoints'
        )
        await checkpointer.setup()

        # Use with LangGraph
        checkpoint = await checkpointer.aget_tuple(config)
        ```
    """

    def __init__(
        self,
        session,  # cassandra_asyncio session
        keyspace: str = DEFAULT_KEYSPACE,
        *,
        serde: Any | None = None,
        thread_id_type: Literal["text", "uuid"] = "text",
        checkpoint_id_type: Literal["text", "uuid"] = "text",
        ttl_seconds: int | None = None,
        read_consistency: ConsistencyLevel | None = ConsistencyLevel.LOCAL_QUORUM,
        write_consistency: ConsistencyLevel | None = ConsistencyLevel.LOCAL_QUORUM,
        queryable_metadata: dict[str, type] | None = None,
        indexed_metadata: list[str] | None = None,
    ) -> None:
        """
        Initialize the AsyncCassandraSaver.

        Args:
            session: Cassandra async session object (from cassandra_asyncio)
            keyspace: Keyspace name for checkpoint tables
            serde: Optional custom serializer
            thread_id_type: Type to use for thread_id column: "text" or "uuid"
            checkpoint_id_type: Type to use for checkpoint_id column: "text" or "uuid"
            ttl_seconds: Optional TTL in seconds for automatic expiration
            read_consistency: Consistency level for read operations
            write_consistency: Consistency level for write operations
            queryable_metadata: Optional dict mapping metadata field names to types
            indexed_metadata: Optional list of metadata field names to create SAI indexes for

        Note:
            You must call `.setup()` before using the checkpointer.
        """
        super().__init__(
            keyspace=keyspace,
            serde=serde,
            thread_id_type=thread_id_type,
            checkpoint_id_type=checkpoint_id_type,
            ttl_seconds=ttl_seconds,
            queryable_metadata=queryable_metadata,
            indexed_metadata=indexed_metadata,
        )

        self.session = session
        self.read_consistency = read_consistency
        self.write_consistency = write_consistency
        self._statements_prepared = False

    @classmethod
    async def from_conn_info(
        cls,
        contact_points: list[str],
        port: int = 9042,
        keyspace: str = DEFAULT_KEYSPACE,
        **kwargs,
    ) -> "AsyncCassandraSaver":
        """
        Create an AsyncCassandraSaver from connection info.

        Args:
            contact_points: List of Cassandra node addresses
            port: Cassandra port (default: 9042)
            keyspace: Keyspace name
            **kwargs: Additional arguments passed to AsyncCassandraSaver constructor

        Returns:
            AsyncCassandraSaver instance
        """
        cluster = Cluster(contact_points, port=port)
        session = cluster.connect()
        return cls(session, keyspace=keyspace, **kwargs)

    async def setup(self, replication_factor: int = 3) -> None:
        """
        Set up the checkpoint database schema asynchronously.

        This method creates the necessary tables using migrations.
        It MUST be called before using the checkpointer.

        Args:
            replication_factor: Replication factor for the keyspace (default: 3)
                              Use 1 for single-node development clusters.
        """
        # Use sync MigrationManager since migrations are infrequent setup operations
        # Converting to async would require async version of entire migration system
        manager = MigrationManager(
            self.session,
            keyspace=self.keyspace,
            thread_id_type=self.thread_id_type,
            checkpoint_id_type=self.checkpoint_id_type,
            replication_factor=replication_factor,
        )

        manager.load_migrations()
        manager.migrate()

        # Setup queryable metadata columns and indexes
        if self.queryable_metadata:
            await self._setup_queryable_metadata()

        # Prepare statements after schema is ready
        await self._prepare_statements()

    async def _setup_queryable_metadata(self) -> None:
        """
        Set up queryable metadata columns and SAI indexes asynchronously.

        Creates dedicated columns for each queryable metadata field and
        optionally creates SAI indexes for efficient server-side filtering.
        """
        logger.info(f"Setting up queryable metadata columns for {len(self.queryable_metadata)} fields")

        for field_name, field_type in self.queryable_metadata.items():
            column_name = f"metadata__{field_name}"
            cql_type = _python_type_to_cql_type(field_type)

            # Add column if it doesn't exist (idempotent)
            alter_stmt = f"""
                ALTER TABLE {self.keyspace}.checkpoints
                ADD {column_name} {cql_type}
            """

            try:
                await self.session.aexecute(alter_stmt)
                logger.info(f"  ✓ Added column {column_name} ({cql_type})")
            except Exception as e:
                if "conflicts with an existing column" in str(e) or "already exists" in str(e):
                    logger.debug(f"  → Column {column_name} already exists")
                else:
                    logger.warning(f"  ✗ Failed to add column {column_name}: {e}")

            # Create SAI index if this field is in indexed_metadata
            if field_name in self.indexed_metadata:
                index_name = f"idx_{self.keyspace}_checkpoints_{column_name}"
                create_index_stmt = f"""
                    CREATE CUSTOM INDEX IF NOT EXISTS {index_name}
                    ON {self.keyspace}.checkpoints ({column_name})
                    USING 'StorageAttachedIndex'
                """

                try:
                    await self.session.aexecute(create_index_stmt)
                    logger.info(f"  ✓ Created SAI index {index_name}")
                except Exception as e:
                    if "already exists" in str(e):
                        logger.debug(f"  → Index {index_name} already exists")
                    else:
                        logger.warning(f"  ✗ Failed to create index {index_name}: {e}")
            else:
                logger.info(f"  → Column {column_name} created without index (will use ALLOW FILTERING)")

    async def _prepare_statements(self) -> None:
        """Prepare CQL statements for reuse."""
        if self._statements_prepared:
            return

        # Note: cassandra-asyncio-driver's prepare() is synchronous
        # Prepared statements are stored on server and reused automatically

        # Checkpoint queries
        self.stmt_get_checkpoint_by_id = self.session.prepare(f"""
            SELECT * FROM {self.keyspace}.checkpoints
            WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?
        """)

        self.stmt_get_latest_checkpoint = self.session.prepare(f"""
            SELECT * FROM {self.keyspace}.checkpoints
            WHERE thread_id = ? AND checkpoint_ns = ?
            ORDER BY checkpoint_id DESC
            LIMIT 1
        """)

        self.stmt_list_checkpoints = self.session.prepare(f"""
            SELECT * FROM {self.keyspace}.checkpoints
            WHERE thread_id = ? AND checkpoint_ns = ?
        """)

        self.stmt_list_checkpoints_before = self.session.prepare(f"""
            SELECT * FROM {self.keyspace}.checkpoints
            WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id < ?
        """)

        # Checkpoint writes queries
        self.stmt_get_writes = self.session.prepare(f"""
            SELECT * FROM {self.keyspace}.checkpoint_writes
            WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?
        """)

        # Insert/update statements
        if self.ttl_seconds:
            self.stmt_insert_checkpoint = self.session.prepare(f"""
                INSERT INTO {self.keyspace}.checkpoints
                (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                USING TTL {self.ttl_seconds}
            """)

            self.stmt_insert_write = self.session.prepare(f"""
                INSERT INTO {self.keyspace}.checkpoint_writes
                (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, value)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                USING TTL {self.ttl_seconds}
            """)
        else:
            self.stmt_insert_checkpoint = self.session.prepare(f"""
                INSERT INTO {self.keyspace}.checkpoints
                (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """)

            self.stmt_insert_write = self.session.prepare(f"""
                INSERT INTO {self.keyspace}.checkpoint_writes
                (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, value)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)

        # Delete statements
        self.stmt_delete_checkpoints = self.session.prepare(f"""
            DELETE FROM {self.keyspace}.checkpoints WHERE thread_id = ?
        """)

        self.stmt_delete_writes = self.session.prepare(f"""
            DELETE FROM {self.keyspace}.checkpoint_writes WHERE thread_id = ?
        """)

        # Set consistency levels if specified
        if self.read_consistency:
            self.stmt_get_checkpoint_by_id.consistency_level = self.read_consistency
            self.stmt_get_latest_checkpoint.consistency_level = self.read_consistency
            self.stmt_list_checkpoints.consistency_level = self.read_consistency
            self.stmt_list_checkpoints_before.consistency_level = self.read_consistency
            self.stmt_get_writes.consistency_level = self.read_consistency

        if self.write_consistency:
            self.stmt_insert_checkpoint.consistency_level = self.write_consistency
            self.stmt_insert_write.consistency_level = self.write_consistency
            self.stmt_delete_checkpoints.consistency_level = self.write_consistency
            self.stmt_delete_writes.consistency_level = self.write_consistency

        self._statements_prepared = True

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """
        Get a checkpoint tuple from Cassandra asynchronously.

        Args:
            config: Configuration containing thread_id, checkpoint_ns, and optionally checkpoint_id

        Returns:
            CheckpointTuple if found, None otherwise
        """
        await self._prepare_statements()

        thread_id_str = config["configurable"]["thread_id"]
        thread_id = self._convert_thread_id(thread_id_str)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id_str = get_checkpoint_id(config)

        # Query checkpoint
        if checkpoint_id_str:
            checkpoint_id = self._convert_checkpoint_id(checkpoint_id_str)
            result = await self.session.aexecute(
                self.stmt_get_checkpoint_by_id,
                (thread_id, checkpoint_ns, checkpoint_id)
            )
        else:
            result = await self.session.aexecute(
                self.stmt_get_latest_checkpoint,
                (thread_id, checkpoint_ns)
            )

        if not result:
            return None

        row = result[0]

        # Deserialize checkpoint and metadata
        checkpoint = self.serde.loads_typed((row.type, row.checkpoint))
        # Metadata is always stored as msgpack
        metadata = self.serde.loads_typed(("msgpack", row.metadata))

        # Query pending writes
        writes_result = await self.session.aexecute(
            self.stmt_get_writes,
            (thread_id, checkpoint_ns, row.checkpoint_id)
        )

        pending_writes = []
        for write_row in writes_result:
            value = self.serde.loads_typed((write_row.type, write_row.value))
            pending_writes.append((write_row.task_id, write_row.channel, value))

        # Build parent config if parent exists
        parent_config = None
        if row.parent_checkpoint_id:
            parent_config = {
                "configurable": {
                    "thread_id": thread_id_str,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": str(row.parent_checkpoint_id),
                }
            }

        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": thread_id_str,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": str(row.checkpoint_id),
                }
            },
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=parent_config,
            pending_writes=pending_writes,
        )

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """
        List checkpoints from Cassandra asynchronously.

        Args:
            config: Base configuration for filtering checkpoints
            filter: Additional filtering criteria for metadata
            before: List checkpoints created before this configuration
            limit: Maximum number of checkpoints to return

        Yields:
            CheckpointTuple objects matching the criteria
        """
        await self._prepare_statements()

        if not config:
            return

        thread_id_str = config["configurable"]["thread_id"]
        thread_id = self._convert_thread_id(thread_id_str)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        # Separate queryable and non-queryable metadata filters
        server_side_filters = {}
        client_side_filters = {}

        if filter:
            for key, value in filter.items():
                if key in self.queryable_metadata:
                    server_side_filters[key] = value
                else:
                    client_side_filters[key] = value

        # Query checkpoints with server-side filtering if applicable
        if server_side_filters:
            # Build dynamic query with metadata filters
            query_parts = [f"SELECT * FROM {self.keyspace}.checkpoints WHERE thread_id = ? AND checkpoint_ns = ?"]
            query_params = [thread_id, checkpoint_ns]
            needs_allow_filtering = False

            # Add server-side metadata filters
            for field_name, field_value in server_side_filters.items():
                column_name = f"metadata__{field_name}"
                field_type = self.queryable_metadata[field_name]

                # Check if filtering on a non-indexed field
                if field_name not in self.indexed_metadata:
                    needs_allow_filtering = True

                # For collection types (list, set, map), use CONTAINS
                if field_type in (list, set) or (hasattr(field_type, "__origin__") and field_type.__origin__ in (list, set)):
                    query_parts.append(f"AND {column_name} CONTAINS ?")
                    query_params.append(field_value)
                elif field_type is dict or (hasattr(field_type, "__origin__") and field_type.__origin__ is dict):
                    # For maps, check if filtering by key or value
                    if isinstance(field_value, dict):
                        # PostgreSQL @> behavior: check if all key-value pairs exist
                        # Note: map[key] = value syntax requires ALLOW FILTERING even with SAI index
                        needs_allow_filtering = True
                        for k, v in field_value.items():
                            query_parts.append(f"AND {column_name}[?] = ?")
                            query_params.extend([k, v])
                    else:
                        # Single value: check if it exists in map values
                        query_parts.append(f"AND {column_name} CONTAINS ?")
                        query_params.append(field_value)
                else:
                    # Scalar types: use equality
                    query_parts.append(f"AND {column_name} = ?")
                    query_params.append(field_value)

            # Add before filter if specified
            if before:
                before_id_str = before["configurable"]["checkpoint_id"]
                before_id_uuid = self._to_uuid(before_id_str)
                before_id_param = str(before_id_uuid) if before_id_uuid else None
                query_parts.append("AND checkpoint_id < ?")
                query_params.append(before_id_param)

            # Only add ALLOW FILTERING if filtering on non-indexed fields
            query = " ".join(query_parts)
            if needs_allow_filtering:
                query += " ALLOW FILTERING"

            prepared_query = self.session.prepare(query)
            result = await self.session.aexecute(prepared_query, query_params)
        else:
            # Use prepared statements for standard queries
            if before:
                before_id_str = before["configurable"]["checkpoint_id"]
                before_id_uuid = self._to_uuid(before_id_str)
                before_id_param = str(before_id_uuid) if before_id_uuid else None
                result = await self.session.aexecute(
                    self.stmt_list_checkpoints_before,
                    (thread_id, checkpoint_ns, before_id_param),
                )
            else:
                result = await self.session.aexecute(
                    self.stmt_list_checkpoints, (thread_id, checkpoint_ns)
                )

        # Collect checkpoints that pass client-side filters
        checkpoints_to_return = []
        count = 0

        for row in result:
            # Deserialize
            checkpoint = self.serde.loads_typed((row.type, row.checkpoint))
            # Metadata is always stored as msgpack
            metadata = self.serde.loads_typed(("msgpack", row.metadata))

            # Apply client-side metadata filters (for non-queryable fields)
            if client_side_filters:
                if not all(metadata.get(k) == v for k, v in client_side_filters.items()):
                    continue

            checkpoints_to_return.append((row, checkpoint, metadata))
            count += 1
            if limit and count >= limit:
                break

        if not checkpoints_to_return:
            return

        # Batch fetch writes for all checkpoints
        checkpoint_ids = [row.checkpoint_id for row, _, _ in checkpoints_to_return]

        # Fetch writes in batches of 250 to avoid too large IN queries
        BATCH_SIZE = 250
        all_writes = []

        for i in range(0, len(checkpoint_ids), BATCH_SIZE):
            batch_ids = checkpoint_ids[i:i+BATCH_SIZE]

            # Prepare query with IN clause for this batch
            batch_query = self.session.prepare(f"""
                SELECT checkpoint_id, task_id, task_path, idx, channel, type, value
                FROM {self.keyspace}.checkpoint_writes
                WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id IN ?
            """)

            batch_result = await self.session.aexecute(
                batch_query,
                (thread_id, checkpoint_ns, batch_ids)
            )
            all_writes.extend(batch_result)

        # Group writes by checkpoint_id
        writes_by_checkpoint = {}
        for write_row in all_writes:
            checkpoint_id_key = str(write_row.checkpoint_id)
            if checkpoint_id_key not in writes_by_checkpoint:
                writes_by_checkpoint[checkpoint_id_key] = []

            value = self.serde.loads_typed((write_row.type, write_row.value))
            writes_by_checkpoint[checkpoint_id_key].append(
                (write_row.task_id, write_row.channel, value)
            )

        # Yield checkpoints with their writes
        for row, checkpoint, metadata in checkpoints_to_return:
            checkpoint_id_key = str(row.checkpoint_id)
            pending_writes = writes_by_checkpoint.get(checkpoint_id_key, [])

            # Build parent config
            parent_config = None
            if row.parent_checkpoint_id:
                parent_config = {
                    "configurable": {
                        "thread_id": thread_id_str,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": str(row.parent_checkpoint_id),
                    }
                }

            yield CheckpointTuple(
                config={
                    "configurable": {
                        "thread_id": thread_id_str,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": str(row.checkpoint_id),
                    }
                },
                checkpoint=checkpoint,
                metadata=metadata,
                parent_config=parent_config,
                pending_writes=pending_writes,
            )

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """
        Save a checkpoint to Cassandra asynchronously.

        Args:
            config: Configuration containing thread_id and checkpoint_ns
            checkpoint: Checkpoint data to save
            metadata: Metadata associated with the checkpoint
            new_versions: Channel versions (unused in current implementation)

        Returns:
            Updated config with checkpoint_id
        """
        await self._prepare_statements()

        thread_id_str = config["configurable"]["thread_id"]
        thread_id = self._convert_thread_id(thread_id_str)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = self._convert_checkpoint_id(checkpoint["id"])

        # Get parent checkpoint ID if it exists
        parent_checkpoint_id = self._convert_checkpoint_id(
            config["configurable"].get("checkpoint_id")
        )

        # Serialize checkpoint and metadata
        type_str, checkpoint_blob = self.serde.dumps_typed(checkpoint)
        # Metadata is always stored as msgpack
        _, metadata_blob = self.serde.dumps_typed(metadata)

        # Build base insert parameters
        base_params = [
            thread_id,
            checkpoint_ns,
            checkpoint_id,
            parent_checkpoint_id,
            type_str,
            checkpoint_blob,
            metadata_blob,
        ]

        # If queryable_metadata is configured, add values for queryable columns
        if self.queryable_metadata:
            # Build dynamic INSERT with queryable metadata columns
            columns = [
                "thread_id", "checkpoint_ns", "checkpoint_id",
                "parent_checkpoint_id", "type", "checkpoint", "metadata"
            ]
            placeholders = ["?" for _ in range(7)]
            params = base_params.copy()

            for field_name in self.queryable_metadata.keys():
                column_name = f"metadata__{field_name}"
                columns.append(column_name)
                placeholders.append("?")

                # Get value from metadata, or None if not present
                field_value = metadata.get(field_name)
                params.append(field_value)

            # Build and execute dynamic INSERT
            columns_str = ", ".join(columns)
            placeholders_str = ", ".join(placeholders)

            if self.ttl_seconds:
                insert_query = f"""
                    INSERT INTO {self.keyspace}.checkpoints ({columns_str})
                    VALUES ({placeholders_str})
                    USING TTL {self.ttl_seconds}
                """
            else:
                insert_query = f"""
                    INSERT INTO {self.keyspace}.checkpoints ({columns_str})
                    VALUES ({placeholders_str})
                """

            prepared_insert = self.session.prepare(insert_query)
            if self.write_consistency:
                prepared_insert.consistency_level = self.write_consistency

            await self.session.aexecute(prepared_insert, params)
        else:
            # Use pre-prepared statement (no queryable metadata)
            await self.session.aexecute(self.stmt_insert_checkpoint, base_params)

        return {
            "configurable": {
                "thread_id": thread_id_str,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """
        Save pending writes to Cassandra asynchronously.

        Args:
            config: Configuration containing thread_id, checkpoint_ns, and checkpoint_id
            writes: Sequence of (channel, value) tuples to save
            task_id: Task identifier
            task_path: Optional task path (default: "")
        """
        await self._prepare_statements()

        thread_id_str = config["configurable"]["thread_id"]
        thread_id = self._convert_thread_id(thread_id_str)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = self._convert_checkpoint_id(config["configurable"]["checkpoint_id"])

        # Insert each write
        for idx, (channel, value) in enumerate(writes):
            type_str, value_blob = self.serde.dumps_typed(value)

            await self.session.aexecute(
                self.stmt_insert_write,
                (
                    thread_id,
                    checkpoint_ns,
                    checkpoint_id,
                    task_id,
                    task_path,
                    idx,
                    channel,
                    type_str,
                    value_blob,
                )
            )

    async def delete_thread(self, thread_id_str: str) -> None:
        """
        Delete all checkpoints and writes for a thread asynchronously.

        Args:
            thread_id_str: Thread ID to delete
        """
        await self._prepare_statements()

        thread_id_param = self._convert_thread_id(thread_id_str)

        # Use logged batch for atomicity across partitions
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(self.stmt_delete_checkpoints, (thread_id_param,))
        batch.add(self.stmt_delete_writes, (thread_id_param,))

        if self.write_consistency:
            batch.consistency_level = self.write_consistency

        await self.session.aexecute(batch)

    # Sync methods - not supported in async saver
    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Sync method not supported. Use aget_tuple() instead."""
        raise NotImplementedError(
            "Sync methods not available on AsyncCassandraSaver. "
            "Use aget_tuple() or switch to CassandraSaver for sync operations."
        )

    def list(self, config: RunnableConfig | None, **kwargs):
        """Sync method not supported. Use alist() instead."""
        raise NotImplementedError(
            "Sync methods not available on AsyncCassandraSaver. "
            "Use alist() or switch to CassandraSaver for sync operations."
        )

    def put(self, config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata, new_versions: ChannelVersions) -> RunnableConfig:
        """Sync method not supported. Use aput() instead."""
        raise NotImplementedError(
            "Sync methods not available on AsyncCassandraSaver. "
            "Use aput() or switch to CassandraSaver for sync operations."
        )

    def put_writes(self, config: RunnableConfig, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str = "") -> None:
        """Sync method not supported. Use aput_writes() instead."""
        raise NotImplementedError(
            "Sync methods not available on AsyncCassandraSaver. "
            "Use aput_writes() or switch to CassandraSaver for sync operations."
        )
