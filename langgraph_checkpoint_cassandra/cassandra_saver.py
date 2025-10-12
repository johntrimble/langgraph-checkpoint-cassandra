"""
Cassandra-based checkpoint saver implementation for LangGraph.

This module provides a CheckpointSaver implementation that stores checkpoints
in Apache Cassandra, following the Option 4+ design (two-table enhanced approach).
"""

import asyncio
import logging
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import contextmanager
from typing import Any, Literal
from uuid import UUID

from cassandra.cluster import Cluster, Session
from cassandra.query import (
    BatchStatement,
    BatchType,
    ConsistencyLevel,
)
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
)

logger = logging.getLogger(__name__)

# Constants
DEFAULT_KEYSPACE = "langgraph_checkpoints"
DEFAULT_CONTACT_POINTS = ["localhost"]
DEFAULT_PORT = 9042


class CassandraSaver(BaseCheckpointSaver):
    """
    Cassandra-based checkpoint saver implementation.

    This implementation uses a two-table design:
    - checkpoints: Stores full serialized checkpoints
    - checkpoint_writes: Stores pending writes

    Features:
    - Native BLOB storage (no base64 encoding needed)
    - Optional TTL support for automatic cleanup
    - Prepared statements for optimal performance
    - Tunable consistency levels
    """

    def __init__(
        self,
        session: Session,
        keyspace: str = DEFAULT_KEYSPACE,
        *,
        serde: Any | None = None,
        thread_id_type: Literal["text", "uuid"] = "text",
        checkpoint_id_type: Literal["text", "uuid"] = "text",
        ttl_seconds: int | None = None,
        read_consistency: ConsistencyLevel | None = ConsistencyLevel.LOCAL_QUORUM,
        write_consistency: ConsistencyLevel | None = ConsistencyLevel.LOCAL_QUORUM,
    ) -> None:
        """
        Initialize the CassandraSaver.

        Args:
            session: Cassandra session object
            keyspace: Keyspace name for checkpoint tables
            serde: Optional custom serializer (uses JsonPlusSerializer by default)
            thread_id_type: Type to use for thread_id column: "text" (default), "uuid", or "timeuuid"
            checkpoint_id_type: Type to use for checkpoint_id column: "text" (default), "uuid"
            ttl_seconds: Optional TTL in seconds for automatic expiration of checkpoints (e.g., 2592000 for 30 days)
            read_consistency: Consistency level for read operations (default: ConsistencyLevel.LOCAL_QUORUM).
                            Set to None to use session default.
            write_consistency: Consistency level for write operations (default: ConsistencyLevel.LOCAL_QUORUM).
                             Set to None to use session default.

        Note:
            You must call `.setup()` before using the checkpointer to create the required tables.
        """
        super().__init__(serde=serde)
        self.session = session
        self.keyspace = keyspace
        self.thread_id_type = thread_id_type
        self.checkpoint_id_type = checkpoint_id_type
        self.ttl_seconds = ttl_seconds
        self.read_consistency = read_consistency
        self.write_consistency = write_consistency

        self._statements_prepared = False

        # # Try to prepare statements if keyspace exists
        # try:
        #     self._prepare_statements()
        # except Exception:
        #     # Keyspace might not exist yet - will prepare on first use
        #     pass

    def _prepare_statements(self) -> None:
        """Prepare CQL statements for reuse."""
        if self._statements_prepared:
            return

        # Checkpoint queries
        self.stmt_get_checkpoint_by_id = self.session.prepare(f"""
            SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
                   type, checkpoint, metadata
            FROM {self.keyspace}.checkpoints
            WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?
        """)

        self.stmt_get_latest_checkpoint = self.session.prepare(f"""
            SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
                   type, checkpoint, metadata
            FROM {self.keyspace}.checkpoints
            WHERE thread_id = ? AND checkpoint_ns = ?
            LIMIT 1
        """)

        self.stmt_list_checkpoints = self.session.prepare(f"""
            SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
                   type, checkpoint, metadata
            FROM {self.keyspace}.checkpoints
            WHERE thread_id = ? AND checkpoint_ns = ?
        """)

        self.stmt_list_checkpoints_before = self.session.prepare(f"""
            SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
                   type, checkpoint, metadata
            FROM {self.keyspace}.checkpoints
            WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id < ?
        """)

        # Prepare insert statement with or without TTL
        if self.ttl_seconds is not None:
            self.stmt_insert_checkpoint = self.session.prepare(f"""
                INSERT INTO {self.keyspace}.checkpoints
                (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
                 type, checkpoint, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                USING TTL {self.ttl_seconds}
            """)
        else:
            self.stmt_insert_checkpoint = self.session.prepare(f"""
                INSERT INTO {self.keyspace}.checkpoints
                (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
                 type, checkpoint, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """)

        self.stmt_delete_checkpoints = self.session.prepare(f"""
            DELETE FROM {self.keyspace}.checkpoints
            WHERE thread_id = ? AND checkpoint_ns = ?
        """)

        # Checkpoint writes queries
        self.stmt_get_writes = self.session.prepare(f"""
            SELECT task_id, task_path, idx, channel, type, value
            FROM {self.keyspace}.checkpoint_writes
            WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?
        """)

        # Prepare write insert statement with or without TTL
        if self.ttl_seconds is not None:
            self.stmt_insert_write = self.session.prepare(f"""
                INSERT INTO {self.keyspace}.checkpoint_writes
                (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path,
                 idx, channel, type, value)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                USING TTL {self.ttl_seconds}
            """)
        else:
            self.stmt_insert_write = self.session.prepare(f"""
                INSERT INTO {self.keyspace}.checkpoint_writes
                (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path,
                 idx, channel, type, value)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)

        self.stmt_delete_writes = self.session.prepare(f"""
            DELETE FROM {self.keyspace}.checkpoint_writes
            WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?
        """)

        self.stmt_get_checkpoint_ids = self.session.prepare(f"""
            SELECT checkpoint_id
            FROM {self.keyspace}.checkpoints
            WHERE thread_id = ? AND checkpoint_ns = ?
        """)
        
        # Thread namespaces queries
        if self.ttl_seconds is not None:
            self.stmt_insert_namespace = self.session.prepare(f"""
                INSERT INTO {self.keyspace}.thread_namespaces
                (thread_id, checkpoint_ns)
                VALUES (?, ?)
                USING TTL {self.ttl_seconds}
            """)
        else:
            self.stmt_insert_namespace = self.session.prepare(f"""
                INSERT INTO {self.keyspace}.thread_namespaces
                (thread_id, checkpoint_ns)
                VALUES (?, ?)
            """)
            
        self.stmt_get_thread_namespaces = self.session.prepare(f"""
            SELECT checkpoint_ns
            FROM {self.keyspace}.thread_namespaces
            WHERE thread_id = ?
        """)
        
        self.stmt_delete_thread_namespace = self.session.prepare(f"""
            DELETE FROM {self.keyspace}.thread_namespaces
            WHERE thread_id = ? AND checkpoint_ns = ?
        """)

        # Set consistency levels on prepared statements
        if self.read_consistency is not None:
            # Read statements
            self.stmt_get_checkpoint_by_id.consistency_level = self.read_consistency
            self.stmt_get_latest_checkpoint.consistency_level = self.read_consistency
            self.stmt_list_checkpoints.consistency_level = self.read_consistency
            self.stmt_list_checkpoints_before.consistency_level = self.read_consistency
            self.stmt_get_writes.consistency_level = self.read_consistency
            self.stmt_get_checkpoint_ids.consistency_level = self.read_consistency
            self.stmt_get_thread_namespaces.consistency_level = self.read_consistency

        if self.write_consistency is not None:
            # Write statements
            self.stmt_insert_checkpoint.consistency_level = self.write_consistency
            self.stmt_delete_checkpoints.consistency_level = self.write_consistency
            self.stmt_insert_write.consistency_level = self.write_consistency
            self.stmt_delete_writes.consistency_level = self.write_consistency
            self.stmt_insert_namespace.consistency_level = self.write_consistency
            self.stmt_delete_thread_namespace.consistency_level = self.write_consistency

        self._statements_prepared = True

    @classmethod
    @contextmanager
    def from_conn_info(
        cls,
        *,
        contact_points: Sequence[str] = DEFAULT_CONTACT_POINTS,
        port: int = DEFAULT_PORT,
        keyspace: str = DEFAULT_KEYSPACE,
        **kwargs: Any,
    ) -> Iterator["CassandraSaver"]:
        """
        Create a CassandraSaver from connection information.

        Args:
            contact_points: List of Cassandra node addresses
            port: Cassandra port (default: 9042)
            keyspace: Keyspace name
            **kwargs: Additional arguments for Cluster or CassandraSaver

        Yields:
            CassandraSaver instance
        """
        cluster = None
        try:
            cluster = Cluster(contact_points, port=port)
            session = cluster.connect()
            saver = cls(session, keyspace=keyspace)
            yield saver
        finally:
            if cluster:
                cluster.shutdown()

    def setup(self, replication_factor: int = 3) -> None:
        """
        Set up the checkpoint database schema.

        This method creates all necessary tables and structures for the checkpoint saver.
        It uses an embedded default migration that creates the standard schema.

        Call this method once when first setting up your application to initialize the database.
        It's safe to call multiple times - it will only create tables if they don't exist.

        Args:
            replication_factor: Replication factor for the keyspace (default: 3).
                               Use 1 for single-node development/test clusters.

        Example:
            ```python
            from cassandra.cluster import Cluster
            from langgraph_checkpoint_cassandra import CassandraSaver

            cluster = Cluster(['localhost'])
            session = cluster.connect()

            checkpointer = CassandraSaver(session, keyspace="my_app")
            checkpointer.setup()  # Creates tables if needed (RF=3 for production)

            # For development with single node:
            checkpointer.setup(replication_factor=1)
            ```
        """
        from langgraph_checkpoint_cassandra.migrations import MigrationManager

        logger.info("Setting up checkpoint schema...")
        manager = MigrationManager(
            session=self.session,
            keyspace=self.keyspace,
            thread_id_type=self.thread_id_type,
            checkpoint_id_type=self.checkpoint_id_type,
            replication_factor=replication_factor,
        )

        # Apply any pending migrations
        count = manager.migrate()
        if count > 0:
            logger.info(f"✓ Applied {count} migration(s)")
        else:
            logger.info("✓ Schema is up to date")

        # Prepare statements now that tables exist
        self._prepare_statements()
        logger.info("✓ Checkpoint schema ready")

    @staticmethod
    def _to_uuid(checkpoint_id: str | None) -> UUID | None:
        """
        Convert checkpoint_id string to UUID object for Cassandra UUID columns.

        Args:
            checkpoint_id: Checkpoint ID as string (UUID format)

        Returns:
            UUID object, or None if checkpoint_id is None or empty

        Raises:
            ValueError: If checkpoint_id is not a valid UUID
        """
        if not checkpoint_id:
            return None
        try:
            return UUID(checkpoint_id)
        except ValueError as e:
            raise ValueError(
                f"checkpoint_id must be a valid UUID string. Got: {checkpoint_id}"
            ) from e

    def _convert_checkpoint_id(self, checkpoint_id: str | None) -> str | UUID | None:
        """
        Convert checkpoint_id string to appropriate type based on configuration.

        Args:
            checkpoint_id: Checkpoint ID as string

        Returns:
            String for "text" type, UUID object for "uuid" type

        Raises:
            ValueError: If checkpoint_id is not a valid UUID when uuid type is configured
        """
        if checkpoint_id is None:
            return None
        elif self.checkpoint_id_type == "text":
            return checkpoint_id
        else:  # uuid
            try:
                return UUID(checkpoint_id)
            except ValueError as e:
                raise ValueError(
                    f"checkpoint_id must be a valid UUID when checkpoint_id_type='{self.checkpoint_id_type}'. "
                    f"Got: {checkpoint_id}"
                ) from e

    def _convert_thread_id(self, thread_id: str) -> str | UUID:
        """
        Convert thread_id string to appropriate type based on configuration.

        Args:
            thread_id: Thread ID as string

        Returns:
            String for "text" type, UUID object for "uuid" or "timeuuid" types

        Raises:
            ValueError: If thread_id is not a valid UUID when uuid/timeuuid type is configured
        """
        if self.thread_id_type == "text":
            return thread_id
        else:  # uuid or timeuuid
            try:
                return UUID(thread_id)
            except ValueError as e:
                raise ValueError(
                    f"thread_id must be a valid UUID when thread_id_type='{self.thread_id_type}'. "
                    f"Got: {thread_id}"
                ) from e

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """
        Get a checkpoint tuple from Cassandra.

        Args:
            config: Configuration specifying which checkpoint to retrieve

        Returns:
            CheckpointTuple if found, None otherwise
        """
        self._prepare_statements()  # Ensure statements are prepared
        thread_id_str = config["configurable"]["thread_id"]
        thread_id = self._convert_thread_id(thread_id_str)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id_str = get_checkpoint_id(config)

        # Get checkpoint
        if checkpoint_id_str:
            checkpoint_id = self._convert_checkpoint_id(checkpoint_id_str)
            result = self.session.execute(
                self.stmt_get_checkpoint_by_id,
                (thread_id, checkpoint_ns, checkpoint_id),
            )
        else:
            result = self.session.execute(
                self.stmt_get_latest_checkpoint, (thread_id, checkpoint_ns)
            )

        row = result.one()
        if not row:
            return None

        # Deserialize checkpoint and metadata
        checkpoint = self.serde.loads_typed((row.type, row.checkpoint))
        metadata = self.serde.loads(row.metadata)

        # Get checkpoint_id for pending writes
        checkpoint_id_str = str(row.checkpoint_id) if row.checkpoint_id else None
        checkpoint_id = self._convert_checkpoint_id(checkpoint_id_str)

        # Get pending writes
        writes_result = self.session.execute(
            self.stmt_get_writes, (thread_id, checkpoint_ns, checkpoint_id)
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
            pending_writes=pending_writes if pending_writes else None,
        )

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """
        List checkpoints from Cassandra.

        Args:
            config: Base configuration for filtering checkpoints
            filter: Additional filtering criteria for metadata (applied client-side)
            before: List checkpoints created before this configuration
            limit: Maximum number of checkpoints to return

        Yields:
            CheckpointTuple objects matching the criteria
        """
        self._prepare_statements()  # Ensure statements are prepared
        if not config:
            return

        thread_id_str = config["configurable"]["thread_id"]
        thread_id = self._convert_thread_id(thread_id_str)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        # Query checkpoints
        if before:
            before_id_str = before["configurable"]["checkpoint_id"]
            before_id_uuid = self._to_uuid(before_id_str)
            before_id_param = str(before_id_uuid) if before_id_uuid else None
            result = self.session.execute(
                self.stmt_list_checkpoints_before,
                (thread_id, checkpoint_ns, before_id_param),
            )
        else:
            result = self.session.execute(
                self.stmt_list_checkpoints, (thread_id, checkpoint_ns)
            )

        # Process results
        count = 0
        for row in result:
            # Deserialize
            checkpoint = self.serde.loads_typed((row.type, row.checkpoint))
            metadata = self.serde.loads(row.metadata)

            # Apply metadata filter if provided (client-side)
            if filter:
                if not all(metadata.get(k) == v for k, v in filter.items()):
                    continue

            # Get pending writes
            writes_result = self.session.execute(
                self.stmt_get_writes, (thread_id, checkpoint_ns, row.checkpoint_id)
            )

            pending_writes = []
            for write_row in writes_result:
                value = self.serde.loads_typed((write_row.type, write_row.value))
                pending_writes.append((write_row.task_id, write_row.channel, value))

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
                pending_writes=pending_writes if pending_writes else None,
            )

            # Apply limit
            count += 1
            if limit and count >= limit:
                break

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """
        Save a checkpoint to Cassandra.

        Args:
            config: Configuration for the checkpoint
            checkpoint: The checkpoint to save
            metadata: Metadata for the checkpoint
            new_versions: New channel versions as of this write

        Returns:
            Updated configuration after storing the checkpoint
        """
        self._prepare_statements()  # Ensure statements are prepared
        thread_id_str = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id_str = checkpoint["id"]
        parent_checkpoint_id_str = config["configurable"].get("checkpoint_id")

        # Convert checkpoint/thread IDs as appropriate
        thread_id_param = self._convert_thread_id(thread_id_str)
        checkpoint_id_param = self._convert_checkpoint_id(checkpoint_id_str)
        parent_checkpoint_id_param = self._convert_checkpoint_id(parent_checkpoint_id_str)

        # Serialize checkpoint and metadata
        type_str, checkpoint_blob = self.serde.dumps_typed(checkpoint)
        metadata_blob = self.serde.dumps(metadata)

        # Insert checkpoint
        logging.info(f"Checkpoint ID type: {self.checkpoint_id_type}, value: {checkpoint_id_param}, typeof: {type(checkpoint_id_param)}")
        logging.info(f"Parent Checkpoint ID type: {self.checkpoint_id_type}, value: {parent_checkpoint_id_param}, typeof: {type(parent_checkpoint_id_param)}")
        self.session.execute(
            self.stmt_insert_checkpoint,
            (
                thread_id_param,
                checkpoint_ns,
                checkpoint_id_param,
                parent_checkpoint_id_param,
                type_str,
                checkpoint_blob,
                metadata_blob,
            )
        )
        
        # Track namespace
        self.session.execute(
            self.stmt_insert_namespace,
            (thread_id_param, checkpoint_ns)
        )

        return {
            "configurable": {
                "thread_id": thread_id_str,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id_str,
            }
        }

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """
        Store intermediate writes linked to a checkpoint.

        Args:
            config: Configuration of the related checkpoint
            writes: List of writes to store as (channel, value) tuples
            task_id: Identifier for the task creating the writes
            task_path: Path of the task creating the writes
        """
        self._prepare_statements()  # Ensure statements are prepared
        thread_id_str = config["configurable"]["thread_id"]
        thread_id_param = self._convert_thread_id(thread_id_str)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id_str = config["configurable"]["checkpoint_id"]
        checkpoint_id_param = self._convert_checkpoint_id(checkpoint_id_str)

        # Insert writes individually for now
        for idx, (channel, value) in enumerate(writes):
            type_str, value_blob = self.serde.dumps_typed(value)
            self.session.execute(
                self.stmt_insert_write,
                (
                    thread_id_param,
                    checkpoint_ns,
                    checkpoint_id_param,
                    task_id,
                    task_path,
                    idx,
                    channel,
                    type_str,
                    value_blob,
                )
            )

        # Track namespace
        self.session.execute(
            self.stmt_insert_namespace,
            (thread_id_param, checkpoint_ns)
        )

    def delete_thread(self, thread_id_str: str) -> None:
        """
        Delete all checkpoints and writes for a thread across all namespaces.
        
        This method uses the thread_namespaces table to find all namespaces 
        associated with this thread, and deletes data from all of them.

        Args:
            thread_id_str: The thread ID whose checkpoints should be deleted
        """
        self._prepare_statements()  # Ensure statements are prepared
        thread_id = self._convert_thread_id(thread_id_str)
        
        # Convert UUIDs to strings for Cassandra statements if needed
        if self.thread_id_type in ("uuid", "timeuuid") and isinstance(thread_id, UUID):
            thread_id_param = str(thread_id)
        else:
            thread_id_param = thread_id
            
        # Get all namespaces for this thread
        namespace_result = self.session.execute(
            self.stmt_get_thread_namespaces, (thread_id_param,)
        )
        
        namespaces = [row.checkpoint_ns for row in namespace_result]
        
        # If no namespaces found, nothing to delete
        if not namespaces:
            return
            
        logger.info(f"Deleting thread {thread_id_str} from {len(namespaces)} namespaces")
        
        # For each namespace, delete all associated data
        for checkpoint_ns in namespaces:
            # Get all checkpoint IDs for this thread and namespace
            result = self.session.execute(
                self.stmt_get_checkpoint_ids, (thread_id_param, checkpoint_ns)
            )
            
            checkpoint_ids = [row.checkpoint_id for row in result]
            
            # Execute individual statements instead of a batch to avoid UUID encoding issues
            
            # Delete all checkpoint writes
            for checkpoint_id in checkpoint_ids:
                checkpoint_id_param = str(checkpoint_id) if checkpoint_id else None
                self.session.execute(self.stmt_delete_writes, (thread_id_param, checkpoint_ns, checkpoint_id_param))
                
            # Delete all checkpoints in this namespace
            self.session.execute(self.stmt_delete_checkpoints, (thread_id_param, checkpoint_ns))
            
            # Delete the namespace entry
            self.session.execute(self.stmt_delete_thread_namespace, (thread_id_param, checkpoint_ns))
            
            logger.debug(f"Deleted {len(checkpoint_ids)} checkpoints in namespace '{checkpoint_ns}'")
            
        logger.info(f"Successfully deleted thread {thread_id_str} data")

    # Async methods
    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Async version of get_tuple."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.get_tuple, config)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """Async version of list."""
        loop = asyncio.get_running_loop()
        # Get the iterator in executor
        iterator = await loop.run_in_executor(
            None, lambda: self.list(config, filter=filter, before=before, limit=limit)
        )
        # Yield items from iterator
        while True:
            try:
                item = await loop.run_in_executor(None, next, iterator, None)
                if item is None:
                    break
                yield item
            except StopIteration:
                break

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Async version of put."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self.put, config, checkpoint, metadata, new_versions
        )

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Async version of put_writes."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, self.put_writes, config, writes, task_id, task_path
        )
