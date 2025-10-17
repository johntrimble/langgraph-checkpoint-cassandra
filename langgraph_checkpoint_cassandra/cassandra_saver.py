"""
Cassandra-based checkpoint saver implementation for LangGraph.
"""

import logging
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import contextmanager
from typing import Any, Literal, get_args, get_origin
from uuid import UUID

from cassandra.cluster import Cluster, Session
from cassandra.query import (
    BatchStatement,
    BatchType,
    ConsistencyLevel,
)
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
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


def _python_type_to_cql_type(python_type: type) -> str:
    """
    Convert Python type to Cassandra CQL type string.

    Supports nested collections like list[int], dict[str, int], set[str], etc.

    Args:
        python_type: Python type (str, int, float, bool, dict, list, set, or parameterized versions)

    Returns:
        CQL type string

    Raises:
        ValueError: If type is not supported

    Examples:
        >>> _python_type_to_cql_type(str)
        'TEXT'
        >>> _python_type_to_cql_type(int)
        'BIGINT'
        >>> _python_type_to_cql_type(list[int])
        'LIST<BIGINT>'
        >>> _python_type_to_cql_type(dict[str, int])
        'MAP<TEXT, BIGINT>'
        >>> _python_type_to_cql_type(set[str])
        'SET<TEXT>'
    """
    type_mapping = {
        str: "TEXT",
        int: "BIGINT",
        float: "DOUBLE",
        bool: "BOOLEAN",
    }

    # Check simple types first
    if python_type in type_mapping:
        return type_mapping[python_type]

    # Handle parameterized collection types (e.g., list[int], dict[str, int])
    origin = get_origin(python_type)
    if origin is not None:
        args = get_args(python_type)

        if origin is list:
            if args:
                # list[T] -> LIST<cql_type(T)>
                inner_type = _python_type_to_cql_type(args[0])
                return f"LIST<{inner_type}>"
            else:
                # Unparameterized list -> LIST<TEXT>
                return "LIST<TEXT>"

        elif origin is set:
            if args:
                # set[T] -> SET<cql_type(T)>
                inner_type = _python_type_to_cql_type(args[0])
                return f"SET<{inner_type}>"
            else:
                # Unparameterized set -> SET<TEXT>
                return "SET<TEXT>"

        elif origin is dict:
            if args and len(args) >= 2:
                # dict[K, V] -> MAP<cql_type(K), cql_type(V)>
                key_type = _python_type_to_cql_type(args[0])
                value_type = _python_type_to_cql_type(args[1])
                return f"MAP<{key_type}, {value_type}>"
            else:
                # Unparameterized dict -> MAP<TEXT, TEXT>
                return "MAP<TEXT, TEXT>"

    # Fallback for unparameterized dict, list, set
    if python_type is dict:
        return "MAP<TEXT, TEXT>"
    elif python_type is list:
        return "LIST<TEXT>"
    elif python_type is set:
        return "SET<TEXT>"

    raise ValueError(
        f"Unsupported type: {python_type}. "
        f"Supported types are: str, int, float, bool, dict, list, set, "
        f"and parameterized versions like list[int], dict[str, int], set[str]"
    )


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
        checkpoint_id_type: Literal["text", "uuid"] = "uuid",
        ttl_seconds: int | None = None,
        read_consistency: ConsistencyLevel | None = ConsistencyLevel.LOCAL_QUORUM,
        write_consistency: ConsistencyLevel | None = ConsistencyLevel.LOCAL_QUORUM,
        queryable_metadata: dict[str, type] | None = None,
        indexed_metadata: list[str] | None = None,
    ) -> None:
        """
        Initialize the CassandraSaver.

        Args:
            session: Cassandra session object
            keyspace: Keyspace name for checkpoint tables
            serde: Optional custom serializer (uses JsonPlusSerializer by default)
            thread_id_type: Type to use for thread_id column: "text" (default) or "uuid"
            checkpoint_id_type: Type to use for checkpoint_id column: "uuid" (default) or "text"
            ttl_seconds: Optional TTL in seconds for automatic expiration of checkpoints (e.g., 2592000 for 30 days)
            read_consistency: Consistency level for read operations (default: ConsistencyLevel.LOCAL_QUORUM).
                            Set to None to use session default.
            write_consistency: Consistency level for write operations (default: ConsistencyLevel.LOCAL_QUORUM).
                             Set to None to use session default.
            queryable_metadata: Optional dictionary mapping metadata field names to their Python types.
                              Fields specified here will have dedicated columns for server-side filtering.
                              Supported types: str, int, float, bool, dict, list, set.
                              Example: {"user_id": str, "step": int, "tags": list}
            indexed_metadata: Optional list of field names from queryable_metadata that should have SAI indexes.
                            If not specified, SAI indexes will be created for all queryable_metadata fields.
                            Fields not indexed will use ALLOW FILTERING (slower but still works).
                            Example: ["user_id", "step"] (creates indexes only for these fields)

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
        self.queryable_metadata = queryable_metadata or {}

        # If indexed_metadata not specified, index all queryable fields
        if indexed_metadata is None:
            self.indexed_metadata = set(self.queryable_metadata.keys())
        else:
            self.indexed_metadata = set(indexed_metadata)
            # Validate that indexed fields are in queryable_metadata
            invalid = self.indexed_metadata - self.queryable_metadata.keys()
            if invalid:
                raise ValueError(
                    f"indexed_metadata contains fields not in queryable_metadata: {invalid}"
                )

        self._statements_prepared = False

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

        # Set consistency levels on prepared statements
        if self.read_consistency is not None:
            # Read statements
            self.stmt_get_checkpoint_by_id.consistency_level = self.read_consistency
            self.stmt_get_latest_checkpoint.consistency_level = self.read_consistency
            self.stmt_list_checkpoints.consistency_level = self.read_consistency
            self.stmt_list_checkpoints_before.consistency_level = self.read_consistency
            self.stmt_get_writes.consistency_level = self.read_consistency

        if self.write_consistency is not None:
            # Write statements
            self.stmt_insert_checkpoint.consistency_level = self.write_consistency
            self.stmt_delete_checkpoints.consistency_level = self.write_consistency
            self.stmt_insert_write.consistency_level = self.write_consistency
            self.stmt_delete_writes.consistency_level = self.write_consistency

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

        This method will attempt to use the async driver (cassandra-asyncio-driver)
        if available, falling back to the sync driver (cassandra-driver) if not.

        To use async operations, install cassandra-asyncio-driver:
            pip install cassandra-asyncio-driver

        Args:
            contact_points: List of Cassandra node addresses
            port: Cassandra port (default: 9042)
            keyspace: Keyspace name
            **kwargs: Additional arguments for Cluster or CassandraSaver

        Yields:
            CassandraSaver instance with async support if cassandra-asyncio-driver
            is installed, otherwise with sync-only support
        """
        cluster = None
        try:
            # Try async driver first
            try:
                from cassandra_asyncio.cluster import Cluster as AsyncCluster

                cluster = AsyncCluster(contact_points, port=port)
                session = cluster.connect()
                logger.info("Using async Cassandra driver (cassandra-asyncio-driver)")
            except ImportError:
                # Fall back to sync driver
                cluster = Cluster(contact_points, port=port)
                session = cluster.connect()
                logger.info(
                    "Using sync Cassandra driver (async operations not available)"
                )

            saver = cls(session, keyspace=keyspace, **kwargs)
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

        # Add queryable metadata columns and indexes if specified
        if self.queryable_metadata:
            logger.info(
                f"Setting up queryable metadata fields: {list(self.queryable_metadata.keys())}"
            )
            self._setup_queryable_metadata()

        # Prepare statements now that tables exist
        self._prepare_statements()
        logger.info("✓ Checkpoint schema ready")

    def _setup_queryable_metadata(self) -> None:
        """
        Create columns and optionally SAI indexes for queryable metadata fields.

        This method is called during setup() if queryable_metadata is specified.
        It adds columns prefixed with "metadata__" and creates SAI indexes
        only for fields specified in indexed_metadata.
        """
        for field_name, field_type in self.queryable_metadata.items():
            column_name = f"metadata__{field_name}"

            try:
                # Get CQL type for this field
                cql_type = _python_type_to_cql_type(field_type)

                # Add column if it doesn't exist
                # Using IF NOT EXISTS would be nice but ALTER TABLE doesn't support it
                # So we try to add and ignore errors if column exists
                try:
                    alter_stmt = f"""
                        ALTER TABLE {self.keyspace}.checkpoints
                        ADD {column_name} {cql_type}
                    """
                    self.session.execute(alter_stmt)
                    logger.info(f"  ✓ Added column: {column_name} ({cql_type})")
                except Exception as e:
                    # Column likely already exists, which is fine
                    if (
                        "conflicts with an existing column" in str(e)
                        or "already exists" in str(e).lower()
                    ):
                        logger.debug(f"  → Column {column_name} already exists")
                    else:
                        logger.warning(f"  ⚠ Could not add column {column_name}: {e}")

                # Create SAI index only if this field is in indexed_metadata
                if field_name in self.indexed_metadata:
                    index_name = f"idx_{column_name}"
                    try:
                        create_index_stmt = f"""
                            CREATE CUSTOM INDEX IF NOT EXISTS {index_name}
                            ON {self.keyspace}.checkpoints ({column_name})
                            USING 'StorageAttachedIndex'
                        """
                        self.session.execute(create_index_stmt)
                        logger.info(f"  ✓ Created SAI index: {index_name}")
                    except Exception as e:
                        logger.debug(f"  → Index {index_name} setup: {e}")
                else:
                    logger.info(
                        f"  → Column {column_name} created without index (will use ALLOW FILTERING)"
                    )

            except ValueError as e:
                logger.error(f"  ✗ Invalid type for field '{field_name}': {e}")
                raise

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
        # Metadata is always stored as msgpack
        metadata = self.serde.loads_typed(("msgpack", row.metadata))

        # Get checkpoint_id for pending writes
        checkpoint_id_str = str(row.checkpoint_id) if row.checkpoint_id else None
        checkpoint_id = self._convert_checkpoint_id(checkpoint_id_str)

        # Get pending writes
        writes_result = self.session.execute(
            self.stmt_get_writes, (thread_id, checkpoint_ns, checkpoint_id)
        )

        # Writes are stored with sequential idx to preserve insertion order
        # Cassandra will return them ordered by clustering key (task_id, idx)
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
            query_parts = [
                f"SELECT * FROM {self.keyspace}.checkpoints WHERE thread_id = ? AND checkpoint_ns = ?"
            ]
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
                if field_type in (list, set) or (
                    hasattr(field_type, "__origin__")
                    and field_type.__origin__ in (list, set)
                ):
                    query_parts.append(f"AND {column_name} CONTAINS ?")
                    query_params.append(field_value)
                elif field_type is dict or (
                    hasattr(field_type, "__origin__") and field_type.__origin__ is dict
                ):
                    # For maps, check if filtering by key or value
                    # If field_value is a dict, check if keys exist
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
            result = self.session.execute(prepared_query, query_params)
        else:
            # Use prepared statements for standard queries
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
                if not all(
                    metadata.get(k) == v for k, v in client_side_filters.items()
                ):
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
            batch_ids = checkpoint_ids[i : i + BATCH_SIZE]

            # Prepare query with IN clause for this batch
            batch_query = self.session.prepare(f"""
                SELECT checkpoint_id, task_id, task_path, idx, channel, type, value
                FROM {self.keyspace}.checkpoint_writes
                WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id IN ?
            """)

            batch_result = self.session.execute(
                batch_query, (thread_id, checkpoint_ns, batch_ids)
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
        parent_checkpoint_id_param = self._convert_checkpoint_id(
            parent_checkpoint_id_str
        )

        # Serialize checkpoint and metadata
        type_str, checkpoint_blob = self.serde.dumps_typed(checkpoint)
        # Metadata is always stored as msgpack
        _, metadata_blob = self.serde.dumps_typed(metadata)

        # Insert checkpoint
        logging.info(
            f"Checkpoint ID type: {self.checkpoint_id_type}, value: {checkpoint_id_param}, typeof: {type(checkpoint_id_param)}"
        )
        logging.info(
            f"Parent Checkpoint ID type: {self.checkpoint_id_type}, value: {parent_checkpoint_id_param}, typeof: {type(parent_checkpoint_id_param)}"
        )

        # If queryable metadata is specified, we need to use dynamic query
        if self.queryable_metadata:
            # Build column list and values
            columns = [
                "thread_id",
                "checkpoint_ns",
                "checkpoint_id",
                "parent_checkpoint_id",
                "type",
                "checkpoint",
                "metadata",
            ]
            params = [
                thread_id_param,
                checkpoint_ns,
                checkpoint_id_param,
                parent_checkpoint_id_param,
                type_str,
                checkpoint_blob,
                metadata_blob,
            ]

            # Add queryable metadata columns and values
            for field_name in self.queryable_metadata:
                column_name = f"metadata__{field_name}"
                columns.append(column_name)
                # Extract value from metadata, or None if not present
                field_value = metadata.get(field_name)
                params.append(field_value)

            # Build INSERT statement
            placeholders = ", ".join(["?"] * len(columns))
            column_list = ", ".join(columns)

            if self.ttl_seconds is not None:
                insert_query = f"""
                    INSERT INTO {self.keyspace}.checkpoints ({column_list})
                    VALUES ({placeholders})
                    USING TTL {self.ttl_seconds}
                """
            else:
                insert_query = f"""
                    INSERT INTO {self.keyspace}.checkpoints ({column_list})
                    VALUES ({placeholders})
                """

            prepared_stmt = self.session.prepare(insert_query)
            self.session.execute(prepared_stmt, params)
        else:
            # Use regular prepared statement
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
                ),
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

        # Simple insert for each write - no deduplication check
        # Use WRITES_IDX_MAP for special channels, enumerate index for regular channels
        # Last write wins (Cassandra will overwrite existing rows with same PRIMARY KEY)
        for idx, (channel, value) in enumerate(writes):
            write_idx = WRITES_IDX_MAP.get(channel, idx)  # Use WRITES_IDX_MAP
            type_str, value_blob = self.serde.dumps_typed(value)

            self.session.execute(
                self.stmt_insert_write,
                (
                    thread_id_param,
                    checkpoint_ns,
                    checkpoint_id_param,
                    task_id,
                    task_path,
                    write_idx,  # Use WRITES_IDX_MAP value
                    channel,
                    type_str,
                    value_blob,
                ),
            )

    def delete_thread(self, thread_id_str: str) -> None:
        """
        Delete all checkpoints and writes for a thread across all namespaces.

        With the new schema where thread_id is the partition key, this is a simple
        partition delete that removes all data for the thread in both tables.

        Args:
            thread_id_str: The thread ID whose checkpoints should be deleted
        """
        from cassandra.query import BatchStatement, BatchType

        self._prepare_statements()  # Ensure statements are prepared
        thread_id = self._convert_thread_id(thread_id_str)

        # Convert UUIDs to strings for Cassandra statements if needed
        if self.thread_id_type in ("uuid", "timeuuid") and isinstance(thread_id, UUID):
            thread_id_param = str(thread_id)
        else:
            thread_id_param = thread_id

        logger.info(f"Deleting thread {thread_id_str}")

        # Use a logged batch to delete from both tables atomically
        # Since thread_id is now the partition key, these are efficient partition deletes
        batch = BatchStatement(batch_type=BatchType.LOGGED)

        # Delete all checkpoints for this thread
        delete_checkpoints_stmt = self.session.prepare(f"""
            DELETE FROM {self.keyspace}.checkpoints WHERE thread_id = ?
        """)
        batch.add(delete_checkpoints_stmt, (thread_id_param,))

        # Delete all checkpoint writes for this thread
        delete_writes_stmt = self.session.prepare(f"""
            DELETE FROM {self.keyspace}.checkpoint_writes WHERE thread_id = ?
        """)
        batch.add(delete_writes_stmt, (thread_id_param,))

        # Execute the batch
        self.session.execute(batch)

        logger.info(f"Successfully deleted thread {thread_id_str}")

    # Async methods - require cassandra-asyncio driver
    def _ensure_async_support(self) -> None:
        """Check if session supports async operations.

        Raises:
            NotImplementedError: If session doesn't have aexecute method (async support).
        """
        if not hasattr(self.session, "aexecute"):
            raise NotImplementedError(
                "Async operations require an async Cassandra session.\n\n"
                "To enable async support:\n"
                "1. Install the async driver:\n"
                "   pip install cassandra-asyncio-driver\n\n"
                "2. Create an async session and pass it to CassandraSaver:\n"
                "   from cassandra_asyncio.cluster import Cluster\n"
                "   cluster = Cluster(['localhost'])\n"
                "   session = cluster.connect()\n"
                "   checkpointer = CassandraSaver(session)\n\n"
                "The CassandraSaver will automatically detect async support and enable async methods."
            )

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """
        Get a checkpoint tuple from Cassandra asynchronously.

        Args:
            config: Configuration containing thread_id, checkpoint_ns, and optionally checkpoint_id

        Returns:
            CheckpointTuple if found, None otherwise

        Raises:
            NotImplementedError: If session doesn't support async operations
        """
        self._ensure_async_support()
        self._prepare_statements()

        thread_id_str = config["configurable"]["thread_id"]
        thread_id = self._convert_thread_id(thread_id_str)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id_str = get_checkpoint_id(config)

        # Query checkpoint
        if checkpoint_id_str:
            checkpoint_id = self._convert_checkpoint_id(checkpoint_id_str)
            result = await self.session.aexecute(
                self.stmt_get_checkpoint_by_id,
                (thread_id, checkpoint_ns, checkpoint_id),
            )
        else:
            result = await self.session.aexecute(
                self.stmt_get_latest_checkpoint, (thread_id, checkpoint_ns)
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
            self.stmt_get_writes, (thread_id, checkpoint_ns, row.checkpoint_id)
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

        Raises:
            NotImplementedError: If session doesn't support async operations
        """
        self._ensure_async_support()
        self._prepare_statements()

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
            query_parts = [
                f"SELECT * FROM {self.keyspace}.checkpoints WHERE thread_id = ? AND checkpoint_ns = ?"
            ]
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
                if field_type in (list, set) or (
                    hasattr(field_type, "__origin__")
                    and field_type.__origin__ in (list, set)
                ):
                    query_parts.append(f"AND {column_name} CONTAINS ?")
                    query_params.append(field_value)
                elif field_type is dict or (
                    hasattr(field_type, "__origin__") and field_type.__origin__ is dict
                ):
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
                if not all(
                    metadata.get(k) == v for k, v in client_side_filters.items()
                ):
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
            batch_ids = checkpoint_ids[i : i + BATCH_SIZE]

            # Prepare query with IN clause for this batch
            batch_query = self.session.prepare(f"""
                SELECT checkpoint_id, task_id, task_path, idx, channel, type, value
                FROM {self.keyspace}.checkpoint_writes
                WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id IN ?
            """)

            batch_result = await self.session.aexecute(
                batch_query, (thread_id, checkpoint_ns, batch_ids)
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

        Raises:
            NotImplementedError: If session doesn't support async operations
        """
        self._ensure_async_support()
        self._prepare_statements()

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
                "thread_id",
                "checkpoint_ns",
                "checkpoint_id",
                "parent_checkpoint_id",
                "type",
                "checkpoint",
                "metadata",
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

        Raises:
            NotImplementedError: If session doesn't support async operations
        """
        self._ensure_async_support()
        self._prepare_statements()

        thread_id_str = config["configurable"]["thread_id"]
        thread_id = self._convert_thread_id(thread_id_str)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = self._convert_checkpoint_id(
            config["configurable"]["checkpoint_id"]
        )

        # Simple insert for each write - no deduplication check
        # Use WRITES_IDX_MAP for special channels, enumerate index for regular channels
        # Last write wins (Cassandra will overwrite existing rows with same PRIMARY KEY)
        for idx, (channel, value) in enumerate(writes):
            write_idx = WRITES_IDX_MAP.get(channel, idx)  # Use WRITES_IDX_MAP
            type_str, value_blob = self.serde.dumps_typed(value)

            await self.session.aexecute(
                self.stmt_insert_write,
                (
                    thread_id,
                    checkpoint_ns,
                    checkpoint_id,
                    task_id,
                    task_path,
                    write_idx,  # Use WRITES_IDX_MAP value
                    channel,
                    type_str,
                    value_blob,
                ),
            )

    async def adelete_thread(self, thread_id_str: str) -> None:
        """
        Delete all checkpoints and writes for a thread asynchronously.

        Args:
            thread_id_str: Thread ID to delete

        Raises:
            NotImplementedError: If session doesn't support async operations
        """
        self._ensure_async_support()
        self._prepare_statements()

        thread_id = self._convert_thread_id(thread_id_str)

        # Convert UUIDs to strings for Cassandra statements if needed
        if self.thread_id_type in ("uuid", "timeuuid") and isinstance(thread_id, UUID):
            thread_id_param = str(thread_id)
        else:
            thread_id_param = thread_id

        logger.info(f"Deleting thread {thread_id_str}")

        # Use a logged batch to delete from both tables atomically
        # Since thread_id is now the partition key, these are efficient partition deletes
        batch = BatchStatement(batch_type=BatchType.LOGGED)

        # Delete all checkpoints for this thread
        delete_checkpoints_stmt = self.session.prepare(f"""
            DELETE FROM {self.keyspace}.checkpoints WHERE thread_id = ?
        """)
        batch.add(delete_checkpoints_stmt, (thread_id_param,))

        # Delete all checkpoint writes for this thread
        delete_writes_stmt = self.session.prepare(f"""
            DELETE FROM {self.keyspace}.checkpoint_writes WHERE thread_id = ?
        """)
        batch.add(delete_writes_stmt, (thread_id_param,))

        if self.write_consistency:
            batch.consistency_level = self.write_consistency

        # Execute the batch
        await self.session.aexecute(batch)

        logger.info(f"Successfully deleted thread {thread_id_str}")
