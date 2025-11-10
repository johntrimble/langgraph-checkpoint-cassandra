"""
Cassandra-based checkpoint saver implementation for LangGraph.
"""

import fnmatch
import logging
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import contextmanager
from textwrap import dedent
from typing import Any, Literal, TypedDict, cast
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

from langgraph_checkpoint_cassandra.migrations import MigrationManager

logger = logging.getLogger(__name__)

# Constants
DEFAULT_KEYSPACE = "langgraph_checkpoints"
DEFAULT_CONTACT_POINTS = ["localhost"]
DEFAULT_PORT = 9042


class FlattenedMetadata(TypedDict):
    metadata_text: dict[str, str]
    metadata_int: dict[str, int]
    metadata_double: dict[str, float]
    metadata_bool: dict[str, bool]
    metadata_null: set[str]


def _escape_dots(key: str) -> str:
    r"""Escape dots in metadata keys to avoid conflicts with dot notation for nesting.

    Uses backslash-dot (\.) as the escape sequence. Backslashes themselves are also
    escaped (\ → \\) to handle keys that contain literal backslash-dot sequences.

    Args:
        key: Original key that may contain dots

    Returns:
        Key with backslashes and dots escaped

    Examples:
        >>> _escape_dots("file.txt")
        'file\\.txt'
        >>> _escape_dots("path\\to\\file.txt")
        'path\\\\to\\\\file\\.txt'
    """
    # Escape backslashes first, then dots
    return key.replace("\\", "\\\\").replace(".", "\\.")


def _unescape_dots(key: str) -> str:
    r"""Unescape dots in metadata keys.

    Reverses the escaping done by _escape_dots(), converting \. back to . and
    \\ back to \.

    Args:
        key: Escaped key

    Returns:
        Original key with escape sequences removed

    Examples:
        >>> _unescape_dots("file\\.txt")
        'file.txt'
        >>> _unescape_dots("path\\\\to\\\\file\\.txt")
        'path\\to\\file.txt'
    """
    # Unescape dots first, then backslashes
    return key.replace("\\.", ".").replace("\\\\", "\\")


def _should_include_field(
    field_path: str,
    includes: list[str] | None,
    excludes: list[str] | None,
) -> bool:
    """Determine if a metadata field should be included based on include/exclude patterns.

    Uses fnmatch for wildcard pattern matching. Supports patterns like:
    - "user.*" - matches all fields starting with "user."
    - "*.id" - matches all fields ending with ".id"
    - "config.database.*" - matches nested fields

    Priority rules:
    1. If includes is specified and not empty, field must match at least one include pattern
    2. Then, if excludes is specified, field must not match any exclude pattern
    3. If field matches both include and exclude, exclude wins (safer default)
    4. Default (both None): include all fields

    Args:
        field_path: Flattened field path (e.g., "user.name", "step")
        includes: List of patterns to include (None or empty list means include all)
        excludes: List of patterns to exclude (None or empty list means exclude none)

    Returns:
        True if field should be included, False otherwise

    Examples:
        >>> _should_include_field("user.name", includes=["user.*"], excludes=None)
        True
        >>> _should_include_field("step", includes=["user.*"], excludes=None)
        False
        >>> _should_include_field("user.password", includes=["user.*"], excludes=["*.password"])
        False
        >>> _should_include_field("user.name", includes=None, excludes=["*.password"])
        True
        >>> _should_include_field("config.db", includes=None, excludes=None)
        True
    """
    # Step 1: Check includes
    if includes is not None and len(includes) > 0:
        # If includes specified, field must match at least one pattern
        matches_include = any(
            fnmatch.fnmatch(field_path, pattern) for pattern in includes
        )
        if not matches_include:
            return False

    # Step 2: Check excludes
    if excludes is not None and len(excludes) > 0:
        # If field matches any exclude pattern, exclude it
        matches_exclude = any(
            fnmatch.fnmatch(field_path, pattern) for pattern in excludes
        )
        if matches_exclude:
            return False

    # Default: include the field
    return True


def _split_filters(
    filter_dict: dict[str, Any],
    includes: list[str] | None,
    excludes: list[str] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split filters into indexed (server-side) and non-indexed (client-side) filters.

    A filter is indexed if:
    1. It's a simple type (str, int, float, bool, None)
    2. The field matches the include/exclude pattern (would be stored in indexed columns)

    Args:
        filter_dict: Dictionary of filter key-value pairs
        includes: Include patterns used when creating the saver
        excludes: Exclude patterns used when creating the saver

    Returns:
        Tuple of (indexed_filters, non_indexed_filters)

    Examples:
        >>> # All fields indexed (default)
        >>> _split_filters({"user.name": "alice", "step": 5}, None, None)
        ({'user.name': 'alice', 'step': 5}, {})

        >>> # Some fields excluded from indexing
        >>> _split_filters({"user.name": "alice", "user.password": "secret"}, None, ["*.password"])
        ({'user.name': 'alice'}, {'user.password': 'secret'})

        >>> # Complex type not indexed
        >>> _split_filters({"user.name": "alice", "tags": ["a", "b"]}, None, None)
        ({'user.name': 'alice'}, {'tags': ['a', 'b']})
    """
    indexed = {}
    non_indexed = {}

    for key, value in filter_dict.items():
        # Complex types (list, dict, set) are never indexed
        if isinstance(value, (list, dict, set)) and not isinstance(value, str):
            non_indexed[key] = value
            continue

        # Check if this field would be included in indexed columns
        if _should_include_field(key, includes, excludes):
            indexed[key] = value
        else:
            non_indexed[key] = value

    return indexed, non_indexed


def _get_nested_value(obj: Mapping[str, Any], path: str) -> Any:
    """Get a value from a nested dictionary using dot notation.

    Args:
        obj: Dictionary to traverse
        path: Dot-separated path (e.g., "user.name")

    Returns:
        The value at the path, or None if not found

    Examples:
        >>> _get_nested_value({"user": {"name": "alice"}}, "user.name")
        'alice'
        >>> _get_nested_value({"step": 5}, "step")
        5
        >>> _get_nested_value({"user": {"name": "alice"}}, "user.age")
        None
    """
    # Handle keys with literal dots (escaped with backslash)
    # Split on unescaped dots only
    parts = []
    current = ""
    i = 0
    while i < len(path):
        if i < len(path) - 1 and path[i] == "\\" and path[i + 1] == ".":
            # Escaped dot - add literal dot to current part
            current += "."
            i += 2
        elif path[i] == ".":
            # Unescaped dot - this is a path separator
            if current:
                parts.append(current)
                current = ""
            i += 1
        else:
            current += path[i]
            i += 1
    if current:
        parts.append(current)

    # Traverse the nested structure
    result: Any = obj
    for part in parts:
        if not isinstance(result, Mapping):
            return None
        result = result.get(part)
        if result is None:
            return None
    return result


def _matches_filter(
    metadata: Mapping[str, Any], filter_dict: Mapping[str, Any]
) -> bool:
    """Check if metadata matches all filters.

    Args:
        metadata: Metadata dictionary to check
        filter_dict: Dictionary of filter key-value pairs

    Returns:
        True if metadata matches all filters, False otherwise

    Examples:
        >>> _matches_filter({"user": {"name": "alice"}}, {"user.name": "alice"})
        True
        >>> _matches_filter({"user": {"name": "alice"}}, {"user.name": "bob"})
        False
        >>> _matches_filter({"step": 5}, {"step": 5, "source": "loop"})
        False
    """
    for key, expected_value in filter_dict.items():
        actual_value = _get_nested_value(metadata, key)
        if actual_value != expected_value:
            return False
    return True


def _flatten_metadata(
    metadata: Mapping[str, Any],
    prefix: str = "",
    includes: list[str] | None = None,
    excludes: list[str] | None = None,
) -> FlattenedMetadata:
    r"""Flatten nested metadata into typed maps using dot notation for nested keys.

    This function recursively flattens nested dictionaries, using dot notation to represent
    nesting levels (e.g., {"user": {"name": "alice"}} becomes {"user.name": "alice"}).
    Values are categorized by type into separate maps for efficient Cassandra querying.

    Dots in actual key names are escaped using backslash notation to avoid conflicts with
    the dot notation used for nesting. For example, a key "file.txt" becomes "file\.txt"
    to distinguish it from nested structure.

    Fields can be filtered using include/exclude patterns. See _should_include_field for
    pattern matching behavior.

    Args:
        metadata: Metadata dictionary to flatten
        prefix: Current key prefix (for recursive calls)
        includes: Optional list of wildcard patterns to include (e.g., ["user.*", "config.db"])
        excludes: Optional list of wildcard patterns to exclude (e.g., ["*.password", "*.secret"])

    Returns:
        Dictionary with keys: metadata_text, metadata_int, metadata_double, metadata_bool, metadata_null
        Each containing a map of flattened keys to values of that type.

    Examples:
        >>> _flatten_metadata({"source": "loop", "step": 1})
        {
            'metadata_text': {'source': 'loop'},
            'metadata_int': {'step': 1},
            'metadata_double': {},
            'metadata_bool': {},
            'metadata_null': set()
        }

        >>> _flatten_metadata({"user": {"name": "alice", "age": 30}})
        {
            'metadata_text': {'user.name': 'alice'},
            'metadata_int': {'user.age': 30},
            'metadata_double': {},
            'metadata_bool': {},
            'metadata_null': set()
        }

        >>> _flatten_metadata({"score": None, "active": True})
        {
            'metadata_text': {},
            'metadata_int': {},
            'metadata_double': {},
            'metadata_bool': {'active': True},
            'metadata_null': {'score'}
        }

        >>> _flatten_metadata({"user": {"name": "alice", "password": "secret"}}, includes=["user.*"], excludes=["*.password"])
        {
            'metadata_text': {'user.name': 'alice'},
            'metadata_int': {},
            'metadata_double': {},
            'metadata_bool': {},
            'metadata_null': set()
        }
    """
    result: FlattenedMetadata = {
        "metadata_text": {},
        "metadata_int": {},
        "metadata_double": {},
        "metadata_bool": {},
        "metadata_null": set[str](),
    }

    for key, value in metadata.items():
        # Escape dots in the key itself to avoid conflicts with dot notation
        escaped_key = _escape_dots(key)
        full_key = f"{prefix}.{escaped_key}" if prefix else escaped_key

        if value is None:
            # Check if field should be included before adding
            if _should_include_field(full_key, includes, excludes):
                result["metadata_null"].add(full_key)
        elif isinstance(value, dict):
            # Recursively flatten nested dicts (pass includes/excludes through)
            nested = _flatten_metadata(
                cast(dict[str, Any], value), full_key, includes, excludes
            )
            result["metadata_text"].update(nested["metadata_text"])
            result["metadata_int"].update(nested["metadata_int"])
            result["metadata_double"].update(nested["metadata_double"])
            result["metadata_bool"].update(nested["metadata_bool"])
            result["metadata_null"].update(nested["metadata_null"])
        elif isinstance(value, bool):
            # Check bool before int (bool is subclass of int in Python)
            if _should_include_field(full_key, includes, excludes):
                result["metadata_bool"][full_key] = value
        elif isinstance(value, int):
            if _should_include_field(full_key, includes, excludes):
                result["metadata_int"][full_key] = value
        elif isinstance(value, float):
            if _should_include_field(full_key, includes, excludes):
                result["metadata_double"][full_key] = value
        elif isinstance(value, str):
            if _should_include_field(full_key, includes, excludes):
                result["metadata_text"][full_key] = value
        # Lists and other complex types are skipped - stay in metadata blob only

    return result


class CassandraSaver(BaseCheckpointSaver):
    """
    Cassandra-based checkpoint saver implementation.
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
        metadata_includes: list[str] | None = None,
        metadata_excludes: list[str] | None = None,
        fetch_size: int | None = None,
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
            metadata_includes: Optional list of wildcard patterns for metadata fields to include in indexed columns.
                             If specified, only fields matching at least one pattern will be indexed.
                             Examples: ["user.*"], ["*.id", "config.database.*"]
                             Default (None): include all fields.
            metadata_excludes: Optional list of wildcard patterns for metadata fields to exclude from indexed columns.
                             Fields matching any pattern will not be indexed (even if they match an include pattern).
                             Examples: ["*.password", "*.secret", "debug.*"]
                             Default (None): exclude no fields.
            fetch_size: Optional Cassandra fetch size (rows per page). Leave as None to use the driver's
                        default (typically 5000). Provide a positive integer to override on all prepared
                        statements.

        Note:
            You must call `.setup()` before using the checkpointer to create the required tables.

            Metadata filtering is automatically supported for all metadata fields using the
            flattened metadata columns (metadata_text, metadata_int, metadata_double, metadata_bool).
            No pre-declaration of queryable fields is required.

            Use metadata_includes/metadata_excludes to control which fields are stored in the
            indexed columns for performance optimization. Fields not stored in indexed columns
            will still be available in the metadata blob but won't be server-side filterable.
        """
        super().__init__(serde=serde)
        self.session = session
        self.keyspace = keyspace
        self.thread_id_type = thread_id_type
        self.checkpoint_id_type = checkpoint_id_type
        self.ttl_seconds = ttl_seconds
        self.read_consistency = read_consistency
        self.write_consistency = write_consistency
        self.metadata_includes = metadata_includes
        self.metadata_excludes = metadata_excludes
        self.fetch_size = fetch_size

        self._prepared_statements: dict[str, Any] = {}

    def _get_prepared_statement(
        self, query: str, *, consistency: ConsistencyLevel | None = None
    ) -> Any:
        """Return a prepared statement for the given query, preparing it lazily."""
        stmt = self._prepared_statements.get(query)
        if stmt is None:
            stmt = self.session.prepare(query)
            if self.fetch_size is not None and hasattr(stmt, "fetch_size"):
                stmt.fetch_size = self.fetch_size
            self._prepared_statements[query] = stmt

        if (
            consistency is not None
            and getattr(stmt, "consistency_level", None) != consistency
        ):
            stmt.consistency_level = consistency

        return stmt

    def _execute_prepared(
        self,
        query: str,
        parameters: Sequence[Any] | tuple[Any, ...] = (),
        *,
        consistency: ConsistencyLevel | None = None,
    ) -> Any:
        """Execute a prepared statement synchronously, preparing it on demand."""
        stmt = self._get_prepared_statement(query, consistency=consistency)
        return self.session.execute(stmt, tuple(parameters))

    async def _aexecute_prepared(
        self,
        query: str,
        parameters: Sequence[Any] | tuple[Any, ...] = (),
        *,
        consistency: ConsistencyLevel | None = None,
    ) -> Any:
        """Execute a prepared statement asynchronously, preparing it on demand."""
        stmt = self._get_prepared_statement(query, consistency=consistency)
        return await self.session.aexecute(stmt, tuple(parameters))

    def _query_get_checkpoint_by_id(self) -> str:
        return dedent(
            f"""
            SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
                   type, checkpoint, metadata
            FROM {self.keyspace}.checkpoints
            WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?
            """
        ).strip()

    def _query_get_latest_checkpoint(self) -> str:
        return dedent(
            f"""
            SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
                   type, checkpoint, metadata
            FROM {self.keyspace}.checkpoints
            WHERE thread_id = ? AND checkpoint_ns = ?
            LIMIT 1
            """
        ).strip()

    def _query_list_checkpoints(self, limit: int | None = None) -> str:
        base = dedent(
            f"""
            SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
                   type, checkpoint, metadata
            FROM {self.keyspace}.checkpoints
            WHERE thread_id = ? AND checkpoint_ns = ?
            """
        ).strip()
        if limit is not None:
            return f"{base} LIMIT {limit}"
        return base

    def _query_list_checkpoints_before(self, limit: int | None = None) -> str:
        base = dedent(
            f"""
            SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
                   type, checkpoint, metadata
            FROM {self.keyspace}.checkpoints
            WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id < ?
            """
        ).strip()
        if limit is not None:
            return f"{base} LIMIT {limit}"
        return base

    def _query_get_writes(self) -> str:
        return dedent(
            f"""
            SELECT task_id, task_path, idx, channel, type, value
            FROM {self.keyspace}.checkpoint_writes
            WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?
            """
        ).strip()

    def _query_fetch_writes_batch(self) -> str:
        return dedent(
            f"""
            SELECT checkpoint_id, task_id, task_path, idx, channel, type, value
            FROM {self.keyspace}.checkpoint_writes
            WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id IN ?
            """
        ).strip()

    def _query_insert_write(self) -> str:
        if self.ttl_seconds is not None:
            return dedent(
                f"""
                INSERT INTO {self.keyspace}.checkpoint_writes
                (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path,
                 idx, channel, type, value)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                USING TTL {self.ttl_seconds}
                """
            ).strip()

        return dedent(
            f"""
            INSERT INTO {self.keyspace}.checkpoint_writes
            (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path,
             idx, channel, type, value)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        ).strip()

    def _query_delete_thread_checkpoints(self) -> str:
        return f"DELETE FROM {self.keyspace}.checkpoints WHERE thread_id = ?"

    def _query_delete_thread_writes(self) -> str:
        return f"DELETE FROM {self.keyspace}.checkpoint_writes WHERE thread_id = ?"

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
        mm = MigrationManager(
            self.session,
            self.keyspace,
            replication_factor=replication_factor,
            thread_id_type=self.thread_id_type,
            checkpoint_id_type=self.checkpoint_id_type,
        )
        mm.migrate()

        logger.info("✓ Checkpoint schema ready")

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
        thread_id_str = config["configurable"]["thread_id"]
        thread_id = self._convert_thread_id(thread_id_str)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id_str = get_checkpoint_id(config)

        stmt, params = self._build_get_checkpoint_query(
            thread_id, checkpoint_ns, checkpoint_id_str
        )
        result = self._execute_prepared(
            stmt,
            params,
            consistency=self.read_consistency,
        )
        row = self._first_row(result)
        if not row:
            return None

        checkpoint, metadata = self._deserialize_checkpoint_row(row)

        checkpoint_id_for_writes = (
            self._convert_checkpoint_id(str(row.checkpoint_id))
            if row.checkpoint_id
            else None
        )

        writes_result = self._execute_prepared(
            self._query_get_writes(),
            (thread_id, checkpoint_ns, checkpoint_id_for_writes),
            consistency=self.read_consistency,
        )

        pending_writes = self._deserialize_writes(writes_result)

        return self._build_checkpoint_tuple(
            row,
            checkpoint,
            metadata,
            thread_id_str,
            checkpoint_ns,
            pending_writes,
        )

    def _split_and_validate_filters(
        self, filter: dict[str, Any] | None
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Split filters into indexed (server-side) and non-indexed (client-side).

        Args:
            filter: Filter dictionary from list() parameters

        Returns:
            Tuple of (indexed_filters, non_indexed_filters)
        """
        indexed_filters: dict[str, Any] = {}
        non_indexed_filters: dict[str, Any] = {}

        if filter:
            indexed_filters, non_indexed_filters = _split_filters(
                filter, self.metadata_includes, self.metadata_excludes
            )

        return indexed_filters, non_indexed_filters

    def _build_list_query(
        self,
        thread_id: Any,
        checkpoint_ns: str,
        indexed_filters: dict[str, Any],
        before: RunnableConfig | None,
        limit: int | None,
    ) -> tuple[str, list[Any]]:
        """Build CQL query and parameters for listing checkpoints with filters.

        Args:
            thread_id: Thread ID (converted type)
            checkpoint_ns: Checkpoint namespace
            indexed_filters: Filters to apply server-side
            before: Optional "before" configuration for pagination
            limit: Desired number of rows (used when all filtering is server-side)

        Returns:
            Tuple of (query_string, query_params)
        """
        limit_val = None
        if limit is not None:
            try:
                limit_val = int(limit)
            except (TypeError, ValueError) as err:
                raise ValueError(f"limit must be an integer, got {limit!r}") from err
            if limit_val < 0:
                raise ValueError("limit cannot be negative")

        if not indexed_filters:
            if before:
                before_id_str = before["configurable"]["checkpoint_id"]
                before_id = self._convert_checkpoint_id(before_id_str)
                return (
                    self._query_list_checkpoints_before(limit_val),
                    [thread_id, checkpoint_ns, before_id],
                )

            return (
                self._query_list_checkpoints(limit_val),
                [thread_id, checkpoint_ns],
            )

        # Build dynamic query with metadata filters
        query_parts = [
            f"SELECT * FROM {self.keyspace}.checkpoints WHERE thread_id = ? AND checkpoint_ns = ?"
        ]
        query_params = [thread_id, checkpoint_ns]

        # Process each indexed filter key-value pair
        for key, value in indexed_filters.items():
            # Filter keys should already be in dot notation format matching stored keys
            # (e.g., "user.name" to match nested {"user": {"name": ...}})
            # No escaping needed - use the key as-is

            if value is None:
                # Check if key exists in null set
                query_parts.append("AND metadata_null CONTAINS ?")
                query_params.append(key)
            elif isinstance(value, bool):
                # Check bool before int (bool is subclass of int)
                query_parts.append("AND metadata_bool[?] = ?")
                query_params.extend([key, value])
            elif isinstance(value, int):
                query_parts.append("AND metadata_int[?] = ?")
                query_params.extend([key, value])
            elif isinstance(value, float):
                query_parts.append("AND metadata_double[?] = ?")
                query_params.extend([key, value])
            elif isinstance(value, str):
                query_parts.append("AND metadata_text[?] = ?")
                query_params.extend([key, value])

        # Add before filter if specified
        if before:
            before_id_str = before["configurable"]["checkpoint_id"]
            before_id = self._convert_checkpoint_id(before_id_str)
            query_parts.append("AND checkpoint_id < ?")
            query_params.append(before_id)

        # Build final query
        if limit_val is not None:
            query_parts.append(f"LIMIT {limit_val}")

        query = " ".join(query_parts)

        return query, query_params

    def _deserialize_checkpoint_row(
        self, row: Any
    ) -> tuple[Checkpoint, CheckpointMetadata]:
        """Deserialize checkpoint and metadata from a database row.

        Args:
            row: Database row containing checkpoint data

        Returns:
            Tuple of (checkpoint, metadata)
        """
        checkpoint = self.serde.loads_typed((row.type, row.checkpoint))
        metadata = self.serde.loads_typed(("msgpack", row.metadata))
        return checkpoint, metadata

    def _deserialize_and_filter_checkpoints(
        self,
        result: Any,
        non_indexed_filters: dict[str, Any],
        limit: int | None,
    ) -> list[tuple[Any, Checkpoint, CheckpointMetadata]]:
        """Deserialize checkpoints from query result and apply client-side filters.

        Args:
            result: Query result iterable
            non_indexed_filters: Filters to apply client-side
            limit: Maximum number of checkpoints to return

        Returns:
            List of (row, checkpoint, metadata) tuples that passed all filters
        """
        checkpoints_to_return = []
        count = 0

        for row in result:
            # Deserialize
            checkpoint, metadata = self._deserialize_checkpoint_row(row)

            # Apply client-side filters (for non-indexed fields)
            if non_indexed_filters and not _matches_filter(
                metadata, non_indexed_filters
            ):
                # Skip this checkpoint - doesn't match non-indexed filters
                continue

            checkpoints_to_return.append((row, checkpoint, metadata))
            count += 1
            if limit and count >= limit:
                break

        return checkpoints_to_return

    def _fetch_writes_for_checkpoints(
        self,
        thread_id: Any,
        checkpoint_ns: str,
        checkpoints: list[tuple[Any, Checkpoint, CheckpointMetadata]],
    ) -> dict[str, list[tuple[str, str, Any]]]:
        """Fetch and group pending writes for multiple checkpoints (sync version).

        Args:
            thread_id: Thread ID (converted type)
            checkpoint_ns: Checkpoint namespace
            checkpoints: List of (row, checkpoint, metadata) tuples

        Returns:
            Dictionary mapping checkpoint_id (as string) to list of writes
        """
        query, params_list = self._prepare_fetch_writes_batches(
            thread_id, checkpoint_ns, checkpoints
        )
        if query is None:
            return {}

        all_writes: list[Any] = []
        for params in params_list:
            batch_result = self._execute_prepared(
                query,
                params,
                consistency=self.read_consistency,
            )
            all_writes.extend(batch_result)

        return self._group_writes_by_checkpoint(all_writes)

    async def _fetch_writes_for_checkpoints_async(
        self,
        thread_id: Any,
        checkpoint_ns: str,
        checkpoints: list[tuple[Any, Checkpoint, CheckpointMetadata]],
    ) -> dict[str, list[tuple[str, str, Any]]]:
        """Fetch and group pending writes for multiple checkpoints (async version).

        Args:
            thread_id: Thread ID (converted type)
            checkpoint_ns: Checkpoint namespace
            checkpoints: List of (row, checkpoint, metadata) tuples

        Returns:
            Dictionary mapping checkpoint_id (as string) to list of writes
        """
        query, params_list = self._prepare_fetch_writes_batches(
            thread_id, checkpoint_ns, checkpoints
        )
        if query is None:
            return {}

        all_writes: list[Any] = []
        for params in params_list:
            batch_result = await self._aexecute_prepared(
                query,
                params,
                consistency=self.read_consistency,
            )
            all_writes.extend(batch_result)

        return self._group_writes_by_checkpoint(all_writes)

    def _build_checkpoint_tuple(
        self,
        row: Any,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        thread_id_str: str,
        checkpoint_ns: str,
        pending_writes: list[tuple[str, str, Any]],
    ) -> CheckpointTuple:
        """Build a CheckpointTuple from row data and associated writes.

        Args:
            row: Database row
            checkpoint: Deserialized checkpoint
            metadata: Deserialized metadata
            thread_id_str: Thread ID as string
            checkpoint_ns: Checkpoint namespace
            pending_writes: List of pending writes for this checkpoint

        Returns:
            CheckpointTuple object
        """
        # Build parent config
        parent_config: RunnableConfig | None = None
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

    def _build_get_checkpoint_query(
        self,
        thread_id: Any,
        checkpoint_ns: str,
        checkpoint_id_str: str | None,
    ) -> tuple[str, tuple[Any, ...]]:
        """Return query and parameters for fetching a checkpoint row."""
        if checkpoint_id_str:
            checkpoint_id = self._convert_checkpoint_id(checkpoint_id_str)
            return (
                self._query_get_checkpoint_by_id(),
                (thread_id, checkpoint_ns, checkpoint_id),
            )
        return (self._query_get_latest_checkpoint(), (thread_id, checkpoint_ns))

    @staticmethod
    def _first_row(result: Any) -> Any | None:
        """Return the first row from a Cassandra result set."""
        if result is None:
            return None

        one = getattr(result, "one", None)
        if callable(one):
            return one()

        try:
            return result[0]
        except (TypeError, KeyError, IndexError):
            pass

        iterator = iter(result)
        try:
            return next(iterator)
        except StopIteration:
            return None

    def _deserialize_writes(self, writes_result: Any) -> list[tuple[str, str, Any]]:
        """Deserialize pending writes rows into task/channel/value tuples."""
        pending_writes: list[tuple[str, str, Any]] = []

        if not writes_result:
            return pending_writes

        for write_row in writes_result:
            value = self.serde.loads_typed((write_row.type, write_row.value))
            pending_writes.append((write_row.task_id, write_row.channel, value))

        return pending_writes

    def _build_list_statement(
        self,
        thread_id: Any,
        checkpoint_ns: str,
        indexed_filters: dict[str, Any],
        before: RunnableConfig | None,
        limit: int | None,
    ) -> tuple[str, tuple[Any, ...]]:
        """Construct the Cassandra query and params for listing checkpoints."""
        query, query_params = self._build_list_query(
            thread_id, checkpoint_ns, indexed_filters, before, limit
        )

        return query, tuple(query_params)

    def _build_checkpoint_tuples_from_writes(
        self,
        checkpoints: list[tuple[Any, Checkpoint, CheckpointMetadata]],
        writes_by_checkpoint: dict[str, list[tuple[str, str, Any]]],
        thread_id_str: str,
        checkpoint_ns: str,
    ) -> list[CheckpointTuple]:
        """Materialize CheckpointTuple objects from decoded rows and pending writes."""
        tuples: list[CheckpointTuple] = []

        for row, checkpoint, metadata in checkpoints:
            checkpoint_id_key = str(row.checkpoint_id)
            pending_writes = writes_by_checkpoint.get(checkpoint_id_key, [])
            tuples.append(
                self._build_checkpoint_tuple(
                    row,
                    checkpoint,
                    metadata,
                    thread_id_str,
                    checkpoint_ns,
                    pending_writes,
                )
            )

        return tuples

    def _prepare_checkpoint_insert(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
    ) -> tuple[str, str, str, str, tuple[Any, ...]]:
        """Prepare query and parameters for inserting a checkpoint."""
        thread_id_str = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id_str = checkpoint["id"]
        parent_checkpoint_id_str = config["configurable"].get("checkpoint_id")

        thread_id = self._convert_thread_id(thread_id_str)
        checkpoint_id = self._convert_checkpoint_id(checkpoint_id_str)
        parent_checkpoint_id = self._convert_checkpoint_id(parent_checkpoint_id_str)

        type_str, checkpoint_blob = self.serde.dumps_typed(checkpoint)
        _, metadata_blob = self.serde.dumps_typed(metadata)

        flattened = _flatten_metadata(
            metadata,
            includes=self.metadata_includes,
            excludes=self.metadata_excludes,
        )

        columns = [
            "thread_id",
            "checkpoint_ns",
            "checkpoint_id",
            "parent_checkpoint_id",
            "type",
            "checkpoint",
            "metadata",
            "metadata_text",
            "metadata_int",
            "metadata_double",
            "metadata_bool",
            "metadata_null",
        ]

        params = (
            thread_id,
            checkpoint_ns,
            checkpoint_id,
            parent_checkpoint_id,
            type_str,
            checkpoint_blob,
            metadata_blob,
            flattened["metadata_text"],
            flattened["metadata_int"],
            flattened["metadata_double"],
            flattened["metadata_bool"],
            flattened["metadata_null"],
        )

        placeholders = ", ".join(["?"] * len(columns))
        column_list = ", ".join(columns)

        if self.ttl_seconds is not None:
            query = f"""
                INSERT INTO {self.keyspace}.checkpoints ({column_list})
                VALUES ({placeholders})
                USING TTL {self.ttl_seconds}
            """
        else:
            query = f"""
                INSERT INTO {self.keyspace}.checkpoints ({column_list})
                VALUES ({placeholders})
            """

        return (
            thread_id_str,
            checkpoint_ns,
            checkpoint_id_str,
            query,
            params,
        )

    def _prepare_write_rows(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str,
    ) -> list[tuple[Any, ...]]:
        """Build parameter tuples for inserting pending writes."""
        thread_id = self._convert_thread_id(config["configurable"]["thread_id"])
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = self._convert_checkpoint_id(
            config["configurable"]["checkpoint_id"]
        )

        params_list: list[tuple[Any, ...]] = []
        for idx, (channel, value) in enumerate(writes):
            write_idx = WRITES_IDX_MAP.get(channel, idx)
            type_str, value_blob = self.serde.dumps_typed(value)
            params_list.append(
                (
                    thread_id,
                    checkpoint_ns,
                    checkpoint_id,
                    task_id,
                    task_path,
                    write_idx,
                    channel,
                    type_str,
                    value_blob,
                )
            )

        return params_list

    def _prepare_fetch_writes_batches(
        self,
        thread_id: Any,
        checkpoint_ns: str,
        checkpoints: list[tuple[Any, Checkpoint, CheckpointMetadata]],
    ) -> tuple[str | None, list[tuple[Any, ...]]]:
        """Prepare batched query parameters for fetching pending writes."""
        checkpoint_ids = [row.checkpoint_id for row, _, _ in checkpoints]
        if not checkpoint_ids:
            return None, []

        params_list: list[tuple[Any, ...]] = []
        BATCH_SIZE = 250
        for i in range(0, len(checkpoint_ids), BATCH_SIZE):
            batch_ids = checkpoint_ids[i : i + BATCH_SIZE]
            params_list.append((thread_id, checkpoint_ns, batch_ids))

        return self._query_fetch_writes_batch(), params_list

    def _group_writes_by_checkpoint(
        self, write_rows: Sequence[Any]
    ) -> dict[str, list[tuple[str, str, Any]]]:
        """Group write rows by checkpoint_id with deserialized values."""
        grouped: dict[str, list[tuple[str, str, Any]]] = {}
        for write_row in write_rows:
            checkpoint_id_key = str(write_row.checkpoint_id)
            value = self.serde.loads_typed((write_row.type, write_row.value))
            grouped.setdefault(checkpoint_id_key, []).append(
                (write_row.task_id, write_row.channel, value)
            )
        return grouped

    def _prepare_delete_thread_batch(
        self, thread_id_str: str
    ) -> tuple[Any, BatchStatement]:
        """Create a batch statement that deletes checkpoints and writes for a thread."""
        thread_id = self._convert_thread_id(thread_id_str)

        batch = BatchStatement(batch_type=BatchType.LOGGED)

        delete_checkpoint_query = self._query_delete_thread_checkpoints()
        delete_checkpoints_stmt = self._get_prepared_statement(
            delete_checkpoint_query,
            consistency=self.write_consistency,
        )
        batch.add(delete_checkpoints_stmt, (thread_id,))

        delete_writes_query = self._query_delete_thread_writes()
        delete_writes_stmt = self._get_prepared_statement(
            delete_writes_query,
            consistency=self.write_consistency,
        )
        batch.add(delete_writes_stmt, (thread_id,))

        if self.write_consistency:
            batch.consistency_level = self.write_consistency

        return thread_id, batch

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
            filter: Additional filtering criteria for metadata.
                   Supports dot notation for nested fields (e.g., {"user.name": "alice"}).
                   Literal dots in key names must be escaped with backslash (e.g., "file\\.txt").
                   Filters are applied server-side using SAI indexes on metadata columns.
            before: List checkpoints created before this configuration
            limit: Maximum number of checkpoints to return

        Yields:
            CheckpointTuple objects matching the criteria

        Examples:
            >>> # Filter by top-level field
            >>> list(config, filter={"source": "loop"})

            >>> # Filter by nested field using dot notation
            >>> list(config, filter={"user.name": "alice", "user.age": 30})

            >>> # Filter with multiple conditions (AND logic)
            >>> list(config, filter={"source": "loop", "step": 5})

            >>> # Filter on keys with literal dots (use backslash escape)
            >>> # For metadata {"file.txt": "content"}, use:
            >>> list(config, filter={"file\\.txt": "content"})

            >>> # For nested metadata {"config": {"file.txt": "content"}}, use:
            >>> list(config, filter={"config.file\\.txt": "content"})
        """
        if not config:
            return iter(())

        thread_id_str = config["configurable"]["thread_id"]
        thread_id = self._convert_thread_id(thread_id_str)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        # Split filters into indexed (server-side) and non-indexed (client-side)
        indexed_filters, non_indexed_filters = self._split_and_validate_filters(filter)

        if limit == 0:
            return iter(())

        server_side_limit = limit if not non_indexed_filters else None

        stmt, params = self._build_list_statement(
            thread_id, checkpoint_ns, indexed_filters, before, server_side_limit
        )
        result = self._execute_prepared(
            stmt,
            params,
            consistency=self.read_consistency,
        )

        # Deserialize checkpoints and apply client-side filters
        checkpoints_to_return = self._deserialize_and_filter_checkpoints(
            result, non_indexed_filters, limit
        )

        if not checkpoints_to_return:
            return iter(())

        # Fetch writes for all checkpoints
        writes_by_checkpoint = self._fetch_writes_for_checkpoints(
            thread_id, checkpoint_ns, checkpoints_to_return
        )

        # Build and return checkpoint tuples iterator
        result = self._build_checkpoint_tuples_from_writes(
            checkpoints_to_return,
            writes_by_checkpoint,
            thread_id_str,
            checkpoint_ns,
        )
        return iter(result or [])

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
        (
            thread_id_str,
            checkpoint_ns,
            checkpoint_id_str,
            query,
            params,
        ) = self._prepare_checkpoint_insert(config, checkpoint, metadata)

        self._execute_prepared(
            query,
            params,
            consistency=self.write_consistency,
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
        params_list = self._prepare_write_rows(config, writes, task_id, task_path)

        query = self._query_insert_write()
        for params in params_list:
            self._execute_prepared(
                query,
                params,
                consistency=self.write_consistency,
            )

    def delete_thread(self, thread_id_str: str) -> None:
        """
        Delete all checkpoints and writes for a thread across all namespaces.

        With the new schema where thread_id is the partition key, this is a simple
        partition delete that removes all data for the thread in both tables.

        Args:
            thread_id_str: The thread ID whose checkpoints should be deleted
        """
        logger.info(f"Deleting thread {thread_id_str}")

        _, batch = self._prepare_delete_thread_batch(thread_id_str)

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

        thread_id_str = config["configurable"]["thread_id"]
        thread_id = self._convert_thread_id(thread_id_str)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id_str = get_checkpoint_id(config)

        stmt, params = self._build_get_checkpoint_query(
            thread_id, checkpoint_ns, checkpoint_id_str
        )
        result = await self._aexecute_prepared(
            stmt,
            params,
            consistency=self.read_consistency,
        )
        row = self._first_row(result)
        if not row:
            return None

        checkpoint, metadata = self._deserialize_checkpoint_row(row)

        checkpoint_id_for_writes = (
            self._convert_checkpoint_id(str(row.checkpoint_id))
            if row.checkpoint_id
            else None
        )

        writes_result = await self._aexecute_prepared(
            self._query_get_writes(),
            (thread_id, checkpoint_ns, checkpoint_id_for_writes),
            consistency=self.read_consistency,
        )

        pending_writes = self._deserialize_writes(writes_result)

        return self._build_checkpoint_tuple(
            row,
            checkpoint,
            metadata,
            thread_id_str,
            checkpoint_ns,
            pending_writes,
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

        if not config:
            return

        thread_id_str = config["configurable"]["thread_id"]
        thread_id = self._convert_thread_id(thread_id_str)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        # Split filters into indexed (server-side) and non-indexed (client-side)
        indexed_filters, non_indexed_filters = self._split_and_validate_filters(filter)

        if limit == 0:
            return

        server_side_limit = limit if not non_indexed_filters else None

        stmt, params = self._build_list_statement(
            thread_id, checkpoint_ns, indexed_filters, before, server_side_limit
        )
        result = await self._aexecute_prepared(
            stmt,
            params,
            consistency=self.read_consistency,
        )

        # Deserialize checkpoints and apply client-side filters
        checkpoints_to_return = self._deserialize_and_filter_checkpoints(
            result, non_indexed_filters, limit
        )

        if not checkpoints_to_return:
            return

        # Fetch writes for all checkpoints
        writes_by_checkpoint = await self._fetch_writes_for_checkpoints_async(
            thread_id, checkpoint_ns, checkpoints_to_return
        )

        # Build and yield checkpoint tuples
        for checkpoint_tuple in self._build_checkpoint_tuples_from_writes(
            checkpoints_to_return,
            writes_by_checkpoint,
            thread_id_str,
            checkpoint_ns,
        ):
            yield checkpoint_tuple

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

        (
            thread_id_str,
            checkpoint_ns,
            checkpoint_id_str,
            query,
            params,
        ) = self._prepare_checkpoint_insert(config, checkpoint, metadata)

        await self._aexecute_prepared(
            query,
            params,
            consistency=self.write_consistency,
        )

        return {
            "configurable": {
                "thread_id": thread_id_str,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id_str,
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

        params_list = self._prepare_write_rows(config, writes, task_id, task_path)

        query = self._query_insert_write()
        for params in params_list:
            await self._aexecute_prepared(
                query,
                params,
                consistency=self.write_consistency,
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

        logger.info(f"Deleting thread {thread_id_str}")

        _, batch = self._prepare_delete_thread_batch(thread_id_str)

        await self.session.aexecute(batch)

        logger.info(f"Successfully deleted thread {thread_id_str}")
