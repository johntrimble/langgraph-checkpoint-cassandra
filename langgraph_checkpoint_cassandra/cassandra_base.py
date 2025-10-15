"""
Base class for Cassandra checkpoint savers.

This module contains shared logic between sync (CassandraSaver) and async (AsyncCassandraSaver)
implementations.
"""

import logging
from typing import Any, Literal, get_args, get_origin
from uuid import UUID

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

logger = logging.getLogger(__name__)

# Constants
DEFAULT_KEYSPACE = "langgraph_checkpoints"


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


class BaseCassandraSaver(BaseCheckpointSaver):
    """
    Base class for Cassandra checkpoint savers.

    Contains shared configuration, helper methods, and schema templates
    that are common between sync and async implementations.
    """

    def __init__(
        self,
        keyspace: str = DEFAULT_KEYSPACE,
        *,
        serde: Any | None = None,
        thread_id_type: Literal["text", "uuid"] = "text",
        checkpoint_id_type: Literal["text", "uuid"] = "text",
        ttl_seconds: int | None = None,
        queryable_metadata: dict[str, type] | None = None,
        indexed_metadata: list[str] | None = None,
    ) -> None:
        """
        Initialize base Cassandra checkpoint saver.

        Args:
            keyspace: Cassandra keyspace name
            serde: Optional serializer (defaults to JsonPlusSerializer)
            thread_id_type: Type for thread_id column ("text", "uuid")
            checkpoint_id_type: Type for checkpoint_id column ("text", "uuid")
            ttl_seconds: Optional TTL in seconds for checkpoints
            queryable_metadata: Optional dict mapping metadata field names to types
            indexed_metadata: Optional list of metadata field names to create SAI indexes for
        """
        super().__init__(serde=serde or JsonPlusSerializer())

        self.keyspace = keyspace
        self.thread_id_type = thread_id_type
        self.checkpoint_id_type = checkpoint_id_type
        self.ttl_seconds = ttl_seconds
        self.queryable_metadata = queryable_metadata or {}

        # Determine which fields should have SAI indexes
        if indexed_metadata is None:
            # Default: index all queryable metadata fields
            self.indexed_metadata = set(self.queryable_metadata.keys())
        else:
            # Only index specified fields
            self.indexed_metadata = set(indexed_metadata)

            # Validate that indexed_metadata is subset of queryable_metadata
            extra_fields = self.indexed_metadata - set(self.queryable_metadata.keys())
            if extra_fields:
                raise ValueError(
                    f"indexed_metadata contains fields not in queryable_metadata: {extra_fields}"
                )

    @staticmethod
    def _to_uuid(checkpoint_id: str | None) -> UUID | None:
        """
        Convert checkpoint_id string to UUID if it looks like a UUID.

        Args:
            checkpoint_id: Checkpoint ID string (may be UUID or text)

        Returns:
            UUID object if input is UUID format, None otherwise
        """
        if checkpoint_id is None:
            return None

        # Check if it's 32 hex characters (UUID without hyphens)
        if len(checkpoint_id) == 32 and all(
            c in "0123456789abcdefABCDEF" for c in checkpoint_id
        ):
            # Insert hyphens to make it a standard UUID string
            uuid_str = f"{checkpoint_id[:8]}-{checkpoint_id[8:12]}-{checkpoint_id[12:16]}-{checkpoint_id[16:20]}-{checkpoint_id[20:]}"
            try:
                return UUID(uuid_str)
            except ValueError:
                return None

        # Try parsing as-is (may have hyphens already)
        try:
            return UUID(checkpoint_id)
        except ValueError:
            return None

    def _convert_checkpoint_id(self, checkpoint_id: str | None) -> str | UUID | None:
        """
        Convert checkpoint_id to appropriate type for Cassandra column.

        Args:
            checkpoint_id: Checkpoint ID string

        Returns:
            UUID if checkpoint_id_type is 'uuid', str otherwise
        """
        if checkpoint_id is None:
            return None

        if self.checkpoint_id_type == "uuid":
            uuid_val = self._to_uuid(checkpoint_id)
            if uuid_val is None:
                raise ValueError(
                    f"checkpoint_id_type is 'uuid' but checkpoint_id '{checkpoint_id}' "
                    f"cannot be converted to UUID"
                )
            return uuid_val
        else:
            return checkpoint_id

    def _convert_thread_id(self, thread_id: str) -> str | UUID:
        """
        Convert thread_id to appropriate type for Cassandra column.

        Args:
            thread_id: Thread ID string

        Returns:
            UUID if thread_id_type is 'uuid', str otherwise
        """
        if self.thread_id_type == "uuid":
            uuid_val = self._to_uuid(thread_id)
            if uuid_val is None:
                raise ValueError(
                    f"thread_id_type is 'uuid' but thread_id '{thread_id}' "
                    f"cannot be converted to UUID"
                )
            return uuid_val
        else:
            return thread_id

    def _get_thread_id_cql_type(self) -> str:
        """Get CQL type for thread_id column based on configuration."""
        return "UUID" if self.thread_id_type == "uuid" else "TEXT"

    def _get_checkpoint_id_cql_type(self) -> str:
        """Get CQL type for checkpoint_id column based on configuration."""
        return "UUID" if self.checkpoint_id_type == "uuid" else "TEXT"

    def _build_queryable_metadata_columns(self) -> list[tuple[str, str]]:
        """
        Build list of (column_name, cql_type) tuples for queryable metadata.

        Returns:
            List of tuples like [("metadata__user_id", "TEXT"), ("metadata__step", "BIGINT")]
        """
        columns = []
        for field_name, field_type in self.queryable_metadata.items():
            column_name = f"metadata__{field_name}"
            cql_type = _python_type_to_cql_type(field_type)
            columns.append((column_name, cql_type))
        return columns

    def _should_add_allow_filtering(self, filter_dict: dict[str, Any]) -> bool:
        """
        Determine if ALLOW FILTERING is needed for a given filter.

        Args:
            filter_dict: Dictionary of metadata filters

        Returns:
            True if ALLOW FILTERING is needed
        """
        for field_name, field_value in filter_dict.items():
            # Skip if not a queryable field (client-side filtering)
            if field_name not in self.queryable_metadata:
                continue

            # Need ALLOW FILTERING if filtering on non-indexed field
            if field_name not in self.indexed_metadata:
                return True

            # Need ALLOW FILTERING for map key-value filtering
            field_type = self.queryable_metadata[field_name]
            if field_type is dict or (
                hasattr(field_type, "__origin__") and field_type.__origin__ is dict
            ):
                if isinstance(field_value, dict):
                    # Filtering map[key]=value requires ALLOW FILTERING
                    return True

        return False
