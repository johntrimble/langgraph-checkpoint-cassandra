"""
Schema initialization and management for Cassandra checkpoint saver.

This module provides functions to manage the Cassandra schema
for the checkpoint saver.
"""

import logging

from cassandra.cluster import Session

logger = logging.getLogger(__name__)

DEFAULT_KEYSPACE = "langgraph_checkpoints"
DEFAULT_REPLICATION_FACTOR = 1


def drop_schema(session: Session, keyspace: str = DEFAULT_KEYSPACE) -> None:
    """
    Drop the entire keyspace (use with caution!).

    Args:
        session: Cassandra session
        keyspace: Keyspace name to drop
    """
    logger.warning(f"Dropping keyspace '{keyspace}'...")
    session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
    logger.info(f"✓ Dropped keyspace '{keyspace}'")


def get_schema_version(
    session: Session, keyspace: str = DEFAULT_KEYSPACE
) -> int | None:
    """
    Get the current schema version.

    Args:
        session: Cassandra session
        keyspace: Keyspace name

    Returns:
        Schema version number, or None if not found
    """
    try:
        result = session.execute(f"""
            SELECT MAX(version) as version FROM {keyspace}.checkpoint_migrations
        """)
        row = result.one()
        return row.version if row else None
    except Exception as e:
        logger.debug(f"Could not get schema version: {e}")
        return None
