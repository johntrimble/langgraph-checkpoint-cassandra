"""LangGraph Checkpoint Saver for Apache Cassandra."""

from langgraph_checkpoint_cassandra.cassandra_saver import CassandraSaver
from langgraph_checkpoint_cassandra.migrations import (
    Migration,
    MigrationManager,
)
from langgraph_checkpoint_cassandra.schema import drop_schema

__version__ = "0.1.0"

__all__ = [
    "CassandraSaver",
    "drop_schema",
    "MigrationManager",
    "Migration",
]

# Try to import AsyncCassandraSaver if cassandra-asyncio-driver is installed
try:
    from langgraph_checkpoint_cassandra.cassandra_saver_async import AsyncCassandraSaver
    __all__.append("AsyncCassandraSaver")
except ImportError:
    # cassandra-asyncio-driver not installed
    pass
