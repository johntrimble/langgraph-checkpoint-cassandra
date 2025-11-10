# LangGraph Checkpoint Cassandra

Implementation of LangGraph CheckpointSaver that uses Apache Cassandra.

## Installation

```bash
pip install langgraph-checkpoint-cassandra
```

## Usage

### Important Note
When using the Cassandra checkpointer for the first time, call `.setup()` to create the required tables.

### Synchronous Usage

```python
from cassandra.cluster import Cluster
from langgraph_checkpoint_cassandra import CassandraSaver
from langgraph.graph import StateGraph, MessagesState, START, END
from langchain_core.messages import HumanMessage, AIMessage

# Connect to Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect()

# Create checkpointer and setup schema
checkpointer = CassandraSaver(session, keyspace='my_checkpoints')
checkpointer.setup()  # Call this the first time to create tables

# Simple echo function
def echo_bot(state: MessagesState):
    # Get the last message and echo it back
    user_message = state["messages"][-1]
    return {"messages": [AIMessage(content=user_message.content)]}

# Build your graph
graph = StateGraph(MessagesState)
graph.add_node("chat", echo_bot)
graph.add_edge(START, "chat")
graph.add_edge("chat", END)

# Compile with checkpointer
app = graph.compile(checkpointer=checkpointer)

# Use with different threads
config = {"configurable": {"thread_id": "user-123"}}
result = app.invoke({"messages": [HumanMessage(content="Hello!")]}, config=config)

# Cleanup
cluster.shutdown()
```

### Asynchronous Usage

```python
from cassandra_asyncio.cluster import Cluster
from langgraph_checkpoint_cassandra import CassandraSaver
from langgraph.graph import StateGraph, MessagesState, START, END
from langchain_core.messages import HumanMessage, AIMessage

async def main():
    # Connect to Cassandra using async driver
    cluster = Cluster(['localhost'])
    session = cluster.connect()

    # Create checkpointer with async session
    # CassandraSaver automatically detects async support
    checkpointer = CassandraSaver(session, keyspace='my_checkpoints')
    checkpointer.setup()  # Setup is still sync

    # Build your graph
    def echo_bot(state: MessagesState):
        user_message = state["messages"][-1]
        return {"messages": [AIMessage(content=user_message.content)]}

    graph = StateGraph(MessagesState)
    graph.add_node("chat", echo_bot)
    graph.add_edge(START, "chat")
    graph.add_edge("chat", END)

    # Compile with checkpointer
    app = graph.compile(checkpointer=checkpointer)

    # Use async methods with LangGraph
    config = {"configurable": {"thread_id": "user-456"}}
    result = await app.ainvoke({"messages": [HumanMessage(content="Hello async!")]}, config=config)

    # Cleanup
    cluster.shutdown()

# Run async code
import asyncio
asyncio.run(main())
```

## Schema

The checkpointer creates two tables in your Cassandra keyspace:

### `checkpoints` table
Stores checkpoint data with the following schema:
```sql
CREATE TABLE checkpoints (
    thread_id TEXT,
    checkpoint_ns TEXT,
    checkpoint_id UUID,         -- Always UUIDv6
    parent_checkpoint_id UUID,  -- Always UUIDv6
    type TEXT,
    checkpoint BLOB,
    metadata BLOB,
    -- Metadata indexing columns for efficient filtering
    metadata_text map<text, text>,
    metadata_int map<text, bigint>,
    metadata_double map<text, double>,
    metadata_bool map<text, boolean>,
    metadata_null set<text>,
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
) WITH CLUSTERING ORDER BY (checkpoint_ns ASC, checkpoint_id DESC)
```

The table includes SAI (Storage Attached Index) indexes on the metadata collections:
```sql
CREATE CUSTOM INDEX checkpoints_metadata_text_idx ON checkpoints (ENTRIES(metadata_text)) USING 'StorageAttachedIndex';
CREATE CUSTOM INDEX checkpoints_metadata_int_idx ON checkpoints (ENTRIES(metadata_int)) USING 'StorageAttachedIndex';
CREATE CUSTOM INDEX checkpoints_metadata_double_idx ON checkpoints (ENTRIES(metadata_double)) USING 'StorageAttachedIndex';
CREATE CUSTOM INDEX checkpoints_metadata_bool_idx ON checkpoints (ENTRIES(metadata_bool)) USING 'StorageAttachedIndex';
CREATE CUSTOM INDEX checkpoints_metadata_null_idx ON checkpoints (VALUES(metadata_null)) USING 'StorageAttachedIndex';
```

### `checkpoint_writes` table
Stores pending writes for checkpoints:
```sql
CREATE TABLE checkpoint_writes (
    thread_id TEXT,
    checkpoint_ns TEXT,
    checkpoint_id UUID,         -- Always UUIDv6
    task_id TEXT,
    task_path TEXT,
    idx INT,
    channel TEXT,
    type TEXT,
    value BLOB,
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
) WITH CLUSTERING ORDER BY (checkpoint_ns ASC, checkpoint_id DESC, task_id ASC, idx ASC)
```

## Advanced Features

### Metadata Filtering (Server-Side)

All metadata fields are automatically queryable using server-side filtering with SAI (Storage Attached Index) indexes. The checkpointer stores metadata in flattened typed maps (`metadata_text`, `metadata_int`, `metadata_double`, `metadata_bool`, `metadata_null`) with ENTRIES indexes for efficient filtering.

**Key features:**
- **Automatic filtering**: No need to pre-declare queryable fields
- **Nested metadata**: Use dot notation to filter on nested fields
- **Literal dots**: Escape literal dots with backslash (`\.`)
- **Type support**: Text, integers, floats, booleans, and null values

```python
checkpointer = CassandraSaver(
    session,
    keyspace='my_checkpoints'
)
checkpointer.setup()

config = {"configurable": {"thread_id": "my-thread"}}

# Filter by top-level field
user_checkpoints = list(checkpointer.list(
    config,
    filter={"user_id": "user-123"}
))

# Filter by nested field using dot notation
specific_checkpoints = list(checkpointer.list(
    config,
    filter={"user.name": "alice", "user.age": 30}
))

# Filter by multiple fields (AND logic)
filtered = list(checkpointer.list(
    config,
    filter={"source": "loop", "step": 5}
))

# Filter on keys with literal dots (use backslash escape)
# For metadata {"file.txt": "content"}
file_checkpoints = list(checkpointer.list(
    config,
    filter={"file\\.txt": "content"}
))

# For nested metadata {"config": {"file.txt": "content"}}
# Navigation dot is unescaped, literal dot is escaped
config_checkpoints = list(checkpointer.list(
    config,
    filter={"config.file\\.txt": "content"}
))
```

**Supported types for metadata filtering:**
- `str` - Text values (stored in `metadata_text` map)
- `int` - Integer values (stored in `metadata_int` map)
- `float` - Floating point values (stored in `metadata_double` map)
- `bool` - Boolean values (stored in `metadata_bool` map)
- `None` - Null values (stored in `metadata_null` set)

#### Performance Optimization with Include/Exclude Patterns

For large-scale applications, you may want to control which metadata fields are stored in the indexed columns to optimize storage and write performance. Use `metadata_includes` and `metadata_excludes` parameters:

```python
# Only index user-related fields and step counter
checkpointer = CassandraSaver(
    session,
    keyspace='my_checkpoints',
    metadata_includes=["user.*", "step"]  # Only these fields will be indexed
)
checkpointer.setup()

# Alternatively, exclude sensitive fields from indexing
checkpointer = CassandraSaver(
    session,
    keyspace='my_checkpoints',
    metadata_excludes=["*.password", "*.secret", "*.token"]  # These won't be indexed
)
checkpointer.setup()

# Combine both: include user fields but exclude passwords
checkpointer = CassandraSaver(
    session,
    keyspace='my_checkpoints',
    metadata_includes=["user.*", "session.*"],
    metadata_excludes=["*.password", "*.token"]
)
checkpointer.setup()
```

**Pattern matching:**
- Patterns use Unix shell-style wildcards (`fnmatch`)
- `*` matches any sequence of characters: `"user.*"` matches `user.name`, `user.age`, etc.
- `?` matches a single character
- `[seq]` matches any character in seq
- Exact matches: `"step"` matches only the field `step`

**Priority rules:**
1. If `metadata_includes` is specified, only fields matching at least one pattern are indexed
2. Then, `metadata_excludes` removes matching fields (even if they matched an include pattern)
3. If both are `None` (default), all fields are indexed

**Important - Server-Side vs Client-Side Filtering:**

Fields are automatically split into two categories when filtering:
- **Indexed fields** (server-side): Filtered efficiently in Cassandra using SAI indexes
  - All fields by default, or only those matching `metadata_includes` patterns
  - Excluded if they match `metadata_excludes` patterns
  - Very fast, scales to large datasets

- **Non-indexed fields** (client-side): Filtered in Python after fetching from database
  - Fields excluded by `metadata_excludes` patterns
  - Fields not matching `metadata_includes` patterns
  - Complex types (list, dict, set) - always client-side
  - Still works correctly, but less efficient for large result sets

**Performance implications:**
- Server-side filters are applied first, minimizing data transfer
- Client-side filters are applied to results after fetching
- Use `metadata_includes`/`metadata_excludes` to control which fields are indexed
- This optimizes storage overhead and write performance for high-cardinality metadata


### TTL (Time To Live)

Automatically expire old checkpoints:

```python
# Checkpoints will be automatically deleted after 30 days
checkpointer = CassandraSaver(
    session,
    keyspace='my_checkpoints',
    ttl_seconds=2592000  # 30 days
)
checkpointer.setup()
```

### Consistency Levels

Configure read and write consistency for your use case:

```python
from cassandra.query import ConsistencyLevel

# Production: Strong consistency
checkpointer = CassandraSaver(
    session,
    keyspace='my_checkpoints',
    read_consistency=ConsistencyLevel.QUORUM,      # Majority of replicas for reads
    write_consistency=ConsistencyLevel.QUORUM      # Majority of replicas for writes
)

# Default: Balanced consistency (LOCAL_QUORUM)
checkpointer = CassandraSaver(session, keyspace='my_checkpoints')

# Use session default (set read_consistency=None, write_consistency=None)
session.default_consistency_level = ConsistencyLevel.ALL
checkpointer = CassandraSaver(
    session,
    keyspace='my_checkpoints',
    read_consistency=None,   # Use session default
    write_consistency=None   # Use session default
)
```

### Thread ID and Checkpoint ID Types

Choose the data type for thread identifiers:

```python
# Use TEXT (default, most flexible)
checkpointer = CassandraSaver(session, thread_id_type="text")

# Use UUID (enforces UUID format)
checkpointer = CassandraSaver(session, thread_id_type="uuid")
```

Choose the data type for checkpoint identifiers:

```python
# Use UUID (default, more efficient storage and queries)
checkpointer = CassandraSaver(session, checkpoint_id_type="uuid")

# Use TEXT (stores UUIDv6 as text, useful for compatibility)
checkpointer = CassandraSaver(session, checkpoint_id_type="text")
```

**Note:** Checkpoint IDs are always generated as UUIDv6. The `checkpoint_id_type` parameter only affects the storage format in Cassandra (native UUID vs TEXT column).

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for information on setting up a development environment, running tests, and contributing.

## License

MIT
