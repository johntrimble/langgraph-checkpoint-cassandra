# LangGraph Checkpoint Cassandra

Implementation of LangGraph CheckpointSaver that uses Apache Cassandra.

## Installation

```bash
pip install langgraph-checkpoint-cassandra
```

For async operations, also install the async Cassandra driver:
```bash
pip install cassandra-asyncio-driver
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

For high-concurrency scenarios (web servers, concurrent operations), use `CassandraSaver` with an async session:

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

**Note:** Async operations require the [cassandra-asyncio-driver](https://github.com/U-238/cassandra-asyncio-driver) package. Install with:
```bash
pip install cassandra-asyncio-driver
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
    PRIMARY KEY ((thread_id, checkpoint_ns), checkpoint_id)
) WITH CLUSTERING ORDER BY (checkpoint_id DESC);
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
    PRIMARY KEY ((thread_id, checkpoint_ns, checkpoint_id), task_id, idx)
);
```

## Advanced Features

### Queryable Metadata (Server-Side Filtering)

For efficient filtering on specific metadata fields, you can designate fields as "queryable" when creating the checkpointer. This creates dedicated columns for fast filtering, with optional SAI (Storage Attached Index) indexes for maximum performance.

```python
# Configure queryable metadata fields
checkpointer = CassandraSaver(
    session,
    keyspace='my_checkpoints',
    queryable_metadata={
        "user_id": str,               # Text field for user IDs
        "step": int,                  # Integer field for step numbers
        "source": str,                # Text field for source tracking
        "tags": list[str],            # List of string tags
        "attributes": dict[str, str], # Key-value attributes
    },
    indexed_metadata=["user_id", "source"]  # Only index these fields (optional)
)
checkpointer.setup()

# Now you can filter efficiently on these fields
config = {"configurable": {"thread_id": "my-thread"}}

# Filter by user_id (server-side with SAI index, very fast)
user_checkpoints = list(checkpointer.list(
    config,
    filter={"user_id": "user-123"}
))

# Filter by multiple fields (server-side)
specific_checkpoints = list(checkpointer.list(
    config,
    filter={"user_id": "user-123", "source": "input"}
))

# Filter on list field with CONTAINS (checks if value is in list)
tagged_checkpoints = list(checkpointer.list(
    config,
    filter={"tags": "python"}  # Matches checkpoints where "python" is in tags list
))

# Filter on dict field (checks if key-value pair exists)
prod_checkpoints = list(checkpointer.list(
    config,
    filter={"attributes": {"env": "prod"}}  # Matches where attributes["env"] = "prod"
))

# Filter on dict field by value only (checks if value exists in any key)
us_checkpoints = list(checkpointer.list(
    config,
    filter={"attributes": "us-east"}  # Matches where any value = "us-east"
))

# Mix queryable and non-queryable filters
# Queryable fields use server-side filtering (fast)
# Non-queryable fields use client-side filtering (slower)
mixed = list(checkpointer.list(
    config,
    filter={
        "user_id": "user-123",        # Server-side (queryable with index)
        "step": 5,                    # Server-side (queryable without index, uses ALLOW FILTERING)
        "custom_field": "value"       # Client-side (not queryable)
    }
))
```

**Supported types for queryable metadata:**
- `str` - Text values
- `int` - Integer values
- `float` - Floating point values
- `bool` - Boolean values
- `dict[K, V]` - Dictionary/map values (supports key-value and value-only filtering)
  - Examples: `dict[str, int]`, `dict[str, str]`, `dict[str, bool]`
- `list[T]` - List values (supports CONTAINS operator)
  - Examples: `list[int]`, `list[str]`, `list[float]`
- `set[T]` - Set values (supports CONTAINS operator)
  - Examples: `set[str]`, `set[int]`, `set[float]`


**Index management with `indexed_metadata`:**

By default, **all queryable metadata fields get SAI indexes** for maximum query performance:
```python
checkpointer = CassandraSaver(
    session,
    queryable_metadata={
        "user_id": str,
        "source": str,
        "step": int,
    }
    # All three fields will be indexed (default behavior)
)
```

To reduce storage overhead, use `indexed_metadata` to index only frequently-queried fields:
```python
checkpointer = CassandraSaver(
    session,
    queryable_metadata={
        "user_id": str,      # Will be indexed (in indexed_metadata)
        "source": str,       # Will be indexed (in indexed_metadata)
        "step": int,         # NOT indexed (queryable but slower)
        "debug_info": str,   # NOT indexed (queryable but slower)
    },
    indexed_metadata=["user_id", "source"]  # Only index these two
)
```

- **Indexed fields**: Fast queries using SAI index
- **Non-indexed queryable fields**: Still filterable server-side, but uses `ALLOW FILTERING` (slower)
- This allows many fields to be queryable while only indexing the most important ones


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
