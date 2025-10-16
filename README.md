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

**When to use async:**
- Web servers handling many concurrent requests (FastAPI, aiohttp, etc.)
- Applications with 100s-1000s of concurrent checkpoint operations
- Scenarios requiring maximum I/O throughput

**When to use sync:**
- Single-threaded applications
- Sequential checkpoint operations
- Simpler code without async/await complexity
- Most typical LangGraph use cases

**Note:** Async operations require the `cassandra-asyncio-driver` package. Install with:
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
    checkpoint_id TEXT,
    parent_checkpoint_id TEXT,
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
    checkpoint_id TEXT,
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
        "user_id": str,      # Text field for user IDs
        "step": int,         # Integer field for step numbers
        "source": str,       # Text field for source tracking
        "tags": list,        # List of tags
        "attributes": dict,  # Key-value attributes
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
- `dict` or `dict[str, T]` - Dictionary/map values (supports key-value and value-only filtering)
  - Examples: `dict`, `dict[str, int]`, `dict[str, str]`
- `list` or `list[T]` - List values (supports CONTAINS operator)
  - Examples: `list`, `list[int]`, `list[str]`, `list[float]`
- `set` or `set[T]` - Set values (supports CONTAINS operator)
  - Examples: `set`, `set[str]`, `set[int]`

**Performance optimization with `indexed_metadata`:**
- By default, all `queryable_metadata` fields get SAI indexes for maximum performance
- Use `indexed_metadata` to specify a subset of fields to index (reduces storage overhead)
- Non-indexed queryable fields still work but use `ALLOW FILTERING` (slightly slower)
- This allows you to make many fields queryable while only indexing the most frequently filtered ones

**Benefits:**
- **Much faster filtering** when you have many checkpoints per thread
- **Reduced network traffic** - only matching checkpoints are returned
- **Scalable** - performance doesn't degrade with large checkpoint counts
- **Flexible** - balance between query performance and storage overhead

**When to use:**
- Multi-tenant applications (filter by `user_id` or `tenant_id`)
- Debugging (filter by `source` to find input vs loop checkpoints)
- Step-based filtering (filter by `step` number)
- Tag-based organization (filter by tags in a list)
- Environment/configuration filtering (filter by key-value attributes)

**Important notes:**
- Queryable fields must be specified before calling `setup()`
- Adding new queryable fields requires calling `setup()` again
- Non-queryable metadata fields can still be filtered, but client-side (slower)
- Collection types (list, set, dict) support CONTAINS operator for efficient membership testing

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
# Uses ConsistencyLevel.LOCAL_QUORUM for both reads and writes

# Development: Fast, eventual consistency
checkpointer = CassandraSaver(
    session,
    keyspace='my_checkpoints',
    read_consistency=ConsistencyLevel.ONE,         # Fastest reads
    write_consistency=ConsistencyLevel.ONE         # Fastest writes
)

# Use session default (set read_consistency=None, write_consistency=None)
session.default_consistency_level = ConsistencyLevel.ALL
checkpointer = CassandraSaver(
    session,
    keyspace='my_checkpoints',
    read_consistency=None,   # Use session default
    write_consistency=None   # Use session default
)
```

**Available consistency levels:**
- `ConsistencyLevel.ONE`, `TWO`, `THREE` - Specific number of replicas
- `ConsistencyLevel.QUORUM` - Majority of replicas (most common)
- `ConsistencyLevel.ALL` - All replicas (strongest consistency, slowest)
- `ConsistencyLevel.LOCAL_QUORUM` - Majority in local datacenter (default, recommended)
- `ConsistencyLevel.EACH_QUORUM` - Majority in each datacenter
- `ConsistencyLevel.LOCAL_ONE` - One replica in local datacenter
- `ConsistencyLevel.ANY` - At least one node (write only, weakest consistency)

### Thread ID Types

Choose the data type for thread identifiers:

```python
# Use TEXT (default, most flexible)
checkpointer = CassandraSaver(session, thread_id_type="text")

# Use UUID (enforces UUID format)
checkpointer = CassandraSaver(session, thread_id_type="uuid")
```

### Managing Conversations

```python
# List checkpoints for a thread
config = {"configurable": {"thread_id": "user-123"}}
checkpoints = list(checkpointer.list(config, limit=10))

for checkpoint_tuple in checkpoints:
    print(f"Checkpoint ID: {checkpoint_tuple.checkpoint['id']}")
    print(f"State: {checkpoint_tuple.checkpoint['channel_values']}")

# Delete a conversation thread
checkpointer.delete_thread("user-123")
```

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for information on setting up a development environment, running tests, and contributing.

## License

MIT
