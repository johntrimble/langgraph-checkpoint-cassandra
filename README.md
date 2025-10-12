# LangGraph Checkpoint Cassandra

Implementation of LangGraph CheckpointSaver that uses Apache Cassandra.

## Installation

```bash
pip install langgraph-checkpoint-cassandra
```

## Usage

### Important Note
When using the Cassandra checkpointer for the first time, call `.setup()` to create the required tables.

### Sync Example

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

### Async Example

```python
from cassandra.cluster import Cluster
from langgraph_checkpoint_cassandra import CassandraSaver

# Connect to Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect()

# Create checkpointer
checkpointer = CassandraSaver(session, keyspace='my_checkpoints')
checkpointer.setup()

# Use with async LangGraph
config = {"configurable": {"thread_id": "user-456"}}
result = await app.ainvoke({"messages": ["Hello async!"]}, config=config)

cluster.shutdown()
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
