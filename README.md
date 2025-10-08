# langgraph-checkpoint-cassandra

A Cassandra implementation of the LangGraph [CheckpointSaver](https://github.com/langchain-ai/langgraph/blob/7f78a011fd6e1a18fd80bd1648ffdc87c324fd39/libs/checkpoint/langgraph/checkpoint/base/__init__.py#L112-L371) interface.

## Features

* Continue conversations across restarts
* Store and retrieve conversation history
* Synchronous and asynchronous support

## Development

### Prerequisites

- Docker and Docker Compose
- Git

### Setup

1. Bootstrap the environment:

```bash
./script/bootstrap
```

2. Open the project in a devcontainer (if using VSCode, it will prompt you to reopen in container).

### Connecting to Cassandra

The Cassandra cluster is available at `cassandra:9042` within the Docker network. From the host machine, you can connect to it at `localhost:9042`.

Example Python code for connecting:

```python
from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['cassandra'])  # Use 'localhost' if connecting from outside Docker
session = cluster.connect()

# Test the connection
rows = session.execute("SELECT release_version FROM system.local")
version = rows[0].release_version
print(f"Connected to Cassandra version: {version}")
```
