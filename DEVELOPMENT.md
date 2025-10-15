# Development Guide

This guide covers how to set up a development environment for contributing to langgraph-checkpoint-cassandra.

## Prerequisites

- Docker and Docker Compose
- Git
- Python 3.12+

## Development Environment Setup

### 1. Bootstrap the Environment

Clone the repository and run the bootstrap script:

```bash
git clone https://github.com/johntrimble/langgraph-checkpoint-cassandra.git
cd langgraph-checkpoint-cassandra
./script/bootstrap
```

The bootstrap script will:
- Setup a `.env` file for development

### 2. Development Container (VSCode)

If using VSCode, the project includes a devcontainer configuration:

1. Open the project in VSCode
2. When prompted, click "Reopen in Container"
3. VSCode will build and start the development container
4. All dependencies will be pre-installed

Alternatively, manually reopen:
- Press `Cmd/Ctrl+Shift+P`
- Select "Dev Containers: Reopen in Container"


## Connecting to Cassandra

### From Inside Docker Network

The Cassandra cluster is available at `cassandra:9042` within the Docker network:

```python
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra'])
session = cluster.connect()
```

### From Host Machine

Connect to Cassandra at `localhost:9042`:

```python
from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])
session = cluster.connect()
```

### Verify Connection

```python
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra'])  # Use 'localhost' if connecting from outside Docker
session = cluster.connect()

# Test the connection
rows = session.execute("SELECT release_version FROM system.local")
version = rows[0].release_version
print(f"Connected to Cassandra version: {version}")
```

### CQL Shell Access

Access the Cassandra CQL shell:

```bash
# From inside container
docker compose exec cassandra cqlsh

# From host (if cqlsh installed)
cqlsh localhost 9042
```

## Running Tests

### Run All Tests

```bash
pytest
```

### Run Specific Test File

```bash
pytest tests/test_cassandra_saver.py
```

### Run with Coverage

```bash
pytest --cov=langgraph_checkpoint_cassandra --cov-report=html
```

### Run with Verbose Output

```bash
pytest -v
```

### Test Categories

- `tests/test_cassandra_saver.py` - Core CassandraSaver functionality tests
- `tests/test_migrations.py` - Migration system tests
- `tests/test_ttl.py` - TTL (Time To Live) functionality tests
- `tests/test_thread_id_types.py` - Thread ID type variations tests
- `tests/test_timeuuid.py` - TIMEUUID functionality tests

## Running Examples

### Interactive Chat Example

```bash
# Run with default thread
python examples/basic_example.py

# Run with specific thread
python examples/basic_example.py --thread-id my-session

# List all threads
python examples/basic_example.py --list-threads

# List checkpoints for a thread
python examples/basic_example.py --list-checkpoints --thread-id my-session
```

The example demonstrates:
- Creating a checkpointer with `.setup()`
- Interactive chat interface
- Persisting conversation history across sessions
- Listing threads and checkpoints

## Development Workflow

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Changes

Edit code in `langgraph_checkpoint_cassandra/` or add tests in `tests/`.

### 3. Run Tests

```bash
pytest
```

### 4. Format Code

```bash
ruff check --fix
ruff format
```

### 5. Commit Changes

```bash
git add .
git commit -m "Description of changes"
```

### 6. Push and Create PR

```bash
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub.


### Adding Schema Changes

Schema changes should be made through the migration system:

1. Add a new migration to `DEFAULT_MIGRATIONS` in `migrations.py`
2. Update migration version number
3. Test the migration thoroughly
4. Update documentation in relevant docs files

Note: The migration system is internal - users just call `.setup()` which applies all migrations automatically.


## Debugging

### Enable Debug Logging

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('cassandra')
logger.setLevel(logging.DEBUG)
```


### Inspect Cassandra Data

```bash
# Connect to CQL shell
docker-compose exec cassandra cqlsh

# List keyspaces
cqlsh> DESCRIBE KEYSPACES;

# Use keyspace
cqlsh> USE test_cassandra_saver;

# List tables
cqlsh> DESCRIBE TABLES;

# Query data
cqlsh> SELECT * FROM checkpoints;
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
