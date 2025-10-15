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
ruff check langgraph_checkpoint_cassandra tests examples
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

## Adding New Features

### Adding a New Method

1. Add method to `CassandraSaver` class in `cassandra_saver.py`
2. Add corresponding CQL prepared statement in `_prepare_statements()`
3. Implement both sync and async versions
4. Add tests in `test_cassandra_saver.py`
5. Update documentation

Example:

```python
# In cassandra_saver.py
def _prepare_statements(self) -> None:
    # ... existing statements ...
    self.stmt_new_operation = self.session.prepare(f"""
        SELECT ... FROM {self.keyspace}.table_name WHERE ...
    """)

def new_operation(self, arg1, arg2):
    """New operation description."""
    result = self.session.execute(self.stmt_new_operation, (arg1, arg2))
    return process_result(result)

async def anew_operation(self, arg1, arg2):
    """Async version of new_operation."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, self.new_operation, arg1, arg2)
```

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

### Common Issues

**Issue: Cassandra not ready**
```
cassandra.cluster.NoHostAvailable: ('Unable to connect to any servers', ...)
```

Solution: Wait for Cassandra to fully start (30-60 seconds):
```bash
docker-compose logs cassandra
# Wait for "Created default superuser role 'cassandra'"
```

**Issue: Schema already exists**
```
cassandra.AlreadyExists: Keyspace 'langgraph_checkpoints' already exists
```

Solution: Schema creation is idempotent via `.setup()` - the method is safe to call multiple times.

**Issue: Connection timeout in tests**
```
cassandra.OperationTimedOut: errors={...}, last_host=...
```

Solution: Increase timeout or wait for schema agreement:
```python
session.default_timeout = 30
```

## Code Style

We follow these conventions:

- **PEP 8** for Python code style
- **Black** for code formatting (line length: 100)
- **isort** for import sorting
- **Type hints** for all public APIs
- **Docstrings** for all public functions/classes (Google style)

Example:

```python
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
    # Implementation...
```

## Documentation

### Updating Documentation

Documentation is in multiple places:

- `README.md` - User-facing documentation
- `DEVELOPMENT.md` (this file) - Developer documentation
- `docs/*.md` - Detailed design docs
- Docstrings - Inline API documentation

When adding features:
1. Update relevant `.md` files
2. Add/update docstrings
3. Add examples if appropriate

### Building Documentation Locally

(TODO: Add docs building once we have sphinx/mkdocs setup)

## Release Process

(TODO: Document release process)

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Create release tag
4. Build package: `python -m build`
5. Upload to PyPI: `python -m twine upload dist/*`

## Getting Help

- **Issues**: [GitHub Issues](https://github.com/yourusername/langgraph-checkpoint-cassandra/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/langgraph-checkpoint-cassandra/discussions)
- **LangGraph Docs**: [LangGraph Documentation](https://langchain-ai.github.io/langgraph/)
- **Cassandra Docs**: [Apache Cassandra Documentation](https://cassandra.apache.org/doc/)

## Contributing Guidelines

### Pull Request Process

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Ensure all tests pass
5. Update documentation
6. Submit a PR with clear description

### PR Checklist

- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] Code formatted (black, isort)
- [ ] Type hints added
- [ ] Docstrings added/updated
- [ ] Examples work correctly
- [ ] No breaking changes (or clearly documented)

### Review Process

1. Automated tests run on PR
2. Code review by maintainers
3. Address feedback
4. Approval and merge

## License

This project is licensed under the MIT License - see the LICENSE file for details.
