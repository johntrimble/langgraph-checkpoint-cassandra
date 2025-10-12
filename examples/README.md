# Examples

This directory contains examples demonstrating the CassandraSaver checkpoint implementation for LangGraph.

## Basic Example with ELIZA

The `basic_example.py` script showcases a complete interactive chatbot using the classic ELIZA algorithm (1966) with persistent conversation state stored in Cassandra.

### Features

- **ELIZA Chatbot**: Pattern-matching psychotherapist simulation
- **State Persistence**: All conversations automatically saved to Cassandra
- **Multiple Modes**: Demo, interactive chat, thread management
- **Checkpoint Resume**: Continue from any point in conversation history

### Running the Examples

#### Demo Mode (Default)

Run predefined conversations to see how state persistence works:

```bash
uv run python examples/basic_example.py
```

This will:
- Create sample conversations with multiple threads
- Demonstrate thread switching and resumption
- Show checkpoint history

#### Interactive Chat Mode

Start a new or resume an existing conversation:

```bash
uv run python examples/basic_example.py --interactive --thread-id my-session
```

**In-chat commands:**
- `/quit` or `/exit` - Exit the chat (conversation is auto-saved)
- `/history` - Show full conversation history
- `/checkpoints` - List all checkpoints for current thread

#### Resume from Checkpoint

Continue from a specific point in conversation history:

```bash
# First, list checkpoints to find the ID you want
uv run python examples/basic_example.py --list-checkpoints --thread-id my-session

# Then resume from that checkpoint
uv run python examples/basic_example.py --interactive --thread-id my-session --checkpoint-id <checkpoint-id>
```

#### List All Threads

See all conversation threads in the system:

```bash
uv run python examples/basic_example.py --list-threads
```

#### List Checkpoints

View all checkpoints for a specific thread:

```bash
uv run python examples/basic_example.py --list-checkpoints --thread-id my-session
```

### Command-Line Options

```
usage: basic_example.py [-h] [--interactive] [--thread-id THREAD_ID]
                        [--checkpoint-id CHECKPOINT_ID] [--list-threads]
                        [--list-checkpoints] [--keyspace KEYSPACE]

options:
  -h, --help            Show help message
  --interactive, -i     Start interactive chat session
  --thread-id, -t       Thread ID for the conversation
  --checkpoint-id, -c   Checkpoint ID to resume from
  --list-threads        List all conversation threads
  --list-checkpoints    List checkpoints for a thread
  --keyspace, -k        Cassandra keyspace (default: example_checkpoints)
```

### Example Session

```bash
# Start a new conversation
$ uv run python examples/basic_example.py -i -t therapy-session-1

============================================================
ELIZA - Interactive Session
============================================================
Thread ID: therapy-session-1
Starting new conversation...

============================================================
Commands: /quit to exit, /history to show conversation, /checkpoints to list
============================================================

You: Hello, I'm feeling stressed
ELIZA: How long have you been feeling stressed?

You: For about a week now
ELIZA: Can you elaborate on that?

You: I have too much work
ELIZA: Why do you tell me you have too much work?

You: /history

============================================================
Conversation History (6 messages)
============================================================
1. You: Hello, I'm feeling stressed
2. ELIZA: How long have you been feeling stressed?
3. You: For about a week now
4. ELIZA: Can you elaborate on that?
5. You: I have too much work
6. ELIZA: Why do you tell me you have too much work?

You: /quit

ELIZA: Goodbye! Your conversation has been saved.
```

### About ELIZA

ELIZA is a classic chatbot created by Joseph Weizenbaum at MIT in 1966. It simulates a Rogerian psychotherapist through:

- **Pattern Matching**: Uses regular expressions to identify key phrases
- **Pronoun Reflection**: Transforms "I" to "you", "my" to "your", etc.
- **Response Templates**: Multiple possible responses for each pattern
- **Simple but Effective**: Despite simplicity, can create surprisingly engaging conversations

This implementation demonstrates how LangGraph's checkpoint system can persist conversation state for any type of conversational agent, from simple pattern matchers to modern LLMs.

### Implementation Notes

- Each user message creates a new checkpoint automatically
- Checkpoints include full conversation history
- Thread IDs allow multiple independent conversations
- Cassandra provides durable, scalable storage
- The example works without any external API keys
