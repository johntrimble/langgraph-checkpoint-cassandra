"""
Basic interactive example of using CassandraSaver with LangGraph.

This example demonstrates:
- Creating a simple chatbot with state persistence
- Storing and retrieving conversation history
- Continuing conversations across sessions
- Listing threads and checkpoints

Usage:
  # Start interactive chat (default thread: "default")
  uv run python examples/basic_example.py

  # Start or continue a specific thread
  uv run python examples/basic_example.py --thread-id my-session

  # Resume from a specific checkpoint
  uv run python examples/basic_example.py --thread-id my-session --checkpoint-id <id>

  # List all threads
  uv run python examples/basic_example.py --list-threads

  # List checkpoints for a thread
  uv run python examples/basic_example.py --list-checkpoints --thread-id my-session
"""

import argparse
import sys
from typing import Annotated

from cassandra.cluster import Cluster
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

from langgraph_checkpoint_cassandra import CassandraSaver


# Define the state
class State(TypedDict):
    """State for our chatbot."""

    messages: Annotated[list, add_messages]


# Define the chatbot node
def chatbot(state: State):
    """
    Echo back the conversation history.

    This simple bot responds by summarizing the chat history,
    demonstrating how state is preserved across interactions.
    """
    messages = state["messages"]
    num_messages = len(messages)

    # Count user messages
    user_messages = [
        m
        for m in messages
        if (
            (hasattr(m, "type") and m.type == "human")
            or (isinstance(m, dict) and m.get("role") == "user")
        )
    ]

    # Create a friendly response showing the history
    if num_messages == 1:
        response = "Hi! I received your first message. I'll keep track of our conversation history."
    else:
        response = f"I see we've exchanged {num_messages} messages so far. "
        response += f"You've sent {len(user_messages)} message(s) to me. "
        response += f'Your last message was: "{messages[-1].content if hasattr(messages[-1], "content") else messages[-1].get("content", "")}"'

    return {"messages": [{"role": "assistant", "content": response}]}


def create_graph(checkpointer):
    """Create the conversation graph."""
    # Build graph
    graph_builder = StateGraph(State)
    graph_builder.add_node("chatbot", chatbot)
    graph_builder.add_edge(START, "chatbot")
    graph_builder.add_edge("chatbot", END)

    # Compile with checkpointer
    return graph_builder.compile(checkpointer=checkpointer)


def setup_cassandra(keyspace="example_checkpoints"):
    """Setup Cassandra connection and schema."""
    print("Connecting to Cassandra...")
    cluster = Cluster(["cassandra"])
    session = cluster.connect()
    print("✓ Connected")

    print(f"Setting up keyspace '{keyspace}'...")
    checkpointer = CassandraSaver(session, keyspace=keyspace, thread_id_type="text")
    checkpointer.setup(replication_factor=1)  # RF=1 for single-node dev environment
    print("✓ Schema ready")

    return session, cluster, checkpointer


def interactive_chat(
    graph, checkpointer, thread_id: str, checkpoint_id: str | None = None
):
    """Run an interactive chat session."""
    print("\n" + "=" * 60)
    print("LangGraph + CassandraSaver - Interactive Chat")
    print("=" * 60)
    print(f"Thread ID: {thread_id}")

    config = {"configurable": {"thread_id": thread_id}}

    # If checkpoint_id is provided, try to resume from it
    if checkpoint_id:
        config["configurable"]["checkpoint_id"] = checkpoint_id
        print(f"Resuming from checkpoint: {checkpoint_id[:16]}...")

    # Check if thread has existing history
    checkpoint_tuple = checkpointer.get_tuple(config)
    if checkpoint_tuple:
        num_messages = len(checkpoint_tuple.checkpoint["channel_values"]["messages"])
        print(f"✓ Found existing conversation with {num_messages} messages")

        # Show recent history
        if num_messages > 0:
            show_history(checkpoint_tuple, limit=6)
    else:
        print("✓ Starting new conversation")

    print("\n" + "=" * 60)
    print("Commands:")
    print("  /quit or /exit - Exit the chat")
    print("  /history      - Show full conversation history")
    print("  /checkpoints  - List all checkpoints for this thread")
    print("=" * 60)
    print()

    # Interactive loop
    while True:
        try:
            user_input = input("You: ").strip()

            if not user_input:
                continue

            # Handle commands
            if user_input.lower() in ["/quit", "/exit", "quit", "exit"]:
                print("\nGoodbye! Your conversation has been saved.")
                break
            elif user_input.lower() == "/history":
                show_conversation_history(checkpointer, config)
                continue
            elif user_input.lower() == "/checkpoints":
                list_checkpoints_for_thread(checkpointer, thread_id)
                continue

            # Process message
            result = graph.invoke(
                {"messages": [{"role": "user", "content": user_input}]}, config=config
            )

            # Extract and print bot's response
            last_msg = result["messages"][-1]
            content = (
                last_msg.content
                if hasattr(last_msg, "content")
                else last_msg["content"]
            )
            print(f"Bot: {content}\n")

        except KeyboardInterrupt:
            print("\n\nSession interrupted. Your conversation has been saved.")
            break
        except EOFError:
            print("\nGoodbye! Your conversation has been saved.")
            break


def show_history(checkpoint_tuple, limit=None):
    """Show recent conversation history from a checkpoint tuple."""
    messages = checkpoint_tuple.checkpoint["channel_values"]["messages"]
    if not messages:
        return

    print("\nRecent history:")
    start_idx = max(0, len(messages) - limit) if limit else 0
    for msg in messages[start_idx:]:
        # Determine if it's a user or assistant message
        is_user = (
            (hasattr(msg, "type") and msg.type == "human")
            or (isinstance(msg, dict) and msg.get("role") == "user")
            or (hasattr(msg, "__class__") and "Human" in msg.__class__.__name__)
        )
        content = msg.content if hasattr(msg, "content") else msg.get("content", "")
        prefix = "You" if is_user else "Bot"
        print(f"  {prefix}: {content}")


def show_conversation_history(checkpointer, config):
    """Show full conversation history."""
    checkpoint_tuple = checkpointer.get_tuple(config)
    if not checkpoint_tuple:
        print("No conversation history found.")
        return

    messages = checkpoint_tuple.checkpoint["channel_values"]["messages"]
    print("\n" + "=" * 60)
    print(f"Conversation History ({len(messages)} messages)")
    print("=" * 60)

    for i, msg in enumerate(messages, 1):
        # Determine if it's a user or assistant message
        is_user = (
            (hasattr(msg, "type") and msg.type == "human")
            or (isinstance(msg, dict) and msg.get("role") == "user")
            or (hasattr(msg, "__class__") and "Human" in msg.__class__.__name__)
        )
        content = msg.content if hasattr(msg, "content") else msg.get("content", "")
        prefix = "You" if is_user else "Bot"
        print(f"{i}. {prefix}: {content}")
    print()


def list_checkpoints_for_thread(checkpointer, thread_id: str, limit: int = 10):
    """List checkpoints for a given thread."""
    config = {"configurable": {"thread_id": thread_id}}
    checkpoints = list(checkpointer.list(config, limit=limit))

    print("\n" + "=" * 60)
    print(f"Checkpoints for thread '{thread_id}'")
    print("=" * 60)

    if not checkpoints:
        print("No checkpoints found.")
        return

    print(f"Found {len(checkpoints)} checkpoint(s):\n")
    for i, cp in enumerate(checkpoints, 1):
        checkpoint_id = cp.checkpoint["id"]
        # Safely get message count
        try:
            num_messages = len(
                cp.checkpoint.get("channel_values", {}).get("messages", [])
            )
            print(f"{i}. ID: {checkpoint_id}")
            print(f"   Messages: {num_messages}")
        except (KeyError, TypeError):
            print(f"{i}. ID: {checkpoint_id}")
            print("   Messages: unknown")
        print()


def list_all_threads(checkpointer, keyspace: str):
    """List all available threads."""
    print("\n" + "=" * 60)
    print("All Conversation Threads")
    print("=" * 60)
    print("\nNote: Listing all threads requires scanning the checkpoints table.")
    print(
        "This is a demonstration - in production, maintain a separate threads index.\n"
    )

    # Query to get all thread_id/checkpoint_ns pairs
    # Note: This requires a table scan and is expensive - for demonstration only
    session = checkpointer.session
    query = f"SELECT thread_id, checkpoint_ns FROM {keyspace}.checkpoints"
    try:
        rows = session.execute(query)
        threads = {}  # thread_id -> set of checkpoint_ns
        for row in rows:
            if row.thread_id not in threads:
                threads[row.thread_id] = set()
            threads[row.thread_id].add(row.checkpoint_ns)

        if threads:
            print(f"Found {len(threads)} thread(s):\n")
            for thread_id in sorted(threads.keys()):
                # Get latest checkpoint for this thread
                config = {"configurable": {"thread_id": thread_id}}
                cp_tuple = checkpointer.get_tuple(config)
                if cp_tuple:
                    num_messages = len(
                        cp_tuple.checkpoint["channel_values"]["messages"]
                    )
                    namespaces = threads[thread_id]
                    ns_str = (
                        f", namespaces: {len(namespaces)}"
                        if len(namespaces) > 1
                        else ""
                    )
                    print(f"  • {thread_id} ({num_messages} messages{ns_str})")
                else:
                    print(f"  • {thread_id}")
        else:
            print("No threads found.")
    except Exception as e:
        print(f"Error listing threads: {e}")
        print(
            "Tip: Cassandra requires efficient queries. Consider maintaining a separate threads index."
        )
    print()


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Interactive chatbot with LangGraph and CassandraSaver",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive chat with default thread
  python examples/basic_example.py

  # Chat with a specific thread
  python examples/basic_example.py --thread-id my-session

  # Resume from checkpoint
  python examples/basic_example.py --thread-id my-session --checkpoint-id <id>

  # List all threads
  python examples/basic_example.py --list-threads

  # List checkpoints for a thread
  python examples/basic_example.py --list-checkpoints --thread-id my-session
        """,
    )

    parser.add_argument(
        "--thread-id",
        "-t",
        type=str,
        default="default",
        help="Thread ID for the conversation (default: 'default')",
    )
    parser.add_argument(
        "--checkpoint-id", "-c", type=str, help="Checkpoint ID to resume from"
    )
    parser.add_argument(
        "--list-threads",
        action="store_true",
        help="List all conversation threads and exit",
    )
    parser.add_argument(
        "--list-checkpoints",
        action="store_true",
        help="List checkpoints for a thread (requires --thread-id) and exit",
    )
    parser.add_argument(
        "--keyspace",
        "-k",
        type=str,
        default="example_checkpoints",
        help="Cassandra keyspace to use (default: example_checkpoints)",
    )

    args = parser.parse_args()

    # Setup Cassandra
    try:
        session, cluster, checkpointer = setup_cassandra(keyspace=args.keyspace)
    except Exception as e:
        print(f"Error connecting to Cassandra: {e}")
        print("Make sure Cassandra is running (use Docker Compose in this repo)")
        sys.exit(1)

    try:
        # Handle different modes
        if args.list_threads:
            list_all_threads(checkpointer, args.keyspace)
        elif args.list_checkpoints:
            list_checkpoints_for_thread(checkpointer, args.thread_id)
        else:
            # Always run interactive mode
            graph = create_graph(checkpointer)
            print("✓ Conversation graph ready\n")
            interactive_chat(graph, checkpointer, args.thread_id, args.checkpoint_id)

    finally:
        # Cleanup
        cluster.shutdown()


if __name__ == "__main__":
    main()
