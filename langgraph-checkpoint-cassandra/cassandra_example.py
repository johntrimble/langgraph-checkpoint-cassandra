"""
Example script showing how to connect to the Cassandra cluster.
"""
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_cassandra(max_attempts=10, delay_seconds=5):
    """Wait for Cassandra to become available."""
    cluster = None
    
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"Connecting to Cassandra (attempt {attempt}/{max_attempts})...")
            # Connect to the Cassandra cluster
            cluster = Cluster(['cassandra'])
            session = cluster.connect()
            
            # Simple query to test the connection
            rows = session.execute("SELECT release_version FROM system.local")
            version = rows[0].release_version
            logger.info(f"Connected to Cassandra version: {version}")
            
            return session, cluster
        except Exception as e:
            logger.warning(f"Failed to connect to Cassandra: {e}")
            if attempt < max_attempts:
                logger.info(f"Retrying in {delay_seconds} seconds...")
                time.sleep(delay_seconds)
            else:
                logger.error("Max attempts reached. Could not connect to Cassandra.")
                raise
        finally:
            if cluster and attempt < max_attempts:
                cluster.shutdown()
    
    return None, None

def create_keyspace_and_table(session):
    """Create a keyspace and table for examples."""
    # Create keyspace
    logger.info("Creating keyspace 'example_keyspace' if it doesn't exist...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS example_keyspace 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    
    # Use the keyspace
    session.execute("USE example_keyspace")
    
    # Create table
    logger.info("Creating table 'users' if it doesn't exist...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id uuid PRIMARY KEY,
            username text,
            email text,
            created_at timestamp
        )
    """)

def insert_sample_data(session):
    """Insert some sample data into the users table."""
    logger.info("Inserting sample data...")
    
    # Prepare the statement
    insert_stmt = session.prepare("""
        INSERT INTO users (user_id, username, email, created_at) 
        VALUES (uuid(), ?, ?, toTimestamp(now()))
    """)
    
    # Insert some sample users
    sample_users = [
        ("alice", "alice@example.com"),
        ("bob", "bob@example.com"),
        ("charlie", "charlie@example.com"),
        ("dana", "dana@example.com"),
        ("evan", "evan@example.com")
    ]
    
    for username, email in sample_users:
        session.execute(insert_stmt, (username, email))
    
    logger.info(f"Inserted {len(sample_users)} sample users.")

def query_data(session):
    """Query and display the data from the users table."""
    logger.info("Querying users...")
    rows = session.execute("SELECT * FROM users")
    
    print("\nUsers in database:")
    print("-" * 80)
    print(f"{'User ID':<36} | {'Username':<15} | {'Email':<25} | {'Created At'}")
    print("-" * 80)
    
    for row in rows:
        print(f"{str(row.user_id):<36} | {row.username:<15} | {row.email:<25} | {row.created_at}")

def main():
    """Main function to demonstrate Cassandra functionality."""
    session, cluster = None, None
    try:
        # Connect to Cassandra
        session, cluster = wait_for_cassandra()
        
        # Create schema
        create_keyspace_and_table(session)
        
        # Insert sample data
        insert_sample_data(session)
        
        # Query data
        query_data(session)
        
        logger.info("Example completed successfully.")
    
    except Exception as e:
        logger.error(f"Error in Cassandra example: {e}")
    
    finally:
        # Clean up
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    main()