from cassandra.cluster import Session


def drop_schema(session: Session, keyspace: str) -> None:
    session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
