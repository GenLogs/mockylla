from cassandra.cluster import Cluster

from mockylla import mock_scylladb


def _setup_basic_table(session):
    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.execute("USE ks")
    session.execute(
        "CREATE TABLE users (id int PRIMARY KEY, name text, email text)"
    )


@mock_scylladb
def test_use_statement_sets_session_keyspace():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.execute("USE ks")

    assert session.keyspace == "ks"


@mock_scylladb
def test_insert_with_ttl_clause():
    cluster = Cluster()
    session = cluster.connect()
    _setup_basic_table(session)

    session.execute(
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com') USING TTL 60"
    )

    row = session.execute("SELECT name FROM users WHERE id = 1").one()
    assert row.name == "Alice"


@mock_scylladb
def test_update_with_timestamp_clause():
    cluster = Cluster()
    session = cluster.connect()
    _setup_basic_table(session)

    session.execute(
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"
    )

    session.execute(
        "UPDATE users USING TIMESTAMP 12345 SET name = 'Bob' WHERE id = 1"
    )

    row = session.execute("SELECT name FROM users WHERE id = 1").one()
    assert row.name == "Bob"


@mock_scylladb
def test_create_index_reflected_in_system_schema():
    cluster = Cluster()
    session = cluster.connect()
    _setup_basic_table(session)

    session.execute("CREATE INDEX ON users (email)")

    indexes = session.execute(
        "SELECT index_name, target FROM system_schema.indexes WHERE keyspace_name = 'ks' AND table_name = 'users'"
    ).all()
    assert len(indexes) == 1
    assert indexes[0].target == "email"

    table_meta = cluster.metadata.get_keyspace("ks").tables["users"]
    assert any(idx["column"] == "email" for idx in table_meta.indexes)
