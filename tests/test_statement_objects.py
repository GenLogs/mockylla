from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

from mockylla import MockBatchStatement, mock_scylladb


def _setup_schema(session):
    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute(
        "CREATE TABLE users (id int PRIMARY KEY, name text, active boolean)"
    )


@mock_scylladb
def test_simple_statement_execution():
    cluster = Cluster()
    session = cluster.connect()
    _setup_schema(session)

    insert_stmt = SimpleStatement(
        "INSERT INTO users (id, name, active) VALUES (%s, %s, %s)"
    )
    session.execute(insert_stmt, (1, "Alice", True))

    select_stmt = SimpleStatement(
        "SELECT name, active FROM users WHERE id = %s"
    )
    row = session.execute(select_stmt, (1,)).one()

    assert row.name == "Alice"
    assert row.active is True


@mock_scylladb
def test_driver_batch_statement():
    cluster = Cluster()
    session = cluster.connect()
    _setup_schema(session)

    batch = BatchStatement()
    batch.add(
        SimpleStatement(
            "INSERT INTO users (id, name, active) VALUES (%s, %s, %s)"
        ),
        (1, "Alice", True),
    )
    batch.add(
        SimpleStatement("UPDATE users SET active = %s WHERE id = %s"),
        (False, 1),
    )

    session.execute(batch)

    row = session.execute("SELECT id, active FROM users WHERE id = 1").one()
    assert row.id == 1
    assert row.active is False


@mock_scylladb
def test_mock_batch_statement_with_prepared():
    cluster = Cluster()
    session = cluster.connect()
    _setup_schema(session)

    insert_ps = session.prepare(
        "INSERT INTO users (id, name, active) VALUES (?, ?, ?)"
    )
    update_ps = session.prepare("UPDATE users SET name = ? WHERE id = ?")

    batch = MockBatchStatement()
    batch.add(insert_ps, (1, "Alice", True))
    batch.add(update_ps.bind({"name": "Bob", "id": 1}))

    session.execute(batch)

    row = session.execute("SELECT name FROM users WHERE id = 1").one()
    assert row.name == "Bob"
