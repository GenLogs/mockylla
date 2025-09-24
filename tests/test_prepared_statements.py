import pytest

from cassandra.cluster import Cluster

from mockylla import mock_scylladb


def _setup_users(session):
    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute(
        "CREATE TABLE users (id int PRIMARY KEY, name text, active boolean)"
    )


@mock_scylladb
def test_prepare_bind_with_mapping():
    cluster = Cluster()
    session = cluster.connect()
    _setup_users(session)

    insert_ps = session.prepare(
        "INSERT INTO users (id, name, active) VALUES (?, ?, ?)"
    )

    bound = insert_ps.bind({"id": 1, "name": "Alice", "active": True})
    assert bound.values == (1, "Alice", True)

    session.execute(bound)

    row = session.execute(
        "SELECT id, name, active FROM users WHERE id = 1"
    ).one()
    assert (row.id, row.name, row.active) == (1, "Alice", True)


@mock_scylladb
def test_execute_prepared_with_mapping_parameters():
    cluster = Cluster()
    session = cluster.connect()
    _setup_users(session)

    session.execute(
        "INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)"
    )

    select_ps = session.prepare("SELECT name FROM users WHERE id = ?")

    row = session.execute(select_ps, {"id": 1}).one()
    assert row.name == "Alice"


@mock_scylladb
def test_prepared_statement_missing_parameter_raises():
    cluster = Cluster()
    session = cluster.connect()
    _setup_users(session)

    ps = session.prepare("UPDATE users SET name = ?, active = ? WHERE id = ?")

    with pytest.raises(ValueError, match="Missing parameters"):
        ps.bind({"name": "Bob", "id": 1})

    with pytest.raises(ValueError, match="Unexpected parameters"):
        ps.bind({"name": "Bob", "active": True, "id": 1, "extra": 5})


@mock_scylladb
def test_prepared_statement_update_and_delete():
    cluster = Cluster()
    session = cluster.connect()
    _setup_users(session)

    insert_ps = session.prepare(
        "INSERT INTO users (id, name, active) VALUES (?, ?, ?)"
    )
    session.execute(insert_ps, (1, "Alice", True))

    update_ps = session.prepare("UPDATE users SET active = ? WHERE id = ?")
    session.execute(update_ps.bind({"active": False, "id": 1}))

    delete_ps = session.prepare("DELETE FROM users WHERE id = ?")
    session.execute(delete_ps.bind((1,)))

    assert session.execute("SELECT * FROM users WHERE id = 1").one() is None
