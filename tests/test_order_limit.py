from cassandra.cluster import Cluster
import pytest

from mockylla import mock_scylladb


@pytest.fixture
@mock_scylladb
def session():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    table_name = "my_table"
    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name TEXT, age INT)"
    )

    session.execute(
        f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 30)"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, age) VALUES (2, 'Bob', 25)"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, age) VALUES (3, 'Charlie', 35)"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, age) VALUES (4, 'David', 25)"
    )
    return session


def test_select_with_limit(session):
    table_name = "my_table"
    rows = session.execute(f"SELECT * FROM {table_name} LIMIT 2")
    assert len(list(rows)) == 2


def test_select_with_order_by_asc(session):
    table_name = "my_table"
    rows = session.execute(f"SELECT * FROM {table_name} ORDER BY age ASC")
    results = list(rows)
    assert len(results) == 4
    assert results[0]["name"] == "Bob"
    assert results[1]["name"] == "David"
    assert results[2]["name"] == "Alice"
    assert results[3]["name"] == "Charlie"


def test_select_with_order_by_desc(session):
    table_name = "my_table"
    rows = session.execute(f"SELECT * FROM {table_name} ORDER BY age DESC")
    results = list(rows)
    assert len(results) == 4
    assert results[0]["name"] == "Charlie"
    assert results[1]["name"] == "Alice"
    assert results[2]["name"] == "Bob"
    assert results[3]["name"] == "David"


def test_select_with_order_by_and_limit(session):
    table_name = "my_table"
    rows = session.execute(
        f"SELECT * FROM {table_name} ORDER BY age DESC LIMIT 2"
    )
    results = list(rows)
    assert len(results) == 2
    assert results[0]["name"] == "Charlie"
    assert results[1]["name"] == "Alice"
