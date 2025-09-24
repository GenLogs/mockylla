import pytest
from cassandra import InvalidRequest
from cassandra.cluster import Cluster

from mockylla import mock_scylladb, get_table_rows


@mock_scylladb
def test_select_all_from_table():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    table_name = "my_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, name text, age int)"
    )

    session.execute(
        f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 30)"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, age) VALUES (2, 'Bob', 25)"
    )

    result = session.execute(f"SELECT * FROM {table_name}")
    rows = list(result)

    assert len(rows) == 2
    assert rows[0] == {"id": 1, "name": "Alice", "age": 30}
    assert rows[1] == {"id": 2, "name": "Bob", "age": 25}

    mock_rows = get_table_rows(keyspace_name, table_name)
    assert len(mock_rows) == 2


@mock_scylladb
def test_select_with_where_clause():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    table_name = "my_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, name text, age int)"
    )

    session.execute(
        f"INSERT INTO {table_name} (id, name, age) VALUES (1, 'Alice', 30)"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, age) VALUES (2, 'Bob', 25)"
    )

    result = session.execute(f"SELECT * FROM {table_name} WHERE id = 2")
    rows = list(result)

    assert len(rows) == 1
    assert rows[0] == {"id": 2, "name": "Bob", "age": 25}


@mock_scylladb
def test_select_with_compound_where_clause():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    table_name = "my_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, name text, city text)"
    )

    session.execute(
        f"INSERT INTO {table_name} (id, name, city) VALUES (1, 'Alice', 'New York')"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, city) VALUES (2, 'Bob', 'Los Angeles')"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, city) VALUES (3, 'Alice', 'Los Angeles')"
    )

    result = session.execute(
        f"SELECT * FROM {table_name} WHERE name = 'Alice' AND city = 'Los Angeles'"
    )
    rows = list(result)

    assert len(rows) == 1
    assert rows[0] == {"id": 3, "name": "Alice", "city": "Los Angeles"}


@mock_scylladb
def test_select_with_allow_filtering():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "allow_keyspace"
    table_name = "allow_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, name text, city text)"
    )

    session.execute(
        f"INSERT INTO {table_name} (id, name, city) VALUES (1, 'Alice', 'Paris')"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, city) VALUES (2, 'Bob', 'Rome')"
    )

    result = session.execute(
        f"SELECT * FROM {table_name} WHERE city = 'Paris' ALLOW FILTERING"
    )
    rows = list(result)

    assert len(rows) == 1
    assert rows[0] == {"id": 1, "name": "Alice", "city": "Paris"}


@mock_scylladb
def test_select_count_star():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    table_name = "my_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, name text)"
    )

    session.execute(f"INSERT INTO {table_name} (id, name) VALUES (1, 'Alice')")
    session.execute(f"INSERT INTO {table_name} (id, name) VALUES (2, 'Bob')")

    result = session.execute(f"SELECT COUNT(*) FROM {table_name}")
    row = result.one()

    assert row is not None
    assert row.count == 2
    assert row[0] == 2
    assert row["count"] == 2


@mock_scylladb
def test_select_count_star_alias():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "alias_keyspace"
    table_name = "alias_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, value text)"
    )

    session.execute(f"INSERT INTO {table_name} (id, value) VALUES (1, 'one')")

    result = session.execute(f"SELECT COUNT(*) AS total FROM {table_name}")
    row = result.one()

    assert row is not None
    assert row.total == 1
    assert row["total"] == 1


@mock_scylladb
def test_select_count_column():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "count_keyspace"
    table_name = "count_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, age int, city text)"
    )

    session.execute(
        f"INSERT INTO {table_name} (id, age, city) VALUES (1, 30, 'Paris')"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, age, city) VALUES (2, 25, 'Rome')"
    )
    session.execute(f"INSERT INTO {table_name} (id, city) VALUES (3, 'Madrid')")

    result = session.execute(f"SELECT COUNT(age) FROM {table_name}")
    row = result.one()

    assert row is not None
    assert row.count == 2
    assert row[0] == 2


@mock_scylladb
def test_select_sum_min_max():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "agg_keyspace"
    table_name = "agg_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, score int)"
    )

    session.execute(f"INSERT INTO {table_name} (id, score) VALUES (1, 10)")
    session.execute(f"INSERT INTO {table_name} (id, score) VALUES (2, 40)")
    session.execute(f"INSERT INTO {table_name} (id, score) VALUES (3, 5)")

    result = session.execute(
        f"SELECT SUM(score) AS total_score, MIN(score) AS min_score, MAX(score) FROM {table_name}"
    )
    row = result.one()

    assert row is not None
    assert row.total_score == 55
    assert row.min_score == 5
    assert row.max == 40


@mock_scylladb
def test_select_aggregate_mixed_columns_error():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "error_keyspace"
    table_name = "error_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, value int)"
    )

    session.execute(f"INSERT INTO {table_name} (id, value) VALUES (1, 5)")

    with pytest.raises(InvalidRequest):
        session.execute(f"SELECT COUNT(*) , id FROM {table_name}")


@mock_scylladb
def test_select_aggregate_with_limit_error():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "limit_keyspace"
    table_name = "limit_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, name text)"
    )

    session.execute(f"INSERT INTO {table_name} (id, name) VALUES (1, 'Alice')")

    with pytest.raises(InvalidRequest):
        session.execute(f"SELECT COUNT(*) FROM {table_name} LIMIT 1")

    with pytest.raises(InvalidRequest):
        session.execute(f"SELECT COUNT(*) FROM {table_name} ORDER BY id")


@mock_scylladb
def test_select_unknown_column_error():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "missing_keyspace"
    table_name = "missing_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, name text)"
    )

    with pytest.raises(InvalidRequest):
        session.execute(f"SELECT unknown_column FROM {table_name}")
