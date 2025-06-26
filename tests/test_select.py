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
