from cassandra.cluster import Cluster
from mockylla import mock_scylladb, get_types, get_tables, get_table_rows


@mock_scylladb
def test_create_udt():
    """
    Tests that a UDT can be created successfully.
    """

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    session.execute(
        f"CREATE KEYSPACE {keyspace_name} "
        "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace(keyspace_name)

    udt_name = "my_udt"
    session.execute(f"CREATE TYPE {udt_name} (first_name text, last_name text)")

    types = get_types(keyspace_name)
    assert udt_name in types
    assert types[udt_name]["fields"] == {
        "first_name": "text",
        "last_name": "text",
    }


@mock_scylladb
def test_create_table_with_udt():
    """
    Tests that a table can be created with a UDT column.
    """

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    session.execute(
        f"CREATE KEYSPACE {keyspace_name} "
        "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace(keyspace_name)

    udt_name = "my_udt"
    session.execute(f"CREATE TYPE {udt_name} (first_name text, last_name text)")

    table_name = "users"
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, user {udt_name})"
    )

    tables = get_tables(keyspace_name)
    assert table_name in tables
    assert tables[table_name]["schema"] == {"id": "int", "user": udt_name}


@mock_scylladb
def test_insert_into_table_with_udt():
    """
    Tests that data can be inserted into a table with a UDT column.
    """

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    session.execute(
        f"CREATE KEYSPACE {keyspace_name} "
        "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace(keyspace_name)

    udt_name = "my_udt"
    session.execute(f"CREATE TYPE {udt_name} (first_name text, last_name text)")

    table_name = "users"
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, user {udt_name})"
    )

    user_data = "{first_name: 'John', last_name: 'Doe'}"
    session.execute(
        f"INSERT INTO {table_name} (id, user) VALUES (1, {user_data})"
    )

    rows = get_table_rows(keyspace_name, table_name)
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["user"] == {"first_name": "John", "last_name": "Doe"}


@mock_scylladb
def test_select_from_table_with_udt():
    """
    Tests that data can be selected from a table with a UDT column.
    """

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    session.execute(
        f"CREATE KEYSPACE {keyspace_name} "
        "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace(keyspace_name)

    udt_name = "my_udt"
    session.execute(f"CREATE TYPE {udt_name} (first_name text, last_name text)")

    table_name = "users"
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, user {udt_name})"
    )

    user_data_literal = "{first_name: 'Jane', last_name: 'Doe'}"
    session.execute(
        f"INSERT INTO {table_name} (id, user) VALUES (1, {user_data_literal})"
    )

    result = session.execute(f"SELECT user FROM {table_name} WHERE id = 1")
    rows = result.all()

    assert len(rows) == 1

    assert rows[0].user == {"first_name": "Jane", "last_name": "Doe"}
