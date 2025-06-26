from cassandra.cluster import Cluster

from mockylla import mock_scylladb


@mock_scylladb
def test_integer_type_casting():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    table_name = "my_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, value int)"
    )

    session.execute(f"INSERT INTO {table_name} (id, value) VALUES (1, 100)")

    result = session.execute(f"SELECT * FROM {table_name} WHERE id = 1")
    rows = list(result)

    assert len(rows) == 1
    assert rows[0]["value"] == 100
    assert isinstance(rows[0]["value"], int)


@mock_scylladb
def test_numeric_comparison_in_where_clause():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    table_name = "my_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, value int)"
    )

    session.execute(f"INSERT INTO {table_name} (id, value) VALUES (1, 5)")
    session.execute(f"INSERT INTO {table_name} (id, value) VALUES (2, 100)")

    result = session.execute(f"SELECT * FROM {table_name} WHERE value > 10")
    rows = list(result)

    assert len(rows) == 1
    assert rows[0]["id"] == 2
    assert rows[0]["value"] == 100


@mock_scylladb
def test_collection_types():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    table_name = "my_table_collections"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} ("
        "id int PRIMARY KEY, "
        "my_list list<text>, "
        "my_set set<int>, "
        "my_map map<text, text>"
        ")"
    )

    list_data = ["a", "b", "c"]
    set_data = {1, 2, 3}
    map_data = {"key1": "value1", "key2": "value2"}

    session.execute(
        f"INSERT INTO {table_name} (id, my_list, my_set, my_map) VALUES (%s, %s, %s, %s)",
        (1, list_data, set_data, map_data),
    )

    result = session.execute(f"SELECT * FROM {table_name} WHERE id = 1")
    row = result.one()

    assert row is not None
    assert row["my_list"] == list_data
    assert isinstance(row["my_list"], list)
    assert row["my_set"] == set_data
    assert isinstance(row["my_set"], set)
    assert row["my_map"] == map_data
    assert isinstance(row["my_map"], dict)
