from cassandra.cluster import Cluster

from mockylla import mock_scylladb


@mock_scylladb
def test_select_with_in_clause():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    table_name = "my_table"
    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id UUID PRIMARY KEY, name TEXT, age INT)"
    )

    session.execute(
        f"INSERT INTO {table_name} (id, name, age) VALUES (uuid(), 'Alice', 30)"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, age) VALUES (uuid(), 'Bob', 40)"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, age) VALUES (uuid(), 'Charlie', 50)"
    )

    rows = session.execute(
        f"SELECT * FROM {table_name} WHERE name IN ('Alice', 'Charlie')"
    )
    matching_rows = list(rows)
    assert len(matching_rows) == 2

    rows = session.execute(f"SELECT * FROM {table_name} WHERE name IN ('Bob')")
    bob_rows = list(rows)
    assert len(bob_rows) == 1
    assert bob_rows[0]["name"] == "Bob"

    rows = session.execute(
        f"SELECT * FROM {table_name} WHERE name IN ('David')"
    )
    assert len(list(rows)) == 0
