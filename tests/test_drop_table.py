from cassandra.cluster import Cluster

from mockylla import mock_scylladb, get_tables


@mock_scylladb
def test_drop_table():
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

    tables_before_drop = get_tables(keyspace_name)
    assert table_name in tables_before_drop

    session.execute(f"DROP TABLE {table_name}")

    tables_after_drop = get_tables(keyspace_name)
    assert table_name not in tables_after_drop
