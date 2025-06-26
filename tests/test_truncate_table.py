from cassandra.cluster import Cluster

from mockylla import mock_scylladb, get_table_rows


@mock_scylladb
def test_truncate_table():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    table_name = "my_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} "
        "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, name text)"
    )
    session.execute(f"INSERT INTO {table_name} (id, name) VALUES (1, 'one')")
    session.execute(f"INSERT INTO {table_name} (id, name) VALUES (2, 'two')")

    rows_before = get_table_rows(keyspace_name, table_name)
    assert len(rows_before) == 2

    session.execute(f"TRUNCATE TABLE {table_name}")

    rows_after = get_table_rows(keyspace_name, table_name)
    assert len(rows_after) == 0
