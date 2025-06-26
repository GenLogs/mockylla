from cassandra.cluster import Cluster
from cassandra.protocol import SyntaxException

import pytest

from mockylla import mock_scylladb, get_tables


@mock_scylladb
def test_alter_table_add_column():
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

    session.execute(f"ALTER TABLE {table_name} ADD new_column int")

    tables = get_tables(keyspace_name)
    assert "new_column" in tables[table_name]["schema"]
    assert tables[table_name]["schema"]["new_column"] == "int"

    session.execute(
        f"INSERT INTO {table_name} (id, name, new_column) VALUES (1, 'one', 100)"
    )

    with pytest.raises(SyntaxException):
        session.execute("ALTER TABLE non_existent_table ADD another_column int")
