from cassandra.cluster import Cluster
from cassandra.protocol import SyntaxException

import pytest

from mockylla import mock_scylladb, get_tables


@mock_scylladb
def test_alter_table_add_column():
    # Arrange
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

    # Act
    session.execute(f"ALTER TABLE {table_name} ADD new_column int")

    # Assert
    tables = get_tables(keyspace_name)
    assert "new_column" in tables[table_name]["schema"]
    assert tables[table_name]["schema"]["new_column"] == "int"

    # Verify that inserting data with the new column works
    session.execute(
        f"INSERT INTO {table_name} (id, name, new_column) "
        "VALUES (1, 'one', 100)"
    )

    # Verify that altering a non-existent table raises an error
    with pytest.raises(SyntaxException):
        session.execute("ALTER TABLE non_existent_table ADD another_column int")
