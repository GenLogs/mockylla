from pyscylladb_mock import mock_scylladb, get_table_rows
from cassandra.cluster import Cluster


@mock_scylladb
def test_insert_into_table():
    """
    Tests that data can be inserted into a table and then retrieved.
    """
    # Arrange
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_app"
    table_name = "users"

    # Setup keyspace and table
    session.execute(
        f"CREATE KEYSPACE {keyspace_name} "
        "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(f"""
        CREATE TABLE {table_name} (
            user_id int PRIMARY KEY,
            name text,
            email text
        )
    """)

    # Act
    insert_query = f"INSERT INTO {table_name} (user_id, name, email) VALUES (1, 'John Doe', 'john.doe@example.com')"
    session.execute(insert_query)

    # Assert
    rows = get_table_rows(keyspace_name, table_name)

    assert len(rows) == 1

    inserted_row = rows[0]
    assert inserted_row["user_id"] == 1
    assert inserted_row["name"] == "John Doe"
    assert inserted_row["email"] == "john.doe@example.com"
