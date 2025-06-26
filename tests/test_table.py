from mockylla import mock_scylladb, get_tables
from cassandra.cluster import Cluster


@mock_scylladb
def test_create_table():
    """
    Tests that a table can be created within a keyspace and inspected.
    """

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_app"
    table_name = "users"

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

    tables_in_keyspace = get_tables(keyspace_name)
    assert table_name in tables_in_keyspace

    schema = tables_in_keyspace[table_name]["schema"]
    assert schema["user_id"] == "int"
    assert schema["name"] == "text"
    assert schema["email"] == "text"
