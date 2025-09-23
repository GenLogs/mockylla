from mockylla import mock_scylladb, get_table_rows
from cassandra.cluster import Cluster


@mock_scylladb
def test_insert_into_table():
    """
    Tests that data can be inserted into a table and then retrieved.
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

    insert_query = f"INSERT INTO {table_name} (user_id, name, email) VALUES (1, 'John Doe', 'john.doe@example.com')"
    session.execute(insert_query)

    rows = get_table_rows(keyspace_name, table_name)

    assert len(rows) == 1

    inserted_row = rows[0]
    assert inserted_row["user_id"] == 1
    assert inserted_row["name"] == "John Doe"
    assert inserted_row["email"] == "john.doe@example.com"


@mock_scylladb
def test_insert_if_not_exists():
    """
    Tests the IF NOT EXISTS clause for INSERT statements.
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

    insert_query = f"INSERT INTO {table_name} (user_id, name, email) VALUES (1, 'John Doe', 'john.doe@example.com')"
    session.execute(insert_query)
    assert len(get_table_rows(keyspace_name, table_name)) == 1

    lwt_query_fail = f"{insert_query} IF NOT EXISTS"
    result_fail = session.execute(lwt_query_fail)
    fail_row = result_fail.one()
    assert fail_row["[applied]"] is False
    assert len(get_table_rows(keyspace_name, table_name)) == 1
    assert fail_row["user_id"] == 1

    lwt_query_success = f"INSERT INTO {table_name} (user_id, name, email) VALUES (2, 'Jane Doe', 'jane.doe@example.com') IF NOT EXISTS"
    result_success = session.execute(lwt_query_success)
    success_row = result_success.one()
    assert success_row["[applied]"] is True
    assert len(get_table_rows(keyspace_name, table_name)) == 2
