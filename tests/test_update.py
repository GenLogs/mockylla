from cassandra.cluster import Cluster

from mockylla import mock_scylladb, get_table_rows


@mock_scylladb
def test_update_with_where_clause():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    table_name = "my_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, name text, city text)"
    )

    session.execute(
        f"INSERT INTO {table_name} (id, name, city) VALUES (1, 'Alice', 'New York')"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, city) VALUES (2, 'Bob', 'Los Angeles')"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, city) VALUES (3, 'Alice', 'Los Angeles')"
    )

    session.execute(
        f"UPDATE {table_name} SET city = 'San Francisco' WHERE name = 'Alice' AND city = 'Los Angeles'"
    )

    all_rows = get_table_rows(keyspace_name, table_name)
    assert len(all_rows) == 3

    updated_row = None
    for row in all_rows:
        if row["id"] == 3:
            updated_row = row
            break

    assert updated_row is not None
    assert updated_row["city"] == "San Francisco"


@mock_scylladb
def test_update_if_exists():
    """
    Tests the IF EXISTS clause for UPDATE statements.
    """

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    keyspace_name = "my_keyspace"
    table_name = "my_table"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    session.set_keyspace(keyspace_name)
    session.execute(
        f"CREATE TABLE {table_name} (id int PRIMARY KEY, name text, value int)"
    )
    session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (1, 'Alice', 10)"
    )

    update_success_query = (
        f"UPDATE {table_name} SET value = 20 WHERE id = 1 IF EXISTS"
    )
    result_success = session.execute(update_success_query)
    assert result_success.one()["[applied]"] is True
    rows = get_table_rows(keyspace_name, table_name)
    assert len(rows) == 1
    assert rows[0]["value"] == 20

    update_fail_query = (
        f"UPDATE {table_name} SET value = 30 WHERE id = 2 IF EXISTS"
    )
    result_fail = session.execute(update_fail_query)
    assert result_fail.one()["[applied]"] is False
    assert len(get_table_rows(keyspace_name, table_name)) == 1
    assert rows[0]["value"] == 20
