import pytest
from cassandra.cluster import Cluster
from mockylla import MockScyllaDB, get_table_rows


@pytest.fixture(scope="module", autouse=True)
def scylla_mock():
    """Start the mock, create a keyspace/table and insert some rows."""

    with MockScyllaDB():
        cluster = Cluster(["127.0.0.1"])
        session = cluster.connect()

        keyspace_name = "preload"
        table_name = "items"

        session.execute(
            f"CREATE KEYSPACE {keyspace_name} "
            "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        session.set_keyspace(keyspace_name)
        session.execute(
            f"CREATE TABLE {table_name} (id int PRIMARY KEY, value text)"
        )

        session.execute(
            f"INSERT INTO {table_name} (id, value) VALUES (1, 'foo')"
        )
        session.execute(
            f"INSERT INTO {table_name} (id, value) VALUES (2, 'bar')"
        )

        yield


def test_preloaded_row_count():
    """Ensure the fixture inserted exactly two rows."""
    rows = get_table_rows("preload", "items")
    assert len(rows) == 2


def test_query_returns_expected_value():
    """Run a SELECT query and check that the correct row is returned."""
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect("preload")

    result = session.execute("SELECT value FROM items WHERE id = 2")
    row = result.one()

    assert row.value == "bar"
