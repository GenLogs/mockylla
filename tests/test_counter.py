from mockylla import mock_scylladb
from cassandra.cluster import Cluster


@mock_scylladb
def test_counter_table_update():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    session.execute(
        "CREATE KEYSPACE test_keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("test_keyspace")
    session.execute("CREATE TABLE counters (id int PRIMARY KEY, c counter)")

    session.execute("UPDATE counters SET c = c + 1 WHERE id = 1")
    rows = session.execute("SELECT * FROM counters WHERE id = 1")
    assert len(rows) == 1
    assert rows[0]["c"] == 1

    session.execute("UPDATE counters SET c = c + 1 WHERE id = 1")
    rows = session.execute("SELECT * FROM counters WHERE id = 1")
    assert rows[0]["c"] == 2

    session.execute("UPDATE counters SET c = c - 10 WHERE id = 1")
    rows = session.execute("SELECT * FROM counters WHERE id = 1")
    assert rows[0]["c"] == -8
