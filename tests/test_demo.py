from cassandra.cluster import Cluster
from mockylla import mock_scylladb


@mock_scylladb
def test_demo():
    s = Cluster().connect()
    s.execute(
        "CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy','replication_factor':1}"
    )
    s.set_keyspace("ks")
    s.execute("CREATE TABLE t (id int PRIMARY KEY, v text)")
    s.execute("INSERT INTO t (id, v) VALUES (1,'A')")
    s.execute("INSERT INTO t (id, v) VALUES (2,'B')")

    s.execute("DELETE FROM t WHERE id = 2;")
    rows = list(s.execute("SELECT * FROM t;"))
    assert len(rows) == 1 and rows[0]["id"] == 1
