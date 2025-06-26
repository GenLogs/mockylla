import unittest
from cassandra.cluster import Cluster
from mockylla import mock_scylladb, get_table_rows


class TestBatchStatements(unittest.TestCase):
    @mock_scylladb
    def test_batch_mixed_operations(self):
        cluster = Cluster(["127.0.0.1"])
        session = cluster.connect()
        session.execute(
            "CREATE KEYSPACE mykeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        session.set_keyspace("mykeyspace")
        session.execute("CREATE TABLE mytable (id int PRIMARY KEY, value text)")

        session.execute("INSERT INTO mytable (id, value) VALUES (1, 'one')")
        session.execute("INSERT INTO mytable (id, value) VALUES (2, 'two')")
        session.execute("INSERT INTO mytable (id, value) VALUES (3, 'three')")

        session.execute("""
        BEGIN BATCH
            INSERT INTO mytable (id, value) VALUES (4, 'four');
            UPDATE mytable SET value = 'new_one' WHERE id = 1;
            DELETE FROM mytable WHERE id = 2;
        APPLY BATCH;
        """)

        rows = sorted(
            list(session.execute("SELECT id, value FROM mytable")),
            key=lambda r: r.id,
        )

        self.assertEqual(len(rows), 3)

        self.assertEqual(rows[0].id, 1)
        self.assertEqual(rows[0].value, "new_one")

        self.assertEqual(rows[1].id, 3)
        self.assertEqual(rows[1].value, "three")

        self.assertEqual(rows[2].id, 4)
        self.assertEqual(rows[2].value, "four")

        table_rows = get_table_rows("mykeyspace", "mytable")
        self.assertEqual(len(table_rows), 3)

        session.execute("DROP TABLE mytable")
        session.execute("DROP KEYSPACE mykeyspace")


if __name__ == "__main__":
    unittest.main()
