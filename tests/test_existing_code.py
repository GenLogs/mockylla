import unittest
from dataclasses import dataclass
from mockylla import mock_scylladb
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel
from typing import List, Tuple


@dataclass
class ScyllaConfig:
    host: str
    keyspace: str
    port: int
    local_data_center: str


class ScyllaDBConnector:
    def __init__(self): ...

    def load_config(self) -> ScyllaConfig:
        try:
            scylla_config = ScyllaConfig(
                host="127.0.0.1",
                keyspace="my_keyspace",
                port=9042,
                local_data_center="datacenter1",
            )
        except Exception as e:
            raise e
        return scylla_config


class DataStorage:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.config: ScyllaConfig = ScyllaDBConnector().load_config()
        self.connect()

    def connect(self):
        try:
            self.cluster = Cluster(
                contact_points=[self.config.host],
                port=self.config.port,
                load_balancing_policy=DCAwareRoundRobinPolicy(
                    local_dc=self.config.local_data_center
                ),
                protocol_version=4,
                compression=True,
            )
            self.session = self.cluster.connect(self.config.keyspace)
            self.session.default_consistency_level = ConsistencyLevel.QUORUM

        except Exception as e:
            raise e

    def get_data(self) -> List[Tuple[str, str]]:
        try:
            query = """
            SELECT * FROM data_storage;
            """
            rows = self.session.execute(query)
            queued_event_data = []
            for row in rows:
                queued_event_data.append((row["id"], row["json_payload"]))
            return queued_event_data

        except Exception as e:
            raise e

    def delete_data(self, record_id: str):
        try:
            query = """
            DELETE FROM data_storage WHERE id = %s;
            """
            self.session.execute(query, (record_id,))
        except Exception as e:
            raise e

    def decrease_counter(self, record_id: str) -> None:
        try:
            update_query = """
            UPDATE counter_table SET c = c - 1 WHERE id = %s;
            """
            self.session.execute(update_query, (record_id,))
        except Exception as e:
            raise e

    def close(self):
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()


class TestExistingCode(unittest.TestCase):
    @mock_scylladb
    def test_repository_can_connect_and_operate(self):
        repo = DataStorage()
        self.assertIsNotNone(repo.session)

        repo.session.execute(
            "CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
        )

        repo.session.execute("""
            CREATE TABLE my_keyspace.data_storage (
                id text PRIMARY KEY,
                json_payload text
            );
        """)

        repo.session.execute("""
            CREATE TABLE my_keyspace.counter_table (
                id text PRIMARY KEY,
                c counter
            );
        """)

        repo.session.execute("""
            INSERT INTO my_keyspace.data_storage (id, json_payload)
            VALUES ('1', '{"key": "value"}');
        """)

        data = repo.get_data()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0][0], "1")
        self.assertEqual(data[0][1], '{"key": "value"}')

        repo.delete_data("1")
        data = repo.get_data()
        self.assertEqual(len(data), 0)

        repo.session.execute("""
            UPDATE my_keyspace.counter_table SET c = c + 5 WHERE id = '1';
        """)
        repo.decrease_counter("1")

        counter_value = (
            repo.session.execute(
                "SELECT c FROM my_keyspace.counter_table WHERE id = '1'"
            )
            .one()
            .c
        )
        self.assertEqual(counter_value, 4)


if __name__ == "__main__":
    unittest.main()
