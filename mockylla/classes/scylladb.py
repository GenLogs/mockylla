from functools import wraps
from unittest.mock import patch

from .metadata import MockMetadata
from .session import MockSession
from .state import ScyllaState, _set_global_state


CONNECTION_FACTORY_PATH = "cassandra.connection.Connection.factory"


class MockScyllaDB:
    def __init__(self):
        self.patcher = patch(CONNECTION_FACTORY_PATH)
        self.state = ScyllaState()

    def __enter__(self):
        self.state.reset()
        _set_global_state(self.state)

        self.patcher.start()

        def mock_cluster_connect(cluster_self, keyspace=None, *args, **kwargs):
            """Mock Cluster.connect with signature flexibility."""

            if keyspace is None and args:
                keyspace = args[0]

            print(f"MockCluster connect called for keyspace: {keyspace}")
            session = MockSession(
                keyspace=keyspace,
                state=self.state,
                cluster=cluster_self,
            )
            cluster_self.metadata = MockMetadata(self.state)
            session.metadata = cluster_self.metadata
            return session

        self.cluster_connect_patcher = patch(
            "cassandra.cluster.Cluster.connect", new=mock_cluster_connect
        )
        self.cluster_connect_patcher.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.patcher.stop()
        self.cluster_connect_patcher.stop()
        _set_global_state(None)


def mock_scylladb(func):
    """Decorator to mock scylla-driver connections."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        with MockScyllaDB():
            return func(*args, **kwargs)

    return wrapper


__all__ = ["MockScyllaDB", "mock_scylladb", "CONNECTION_FACTORY_PATH"]
