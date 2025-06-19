from functools import wraps
from unittest.mock import patch

from pyscylladb_mock.parser import handle_query

# This is the path to the Connection factory in the scylla-driver
CONNECTION_FACTORY_PATH = "cassandra.connection.Connection.factory"


class ScyllaState:
    """Manages the in-memory state of the mock ScyllaDB."""

    def __init__(self):
        self.keyspaces = {
            "system": {
                "tables": {
                    "local": {
                        "schema": {
                            "key": "text",
                            "rpc_address": "inet",
                            "data_center": "text",
                            "rack": "text",
                        },
                        "data": [
                            {
                                "key": "local",
                                "rpc_address": "127.0.0.1",
                                "data_center": "datacenter1",
                                "rack": "rack1",
                            }
                        ],
                    }
                }
            }
        }

    def reset(self):
        """Resets the state to a clean slate."""
        self.__init__()


# Global instance of our mock state
_global_state = None


class MockScyllaDB:
    def __init__(self):
        # We patch the connection factory to prevent real connections
        self.patcher = patch(CONNECTION_FACTORY_PATH)
        self.state = ScyllaState()

    def __enter__(self):
        # Reset the state for each test
        self.state.reset()
        _set_global_state(self.state)

        # When patching starts, any attempt to connect will be blocked
        self.patcher.start()

        # We also need to patch the Cluster's connect method to return our MockSession.
        # The new function needs to accept the cluster instance as its first argument.
        def mock_cluster_connect(cluster_self, keyspace=None):
            """A mock replacement for Cluster.connect() that correctly handles the instance."""
            print(f"MockCluster connect called for keyspace: {keyspace}")
            return MockSession(keyspace=keyspace, state=self.state)

        self.cluster_connect_patcher = patch(
            "cassandra.cluster.Cluster.connect", new=mock_cluster_connect
        )
        self.cluster_connect_patcher.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.patcher.stop()
        self.cluster_connect_patcher.stop()
        _set_global_state(None)


def mock_scylladb(func):
    """
    Decorator to mock scylla-driver connections.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        with MockScyllaDB():
            return func(*args, **kwargs)

    return wrapper


# This class is no longer used for patching but could be useful later.
class MockCluster:
    pass


class MockSession:
    def __init__(self, keyspace=None, state=None):
        if state is None:
            raise ValueError(
                "MockSession must be initialized with a state object."
            )
        self.keyspace = keyspace
        self.state = state

    def set_keyspace(self, keyspace):
        """Sets the current keyspace for the session."""
        if keyspace not in self.state.keyspaces:
            raise Exception(f"Keyspace '{keyspace}' does not exist")
        self.keyspace = keyspace
        print(f"Set keyspace to: {keyspace}")

    def execute(self, query, parameters=None):
        print(f"MockSession execute called with query: {query}")
        return handle_query(query, self, self.state)


# --- Public API for test inspection ---


def _set_global_state(state):
    """Sets the global state for the mock."""
    global _global_state
    _global_state = state


def get_keyspaces():
    """Returns a dictionary of the created keyspaces in the mock state."""
    if _global_state is None:
        raise Exception("Mock is not active.")
    return _global_state.keyspaces


def get_tables(keyspace_name):
    """Returns a dictionary of the created tables for a given keyspace."""
    if _global_state is None:
        raise Exception("Mock is not active.")
    if keyspace_name not in _global_state.keyspaces:
        raise Exception(
            f"Keyspace '{keyspace_name}' does not exist in mock state."
        )
    return _global_state.keyspaces[keyspace_name]["tables"]


def get_table_rows(keyspace_name, table_name):
    """Returns a list of rows for a given table in a keyspace."""
    tables = get_tables(keyspace_name)
    if table_name not in tables:
        raise Exception(
            f"Table '{table_name}' does not exist in keyspace '{keyspace_name}'."
        )
    return tables[table_name]["data"]
