from functools import wraps
from unittest.mock import patch

from mockylla.parser import handle_query


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
                },
                "types": {},
            }
        }

    def reset(self):
        """Resets the state to a clean slate."""
        self.__init__()


_global_state = None


class MockScyllaDB:
    def __init__(self):
        self.patcher = patch(CONNECTION_FACTORY_PATH)
        self.state = ScyllaState()

    def __enter__(self):
        self.state.reset()
        _set_global_state(self.state)

        self.patcher.start()

        def mock_cluster_connect(cluster_self, keyspace=None, *args, **kwargs):
            """A mock replacement for Cluster.connect() that correctly handles the instance.

            The real driver's ``Cluster.connect`` method signature can vary between
            releases (it may include parameters such as ``wait_for_all_pools`` or
            ``execution_profile``). Accepting *args and **kwargs makes the mock
            resilient to such changes while still focusing on the *keyspace*
            argument that we care about.
            """

            if keyspace is None and args:
                keyspace = args[0]

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


class MockCluster:
    """Placeholder for potential cluster-level behaviour."""

    def shutdown(self):
        """Maintain parity with driver Cluster.shutdown()."""
        print("MockCluster shutdown called")


class MockSession:
    def __init__(self, keyspace=None, state=None):
        if state is None:
            raise ValueError(
                "MockSession must be initialized with a state object."
            )
        self.keyspace = keyspace
        self.state = state
        self._is_shutdown = False
        print(f"Set keyspace to: {keyspace}")

    def set_keyspace(self, keyspace):
        """Sets the current keyspace for the session."""
        self._ensure_open()
        if keyspace not in self.state.keyspaces:
            raise Exception(f"Keyspace '{keyspace}' does not exist")
        self.keyspace = keyspace
        print(f"Set keyspace to: {keyspace}")

    def execute(
        self,
        query,
        parameters=None,
        execution_profile=None,
        **kwargs,
    ):
        """Executes a CQL query against the in-memory mock.

        Only *query* and *parameters* are used by the mock implementation. All
        additional keyword arguments (such as *execution_profile*, *timeout*,
        etc.) are accepted for compatibility with the real ScyllaDB/DataStax
        driver but are currently ignored.
        """

        self._ensure_open()
        print(
            f"MockSession execute called with query: {query}; "
            f"execution_profile={execution_profile}"
        )

        return handle_query(query, self, self.state, parameters=parameters)

    def shutdown(self):
        """Release session resources and prevent further queries."""

        if self._is_shutdown:
            return
        self._is_shutdown = True
        print("MockSession shutdown called")

    close = shutdown

    @property
    def is_shutdown(self):
        return self._is_shutdown

    def _ensure_open(self):
        if self._is_shutdown:
            raise RuntimeError(
                "MockSession has been shut down; create a new session if needed."
            )


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


def get_types(keyspace_name):
    """Returns a dictionary of the created types for a given keyspace."""
    if _global_state is None:
        raise Exception("Mock is not active.")
    if keyspace_name not in _global_state.keyspaces:
        raise Exception(
            f"Keyspace '{keyspace_name}' does not exist in mock state."
        )
    return _global_state.keyspaces[keyspace_name].get("types", {})
