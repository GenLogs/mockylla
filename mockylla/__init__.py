import re
from collections.abc import Mapping, Sequence
from functools import wraps
from unittest.mock import patch

from cassandra import InvalidRequest
from cassandra.query import BatchStatement as DriverBatchStatement
from cassandra.query import Statement as DriverStatement

from mockylla.parser import handle_query
from mockylla.results import ResultSet


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
                        "indexes": [],
                    }
                },
                "types": {},
                "replication": {
                    "class": "SimpleStrategy",
                    "replication_factor": "1",
                },
                "durable_writes": True,
            }
        }
        self._ensure_system_schema_structure()
        self.update_system_schema()

    def reset(self):
        """Resets the state to a clean slate."""
        self.__init__()

    def _ensure_system_schema_structure(self):
        if "system_schema" not in self.keyspaces:
            self.keyspaces["system_schema"] = {
                "tables": {
                    "keyspaces": {
                        "schema": {
                            "keyspace_name": "text",
                            "durable_writes": "boolean",
                            "replication": "map<text, text>",
                        },
                        "primary_key": ["keyspace_name"],
                        "data": [],
                    },
                    "tables": {
                        "schema": {
                            "keyspace_name": "text",
                            "table_name": "text",
                        },
                        "primary_key": ["keyspace_name", "table_name"],
                        "data": [],
                    },
                    "columns": {
                        "schema": {
                            "keyspace_name": "text",
                            "table_name": "text",
                            "column_name": "text",
                            "kind": "text",
                            "type": "text",
                        },
                        "primary_key": [
                            "keyspace_name",
                            "table_name",
                            "column_name",
                        ],
                        "data": [],
                    },
                    "indexes": {
                        "schema": {
                            "keyspace_name": "text",
                            "table_name": "text",
                            "index_name": "text",
                            "target": "text",
                        },
                        "primary_key": [
                            "keyspace_name",
                            "table_name",
                            "index_name",
                        ],
                        "data": [],
                    },
                },
                "types": {},
                "replication": {
                    "class": "SimpleStrategy",
                    "replication_factor": "1",
                },
                "durable_writes": True,
            }

    def update_system_schema(self):
        self._ensure_system_schema_structure()

        system_schema_tables = self.keyspaces["system_schema"]["tables"]
        keyspaces_rows = []
        tables_rows = []
        columns_rows = []
        indexes_rows = []

        for keyspace_name, keyspace_info in self.keyspaces.items():
            replication = {
                str(k): str(v)
                for k, v in keyspace_info.get("replication", {}).items()
            }
            if not replication:
                replication = {
                    "class": "SimpleStrategy",
                    "replication_factor": "1",
                }

            keyspaces_rows.append(
                {
                    "keyspace_name": keyspace_name,
                    "durable_writes": keyspace_info.get("durable_writes", True),
                    "replication": replication,
                }
            )

            tables = keyspace_info.get("tables", {})
            for table_name, table_info in tables.items():
                tables_rows.append(
                    {
                        "keyspace_name": keyspace_name,
                        "table_name": table_name,
                    }
                )

                schema = table_info.get("schema", {})
                primary_key = table_info.get("primary_key", [])
                partition_keys = primary_key[:1]
                clustering_keys = primary_key[1:]

                for column_name, data_type in schema.items():
                    if column_name in partition_keys:
                        kind = "partition_key"
                    elif column_name in clustering_keys:
                        kind = "clustering"
                    else:
                        kind = "regular"

                    columns_rows.append(
                        {
                            "keyspace_name": keyspace_name,
                            "table_name": table_name,
                            "column_name": column_name,
                            "kind": kind,
                            "type": data_type,
                        }
                    )

                for index in table_info.get("indexes", []) or []:
                    indexes_rows.append(
                        {
                            "keyspace_name": keyspace_name,
                            "table_name": table_name,
                            "index_name": index.get("name"),
                            "target": index.get("column"),
                        }
                    )

        system_schema_tables["keyspaces"]["data"] = keyspaces_rows
        system_schema_tables["tables"]["data"] = tables_rows
        system_schema_tables["columns"]["data"] = columns_rows
        system_schema_tables["indexes"]["data"] = indexes_rows


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
    """
    Decorator to mock scylla-driver connections.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        with MockScyllaDB():
            return func(*args, **kwargs)

    return wrapper


class MockPreparedStatement:
    """Minimal prepared statement representation."""

    def __init__(self, query_string, session):
        self._original_query = query_string
        self._internal_query = _normalise_placeholders(query_string)
        self._param_order = _extract_parameter_order(query_string)
        self.keyspace = session.keyspace
        self.session = session

    @property
    def query_string(self):
        return self._original_query

    @property
    def param_order(self):
        return self._param_order

    def bind(self, values=None):
        return MockBoundStatement(self, values)


class MockBoundStatement:
    """Represents a prepared statement bound with positional values."""

    def __init__(self, prepared_statement, values=None):
        self.prepared_statement = prepared_statement
        self._internal_query = prepared_statement._internal_query
        self._param_order = prepared_statement.param_order
        self._values = _coerce_parameters(values, self._param_order)

    @property
    def values(self):
        return self._values

    @property
    def query_string(self):
        return self.prepared_statement.query_string


class MockBatchStatement:
    """Lightweight batch statement for grouping CQL commands."""

    def __init__(self, batch_type="LOGGED"):
        self.batch_type = batch_type
        self.consistency_level = None
        self._statements = []

    def add(self, statement, parameters=None):
        self._statements.append((statement, parameters))

    def add_all(self, statements):
        for statement, parameters in statements:
            self.add(statement, parameters)

    def clear(self):
        self._statements.clear()

    @property
    def statements_and_parameters(self):
        return list(self._statements)


class MockMetadata:
    """Minimal metadata facade mirroring cassandra.cluster.Metadata."""

    def __init__(self, state):
        self._state = state

    @property
    def keyspaces(self):
        return {
            name: MockKeyspaceMetadata(name, info)
            for name, info in self._state.keyspaces.items()
        }

    def get_keyspace(self, name):
        return self.keyspaces.get(name)

    def refresh(self):
        return self


class MockKeyspaceMetadata:
    """Represents keyspace metadata."""

    def __init__(self, name, info):
        self.name = name
        self.durable_writes = info.get("durable_writes", True)
        self.replication_strategy = info.get("replication", {})
        self.tables = {
            table_name: MockTableMetadata(name, table_name, table_info)
            for table_name, table_info in info.get("tables", {}).items()
        }
        self.user_types = info.get("types", {})

    def table(self, name):
        return self.tables.get(name)


class MockTableMetadata:
    """Represents table metadata."""

    def __init__(self, keyspace_name, table_name, table_info):
        self.keyspace = keyspace_name
        self.name = table_name
        schema = table_info.get("schema", {})
        self.columns = {
            column_name: MockColumnMetadata(column_name, column_type)
            for column_name, column_type in schema.items()
        }
        primary_key = table_info.get("primary_key", [])
        self.partition_key = [
            self.columns[col] for col in primary_key[:1] if col in self.columns
        ]
        self.clustering_key = [
            self.columns[col] for col in primary_key[1:] if col in self.columns
        ]
        self.primary_key = [
            self.columns[col] for col in primary_key if col in self.columns
        ]
        self.indexes = [
            {
                "name": idx.get("name"),
                "column": idx.get("column"),
            }
            for idx in table_info.get("indexes", []) or []
        ]

    def column(self, name):
        return self.columns.get(name)


class MockColumnMetadata:
    """Represents column metadata."""

    def __init__(self, name, cql_type):
        self.name = name
        self.cql_type = cql_type
        self.typestring = cql_type


class MockResponseFuture:
    """Simple future-like wrapper for execute_async."""

    def __init__(self, result):
        self._result = result
        self._cancelled = False

    def result(self, timeout=None):  # noqa: ARG002 (parity with driver)
        return self._result

    def add_callbacks(self, callback=None, errback=None):
        if callback:
            callback(self._result)
        return self

    def add_callback(self, callback):
        if callback:
            callback(self._result)
        return self

    def add_errback(self, errback):
        return self

    def exception(self, timeout=None):  # noqa: ARG002
        return None

    def cancel(self):
        self._cancelled = True
        return False

    def cancelled(self):
        return self._cancelled

    def done(self):
        return True


class MockCluster:
    """Placeholder for potential cluster-level behaviour."""

    def shutdown(self):
        """Maintain parity with driver Cluster.shutdown()."""
        print("MockCluster shutdown called")


class MockSession:
    def __init__(self, *, keyspace=None, state=None, cluster=None):
        if state is None:
            raise ValueError(
                "MockSession must be initialized with a state object."
            )
        self.keyspace = keyspace
        self.state = state
        self.cluster = cluster
        self.row_factory = None
        self.default_timeout = None
        self._is_shutdown = False
        self._prepared_statements = []
        print(f"Set keyspace to: {keyspace}")

    def set_keyspace(self, keyspace):
        """Sets the current keyspace for the session."""
        self._ensure_open()
        if keyspace not in self.state.keyspaces:
            raise InvalidRequest(f"Keyspace '{keyspace}' does not exist")
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

        if isinstance(query, (DriverBatchStatement, MockBatchStatement)):
            return self._execute_batch_statement(
                query,
                execution_profile=execution_profile,
                parameters=parameters,
                **kwargs,
            )

        query_string, bound_values = self._normalise_query_input(
            query, parameters
        )

        print(
            f"MockSession execute called with query: {query_string}; "
            f"execution_profile={execution_profile}"
        )

        return self._run_query(
            query_string,
            bound_values,
            execution_profile=execution_profile,
            **kwargs,
        )

    def execute_async(
        self,
        query,
        parameters=None,
        execution_profile=None,
        **kwargs,
    ):
        """Asynchronous execute analogue returning a future-like object."""

        result = self.execute(
            query,
            parameters=parameters,
            execution_profile=execution_profile,
            **kwargs,
        )
        return MockResponseFuture(result)

    def prepare(self, query):
        """Prepare a CQL statement for later execution."""

        self._ensure_open()
        prepared = MockPreparedStatement(query, session=self)
        self._prepared_statements.append(prepared)
        return prepared

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

    def _normalise_query_input(self, query, parameters):
        if isinstance(query, MockBoundStatement):
            return query._internal_query, query.values
        if isinstance(query, MockPreparedStatement):
            return query._internal_query, _coerce_parameters(
                parameters, query.param_order
            )
        if isinstance(query, DriverStatement):
            return _normalise_placeholders(query.query_string), parameters
        return query, parameters

    def _run_query(self, query, parameters, **kwargs):
        return handle_query(query, self, self.state, parameters=parameters)

    def _execute_batch_statement(
        self, batch, *, execution_profile=None, parameters=None, **kwargs
    ):
        del parameters  # Not supported for driver batches
        last_result = None

        for statement, bound_params in _iter_batch_items(batch):
            query_string, values = self._normalise_query_input(
                statement, bound_params
            )
            last_result = self._run_query(
                query_string,
                values,
                execution_profile=execution_profile,
                **kwargs,
            )

        return last_result if last_result is not None else ResultSet([])


def _normalise_placeholders(query):
    """Replace question-mark placeholders with %s for internal parsing."""

    return re.sub(r"\?", "%s", query)


def _extract_parameter_order(query):
    query_clean = " ".join(query.strip().split())
    order = []

    insert_match = re.search(
        r"INSERT\s+INTO\s+[^\(]+\(([^\)]+)\)\s+VALUES\s*\(([^\)]+)\)",
        query_clean,
        flags=re.IGNORECASE,
    )
    if insert_match:
        columns = [col.strip() for col in insert_match.group(1).split(",")]
        placeholders = insert_match.group(2).count("?")
        if placeholders == len(columns):
            return columns

    update_match = re.search(
        r"SET\s+(.+?)\s+WHERE\s+(.+)", query_clean, flags=re.IGNORECASE
    )
    if update_match:
        set_part, where_part = update_match.groups()
        for assignment in set_part.split(","):
            left, _, right = assignment.partition("=")
            if "?" in right:
                order.append(left.strip())
        order.extend(_extract_where_parameters(where_part))
        return order

    select_match = re.search(
        r"WHERE\s+(.+?)(?:\s+ORDER\b|\s+LIMIT\b|\s+ALLOW\b|$)",
        query_clean,
        flags=re.IGNORECASE,
    )
    if select_match:
        order.extend(_extract_where_parameters(select_match.group(1)))
        return order

    delete_match = re.search(
        r"DELETE\s+.*?FROM\s+.+?WHERE\s+(.+)",
        query_clean,
        flags=re.IGNORECASE,
    )
    if delete_match:
        order.extend(_extract_where_parameters(delete_match.group(1)))
        return order

    return order


def _extract_where_parameters(where_clause):
    order = []
    for condition in re.split(r"\s+AND\s+", where_clause, flags=re.IGNORECASE):
        if "?" not in condition:
            continue
        match = re.match(r"\s*(\w+)", condition)
        if match:
            order.append(match.group(1))
    return order


def _coerce_parameters(values, param_order=None):
    if values is None:
        return None
    if isinstance(values, MockBoundStatement):
        return values.values
    if isinstance(values, Mapping):
        if not param_order:
            return tuple(values[key] for key in values.keys())
        missing = [name for name in param_order if name not in values]
        if missing:
            missing_str = ", ".join(missing)
            raise ValueError(
                f"Missing parameters for prepared statement: {missing_str}"
            )
        ordered = tuple(values[name] for name in param_order)
        extras = set(values.keys()) - set(param_order)
        if extras:
            extra_str = ", ".join(sorted(extras))
            raise ValueError(
                f"Unexpected parameters for prepared statement: {extra_str}"
            )
        return ordered
    if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
        return tuple(values)
    return (values,)


def _iter_batch_items(batch):
    if isinstance(batch, MockBatchStatement):
        for statement, params in batch.statements_and_parameters:
            yield statement, params
        return

    if isinstance(batch, DriverBatchStatement):
        entries = getattr(batch, "_statements_and_parameters", [])
        for _, statement, params in entries:
            if isinstance(statement, MockBoundStatement):
                yield statement, statement.values
            else:
                yield statement, params or None


def _set_global_state(state):
    """Sets the global state for the mock."""
    global _global_state
    _global_state = state


def get_keyspaces():
    """Returns a dictionary of the created keyspaces in the mock state."""
    if _global_state is None:
        raise InvalidRequest("Mock is not active.")
    return _global_state.keyspaces


def get_tables(keyspace_name):
    """Returns a dictionary of the created tables for a given keyspace."""
    if _global_state is None:
        raise InvalidRequest("Mock is not active.")
    if keyspace_name not in _global_state.keyspaces:
        raise InvalidRequest(
            f"Keyspace '{keyspace_name}' does not exist in mock state."
        )
    return _global_state.keyspaces[keyspace_name]["tables"]


def get_table_rows(keyspace_name, table_name):
    """Returns a list of rows for a given table in a keyspace."""
    tables = get_tables(keyspace_name)
    if table_name not in tables:
        raise InvalidRequest(
            f"Table '{table_name}' does not exist in keyspace '{keyspace_name}'."
        )
    return tables[table_name]["data"]


def get_types(keyspace_name):
    """Returns a dictionary of the created types for a given keyspace."""
    if _global_state is None:
        raise InvalidRequest("Mock is not active.")
    if keyspace_name not in _global_state.keyspaces:
        raise InvalidRequest(
            f"Keyspace '{keyspace_name}' does not exist in mock state."
        )
    return _global_state.keyspaces[keyspace_name].get("types", {})
