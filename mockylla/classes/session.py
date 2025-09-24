from cassandra import InvalidRequest
from cassandra.query import BatchStatement as DriverBatchStatement
from cassandra.query import Statement as DriverStatement

from mockylla.parser import handle_query
from mockylla.results import ResultSet

from .statements import (
    MockBatchStatement,
    MockBoundStatement,
    MockPreparedStatement,
    _coerce_parameters,
    _iter_batch_items,
    _normalise_placeholders,
)


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
        """Executes a CQL query against the in-memory mock."""

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


__all__ = ["MockSession", "MockResponseFuture", "MockCluster"]
