import re
from collections.abc import Mapping, Sequence

from cassandra.query import BatchStatement as DriverBatchStatement


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


__all__ = [
    "MockPreparedStatement",
    "MockBoundStatement",
    "MockBatchStatement",
    "_normalise_placeholders",
    "_extract_parameter_order",
    "_coerce_parameters",
    "_iter_batch_items",
]
