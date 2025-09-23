import re

from mockylla.results import ResultSet
from mockylla.parser.alter import handle_alter_table, handle_alter_table_with
from mockylla.parser.batch import handle_batch
from mockylla.parser.create import (
    handle_create_keyspace,
    handle_create_table,
)
from mockylla.parser.delete import handle_delete_from
from mockylla.parser.drop import (
    handle_drop_index,
    handle_drop_keyspace,
    handle_drop_table,
)
from mockylla.parser.insert import handle_insert_into
from mockylla.parser.index import handle_create_index
from mockylla.parser.materialized_view import (
    handle_create_materialized_view,
    handle_drop_materialized_view,
)
from mockylla.parser.select import handle_select_from
from mockylla.parser.truncate import handle_truncate_table
from mockylla.parser.update import handle_update
from mockylla.parser.type import handle_create_type


def handle_query(query, session, state, parameters=None):
    """
    Parses and handles a CQL query.
    """
    query = query.strip()

    handlers = [
        _handle_use,
        _handle_batch,
        _handle_create_keyspace,
        _handle_create_table,
        _handle_create_type,
        _handle_create_materialized_view,
        _handle_insert,
        _handle_select,
        _handle_update,
        _handle_delete,
        _handle_drop_keyspace,
        _handle_drop_table,
        _handle_drop_materialized_view,
        _handle_truncate_table,
        _handle_alter_table,
        _handle_alter_table_with,
        _handle_create_index,
        _handle_drop_index,
    ]

    for handler in handlers:
        result = handler(query, session, state, parameters)
        if result is not None:
            return result

    return f"Error: Unsupported query: {query}"


def _handle_use(query, session, _state, _parameters):
    match = re.match(r"^\s*USE\s+(\w+)\s*;?\s*$", query, re.IGNORECASE)
    if not match:
        return None
    session.set_keyspace(match.group(1))
    return ResultSet([])


def _handle_batch(query, session, state, parameters):
    match = re.match(
        r"^\s*BEGIN\s+BATCH\s+(.*?)\s+APPLY\s+BATCH\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    handle_batch(match, session, state, parameters=parameters)
    return ResultSet([])


def _handle_create_keyspace(query, _session, state, _parameters):
    match = re.match(
        r"^\s*CREATE\s+KEYSPACE\s+(?:IF NOT EXISTS\s+)?(\w+)\s+WITH\s+REPLICATION\s*=\s*({.*})\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if not match:
        return None
    handle_create_keyspace(match, state)
    return ResultSet([])


def _handle_create_table(query, session, state, _parameters):
    match = re.match(
        r"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w\.]+)\s*\((.*)\)\s*(?:WITH\s+(.*))?\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    handle_create_table(match, session, state)
    return ResultSet([])


def _handle_create_type(query, session, state, _parameters):
    match = re.match(
        r"^\s*CREATE\s+TYPE\s+(?:IF NOT EXISTS\s+)?([\w\.]+)\s*\((.*)\)\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    handle_create_type(match, session, state)
    return ResultSet([])


def _handle_create_materialized_view(query, session, state, _parameters):
    match = re.match(
        (
            r"^\s*CREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w\.]+)\s+AS\s+SELECT\s+"
            r"(.*?)\s+FROM\s+([\w\.]+)\s+WHERE\s+(.*?)\s+PRIMARY\s+KEY\s*\((.*?)\)"
            r"\s*(?:WITH\s+(.*))?\s*;?\s*$"
        ),
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    handle_create_materialized_view(match, session, state)
    return ResultSet([])


def _handle_insert(query, session, state, parameters):
    match = re.match(
        r"^\s*INSERT\s+INTO\s+([\w\.]+)\s*\(([\w\s,]+)\)\s+VALUES\s*\((.*)\)\s*(?:USING\s+(.*?))?\s*(IF\s+NOT\s+EXISTS)?\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    result = handle_insert_into(match, session, state, parameters=parameters)
    return ResultSet(result)


def _handle_select(query, session, state, parameters):
    match = re.match(
        (
            r"^\s*SELECT\s+(.*?)\s+FROM\s+([\w\.]+)"
            r"(?:\s+WHERE\s+(.*?))?"
            r"(?:\s+ORDER BY\s+(.*?))?"
            r"(?:\s+LIMIT\s+(\d+))?"
            r"\s*;?\s*$"
        ),
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    rows = handle_select_from(match, session, state, parameters=parameters)
    return ResultSet(rows)


def _handle_update(query, session, state, parameters):
    match = re.match(
        r"^\s*UPDATE\s+([\w\.]+)(?:\s+USING\s+(.*?))?\s+SET\s+(.*)\s+WHERE\s+(.*?)\s*(IF\s+EXISTS)?\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    result = handle_update(match, session, state, parameters=parameters)
    return ResultSet(result)


def _handle_delete(query, session, state, parameters):
    match = re.match(
        r"^\s*DELETE\s+FROM\s+([\w\.]+)\s+WHERE\s+(.*?)\s*(IF EXISTS)?\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if not match:
        return None
    result = handle_delete_from(match, session, state, parameters=parameters)
    return ResultSet(result)


def _handle_drop_keyspace(query, _session, state, _parameters):
    match = re.match(
        r"^\s*DROP\s+KEYSPACE\s+(?:IF\s+EXISTS\s+)?(\w+)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if not match:
        return None
    handle_drop_keyspace(match, state)
    return ResultSet([])


def _handle_drop_table(query, session, state, _parameters):
    match = re.match(
        r"^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([\w\.]+)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if not match:
        return None
    handle_drop_table(match, session, state)
    return ResultSet([])


def _handle_drop_materialized_view(query, session, state, _parameters):
    match = re.match(
        r"^\s*DROP\s+MATERIALIZED\s+VIEW\s+(?:IF\s+EXISTS\s+)?([\w\.]+)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if not match:
        return None
    handle_drop_materialized_view(match, session, state)
    return ResultSet([])


def _handle_truncate_table(query, session, state, _parameters):
    match = re.match(
        r"^\s*TRUNCATE\s+(?:TABLE\s+)?([\w\.]+)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if not match:
        return None
    handle_truncate_table(match, session, state)
    return ResultSet([])


def _handle_alter_table(query, session, state, _parameters):
    match = re.match(
        r"^\s*ALTER\s+TABLE\s+([\w\.]+)\s+ADD\s+([\w\s,]+)\s+([\w\s,]+)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if not match:
        return None
    handle_alter_table(match, session, state)
    return ResultSet([])


def _handle_alter_table_with(query, session, state, _parameters):
    match = re.match(
        r"^\s*ALTER\s+TABLE\s+([\w\.]+)\s+WITH\s+(.*)\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    handle_alter_table_with(match, session, state)
    return ResultSet([])


def _handle_create_index(query, session, state, _parameters):
    match = re.match(
        r"^\s*CREATE\s+(?:CUSTOM\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:([\w]+)\s+)?ON\s+([\w\.]+)\s*\(([^\)]+)\)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if not match:
        return None
    handle_create_index(match, session, state)
    return ResultSet([])


def _handle_drop_index(query, session, state, _parameters):
    match = re.match(
        r"^\s*DROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?([\w\.]+)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if not match:
        return None
    handle_drop_index(match, session, state)
    return ResultSet([])
