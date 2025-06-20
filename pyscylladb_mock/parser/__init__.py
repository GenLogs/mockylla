import re

from pyscylladb_mock.results import ResultSet
from pyscylladb_mock.parser.alter import handle_alter_table
from pyscylladb_mock.parser.batch import handle_batch
from pyscylladb_mock.parser.create import (
    handle_create_keyspace,
    handle_create_table,
)
from pyscylladb_mock.parser.delete import handle_delete_from
from pyscylladb_mock.parser.drop import handle_drop_table
from pyscylladb_mock.parser.insert import handle_insert_into
from pyscylladb_mock.parser.select import handle_select_from
from pyscylladb_mock.parser.truncate import handle_truncate_table
from pyscylladb_mock.parser.update import handle_update
from pyscylladb_mock.parser.type import handle_create_type


def handle_query(query, session, state, parameters=None):
    """
    Parses and handles a CQL query.
    """
    query = query.strip()

    # BATCH statement parser
    batch_match = re.match(
        r"^\s*BEGIN\s+BATCH\s+(.*?)\s+APPLY\s+BATCH\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if batch_match:
        handle_batch(batch_match, session, state, parameters=parameters)
        return ResultSet([])

    # Simple CREATE KEYSPACE parser
    create_keyspace_match = re.match(
        r"^\s*CREATE\s+KEYSPACE\s+(?:IF NOT EXISTS\s+)?(\w+)\s+WITH\s+REPLICATION\s*=\s*({.*})\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if create_keyspace_match:
        handle_create_keyspace(create_keyspace_match, state)
        return ResultSet([])

    # Simple CREATE TABLE parser
    create_table_match = re.match(
        r"^\s*CREATE\s+TABLE\s+(?:IF NOT EXISTS\s+)?([\w\.]+)\s*\((.*)\)\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,  # DOTALL allows . to match newlines
    )
    if create_table_match:
        handle_create_table(create_table_match, session, state)
        return ResultSet([])

    # Simple CREATE TYPE parser
    create_type_match = re.match(
        r"^\s*CREATE\s+TYPE\s+(?:IF NOT EXISTS\s+)?([\w\.]+)\s*\((.*)\)\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if create_type_match:
        handle_create_type(create_type_match, session, state)
        return ResultSet([])

    # Simple INSERT INTO parser
    insert_match = re.match(
        r"^\s*INSERT\s+INTO\s+([\w\.]+)\s*\(([\w\s,]+)\)\s+VALUES\s*\((.*)\)\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if insert_match:
        handle_insert_into(insert_match, session, state, parameters=parameters)
        return ResultSet([])

    select_match = re.match(
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
    if select_match:
        rows = handle_select_from(select_match, session, state)
        return ResultSet(rows)

    update_match = re.match(
        r"^\s*UPDATE\s+([\w\.]+)\s+SET\s+(.*)\s+WHERE\s+(.*)\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if update_match:
        handle_update(update_match, session, state)
        return ResultSet([])

    delete_match = re.match(
        r"^\s*DELETE\s+FROM\s+([\w\.]+)\s+WHERE\s+(.*)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if delete_match:
        handle_delete_from(delete_match, session, state, parameters=parameters)
        return ResultSet([])

    drop_table_match = re.match(
        r"^\s*DROP\s+TABLE\s+(?:IF EXISTS\s+)?([\w\.]+)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if drop_table_match:
        handle_drop_table(drop_table_match, session, state)
        return ResultSet([])

    truncate_table_match = re.match(
        r"^\s*TRUNCATE\s+(?:TABLE\s+)?([\w\.]+)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if truncate_table_match:
        handle_truncate_table(truncate_table_match, session, state)
        return ResultSet([])

    alter_table_match = re.match(
        r"^\s*ALTER\s+TABLE\s+([\w\.]+)\s+ADD\s+([\w\s,]+)\s+([\w\s,]+)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if alter_table_match:
        handle_alter_table(alter_table_match, session, state)
        return ResultSet([])

    return f"Error: Unsupported query: {query}"
