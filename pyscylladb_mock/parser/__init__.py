import re

from .create import handle_create_keyspace, handle_create_table
from .delete import handle_delete_from
from .drop import handle_drop_table
from .insert import handle_insert_into
from .select import handle_select_from
from .update import handle_update


def handle_query(query, session, state):
    """
    Parses and handles a CQL query.
    """
    query = query.strip()

    # Simple CREATE KEYSPACE parser
    create_keyspace_match = re.match(
        r"^\s*CREATE\s+KEYSPACE\s+(?:IF NOT EXISTS\s+)?(\w+)\s+WITH\s+REPLICATION\s*=\s*({.*})\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if create_keyspace_match:
        return handle_create_keyspace(create_keyspace_match, state)

    # Simple CREATE TABLE parser
    create_table_match = re.match(
        r"^\s*CREATE\s+TABLE\s+(?:IF NOT EXISTS\s+)?([\w\.]+)\s*\((.*)\)\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,  # DOTALL allows . to match newlines
    )
    if create_table_match:
        return handle_create_table(create_table_match, session, state)

    # Simple INSERT INTO parser
    insert_match = re.match(
        r"^\s*INSERT\s+INTO\s+([\w\.]+)\s*\(([\w\s,]+)\)\s+VALUES\s*\((.*)\)\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if insert_match:
        return handle_insert_into(insert_match, session, state)

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
        return handle_select_from(select_match, session, state)

    update_match = re.match(
        r"^\s*UPDATE\s+([\w\.]+)\s+SET\s+(.*)\s+WHERE\s+(.*)\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if update_match:
        return handle_update(update_match, session, state)

    delete_match = re.match(
        r"^\s*DELETE\s+FROM\s+([\w\.]+)\s+WHERE\s+(.*)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if delete_match:
        return handle_delete_from(delete_match, session, state)

    drop_table_match = re.match(
        r"^\s*DROP\s+TABLE\s+(?:IF EXISTS\s+)?([\w\.]+)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if drop_table_match:
        return handle_drop_table(drop_table_match, session, state)

    return []
