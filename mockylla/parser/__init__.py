import re

from mockylla.results import ResultSet
from mockylla.parser.alter import handle_alter_table
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
from mockylla.parser.select import handle_select_from
from mockylla.parser.truncate import handle_truncate_table
from mockylla.parser.update import handle_update
from mockylla.parser.type import handle_create_type


def handle_query(query, session, state, parameters=None):
    """
    Parses and handles a CQL query.
    """
    query = query.strip()

    use_match = re.match(
        r"^\s*USE\s+(\w+)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if use_match:
        session.set_keyspace(use_match.group(1))
        return ResultSet([])

    batch_match = re.match(
        r"^\s*BEGIN\s+BATCH\s+(.*?)\s+APPLY\s+BATCH\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if batch_match:
        handle_batch(batch_match, session, state, parameters=parameters)
        return ResultSet([])

    create_keyspace_match = re.match(
        r"^\s*CREATE\s+KEYSPACE\s+(?:IF NOT EXISTS\s+)?(\w+)\s+WITH\s+REPLICATION\s*=\s*({.*})\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if create_keyspace_match:
        handle_create_keyspace(create_keyspace_match, state)
        return ResultSet([])

    create_table_match = re.match(
        r"^\s*CREATE\s+TABLE\s+(?:IF NOT EXISTS\s+)?([\w\.]+)\s*\((.*)\)\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if create_table_match:
        handle_create_table(create_table_match, session, state)
        return ResultSet([])

    create_type_match = re.match(
        r"^\s*CREATE\s+TYPE\s+(?:IF NOT EXISTS\s+)?([\w\.]+)\s*\((.*)\)\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if create_type_match:
        handle_create_type(create_type_match, session, state)
        return ResultSet([])

    insert_match = re.match(
        r"^\s*INSERT\s+INTO\s+([\w\.]+)\s*\(([\w\s,]+)\)\s+VALUES\s*\((.*)\)\s*(?:USING\s+(.*?))?\s*(IF\s+NOT\s+EXISTS)?\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if insert_match:
        result = handle_insert_into(
            insert_match, session, state, parameters=parameters
        )
        return ResultSet(result)

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
        rows = handle_select_from(
            select_match, session, state, parameters=parameters
        )
        return ResultSet(rows)

    update_match = re.match(
        r"^\s*UPDATE\s+([\w\.]+)(?:\s+USING\s+(.*?))?\s+SET\s+(.*)\s+WHERE\s+(.*?)\s*(IF\s+EXISTS)?\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if update_match:
        result = handle_update(
            update_match, session, state, parameters=parameters
        )
        return ResultSet(result)

    delete_match = re.match(
        r"^\s*DELETE\s+FROM\s+([\w\.]+)\s+WHERE\s+(.*?)\s*(IF EXISTS)?\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if delete_match:
        result = handle_delete_from(
            delete_match, session, state, parameters=parameters
        )
        return ResultSet(result)

    drop_keyspace_match = re.match(
        r"^\s*DROP\s+KEYSPACE\s+(?:IF\s+EXISTS\s+)?(\w+)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if drop_keyspace_match:
        handle_drop_keyspace(drop_keyspace_match, state)
        return ResultSet([])

    drop_table_match = re.match(
        r"^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([\w\.]+)\s*;?\s*$",
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

    create_index_match = re.match(
        r"^\s*CREATE\s+(?:CUSTOM\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:([\w]+)\s+)?ON\s+([\w\.]+)\s*\(([^\)]+)\)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if create_index_match:
        handle_create_index(create_index_match, session, state)
        return ResultSet([])

    drop_index_match = re.match(
        r"^\s*DROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?([\w\.]+)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if drop_index_match:
        handle_drop_index(drop_index_match, session, state)
        return ResultSet([])

    return f"Error: Unsupported query: {query}"
