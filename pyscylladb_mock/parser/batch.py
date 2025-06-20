import re

from pyscylladb_mock.parser.insert import handle_insert_into
from pyscylladb_mock.parser.update import handle_update
from pyscylladb_mock.parser.delete import handle_delete_from


def handle_batch(batch_match, session, state, parameters=None):
    """
    Handles a BATCH query by parsing and executing each inner query.
    """
    inner_queries_str = batch_match.group(1).strip()
    queries = [q.strip() for q in inner_queries_str.split(";") if q.strip()]

    for query in queries:
        # Attempt to match an INSERT statement
        insert_match = re.match(
            r"^\s*INSERT\s+INTO\s+([\w\.]+)\s*\(([\w\s,]+)\)\s+VALUES\s*\((.*)\)\s*$",
            query,
            re.IGNORECASE | re.DOTALL,
        )
        if insert_match:
            handle_insert_into(
                insert_match, session, state, parameters=parameters
            )
            continue

        # Attempt to match an UPDATE statement
        update_match = re.match(
            r"^\s*UPDATE\s+([\w\.]+)\s+SET\s+(.*)\s+WHERE\s+(.*)\s*$",
            query,
            re.IGNORECASE | re.DOTALL,
        )
        if update_match:
            handle_update(update_match, session, state)
            continue

        # Attempt to match a DELETE statement
        delete_match = re.match(
            r"^\s*DELETE\s+FROM\s+([\w\.]+)\s+WHERE\s+(.*)\s*$",
            query,
            re.IGNORECASE,
        )
        if delete_match:
            handle_delete_from(
                delete_match, session, state, parameters=parameters
            )
            continue

        # If no match is found, you might want to raise an error
        # For now, we'll just ignore unsupported statements in a batch
