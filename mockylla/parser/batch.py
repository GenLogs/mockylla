import re

from mockylla.parser.insert import handle_insert_into
from mockylla.parser.update import handle_update
from mockylla.parser.delete import handle_delete_from


def handle_batch(batch_match, session, state, parameters=None):
    """
    Handles a BATCH query by parsing and executing each inner query.
    """
    inner_queries_str = batch_match.group(1).strip()
    queries = [q.strip() for q in inner_queries_str.split(";") if q.strip()]

    for query in queries:
        insert_match = re.match(
            r"^\s*INSERT\s+INTO\s+([\w\.]+)\s*\(([\w\s,]+)\)\s+VALUES\s*\((.*)\)\s*(?:USING\s+(.*?))?\s*(IF\s+NOT\s+EXISTS)?\s*;?\s*$",
            query,
            re.IGNORECASE | re.DOTALL,
        )
        if insert_match:
            handle_insert_into(
                insert_match, session, state, parameters=parameters
            )
            continue

        update_match = re.match(
            r"^\s*UPDATE\s+([\w\.]+)(?:\s+USING\s+(.*?))?\s+SET\s+(.*)\s+WHERE\s+(.*?)\s*(IF\s+EXISTS)?\s*;?\s*$",
            query,
            re.IGNORECASE | re.DOTALL,
        )
        if update_match:
            handle_update(update_match, session, state)
            continue

        delete_match = re.match(
            r"^\s*DELETE\s+FROM\s+([\w\.]+)\s+WHERE\s+(.*?)\s*(IF EXISTS)?\s*;?\s*$",
            query,
            re.IGNORECASE,
        )
        if delete_match:
            handle_delete_from(
                delete_match, session, state, parameters=parameters
            )
            continue
