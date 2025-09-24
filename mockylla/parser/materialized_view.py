import re

from cassandra import InvalidRequest

from mockylla.parser.utils import (
    get_keyspace_and_name,
    get_table,
    parse_with_options,
)


def handle_create_materialized_view(match, session, state):
    (
        view_name_full,
        select_clause,
        base_table_full,
        where_clause,
        primary_key_str,
        options_str,
    ) = match.groups()

    keyspace_name, view_name = get_keyspace_and_name(
        view_name_full, session.keyspace
    )
    base_keyspace_name, base_table_name, _ = get_table(
        base_table_full, session, state
    )

    if keyspace_name not in state.keyspaces:
        raise InvalidRequest(f"Keyspace '{keyspace_name}' does not exist")

    views = state.keyspaces[keyspace_name].setdefault("views", {})
    if view_name in views:
        if "IF NOT EXISTS" in match.string.upper():
            return []
        raise InvalidRequest(
            f"Materialized view '{view_name_full}' already exists"
        )

    primary_key = [
        col.strip() for col in re.split(r",", primary_key_str) if col.strip()
    ]

    options = parse_with_options(options_str)

    views[view_name] = {
        "select": select_clause.strip(),
        "base_table": base_table_name,
        "base_keyspace": base_keyspace_name,
        "where_clause": where_clause.strip(),
        "primary_key": primary_key,
        "options": options,
    }

    state.update_system_schema()
    print(
        f"Created materialized view '{view_name}' on base table '{base_table_name}'"
    )
    return []


def handle_drop_materialized_view(match, session, state):
    view_name_full = match.group(1)
    keyspace_name, view_name = get_keyspace_and_name(
        view_name_full, session.keyspace
    )

    if keyspace_name not in state.keyspaces:
        if "IF EXISTS" in match.string.upper():
            return []
        raise InvalidRequest(f"Keyspace '{keyspace_name}' does not exist")

    views = state.keyspaces[keyspace_name].setdefault("views", {})
    if view_name not in views:
        if "IF EXISTS" in match.string.upper():
            return []
        raise InvalidRequest(
            f"Materialized view '{view_name_full}' does not exist"
        )

    del views[view_name]
    state.update_system_schema()
    print(
        f"Dropped materialized view '{view_name}' in keyspace '{keyspace_name}'"
    )
    return []
