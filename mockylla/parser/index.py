from cassandra import InvalidRequest

from mockylla.parser.utils import get_table


def handle_create_index(match, session, state):
    index_name, table_name_full, column_str = match.groups()
    index_name = index_name or _generate_index_name(table_name_full, column_str)
    target_column = column_str.strip()

    keyspace_name, table_name, table_info = get_table(
        table_name_full, session, state
    )

    indexes = table_info.setdefault("indexes", [])
    existing = next(
        (idx for idx in indexes if idx["name"].lower() == index_name.lower()),
        None,
    )
    if existing:
        return []

    if target_column not in table_info.get("schema", {}):
        raise InvalidRequest(
            f"Column '{target_column}' does not exist on table '{table_name_full}'"
        )

    indexes.append({"name": index_name, "column": target_column})
    state.update_system_schema()
    return []


def _generate_index_name(table_name_full, column_str):
    sanitized = column_str.strip().replace(" ", "_")
    table_part = table_name_full.replace(".", "_")
    return f"{table_part}_{sanitized}_idx"
