from pyscylladb_mock.parser.utils import (
    get_table,
    parse_where_clause,
    check_row_conditions,
)


def handle_delete_from(delete_match, session, state):
    table_name_full, where_clause_str = delete_match.groups()

    # Get table info
    keyspace_name, table_name, table_info = get_table(
        table_name_full, session, state
    )
    table_data = table_info["data"]
    schema = table_info["schema"]

    # Parse WHERE clause to find rows to delete
    if not where_clause_str:
        # For safety, don't allow DELETE without a valid WHERE clause condition
        return []

    parsed_conditions = parse_where_clause(where_clause_str, schema)
    if not parsed_conditions:
        # For safety, don't allow DELETE without a valid WHERE clause condition
        return []

    rows_to_keep = [
        row
        for row in table_data
        if not check_row_conditions(row, parsed_conditions)
    ]

    deleted_count = len(table_data) - len(rows_to_keep)
    state.keyspaces[keyspace_name]["tables"][table_name]["data"] = rows_to_keep

    print(f"Deleted {deleted_count} rows from '{table_name}'")
    return []
