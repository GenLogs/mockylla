from pyscylladb_mock.parser.utils import (
    get_table,
    parse_where_clause,
    check_row_conditions,
)


def handle_delete_from(delete_match, session, state, parameters=None):
    table_name_full, where_clause_str = delete_match.groups()

    # Get table info
    keyspace_name, table_name, table_info = get_table(
        table_name_full, session, state
    )
    table_data = table_info["data"]
    schema = table_info["schema"]

    # If parameters are provided, substitute them into the WHERE clause
    if parameters:
        query_parts = where_clause_str.split("%s")
        if len(query_parts) - 1 != len(parameters):
            raise ValueError(
                "Number of parameters does not match number of placeholders in WHERE clause"
            )

        final_where = query_parts[0]
        for i, param in enumerate(parameters):
            # Quote string parameters for correct parsing later
            param_str = f"'{param}'" if isinstance(param, str) else str(param)
            final_where += param_str + query_parts[i + 1]
        where_clause_str = final_where

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
