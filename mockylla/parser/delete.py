from mockylla.parser.utils import (
    get_table,
    parse_where_clause,
    check_row_conditions,
)
from mockylla.row import Row


def handle_delete_from(delete_match, session, state, parameters=None):
    table_name_full, where_clause_str, if_exists = delete_match.groups()

    keyspace_name, table_name, table_info = get_table(
        table_name_full, session, state
    )
    table_data = table_info["data"]
    schema = table_info["schema"]

    if parameters:
        query_parts = where_clause_str.split("%s")
        if len(query_parts) - 1 != len(parameters):
            raise ValueError(
                "Number of parameters does not match number of placeholders in WHERE clause"
            )

        final_where = query_parts[0]
        for i, param in enumerate(parameters):
            param_str = f"'{param}'" if isinstance(param, str) else str(param)
            final_where += param_str + query_parts[i + 1]
        where_clause_str = final_where

    if not where_clause_str:
        return []

    parsed_conditions = parse_where_clause(where_clause_str, schema)
    if not parsed_conditions:
        return []

    rows_to_keep = [
        row
        for row in table_data
        if not check_row_conditions(row, parsed_conditions)
    ]

    deleted_count = len(table_data) - len(rows_to_keep)

    if deleted_count > 0:
        state.keyspaces[keyspace_name]["tables"][table_name]["data"] = (
            rows_to_keep
        )
        print(f"Deleted {deleted_count} rows from '{table_name}'")

    if if_exists:
        return [Row(["[applied]"], [deleted_count > 0])]
    else:
        return []
