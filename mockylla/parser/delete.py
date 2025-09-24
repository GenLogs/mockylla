from mockylla.parser.utils import (
    build_lwt_result,
    check_row_conditions,
    get_table,
    parse_lwt_clause,
    parse_where_clause,
    purge_expired_rows,
)


def handle_delete_from(delete_match, session, state, parameters=None):
    table_name_full, where_clause_str, if_clause = delete_match.groups()

    keyspace_name, table_name, table_info = get_table(
        table_name_full, session, state
    )
    purge_expired_rows(table_info)
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

    clause_info = parse_lwt_clause(if_clause, schema)
    condition_type = clause_info["type"]
    lwt_conditions = clause_info.get("conditions", [])

    rows_to_delete = []
    rows_to_keep = []
    for row in table_data:
        if check_row_conditions(row, parsed_conditions):
            rows_to_delete.append(row)
        else:
            rows_to_keep.append(row)

    deleted_count = len(rows_to_delete)

    if condition_type == "if_not_exists":
        if deleted_count:
            return [build_lwt_result(False, rows_to_delete[0])]
        return [build_lwt_result(True)]

    if condition_type == "if_exists":
        if not deleted_count:
            return [build_lwt_result(False)]
        state.keyspaces[keyspace_name]["tables"][table_name]["data"] = (
            rows_to_keep
        )
        print(f"Deleted {deleted_count} rows from '{table_name}'")
        return [build_lwt_result(True)]

    if condition_type == "conditions":
        if not deleted_count:
            return [build_lwt_result(False)]
        for row in rows_to_delete:
            if not check_row_conditions(row, lwt_conditions):
                return [build_lwt_result(False, row)]
        state.keyspaces[keyspace_name]["tables"][table_name]["data"] = (
            rows_to_keep
        )
        print(f"Deleted {deleted_count} rows from '{table_name}'")
        return [build_lwt_result(True)]

    if deleted_count > 0:
        state.keyspaces[keyspace_name]["tables"][table_name]["data"] = (
            rows_to_keep
        )
        print(f"Deleted {deleted_count} rows from '{table_name}'")

    return []
