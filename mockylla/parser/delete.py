from mockylla.parser.materialized_view import rebuild_materialized_views
from mockylla.parser.utils import (
    build_lwt_result,
    check_row_conditions,
    get_table,
    parse_lwt_clause,
    parse_where_clause,
    purge_expired_rows,
)


def _normalise_where_clause(where_clause_str, parameters):
    if not parameters:
        return where_clause_str

    query_parts = where_clause_str.split("%s")
    if len(query_parts) - 1 != len(parameters):
        raise ValueError(
            "Number of parameters does not match number of placeholders in WHERE clause"
        )

    final_where = query_parts[0]
    for idx, param in enumerate(parameters):
        param_str = f"'{param}'" if isinstance(param, str) else str(param)
        final_where += param_str + query_parts[idx + 1]
    return final_where


def _select_rows_matching_conditions(table_data, parsed_conditions):
    rows_to_delete = []
    rows_to_keep = []

    for row in table_data:
        if check_row_conditions(row, parsed_conditions):
            rows_to_delete.append(row)
        else:
            rows_to_keep.append(row)

    return rows_to_delete, rows_to_keep


def _apply_lwt_delete(
    condition_type,
    rows_to_delete,
    rows_to_keep,
    lwt_conditions,
    keyspace_name,
    table_name,
    state,
):
    deleted_count = len(rows_to_delete)

    if condition_type == "if_not_exists":
        if deleted_count:
            return [build_lwt_result(False, rows_to_delete[0])], False
        return [build_lwt_result(True)], False

    if condition_type == "if_exists":
        if not deleted_count:
            return [build_lwt_result(False)], False
        state.keyspaces[keyspace_name]["tables"][table_name]["data"] = (
            rows_to_keep
        )
        print(f"Deleted {deleted_count} rows from '{table_name}'")
        return [build_lwt_result(True)], True

    if condition_type == "conditions":
        if not deleted_count:
            return [build_lwt_result(False)], False
        for row in rows_to_delete:
            if not check_row_conditions(row, lwt_conditions):
                return [build_lwt_result(False, row)], False
        state.keyspaces[keyspace_name]["tables"][table_name]["data"] = (
            rows_to_keep
        )
        print(f"Deleted {deleted_count} rows from '{table_name}'")
        return [build_lwt_result(True)], True

    return None, deleted_count > 0


def handle_delete_from(delete_match, session, state, parameters=None):
    table_name_full, where_clause_str, if_clause = delete_match.groups()

    keyspace_name, table_name, table_info = get_table(
        table_name_full, session, state
    )
    purge_expired_rows(table_info)
    table_data = table_info["data"]
    schema = table_info["schema"]

    where_clause_str = _normalise_where_clause(where_clause_str, parameters)

    if not where_clause_str:
        return []

    parsed_conditions = parse_where_clause(where_clause_str, schema)
    if not parsed_conditions:
        return []

    clause_info = parse_lwt_clause(if_clause, schema)
    condition_type = clause_info["type"]
    lwt_conditions = clause_info.get("conditions", [])

    rows_to_delete, rows_to_keep = _select_rows_matching_conditions(
        table_data, parsed_conditions
    )

    result, mutates_table = _apply_lwt_delete(
        condition_type,
        rows_to_delete,
        rows_to_keep,
        lwt_conditions,
        keyspace_name,
        table_name,
        state,
    )

    if result is not None:
        if mutates_table:
            rebuild_materialized_views(state, keyspace_name, table_name)
        return result

    if rows_to_delete:
        state.keyspaces[keyspace_name]["tables"][table_name]["data"] = (
            rows_to_keep
        )
        print(f"Deleted {len(rows_to_delete)} rows from '{table_name}'")
        rebuild_materialized_views(state, keyspace_name, table_name)

    return []
