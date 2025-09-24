from cassandra import InvalidRequest

from .utils import get_table, parse_where_clause, check_row_conditions
from mockylla.row import Row


def handle_select_from(select_match, session, state, parameters=None):
    (
        columns_str,
        table_name_full,
        where_clause_str,
        order_by_clause_str,
        limit_str,
    ) = select_match.groups()

    if parameters and where_clause_str and "%s" in where_clause_str:
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

    _, table_name, table_info = get_table(table_name_full, session, state)
    table_data = table_info["data"]
    schema = table_info["schema"]

    filtered_data = __apply_where_filters(table_data, where_clause_str, schema)

    if order_by_clause_str:
        filtered_data = __apply_order_by(
            filtered_data, order_by_clause_str, schema
        )

    if limit_str:
        filtered_data = __apply_limit(filtered_data, limit_str)

    result_set = __select_columns(filtered_data, columns_str, schema)

    print(f"Selected {len(result_set)} rows from '{table_name}'")
    return result_set


def __apply_where_filters(table_data, where_clause_str, schema):
    """Apply WHERE clause filters to the table data."""
    if not where_clause_str:
        return list(table_data)

    parsed_conditions = parse_where_clause(where_clause_str, schema)
    return [
        row
        for row in table_data
        if check_row_conditions(row, parsed_conditions)
    ]


def __apply_order_by(filtered_data, order_by_clause_str, schema):
    """Apply ORDER BY clause to filtered data."""
    order_by_clause_str = order_by_clause_str.strip()
    parts = order_by_clause_str.split()
    order_col = parts[0]
    order_dir = parts[1].upper() if len(parts) > 1 else "ASC"

    if order_dir not in ["ASC", "DESC"]:
        raise InvalidRequest(f"Invalid ORDER BY direction: {order_dir}")

    if filtered_data and order_col not in schema:
        raise InvalidRequest(
            f"Column '{order_col}' in ORDER BY not found in table schema"
        )

    return sorted(
        filtered_data,
        key=lambda row: row.get(order_col, None),
        reverse=(order_dir == "DESC"),
    )


def __apply_limit(filtered_data, limit_str):
    """Apply LIMIT clause to filtered data."""
    return filtered_data[: int(limit_str)]


def __select_columns(filtered_data, columns_str, schema):
    """Select specified columns from filtered data."""
    select_cols_str = columns_str.strip()
    ordered_keys = list(schema.keys())

    if select_cols_str == "*":
        select_cols = ordered_keys
    else:
        select_cols = [c.strip() for c in select_cols_str.split(",")]

    result_set = []
    for row_dict in filtered_data:
        values = [row_dict.get(col) for col in select_cols]
        result_set.append(Row(names=select_cols, values=values))

    return result_set
