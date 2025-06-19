from .utils import get_table, parse_where_clause, check_row_conditions


def handle_select_from(select_match, session, state):
    # Parse query components
    (
        columns_str,
        table_name_full,
        where_clause_str,
        order_by_clause_str,
        limit_str,
    ) = select_match.groups()

    # Get table info
    _, table_name, table_info = get_table(table_name_full, session, state)
    table_data = table_info["data"]
    schema = table_info["schema"]

    # Apply filters and get result set
    filtered_data = __apply_where_filters(table_data, where_clause_str, schema)

    # Apply ordering if specified
    if order_by_clause_str:
        filtered_data = __apply_order_by(
            filtered_data, order_by_clause_str, schema
        )

    # Apply limit if specified
    if limit_str:
        filtered_data = __apply_limit(filtered_data, limit_str)

    # Select requested columns
    result_set = __select_columns(filtered_data, columns_str)

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
        raise Exception(f"Invalid ORDER BY direction: {order_dir}")

    if filtered_data and order_col not in schema:
        raise Exception(
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


def __select_columns(filtered_data, columns_str):
    """Select specified columns from filtered data."""
    select_cols_str = columns_str.strip()
    if select_cols_str == "*":
        return filtered_data

    select_cols = [c.strip() for c in select_cols_str.split(",")]
    return [{col: row.get(col) for col in select_cols} for row in filtered_data]
