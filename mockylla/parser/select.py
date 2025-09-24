import re

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

    select_items = __parse_select_items(columns_str, schema)
    has_aggregates = any(item["type"] == "aggregate" for item in select_items)
    has_non_aggregates = any(
        item["type"] in {"column", "wildcard"} for item in select_items
    )

    if has_aggregates and has_non_aggregates:
        raise InvalidRequest(
            "Cannot mix aggregate and non-aggregate columns without GROUP BY"
        )

    filtered_data = __apply_where_filters(table_data, where_clause_str, schema)

    if has_aggregates:
        if order_by_clause_str:
            raise InvalidRequest(
                "ORDER BY is not supported with aggregate functions"
            )
        if limit_str:
            raise InvalidRequest(
                "LIMIT is not supported with aggregate functions"
            )
        result_set = __select_aggregates(filtered_data, select_items)
    else:
        if order_by_clause_str:
            filtered_data = __apply_order_by(
                filtered_data, order_by_clause_str, schema
            )

        if limit_str:
            filtered_data = __apply_limit(filtered_data, limit_str)

        result_set = __select_columns(filtered_data, select_items, schema)

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


def __parse_select_items(columns_str, schema):
    """Parse the SELECT clause into structured items."""
    select_cols_str = columns_str.strip()
    if not select_cols_str:
        raise InvalidRequest("No columns specified in SELECT clause")

    if select_cols_str == "*":
        return [{"type": "wildcard"}]

    raw_items = [
        part.strip() for part in select_cols_str.split(",") if part.strip()
    ]
    if not raw_items:
        raise InvalidRequest("No columns specified in SELECT clause")

    items = []
    agg_pattern = re.compile(
        r"(count|sum|min|max)\s*\(\s*([^\s\)]+)\s*\)\s*(?:as\s+(\w+)|(\w+))?",
        re.IGNORECASE,
    )

    column_pattern = re.compile(r"(\w+)(?:\s+AS\s+(\w+))?", re.IGNORECASE)

    for raw in raw_items:
        agg_match = agg_pattern.fullmatch(raw)
        if agg_match:
            func = agg_match.group(1).lower()
            argument = agg_match.group(2)
            alias = agg_match.group(3) or agg_match.group(4) or func

            argument = argument.strip()
            if func != "count" and argument in {"*", "1"}:
                raise InvalidRequest(
                    f"Aggregate function '{func}' requires a column argument"
                )

            if argument not in {"*", "1"}:
                argument = __resolve_column_name(argument, schema)

            items.append(
                {
                    "type": "aggregate",
                    "func": func,
                    "arg": argument,
                    "alias": alias.lower(),
                }
            )
            continue

        column_match = column_pattern.fullmatch(raw)
        if column_match:
            column_name = __resolve_column_name(column_match.group(1), schema)
            alias = column_match.group(2)
            items.append(
                {
                    "type": "column",
                    "name": column_name,
                    "alias": (alias or column_name),
                }
            )
            continue

        raise InvalidRequest(f"Unsupported SELECT expression: {raw}")

    return items


def __resolve_column_name(column, schema):
    """Resolve a column name against the schema in a case-insensitive manner."""
    for name in schema.keys():
        if name.lower() == column.lower():
            return name
    raise InvalidRequest(f"Column '{column}' not found in table schema")


def __select_aggregates(filtered_data, select_items):
    """Compute aggregate SELECT expressions."""
    names = [item["alias"] for item in select_items]
    values = [__compute_aggregate(filtered_data, item) for item in select_items]
    return [Row(names=names, values=values)]


def __compute_aggregate(filtered_data, item):
    func = item["func"]
    argument = item["arg"]

    if func == "count":
        if argument in {"*", "1"}:
            return len(filtered_data)
        return sum(1 for row in filtered_data if row.get(argument) is not None)

    values = [
        row.get(argument)
        for row in filtered_data
        if row.get(argument) is not None
    ]

    if func == "sum":
        return sum(values) if values else 0
    if func == "min":
        return min(values) if values else None
    if func == "max":
        return max(values) if values else None

    raise InvalidRequest(f"Unsupported aggregate function '{func}'")


def __select_columns(filtered_data, select_items, schema):
    """Project non-aggregate columns from filtered data."""
    ordered_keys = list(schema.keys())

    if len(select_items) == 1 and select_items[0]["type"] == "wildcard":
        select_cols = ordered_keys
        names = select_cols
    else:
        select_cols = []
        names = []
        for item in select_items:
            if item["type"] != "column":
                raise InvalidRequest("Unsupported SELECT configuration")
            select_cols.append(item["name"])
            names.append(item["alias"])

    result_set = []
    for row_dict in filtered_data:
        values = [row_dict.get(col) for col in select_cols]
        result_set.append(Row(names=names, values=values))

    return result_set
