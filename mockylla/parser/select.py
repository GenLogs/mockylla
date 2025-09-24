import re

from cassandra import InvalidRequest

from .utils import (
    check_row_conditions,
    get_table,
    parse_where_clause,
    purge_expired_rows,
)
from mockylla.row import Row


_AGG_SELECT_PATTERN = re.compile(
    r"(count|sum|min|max|avg)\s*\(\s*(distinct\s+)?([^\s\)]+)\s*\)\s*(?:as\s+(\w+)|(\w+))?",
    re.IGNORECASE,
)

_FUNCTION_SELECT_PATTERN = re.compile(
    r"(writetime|ttl)\s*\(\s*(\w+)\s*\)\s*(?:as\s+(\w+)|(\w+))?",
    re.IGNORECASE,
)

_COLUMN_SELECT_PATTERN = re.compile(
    r"(\w+)(?:\s+AS\s+(\w+))?",
    re.IGNORECASE,
)

_HAVING_SPLIT_PATTERN = re.compile(r"\s+AND\s+", re.IGNORECASE)

_HAVING_PATTERN = re.compile(
    r"(count|sum|min|max|avg)\s*\(\s*(distinct\s+)?([^\s\)]+)\s*\)\s*(=|!=|>=|<=|>|<)\s*(.+)",
    re.IGNORECASE,
)


def handle_select_from(select_match, session, state, parameters=None):
    (
        columns_str,
        table_name_full,
        where_clause_str,
        group_by_clause_str,
        having_clause_str,
        order_by_clause_str,
        limit_str,
    ) = select_match.groups()

    positional_parameters, named_parameters = __normalize_parameters(parameters)
    where_clause_str, positional_parameters = __substitute_where_placeholders(
        where_clause_str, positional_parameters
    )

    limit_value, positional_parameters = __resolve_limit_value(
        limit_str, positional_parameters, named_parameters
    )

    _, table_name, table_info = get_table(table_name_full, session, state)
    purge_expired_rows(table_info)
    table_data = table_info["data"]
    schema = table_info["schema"]

    columns_str, is_distinct = __extract_distinct_clause(columns_str)

    select_items, group_by_columns, having_conditions = (
        __parse_select_components(
            columns_str, group_by_clause_str, having_clause_str, schema
        )
    )
    select_flags = __compute_select_flags(select_items)

    __validate_select_configuration(
        select_items,
        group_by_columns,
        having_conditions,
        order_by_clause_str,
        is_distinct,
        select_flags,
    )

    filtered_data = __apply_where_filters(table_data, where_clause_str, schema)

    result_set = __execute_select_query(
        filtered_data,
        select_items,
        schema,
        select_flags,
        group_by_columns,
        having_conditions,
        order_by_clause_str,
        limit_value,
        is_distinct,
        limit_str is not None,
    )

    print(f"Selected {len(result_set)} rows from '{table_name}'")
    return result_set


def __normalize_parameters(parameters):
    """Split provided parameters into positional and named collections."""
    if parameters is None:
        return [], {}

    if isinstance(parameters, dict):
        return [], parameters

    if isinstance(parameters, (list, tuple)):
        return list(parameters), {}

    return list(parameters), {}


def __substitute_where_placeholders(where_clause_str, positional_parameters):
    """Inline positional parameters into WHERE clause placeholders."""
    if not where_clause_str or "%s" not in where_clause_str:
        return where_clause_str, positional_parameters

    if not positional_parameters:
        raise ValueError(
            "Positional parameters required for WHERE clause placeholders"
        )

    query_parts = where_clause_str.split("%s")
    placeholder_count = len(query_parts) - 1
    if placeholder_count > len(positional_parameters):
        raise ValueError(
            "Number of parameters does not match number of placeholders in WHERE clause"
        )

    final_where = query_parts[0]
    for index in range(placeholder_count):
        param = positional_parameters[index]
        param_str = f"'{param}'" if isinstance(param, str) else str(param)
        final_where += param_str + query_parts[index + 1]

    remaining_parameters = positional_parameters[placeholder_count:]
    return final_where, remaining_parameters


def __extract_distinct_clause(columns_str):
    """Determine DISTINCT usage and return the cleaned columns clause."""
    columns_clause = columns_str.strip()
    lowered = columns_clause.lower()

    if lowered.startswith("distinct "):
        return columns_clause[8:].lstrip(), True
    if lowered == "distinct":
        raise InvalidRequest("SELECT DISTINCT requires at least one column")

    return columns_clause, False


def __parse_select_components(
    columns_str, group_by_clause_str, having_clause_str, schema
):
    """Parse SELECT, GROUP BY, and HAVING clauses for the query."""
    select_items = __parse_select_items(columns_str, schema)
    group_by_columns = __parse_group_by(group_by_clause_str, schema)
    having_conditions = __parse_having_conditions(
        having_clause_str, schema, group_by_columns
    )
    return select_items, group_by_columns, having_conditions


def __compute_select_flags(select_items):
    """Collect common boolean flags from parsed SELECT items."""
    return {
        "has_aggregates": any(
            item["type"] == "aggregate" for item in select_items
        ),
        "has_columns": any(item["type"] == "column" for item in select_items),
        "has_wildcard": any(
            item["type"] == "wildcard" for item in select_items
        ),
        "has_functions": any(
            item["type"] == "function" for item in select_items
        ),
    }


def __validate_select_configuration(
    select_items,
    group_by_columns,
    having_conditions,
    order_by_clause_str,
    is_distinct,
    select_flags,
):
    """Ensure the parsed SELECT request follows Cassandra CQL rules."""
    has_aggregates = select_flags["has_aggregates"]
    has_columns = select_flags["has_columns"]
    has_wildcard = select_flags["has_wildcard"]
    has_functions = select_flags["has_functions"]

    __validate_wildcard_usage(
        has_wildcard, has_aggregates, group_by_columns, is_distinct
    )
    __validate_distinct_usage(is_distinct, has_aggregates, group_by_columns)
    __validate_function_usage(
        has_functions, has_aggregates, group_by_columns, is_distinct
    )
    __validate_aggregate_column_mix(
        has_aggregates, has_columns, group_by_columns
    )
    __validate_group_by_having(
        select_items, group_by_columns, having_conditions
    )
    __validate_order_by_usage(
        has_aggregates, group_by_columns, order_by_clause_str
    )


def __validate_wildcard_usage(
    has_wildcard, has_aggregates, group_by_columns, is_distinct
):
    """Disallow wildcard usage when it conflicts with other clauses."""
    if has_wildcard and (has_aggregates or group_by_columns or is_distinct):
        raise InvalidRequest(
            "SELECT * is not allowed with aggregates, GROUP BY, or DISTINCT"
        )


def __validate_distinct_usage(is_distinct, has_aggregates, group_by_columns):
    """Validate DISTINCT usage alongside aggregates and grouping."""
    if is_distinct and has_aggregates:
        raise InvalidRequest(
            "SELECT DISTINCT cannot be combined with aggregate functions"
        )

    if is_distinct and group_by_columns:
        raise InvalidRequest("SELECT DISTINCT cannot be used with GROUP BY")


def __validate_function_usage(
    has_functions, has_aggregates, group_by_columns, is_distinct
):
    """Ensure special functions appear only in simple SELECT queries."""
    if has_functions and (has_aggregates or group_by_columns or is_distinct):
        raise InvalidRequest(
            "WRITETIME and TTL functions are only supported in simple SELECT queries"
        )


def __validate_aggregate_column_mix(
    has_aggregates, has_columns, group_by_columns
):
    """Guard against mixing aggregate and non-aggregate columns improperly."""
    if has_aggregates and has_columns and not group_by_columns:
        raise InvalidRequest(
            "Cannot mix aggregate and non-aggregate columns without GROUP BY"
        )


def __validate_group_by_having(
    select_items, group_by_columns, having_conditions
):
    """Verify GROUP BY and HAVING clauses are consistent."""
    if group_by_columns:
        __validate_group_by_columns(select_items, group_by_columns)
    elif having_conditions:
        raise InvalidRequest("HAVING clause requires GROUP BY")


def __validate_order_by_usage(
    has_aggregates, group_by_columns, order_by_clause_str
):
    """Check ORDER BY usage with aggregate or grouped queries."""
    if has_aggregates and order_by_clause_str:
        if group_by_columns:
            raise InvalidRequest(
                "ORDER BY is not supported with aggregate functions and GROUP BY"
            )
        raise InvalidRequest(
            "ORDER BY is not supported with aggregate functions"
        )


def __execute_select_query(
    filtered_data,
    select_items,
    schema,
    select_flags,
    group_by_columns,
    having_conditions,
    order_by_clause_str,
    limit_value,
    is_distinct,
    limit_present,
):
    """Run the appropriate SELECT flow based on the parsed query."""
    has_aggregates = select_flags["has_aggregates"]

    if has_aggregates:
        if limit_present:
            raise InvalidRequest(
                "LIMIT is not supported with aggregate functions"
            )
        return __select_with_aggregates(
            filtered_data, select_items, group_by_columns, having_conditions
        )

    if group_by_columns:
        if order_by_clause_str:
            raise InvalidRequest(
                "ORDER BY is not supported with GROUP BY queries"
            )
        result_set = __select_group_by_only(
            filtered_data, select_items, group_by_columns
        )
        if limit_value is not None:
            return result_set[:limit_value]
        return result_set

    if is_distinct:
        if order_by_clause_str:
            raise InvalidRequest(
                "ORDER BY is not supported with SELECT DISTINCT"
            )
        result_rows = __select_columns(filtered_data, select_items, schema)
        result_rows = __deduplicate_rows(result_rows)
        if limit_value is not None:
            result_rows = result_rows[:limit_value]
        return result_rows

    if order_by_clause_str:
        filtered_data = __apply_order_by(
            filtered_data, order_by_clause_str, schema
        )

    if limit_value is not None:
        filtered_data = __apply_limit(filtered_data, limit_value)

    return __select_columns(filtered_data, select_items, schema)


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


def __apply_limit(filtered_data, limit_value):
    """Apply LIMIT clause to filtered data."""
    if limit_value is None:
        return filtered_data
    return filtered_data[:limit_value]


def __coerce_limit(value):
    """Convert a LIMIT value to an integer."""
    if value is None:
        raise InvalidRequest("LIMIT value must be an integer literal")

    literal = str(value).strip()
    if not literal:
        raise InvalidRequest("LIMIT value must be an integer literal")

    try:
        return int(literal)
    except ValueError as exc:
        raise InvalidRequest("LIMIT value must be an integer literal") from exc


def __resolve_limit_value(limit_token, positional_params, named_params):
    """Resolve the LIMIT value from literals or placeholders."""
    if limit_token is None:
        return None, positional_params

    token = limit_token.strip()
    if not token:
        return None, positional_params

    if token.isdigit():
        return __coerce_limit(token), positional_params

    if token in {"%s", "?"}:
        if not positional_params:
            raise InvalidRequest(
                "LIMIT placeholder requires an additional positional parameter"
            )
        value = positional_params[0]
        limit_value = __coerce_limit(value)
        return limit_value, positional_params[1:]

    if token.startswith(":"):
        key = token[1:]
        if key not in named_params:
            raise InvalidRequest(
                f"LIMIT placeholder '{token}' not found in parameters"
            )
        limit_value = __coerce_limit(named_params[key])
        return limit_value, positional_params

    raise InvalidRequest(f"Unsupported LIMIT placeholder '{limit_token}'")


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

    for raw in raw_items:
        item = __parse_aggregate_select(raw, schema)
        if item is not None:
            items.append(item)
            continue

        item = __parse_function_select(raw, schema)
        if item is not None:
            items.append(item)
            continue

        item = __parse_column_select(raw, schema)
        if item is not None:
            items.append(item)
            continue

        raise InvalidRequest(f"Unsupported SELECT expression: {raw}")

    return items


def __parse_aggregate_select(raw, schema):
    """Parse an aggregate select expression if present."""
    match = _AGG_SELECT_PATTERN.fullmatch(raw)
    if not match:
        return None

    func = match.group(1).lower()
    distinct_flag = bool(match.group(2))
    argument = match.group(3).strip()
    alias = match.group(4) or match.group(5) or func

    if distinct_flag:
        if func != "count":
            raise InvalidRequest("DISTINCT is only supported with COUNT")
        if argument in {"*", "1"}:
            raise InvalidRequest("COUNT DISTINCT requires a column argument")

    if func != "count" and argument in {"*", "1"}:
        raise InvalidRequest(
            f"Aggregate function '{func}' requires a column argument"
        )

    if argument not in {"*", "1"}:
        argument = __resolve_column_name(argument, schema)

    return {
        "type": "aggregate",
        "func": func,
        "arg": argument,
        "alias": alias.lower(),
        "distinct": distinct_flag,
    }


def __parse_function_select(raw, schema):
    """Parse a per-row function select expression if present."""
    match = _FUNCTION_SELECT_PATTERN.fullmatch(raw)
    if not match:
        return None

    func = match.group(1).lower()
    column_name = __resolve_column_name(match.group(2), schema)
    alias = match.group(3) or match.group(4)
    if not alias:
        alias = f"{func}({column_name})"

    return {
        "type": "function",
        "func": func,
        "arg": column_name,
        "alias": alias,
    }


def __parse_column_select(raw, schema):
    """Parse a column select expression if present."""
    match = _COLUMN_SELECT_PATTERN.fullmatch(raw)
    if not match:
        return None

    column_name = __resolve_column_name(match.group(1), schema)
    alias = match.group(2)

    return {
        "type": "column",
        "name": column_name,
        "alias": (alias or column_name),
    }


def __parse_group_by(group_by_clause_str, schema):
    """Parse the GROUP BY clause into a list of column names."""
    if not group_by_clause_str:
        return []

    columns = [
        part.strip() for part in group_by_clause_str.split(",") if part.strip()
    ]
    if not columns:
        raise InvalidRequest("GROUP BY clause cannot be empty")

    resolved = [__resolve_column_name(column, schema) for column in columns]
    return resolved


def __validate_group_by_columns(select_items, group_by_columns):
    """Ensure SELECT items are compatible with GROUP BY columns."""
    for item in select_items:
        if item["type"] == "column" and item["name"] not in group_by_columns:
            raise InvalidRequest(
                "Non-aggregated columns must appear in the GROUP BY clause"
            )
        if item["type"] == "wildcard":
            raise InvalidRequest("SELECT * is not allowed with GROUP BY")


def __resolve_column_name(column, schema):
    """Resolve a column name against the schema in a case-insensitive manner."""
    for name in schema.keys():
        if name.lower() == column.lower():
            return name
    raise InvalidRequest(f"Column '{column}' not found in table schema")


def __group_rows(filtered_data, group_by_columns):
    """Group rows by the specified columns."""
    groups = {}
    for row in filtered_data:
        key = tuple(row.get(column) for column in group_by_columns)
        groups.setdefault(key, []).append(row)
    return groups


def __select_with_aggregates(
    filtered_data, select_items, group_by_columns, having_conditions
):
    """Compute aggregate SELECT expressions, optionally grouped."""
    names = [item["alias"] for item in select_items]

    if not group_by_columns:
        if having_conditions:
            raise InvalidRequest("HAVING clause requires GROUP BY")
        values = [
            __compute_aggregate(filtered_data, item) for item in select_items
        ]
        return [Row(names=names, values=values)]

    groups = __group_rows(filtered_data, group_by_columns)
    result_set = []
    for key, rows in groups.items():
        if having_conditions and not __check_having_conditions(
            rows, having_conditions
        ):
            continue

        row_values = []
        for item in select_items:
            if item["type"] == "column":
                idx = group_by_columns.index(item["name"])
                row_values.append(key[idx])
            else:
                row_values.append(__compute_aggregate(rows, item))

        result_set.append(Row(names=names, values=row_values))

    return result_set


def __select_group_by_only(filtered_data, select_items, group_by_columns):
    """Handle GROUP BY queries without aggregate functions."""
    for item in select_items:
        if item["type"] != "column":
            raise InvalidRequest(
                "GROUP BY queries without aggregates can only select grouped columns"
            )

    groups = __group_rows(filtered_data, group_by_columns)
    names = [item["alias"] for item in select_items]
    result_set = []

    for key in groups.keys():
        values = []
        for item in select_items:
            idx = group_by_columns.index(item["name"])
            values.append(key[idx])
        result_set.append(Row(names=names, values=values))

    return result_set


def __compute_aggregate(filtered_data, item):
    func = item["func"]
    argument = item["arg"]

    if func == "count":
        if item.get("distinct"):
            values = {
                row.get(argument)
                for row in filtered_data
                if row.get(argument) is not None
            }
            return len(values)
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
    elif func == "min":
        return min(values) if values else None
    elif func == "max":
        return max(values) if values else None
    elif func == "avg":
        return (sum(values) / len(values)) if values else None

    raise InvalidRequest(f"Unsupported aggregate function '{func}'")


def __parse_having_conditions(having_clause_str, schema, group_by_columns):
    """Parse the HAVING clause into structured aggregate conditions."""
    if not having_clause_str:
        return []

    if not group_by_columns:
        raise InvalidRequest("HAVING clause requires GROUP BY")

    conditions = []
    for raw_condition in __split_having_conditions(having_clause_str):
        if not raw_condition:
            continue
        conditions.append(
            __parse_single_having_condition(raw_condition, schema)
        )

    return conditions


def __split_having_conditions(having_clause_str):
    """Split a HAVING clause into individual condition strings."""
    stripped = having_clause_str.strip()
    return [part.strip() for part in _HAVING_SPLIT_PATTERN.split(stripped)]


def __parse_single_having_condition(raw_condition, schema):
    """Parse a single HAVING condition into a structured representation."""
    match = _HAVING_PATTERN.fullmatch(raw_condition)
    if not match:
        raise InvalidRequest(f"Unsupported HAVING condition: {raw_condition}")

    func = match.group(1).lower()
    distinct_flag = bool(match.group(2))
    argument = match.group(3).strip()
    operator = match.group(4)
    value_literal = match.group(5).strip()

    argument = __resolve_having_argument(func, argument, distinct_flag, schema)
    value = __parse_literal(value_literal)

    return {
        "func": func,
        "arg": argument,
        "operator": operator,
        "distinct": distinct_flag,
        "value": value,
    }


def __resolve_having_argument(func, argument, distinct_flag, schema):
    """Validate and normalize the aggregate argument used in HAVING."""
    if distinct_flag:
        if func != "count":
            raise InvalidRequest(
                "DISTINCT is only supported with COUNT in HAVING"
            )
        if argument in {"*", "1"}:
            raise InvalidRequest(
                "COUNT DISTINCT in HAVING requires a column argument"
            )

    if func != "count" and argument in {"*", "1"}:
        raise InvalidRequest(
            f"Aggregate function '{func}' in HAVING requires a column argument"
        )

    if argument not in {"*", "1"}:
        return __resolve_column_name(argument, schema)

    return argument


def __check_having_conditions(group_rows, conditions):
    """Evaluate HAVING conditions for a grouped set of rows."""
    for condition in conditions:
        computed = __compute_aggregate(
            group_rows,
            {
                "func": condition["func"],
                "arg": condition["arg"],
                "distinct": condition.get("distinct", False),
            },
        )
        if not __compare_values(
            computed, condition["operator"], condition["value"]
        ):
            return False
    return True


def __parse_literal(literal):
    """Parse a literal value from a HAVING clause."""
    stripped = literal.strip()

    if stripped.startswith("'") and stripped.endswith("'"):
        return stripped[1:-1]
    if stripped.startswith('"') and stripped.endswith('"'):
        return stripped[1:-1]

    lowered = stripped.lower()
    if lowered == "null":
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    try:
        return int(stripped)
    except ValueError:
        pass

    try:
        return float(stripped)
    except ValueError:
        pass

    return stripped


def __compare_values(left, operator, right):
    """Compare two values using a CQL comparison operator."""
    if left is None and operator not in {"=", "!="}:
        return False
    if operator == "=":
        return left == right
    if operator == "!=":
        return left != right
    if operator == ">":
        return left > right
    if operator == "<":
        return left < right
    if operator == ">=":
        return left >= right
    if operator == "<=":
        return left <= right
    raise InvalidRequest(f"Unsupported comparison operator '{operator}'")


def __select_columns(filtered_data, select_items, schema):
    """Project non-aggregate columns from filtered data."""
    ordered_keys = list(schema.keys())

    if len(select_items) == 1 and select_items[0]["type"] == "wildcard":
        compiled = [("column", column) for column in ordered_keys]
        names = ordered_keys
    else:
        names = []
        compiled = []
        for item in select_items:
            if item["type"] == "column":
                compiled.append(("column", item["name"]))
                names.append(item["alias"])
            elif item["type"] == "function":
                compiled.append(("function", item))
                names.append(item["alias"])
            else:
                raise InvalidRequest("Unsupported SELECT configuration")

    result_set = []
    for row_dict in filtered_data:
        values = []
        for item_type, payload in compiled:
            if item_type == "column":
                values.append(row_dict.get(payload))
            elif item_type == "function":
                values.append(__compute_row_function(row_dict, payload))
        result_set.append(Row(names=names, values=values))

    return result_set


def __deduplicate_rows(rows):
    """Remove duplicate rows while preserving order."""
    unique_rows = []
    seen = set()

    for row in rows:
        key = tuple(row)
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)

    return unique_rows


def __compute_row_function(row_dict, item):
    """Compute per-row functions like WRITETIME and TTL."""
    func = item["func"]
    _column = item["arg"]

    if func == "writetime":
        # Writetime metadata is not tracked; return None as Cassandra would for unset
        return None
    if func == "ttl":
        # TTL metadata is not tracked; return None for unset columns
        return None

    raise InvalidRequest(f"Unsupported function '{func}' in SELECT clause")
