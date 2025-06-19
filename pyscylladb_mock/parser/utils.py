import re


def cast_value(value, cql_type):
    """Casts a string value to a Python type based on CQL type."""
    cql_type = cql_type.lower()

    match cql_type:
        case "int" | "counter":
            return int(value)
        case "text" | "varchar":
            return str(value)
        case _:
            return value


def get_table(table_name_full, session, state):
    """Get keyspace, table name and table data from state."""
    if "." in table_name_full:
        keyspace_name, table_name = table_name_full.split(".", 1)
    elif session.keyspace:
        keyspace_name, table_name = session.keyspace, table_name_full
    else:
        raise Exception(f"No keyspace specified for table {table_name_full}")

    if (
        keyspace_name not in state.keyspaces
        or table_name not in state.keyspaces[keyspace_name]["tables"]
    ):
        raise Exception(f"Table '{table_name_full}' does not exist")

    table_info = state.keyspaces[keyspace_name]["tables"][table_name]
    return keyspace_name, table_name, table_info


def parse_where_clause(where_clause_str, schema):
    """Parse WHERE clause conditions into structured format."""
    if not where_clause_str:
        return []

    conditions = [
        cond.strip()
        for cond in re.split(
            r"\s+AND\s+", where_clause_str, flags=re.IGNORECASE
        )
    ]

    return __parse_conditions(conditions, schema)


def __parse_conditions(conditions, schema):
    """Parse conditions into structured format."""
    parsed_conditions = []
    for cond in conditions:
        in_match = re.match(
            r"(\w+)\s+IN\s+\((.*)\)", cond.strip(), re.IGNORECASE
        )
        if in_match:
            parsed_conditions.append(__parse_in_condition(in_match, schema))
            continue

        match = re.match(
            r"(\w+)\s*([<>=]+)\s*(?:'([^']*)'|\"([^\"]*)\"|([\w\.-]+))",
            cond.strip(),
        )
        if match:
            parsed_conditions.append(
                __parse_comparison_condition(match, schema)
            )
    return parsed_conditions


def __parse_in_condition(in_match, schema):
    """Parse IN condition from regex match."""
    col, values_str = in_match.groups()
    values = [v.strip().strip("'\"") for v in values_str.split(",")]

    cql_type = schema.get(col)
    if cql_type:
        values = [cast_value(v, cql_type) for v in values]

    return (col, "IN", values)


def __parse_comparison_condition(match, schema):
    """Parse comparison condition from regex match."""
    col, op, v1, v2, v3 = match.groups()
    val = next((v for v in [v1, v2, v3] if v is not None), None)

    cql_type = schema.get(col)
    if cql_type:
        val = cast_value(val, cql_type)

    return (col, op, val)


def check_row_conditions(row, parsed_conditions):
    """Check if a row matches all parsed conditions."""
    for col, op, val in parsed_conditions:
        row_val = row.get(col)
        if row_val is None:
            return False

        if not __check_condition(row_val, op, val):
            return False
    return True


def __check_condition(row_val, op, val):
    """Check if a single condition is met."""
    if op == "=":
        return row_val == val
    elif op == ">":
        return row_val > val
    elif op == "<":
        return row_val < val
    elif op == ">=":
        return row_val >= val
    elif op == "<=":
        return row_val <= val
    elif op == "IN":
        return row_val in val
    return False
