import re
from pyscylladb_mock.parser.utils import cast_value


def _parse_udt_literal(literal):
    """Parses a UDT literal string like '{key1: val1, key2: val2}' into a dict."""
    if isinstance(literal, dict):
        return literal
    if not literal.startswith("{") or not literal.endswith("}"):
        return literal  # Not a UDT literal

    content = literal[1:-1].strip()
    udt_dict = {}
    # This regex handles simple key-value pairs, including quoted values.
    for match in re.finditer(r"(\w+)\s*:\s*(?:'([^']*)'|([^,}\s]+))", content):
        key, val_quoted, val_unquoted = match.groups()
        val = val_quoted if val_quoted is not None else val_unquoted
        udt_dict[key.strip()] = val
    return udt_dict


def _parse_values(values_str):
    """
    Parses a string of CQL values, respecting parentheses, brackets, and braces.
    Example: "1, {key: 'val', key2: 'val2'}, [1, 2, 3]"
    """
    values = []
    current_val = ""
    level = 0
    in_string = False

    for char in values_str:
        if char == "'" and (len(current_val) == 0 or current_val[-1] != "\\\\"):
            in_string = not in_string
        elif char in "({[" and not in_string:
            level += 1
        elif char in ")}]" and not in_string:
            level -= 1
        elif char == "," and level == 0 and not in_string:
            values.append(current_val.strip())
            current_val = ""
            continue
        current_val += char

    values.append(current_val.strip())
    return values


def handle_insert_into(insert_match, session, state, parameters=None):
    table_name_full, columns_str, values_str = insert_match.groups()

    # Determine keyspace and table name
    if "." in table_name_full:
        keyspace_name, table_name = table_name_full.split(".", 1)
    elif session.keyspace:
        keyspace_name, table_name = session.keyspace, table_name_full
    else:
        raise Exception("No keyspace specified for INSERT")

    table_info = state.keyspaces[keyspace_name]["tables"][table_name]
    table_schema = table_info["schema"]
    defined_types = state.keyspaces[keyspace_name].get("types", {})

    if (
        keyspace_name not in state.keyspaces
        or table_name not in state.keyspaces[keyspace_name]["tables"]
    ):
        raise Exception(f"Table '{table_name_full}' does not exist")

    columns = [c.strip() for c in columns_str.split(",")]

    if parameters:
        values = parameters
    else:
        values = _parse_values(values_str)

    if len(columns) != len(values):
        raise Exception("Number of columns does not match number of values")

    row_data = {}
    for col, val in zip(columns, values):
        cql_type = table_schema.get(col)
        if cql_type in defined_types:
            # It's a UDT
            if isinstance(val, str):
                row_data[col] = _parse_udt_literal(val)
            else:
                row_data[col] = val
        elif cql_type:
            # It's a standard type
            if isinstance(val, str):
                row_data[col] = cast_value(val.strip("'\""), cql_type)
            else:
                row_data[col] = cast_value(val, cql_type)
        else:
            row_data[col] = val  # Fallback for unknown columns

    state.keyspaces[keyspace_name]["tables"][table_name]["data"].append(
        row_data
    )
    print(f"Inserted row into '{table_name}': {row_data}")
    return []
