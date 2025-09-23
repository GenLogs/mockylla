import re

from cassandra import InvalidRequest

from mockylla.parser.utils import cast_value
from mockylla.row import Row


def _parse_udt_literal(literal):
    """Parses a UDT literal string like '{key1: val1, key2: val2}' into a dict."""
    if isinstance(literal, dict):
        return literal
    if not literal.startswith("{") or not literal.endswith("}"):
        return literal

    content = literal[1:-1].strip()
    udt_dict = {}

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


def assign_row_data_value(val, cql_type, defined_types):
    if cql_type in defined_types:
        if isinstance(val, str):
            return _parse_udt_literal(val)
        else:
            return cast_value(val, cql_type)
    elif cql_type:
        if isinstance(val, str):
            return cast_value(val.strip("'\""), cql_type)
        else:
            return cast_value(val, cql_type)
    else:
        return val


def handle_insert_into(insert_match, session, state, parameters=None):
    (
        table_name_full,
        columns_str,
        values_str,
        using_clause,
        if_not_exists,
    ) = insert_match.groups()

    del using_clause

    if "." in table_name_full:
        keyspace_name, table_name = table_name_full.split(".", 1)
    elif session.keyspace:
        keyspace_name, table_name = session.keyspace, table_name_full
    else:
        raise InvalidRequest("No keyspace specified for INSERT")

    if keyspace_name not in state.keyspaces:
        raise InvalidRequest(f"Keyspace '{keyspace_name}' does not exist")

    tables = state.keyspaces[keyspace_name]["tables"]
    if table_name not in tables:
        raise InvalidRequest(f"Table '{table_name_full}' does not exist")

    table_info = tables[table_name]
    table_schema = table_info["schema"]
    primary_key_cols = table_info.get("primary_key", [])
    defined_types = state.keyspaces[keyspace_name].get("types", {})

    columns = [c.strip() for c in columns_str.split(",")]

    if parameters:
        values = parameters
    else:
        values = _parse_values(values_str)

    if len(columns) != len(values):
        raise InvalidRequest(
            "Number of columns does not match number of values"
        )

    row_data = {}
    for col, val in zip(columns, values):
        cql_type = table_schema.get(col)
        row_data[col] = assign_row_data_value(val, cql_type, defined_types)

    if if_not_exists:
        pk_to_insert = {
            k: v for k, v in row_data.items() if k in primary_key_cols
        }

        for existing_row in table_info["data"]:
            pk_existing = {
                k: v for k, v in existing_row.items() if k in primary_key_cols
            }
            if pk_to_insert == pk_existing:
                result_names = ["[applied]"] + list(existing_row.keys())
                result_values = [False] + list(existing_row.values())
                return [Row(result_names, result_values)]

        state.keyspaces[keyspace_name]["tables"][table_name]["data"].append(
            row_data
        )
        return [Row(["[applied]"], [True])]
    else:
        state.keyspaces[keyspace_name]["tables"][table_name]["data"].append(
            row_data
        )
        print(f"Inserted row into '{table_name}': {row_data}")
        return []
