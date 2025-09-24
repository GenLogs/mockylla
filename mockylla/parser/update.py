import re
from mockylla.parser.utils import (
    get_table,
    parse_where_clause,
    cast_value,
)
from mockylla.row import Row


def replace_placeholders(segment, params, start_idx):
    if not segment or "%s" not in segment:
        return segment, start_idx

    parts = segment.split("%s")
    if len(parts) - 1 + start_idx > len(params):
        raise ValueError(
            "Number of parameters does not match number of placeholders in UPDATE query"
        )

    new_segment = parts[0]
    idx = start_idx
    for i in range(len(parts) - 1):
        param = params[idx]
        param_str = f"'{param}'" if isinstance(param, str) else str(param)
        new_segment += param_str + parts[i + 1]
        idx += 1
    return new_segment, idx


def handle_update(update_match, session, state, parameters=None):
    """Handle UPDATE query by parsing and executing the update operation."""
    (
        table_name_full,
        using_clause,
        set_clause_str,
        where_clause_str,
        if_exists,
    ) = update_match.groups()

    del using_clause

    if parameters and "%s" in (set_clause_str + where_clause_str):
        set_clause_str, next_idx = replace_placeholders(
            set_clause_str, parameters, 0
        )
        where_clause_str, _ = replace_placeholders(
            where_clause_str, parameters, next_idx
        )

    _, table_name, table = get_table(table_name_full, session, state)
    schema = table["schema"]

    set_operations, counter_operations = __parse_set_clause(
        set_clause_str, schema
    )
    parsed_conditions = parse_where_clause(where_clause_str, schema)

    rows_updated = __update_existing_rows(
        table, parsed_conditions, set_operations, counter_operations
    )

    if if_exists:
        if rows_updated > 0:
            print(f"Updated {rows_updated} rows in '{table_name}'")
            return [Row(["[applied]"], [True])]
        else:
            return [Row(["[applied]"], [False])]

    if rows_updated > 0:
        print(f"Updated {rows_updated} rows in '{table_name}'")
        return []

    __handle_upsert(
        table, table_name, parsed_conditions, set_operations, counter_operations
    )
    return []


def __parse_set_clause(set_clause_str, schema):
    """Parse SET clause into regular and counter operations."""
    set_operations = {}
    counter_operations = {}
    set_pairs = [s.strip() for s in set_clause_str.split(",")]

    for pair in set_pairs:
        counter_match = re.match(
            r"(\w+)\s*=\s*\1\s*([+-])\s*(\d+)", pair, re.IGNORECASE
        )
        if counter_match:
            col, op, val_str = counter_match.groups()
            val = int(val_str)
            if op == "-":
                val = -val
            counter_operations[col] = val
        else:
            col, val_str = [p.strip() for p in pair.split("=", 1)]
            val = val_str.strip("'\"")
            cql_type = schema.get(col)
            if cql_type:
                set_operations[col] = cast_value(val, cql_type)
            else:
                set_operations[col] = val

    return set_operations, counter_operations


def __update_existing_rows(
    table, parsed_conditions, set_operations, counter_operations
):
    """Update existing rows that match conditions."""
    rows_updated = 0
    for row in table["data"]:
        if __handle_update_check_row(row, parsed_conditions):
            row.update(set_operations)
            for col, val in counter_operations.items():
                row[col] = row.get(col, 0) + val
            rows_updated += 1
    return rows_updated


def __handle_upsert(
    table, table_name, parsed_conditions, set_operations, counter_operations
):
    """Handle upsert case when no existing rows were updated."""
    new_row = {}
    is_upsert = False
    for col, op, val in parsed_conditions:
        if op == "=":
            new_row[col] = val
            is_upsert = True

    if not is_upsert:
        return

    new_row.update(set_operations)
    for col, val in counter_operations.items():
        new_row[col] = val

    table["data"].append(new_row)
    print(f"Upserted row in '{table_name}': {new_row}")


def __handle_update_check_row(row, parsed_conditions):
    """Check if a row matches the update conditions."""
    for col, op, val in parsed_conditions:
        row_val = row.get(col)
        if op == "=" and row_val != val:
            return False
    return True
