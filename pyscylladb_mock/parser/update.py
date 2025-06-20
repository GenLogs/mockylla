import re
from pyscylladb_mock.parser.utils import (
    get_table,
    parse_where_clause,
    cast_value,
)


def handle_update(update_match, session, state):
    """Handle UPDATE query by parsing and executing the update operation."""
    table_name_full, set_clause_str, where_clause_str = update_match.groups()

    # Get table info
    _, table_name, table = get_table(table_name_full, session, state)
    schema = table["schema"]

    # Parse SET and WHERE clauses
    set_operations, counter_operations = __parse_set_clause(
        set_clause_str, schema
    )
    parsed_conditions = parse_where_clause(where_clause_str, schema)

    # Update existing rows
    rows_updated = __update_existing_rows(
        table, parsed_conditions, set_operations, counter_operations
    )
    if rows_updated > 0:
        print(f"Updated {rows_updated} rows in '{table_name}'")
        return []

    # Handle upsert case
    __handle_upsert(
        table, table_name, parsed_conditions, set_operations, counter_operations
    )
    print(f"Updated {rows_updated} rows in '{table_name}'")
    return []


def __parse_set_clause(set_clause_str, schema):
    """Parse SET clause into regular and counter operations."""
    set_operations = {}
    counter_operations = {}
    set_pairs = [s.strip() for s in set_clause_str.split(",")]

    for pair in set_pairs:
        # Check for counter update: `c = c + 1`
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
            # Regular set: `col = value`
            col, val_str = [p.strip() for p in pair.split("=", 1)]
            val = val_str.strip("'\"")  # Basic string literal parsing
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
