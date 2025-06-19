import re


def handle_query(query, session, state):
    """
    Parses and handles a CQL query.
    """
    query = query.strip()

    # Simple CREATE KEYSPACE parser
    create_keyspace_match = re.match(
        r"^\s*CREATE\s+KEYSPACE\s+(?:IF NOT EXISTS\s+)?(\w+)\s+WITH\s+REPLICATION\s*=\s*({.*})\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if create_keyspace_match:
        return __handle_create_keyspace(create_keyspace_match, state)

    # Simple CREATE TABLE parser
    create_table_match = re.match(
        r"^\s*CREATE\s+TABLE\s+(?:IF NOT EXISTS\s+)?([\w\.]+)\s*\((.*)\)\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,  # DOTALL allows . to match newlines
    )
    if create_table_match:
        return __handle_create_table(create_table_match, session, state)

    # Simple INSERT INTO parser
    insert_match = re.match(
        r"^\s*INSERT\s+INTO\s+([\w\.]+)\s*\(([\w\s,]+)\)\s+VALUES\s*\((.*)\)\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if insert_match:
        return __handle_insert_into(insert_match, session, state)

    select_match = re.match(
        r"^\s*SELECT\s+(.*)\s+FROM\s+([\w\.]+)(?:\s+WHERE\s+(.*))?\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if select_match:
        return __handle_select_from(select_match, session, state)

    update_match = re.match(
        r"^\s*UPDATE\s+([\w\.]+)\s+SET\s+(.*)\s+WHERE\s+(.*)\s*;?\s*$",
        query,
        re.IGNORECASE | re.DOTALL,
    )
    if update_match:
        return __handle_update(update_match, session, state)

    delete_match = re.match(
        r"^\s*DELETE\s+FROM\s+([\w\.]+)\s+WHERE\s+(.*)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if delete_match:
        return __handle_delete_from(delete_match, session, state)

    drop_table_match = re.match(
        r"^\s*DROP\s+TABLE\s+(?:IF EXISTS\s+)?([\w\.]+)\s*;?\s*$",
        query,
        re.IGNORECASE,
    )
    if drop_table_match:
        return __handle_drop_table(drop_table_match, session, state)

    return []


def __handle_create_keyspace(create_keyspace_match, state):
    keyspace_name = create_keyspace_match.group(1)
    if keyspace_name in state.keyspaces:
        raise Exception(f"Keyspace '{keyspace_name}' already exists")

    state.keyspaces[keyspace_name] = {"tables": {}}
    print(f"Created keyspace: {keyspace_name}")
    return []


def __handle_create_table(create_table_match, session, state):
    table_name_full, columns_str = create_table_match.groups()

    # Determine keyspace and table name
    if "." in table_name_full:
        keyspace_name, table_name = table_name_full.split(".", 1)
    elif session.keyspace:
        keyspace_name, table_name = session.keyspace, table_name_full
    else:
        raise Exception("No keyspace specified for CREATE TABLE")

    if keyspace_name not in state.keyspaces:
        raise Exception(f"Keyspace '{keyspace_name}' does not exist")

    if table_name in state.keyspaces[keyspace_name]["tables"]:
        raise Exception(
            f"Table '{table_name}' already exists in keyspace '{keyspace_name}'"
        )

    # A more robust column parser that handles inline and separate PRIMARY KEY definitions.
    column_defs = [c.strip() for c in columns_str.split(",") if c.strip()]
    columns = [
        c.split()
        for c in column_defs
        if not c.upper().startswith("PRIMARY KEY")
    ]
    schema = {
        name: type_ for name, type_, *_ in columns
    }  # Use _ to ignore extra parts like 'PRIMARY KEY' in column def

    state.keyspaces[keyspace_name]["tables"][table_name] = {
        "schema": schema,
        "data": [],
    }
    print(
        f"Created table '{table_name}' in keyspace '{keyspace_name}' with schema: {schema}"
    )
    return []


def __handle_insert_into(insert_match, session, state):
    table_name_full, columns_str, values_str = insert_match.groups()

    # Determine keyspace and table name
    if "." in table_name_full:
        keyspace_name, table_name = table_name_full.split(".", 1)
    elif session.keyspace:
        keyspace_name, table_name = session.keyspace, table_name_full
    else:
        raise Exception("No keyspace specified for INSERT")

    table_schema = state.keyspaces[keyspace_name]["tables"][table_name][
        "schema"
    ]

    if (
        keyspace_name not in state.keyspaces
        or table_name not in state.keyspaces[keyspace_name]["tables"]
    ):
        raise Exception(f"Table '{table_name_full}' does not exist")

    columns = [c.strip() for c in columns_str.split(",")]
    # This is a very simplistic value parser. It doesn't handle strings with commas, etc.
    values = [v.strip().strip("'\"") for v in values_str.split(",")]

    if len(columns) != len(values):
        raise Exception("Number of columns does not match number of values")

    row_data = {}
    for col, val in zip(columns, values):
        cql_type = table_schema.get(col)
        if cql_type:
            row_data[col] = _cast_value(val, cql_type)
        else:
            row_data[col] = val  # Fallback for unknown columns

    state.keyspaces[keyspace_name]["tables"][table_name]["data"].append(
        row_data
    )
    print(f"Inserted row into '{table_name}': {row_data}")
    return []


def __handle_select_from(select_match, session, state):
    _, table_name_full, where_clause_str = select_match.groups()

    # Determine keyspace and table name
    if "." in table_name_full:
        keyspace_name, table_name = table_name_full.split(".", 1)
    elif session.keyspace:
        keyspace_name, table_name = session.keyspace, table_name_full
    else:
        raise Exception("No keyspace specified for SELECT")

    # Check if keyspace and table exist
    if (
        keyspace_name not in state.keyspaces
        or table_name not in state.keyspaces[keyspace_name]["tables"]
    ):
        raise Exception(f"Table '{table_name_full}' does not exist")

    table_data = state.keyspaces[keyspace_name]["tables"][table_name]["data"]
    schema = state.keyspaces[keyspace_name]["tables"][table_name]["schema"]

    # Filter data based on WHERE clause
    filtered_data = table_data
    if where_clause_str:
        conditions = [
            cond.strip()
            for cond in re.split(
                r"\s+AND\s+", where_clause_str, flags=re.IGNORECASE
            )
        ]

        parsed_conditions = []
        for cond in conditions:
            match = re.match(
                r"(\w+)\s*([<>=]+)\s*(?:'([^']*)'|\"([^\"]*)\"|([\w\.-]+))",
                cond.strip(),
            )
            if match:
                col, op, v1, v2, v3 = match.groups()
                val = next((v for v in [v1, v2, v3] if v is not None), None)

                cql_type = schema.get(col)
                if cql_type:
                    val = _cast_value(val, cql_type)

                parsed_conditions.append((col, op, val))

        def check_row(row):
            for col, op, val in parsed_conditions:
                row_val = row.get(col)
                if row_val is None:
                    return False

                if op == "=":
                    if not (row_val == val):
                        return False
                elif op == ">":
                    if not (row_val > val):
                        return False
                elif op == "<":
                    if not (row_val < val):
                        return False
                elif op == ">=":
                    if not (row_val >= val):
                        return False
                elif op == "<=":
                    if not (row_val <= val):
                        return False
                else:  # Unsupported operator
                    return False
            return True

        if parsed_conditions:
            filtered_data = [row for row in table_data if check_row(row)]

    result_set = []
    for row in filtered_data:
        new_row = {}
        for col_name, value in row.items():
            col_type = schema.get(col_name)
            # The data is already typed, so we can just use it.
            # But the driver might expect specific types, so we keep casting for the output.
            if col_type == "int":
                new_row[col_name] = int(value)
            else:
                new_row[col_name] = value
        result_set.append(new_row)

    print(f"Selected {len(result_set)} rows from '{table_name}'")
    return result_set


def __handle_update(update_match, session, state):
    table_name_full, set_clause_str, where_clause_str = update_match.groups()

    # Determine keyspace and table name
    if "." in table_name_full:
        keyspace_name, table_name = table_name_full.split(".", 1)
    elif session.keyspace:
        keyspace_name, table_name = session.keyspace, table_name_full
    else:
        raise Exception("No keyspace specified for UPDATE")

    # Check if keyspace and table exist
    if (
        keyspace_name not in state.keyspaces
        or table_name not in state.keyspaces[keyspace_name]["tables"]
    ):
        raise Exception(f"Table '{table_name_full}' does not exist")

    table = state.keyspaces[keyspace_name]["tables"][table_name]
    schema = table["schema"]

    # Parse SET clause
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
                set_operations[col] = _cast_value(val, cql_type)
            else:
                set_operations[col] = val

    # Parse WHERE clause to identify rows to update
    conditions = [
        cond.strip()
        for cond in re.split(
            r"\s+AND\s+", where_clause_str, flags=re.IGNORECASE
        )
    ]
    parsed_conditions = []
    for cond in conditions:
        match = re.match(
            r"(\w+)\s*([<>=]+)\s*(?:'([^']*)'|\"([^\"]*)\"|([\w\.-]+))",
            cond.strip(),
        )
        if match:
            col, op, v1, v2, v3 = match.groups()
            val = next((v for v in [v1, v2, v3] if v is not None), None)
            cql_type = schema.get(col)
            if cql_type:
                val = _cast_value(val, cql_type)
            parsed_conditions.append((col, op, val))

    def check_row(row):
        for col, op, val in parsed_conditions:
            row_val = row.get(col)
            if op == "=":
                if row_val != val:
                    return False
            # Add other operators if necessary
        return True

    # Apply updates
    rows_updated = 0
    for row in table["data"]:
        if check_row(row):
            row.update(set_operations)
            for col, val in counter_operations.items():
                row[col] = row.get(col, 0) + val
            rows_updated += 1

    # If no rows were updated, it might be an upsert (especially for counters)
    if rows_updated == 0:
        # Create a new row from the WHERE clause conditions
        new_row = {}
        is_upsert = False
        for col, op, val in parsed_conditions:
            if op == "=":
                new_row[col] = val
                is_upsert = True

        if is_upsert:
            # Apply SET operations
            new_row.update(set_operations)
            # Apply counter operations to the new row
            for col, val in counter_operations.items():
                new_row[col] = val  # Initialize with the value

            table["data"].append(new_row)
            print(f"Upserted row in '{table_name}': {new_row}")

    print(f"Updated {rows_updated} rows in '{table_name}'")
    return []


def __handle_delete_from(delete_match, session, state):
    table_name_full, where_clause_str = delete_match.groups()

    # Determine keyspace and table name
    if "." in table_name_full:
        keyspace_name, table_name = table_name_full.split(".", 1)
    elif session.keyspace:
        keyspace_name, table_name = session.keyspace, table_name_full
    else:
        raise Exception("No keyspace specified for DELETE")

    table_schema = state.keyspaces[keyspace_name]["tables"][table_name][
        "schema"
    ]

    # Check if keyspace and table exist
    if (
        keyspace_name not in state.keyspaces
        or table_name not in state.keyspaces[keyspace_name]["tables"]
    ):
        raise Exception(f"Table '{table_name_full}' does not exist")

    table_data = state.keyspaces[keyspace_name]["tables"][table_name]["data"]

    # Parse WHERE clause to find rows to delete
    parsed_conditions = []
    if where_clause_str:
        conditions = [
            cond.strip()
            for cond in re.split(
                r"\s+AND\s+", where_clause_str, flags=re.IGNORECASE
            )
        ]
        for cond in conditions:
            match = re.match(
                r"(\w+)\s*([<>=]+)\s*(?:'([^']*)'|\"([^\"]*)\"|([\w\.-]+))",
                cond.strip(),
            )
            if match:
                col, op, v1, v2, v3 = match.groups()
                val = next((v for v in [v1, v2, v3] if v is not None), None)

                cql_type = table_schema.get(col)
                if cql_type:
                    val = _cast_value(val, cql_type)

                parsed_conditions.append((col, op, val))

    if not parsed_conditions:
        # For safety, don't allow DELETE without a valid WHERE clause condition
        return []

    def check_row(row):
        for col, op, val in parsed_conditions:
            row_val = row.get(col)
            if row_val is None:
                return False

            if op == "=":
                if not (row_val == val):
                    return False
            elif op == ">":
                if not (row_val > val):
                    return False
            elif op == "<":
                if not (row_val < val):
                    return False
            elif op == ">=":
                if not (row_val >= val):
                    return False
            elif op == "<=":
                if not (row_val <= val):
                    return False
            else:  # Unsupported operator
                return False
        return True

    rows_to_keep = [row for row in table_data if not check_row(row)]

    deleted_count = len(table_data) - len(rows_to_keep)
    state.keyspaces[keyspace_name]["tables"][table_name]["data"] = rows_to_keep

    print(f"Deleted {deleted_count} rows from '{table_name}'")
    return []


def __handle_drop_table(drop_table_match, session, state):
    table_name_full = drop_table_match.group(1)

    # Determine keyspace and table name
    if "." in table_name_full:
        keyspace_name, table_name = table_name_full.split(".", 1)
    elif session.keyspace:
        keyspace_name, table_name = session.keyspace, table_name_full
    else:
        raise Exception("No keyspace specified for DROP TABLE")

    if (
        keyspace_name not in state.keyspaces
        or table_name not in state.keyspaces[keyspace_name]["tables"]
    ):
        # Allow IF EXISTS to proceed without error
        if "IF EXISTS" in drop_table_match.string.upper():
            return []
        raise Exception(f"Table '{table_name_full}' does not exist")

    del state.keyspaces[keyspace_name]["tables"][table_name]
    print(f"Dropped table '{table_name}' from keyspace '{keyspace_name}'")
    return []


def _cast_value(value, cql_type):
    """Casts a string value to a Python type based on CQL type."""
    cql_type = cql_type.lower()
    if cql_type == "int":
        return int(value)
    if cql_type == "text" or cql_type == "varchar":
        return str(value)
    if cql_type == "counter":
        return int(value)
    # Add more type mappings here as needed
    return value
