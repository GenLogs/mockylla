from pyscylladb_mock.parser.utils import cast_value


def handle_insert_into(insert_match, session, state):
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
            row_data[col] = cast_value(val, cql_type)
        else:
            row_data[col] = val  # Fallback for unknown columns

    state.keyspaces[keyspace_name]["tables"][table_name]["data"].append(
        row_data
    )
    print(f"Inserted row into '{table_name}': {row_data}")
    return []
