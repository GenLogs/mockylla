import re


def handle_create_keyspace(create_keyspace_match, state):
    keyspace_name = create_keyspace_match.group(1)
    if keyspace_name in state.keyspaces:
        raise Exception(f"Keyspace '{keyspace_name}' already exists")

    state.keyspaces[keyspace_name] = {"tables": {}}
    print(f"Created keyspace: {keyspace_name}")
    return []


def handle_create_table(create_table_match, session, state):
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

    # First, remove the PRIMARY KEY clause to handle it separately if needed.
    pk_match = re.search(
        r"PRIMARY\s+KEY\s*\((.*?)\)", columns_str, re.IGNORECASE
    )
    if pk_match:
        # For now, we just remove it to correctly parse columns.
        # We could store the primary key info later if needed.
        columns_str = (
            columns_str[: pk_match.start()] + columns_str[pk_match.end() :]
        )

    column_defs = [c.strip() for c in columns_str.split(",") if c.strip()]
    columns = [c.split() for c in column_defs]

    schema = {
        name: type_ for name, type_, *_ in columns if name
    }  # Use _ to ignore extra parts and check for name

    state.keyspaces[keyspace_name]["tables"][table_name] = {
        "schema": schema,
        "data": [],
    }
    print(
        f"Created table '{table_name}' in keyspace '{keyspace_name}' with schema: {schema}"
    )
    return []
