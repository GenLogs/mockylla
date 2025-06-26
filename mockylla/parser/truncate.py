def handle_truncate_table(match, session, state):
    """
    Handles a TRUNCATE TABLE statement.
    """
    table_name = match.group(1)
    if "." in table_name:
        keyspace_name, table_name = table_name.split(".")
    else:
        keyspace_name = session.keyspace

    if not keyspace_name:
        raise Exception("No keyspace specified for TRUNCATE operation.")

    if keyspace_name not in state.keyspaces:
        raise Exception(f"Keyspace '{keyspace_name}' does not exist.")

    if table_name not in state.keyspaces[keyspace_name]["tables"]:
        raise Exception(f"Table '{table_name}' does not exist.")

    # Truncate the table by clearing its data
    state.keyspaces[keyspace_name]["tables"][table_name]["data"] = []
    print(f"Truncated table '{table_name}' in keyspace '{keyspace_name}'")
    return []
