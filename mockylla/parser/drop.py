from cassandra import InvalidRequest


def handle_drop_table(drop_table_match, session, state):
    table_name_full = drop_table_match.group(1)

    if "." in table_name_full:
        keyspace_name, table_name = table_name_full.split(".", 1)
    elif session.keyspace:
        keyspace_name, table_name = session.keyspace, table_name_full
    else:
        raise InvalidRequest("No keyspace specified for DROP TABLE")

    if (
        keyspace_name not in state.keyspaces
        or table_name not in state.keyspaces[keyspace_name]["tables"]
    ):
        if "IF EXISTS" in drop_table_match.string.upper():
            return []
        raise InvalidRequest(f"Table '{table_name_full}' does not exist")

    del state.keyspaces[keyspace_name]["tables"][table_name]
    print(f"Dropped table '{table_name}' from keyspace '{keyspace_name}'")
    return []
