from cassandra.protocol import SyntaxException


def handle_alter_table(match, session, state):
    """
    Handles an ALTER TABLE statement.
    """
    table_name_full = match.group(1)
    new_column_name = match.group(2)
    new_column_type = match.group(3)

    if "." in table_name_full:
        keyspace_name, table_name = table_name_full.split(".", 1)
    elif session.keyspace:
        keyspace_name, table_name = session.keyspace, table_name_full
    else:
        raise Exception("No keyspace specified for ALTER TABLE")

    if (
        keyspace_name not in state.keyspaces
        or table_name not in state.keyspaces[keyspace_name]["tables"]
    ):
        raise SyntaxException(
            code=SyntaxException.error_code,
            message=f"Table '{table_name_full}' does not exist",
            info=None,
        )

    state.keyspaces[keyspace_name]["tables"][table_name]["schema"][new_column_name] = (
        new_column_type
    )
    print(
        f"Altered table '{table_name}' in keyspace '{keyspace_name}': "
        f"added column '{new_column_name} {new_column_type}'"
    )

    return []
