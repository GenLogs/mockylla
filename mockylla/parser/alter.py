from cassandra import InvalidRequest
from cassandra.protocol import SyntaxException

from mockylla.parser.utils import parse_with_options, get_table


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
        raise InvalidRequest("No keyspace specified for ALTER TABLE")

    if (
        keyspace_name not in state.keyspaces
        or table_name not in state.keyspaces[keyspace_name]["tables"]
    ):
        raise SyntaxException(
            code=SyntaxException.error_code,
            message=f"Table '{table_name_full}' does not exist",
            info=None,
        )

    state.keyspaces[keyspace_name]["tables"][table_name]["schema"][
        new_column_name
    ] = new_column_type
    print(
        f"Altered table '{table_name}' in keyspace '{keyspace_name}': "
        f"added column '{new_column_name} {new_column_type}'"
    )

    state.update_system_schema()
    return []


def handle_alter_table_with(match, session, state):
    table_name_full, options_str = match.groups()

    keyspace_name, table_name, table_info = get_table(
        table_name_full, session, state
    )

    new_options = parse_with_options(options_str)
    existing_options = table_info.setdefault("options", {})
    existing_options.update(new_options)

    print(
        f"Altered table '{table_name}' in keyspace '{keyspace_name}' options: {new_options}"
    )

    state.update_system_schema()
    return []
