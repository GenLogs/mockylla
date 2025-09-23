from cassandra import InvalidRequest

from mockylla.parser.utils import get_keyspace_and_name


def handle_create_type(match, session, state):
    """
    Handles a CREATE TYPE statement.
    """
    type_name_str, fields_str = match.groups()
    keyspace_name, type_name = get_keyspace_and_name(
        type_name_str, session.keyspace
    )

    if keyspace_name not in state.keyspaces:
        raise InvalidRequest(f"Keyspace '{keyspace_name}' does not exist")

    if "types" not in state.keyspaces[keyspace_name]:
        state.keyspaces[keyspace_name]["types"] = {}

    if type_name in state.keyspaces[keyspace_name]["types"]:
        return

    field_defs = [f.strip() for f in fields_str.split(",") if f.strip()]
    fields = {}
    for f in field_defs:
        parts = f.split(None, 1)
        if len(parts) == 2:
            name, type_ = parts
            fields[name.strip()] = type_.strip()

    state.keyspaces[keyspace_name]["types"][type_name] = {"fields": fields}
