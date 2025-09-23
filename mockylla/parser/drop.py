from cassandra import InvalidRequest


def handle_drop_keyspace(match, state):
    keyspace_name = match.group(1)

    if keyspace_name not in state.keyspaces:
        if "IF EXISTS" in match.string.upper():
            return []
        raise InvalidRequest(f"Keyspace '{keyspace_name}' does not exist")

    if keyspace_name in {"system", "system_schema"}:
        raise InvalidRequest(f"Cannot drop system keyspace '{keyspace_name}'")

    del state.keyspaces[keyspace_name]
    state.update_system_schema()
    print(f"Dropped keyspace '{keyspace_name}'")
    return []


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
    views = state.keyspaces[keyspace_name].get("views", {})
    for view_name, view_info in list(views.items()):
        if view_info.get("base_table") == table_name:
            del views[view_name]
    state.update_system_schema()
    print(f"Dropped table '{table_name}' from keyspace '{keyspace_name}'")
    return []


def _validate_drop_index_keyspace(match, state, keyspace_name):
    if not keyspace_name:
        raise InvalidRequest("No keyspace specified for DROP INDEX")

    if keyspace_name not in state.keyspaces:
        if "IF EXISTS" in match.string.upper():
            return False
        raise InvalidRequest(f"Keyspace '{keyspace_name}' does not exist")

    return True


def handle_drop_index(match, session, state):
    index_name_full = match.group(1)

    keyspace_name = session.keyspace
    if "." in index_name_full:
        keyspace_name, index_name = index_name_full.split(".", 1)
    else:
        index_name = index_name_full

    if not _validate_drop_index_keyspace(match, state, keyspace_name):
        return []

    tables = state.keyspaces[keyspace_name]["tables"]
    found = False
    for table_info in tables.values():
        indexes = table_info.get("indexes", []) or []
        for idx in list(indexes):
            if idx.get("name", "").lower() == index_name.lower():
                indexes.remove(idx)
                found = True
                break
        if found:
            break

    if not found and "IF EXISTS" not in match.string.upper():
        raise InvalidRequest(f"Index '{index_name_full}' does not exist")

    state.update_system_schema()
    print(f"Dropped index '{index_name_full}' in keyspace '{keyspace_name}'")
    return []
