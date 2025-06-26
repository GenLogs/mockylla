import re


def _parse_column_defs(columns_str):
    """
    Parses a string of CQL column definitions, respecting < > for collection types.
    """
    defs = []
    current_def = ""
    level = 0

    for char in columns_str:
        if char == "<":
            level += 1
        elif char == ">":
            level -= 1
        elif char == "," and level == 0:
            defs.append(current_def.strip())
            current_def = ""
            continue
        current_def += char

    defs.append(current_def.strip())
    return [d for d in defs if d]


def handle_create_keyspace(create_keyspace_match, state):
    keyspace_name = create_keyspace_match.group(1)
    if keyspace_name in state.keyspaces:
        raise Exception(f"Keyspace '{keyspace_name}' already exists")

    state.keyspaces[keyspace_name] = {"tables": {}}
    print(f"Created keyspace: {keyspace_name}")
    return []


def handle_create_table(create_table_match, session, state):
    table_name_full, columns_str = create_table_match.groups()

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

    primary_key = []
    pk_match = re.search(r"PRIMARY\s+KEY\s*\((.*?)\)", columns_str, re.IGNORECASE)
    if pk_match:
        pk_def = pk_match.group(1)

        pk_columns_str = pk_def.replace("(", "").replace(")", "")
        pk_cols = [c.strip() for c in pk_columns_str.split(",") if c.strip()]
        primary_key.extend(pk_cols)

        columns_str = columns_str[: pk_match.start()] + columns_str[pk_match.end() :]

    column_defs = _parse_column_defs(columns_str)

    columns = []
    for c in column_defs:
        parts = c.split(None, 1)
        if len(parts) == 2:
            name, type_ = parts

            if "PRIMARY KEY" in type_.upper():
                if name not in primary_key:
                    primary_key.append(name)
            type_ = re.sub(r"\s+PRIMARY\s+KEY", "", type_, flags=re.IGNORECASE).strip()
            columns.append((name, type_))

    schema = {name: type_ for name, type_ in columns if name}

    state.keyspaces[keyspace_name]["tables"][table_name] = {
        "schema": schema,
        "primary_key": primary_key,
        "data": [],
    }
    print(
        f"Created table '{table_name}' in keyspace '{keyspace_name}' with schema: {schema}"
    )
    return []
