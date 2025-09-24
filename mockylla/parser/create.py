import ast
import re

from cassandra import InvalidRequest


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
    replication_str = create_keyspace_match.group(2)
    if keyspace_name in state.keyspaces:
        raise InvalidRequest(f"Keyspace '{keyspace_name}' already exists")

    replication = _parse_replication(replication_str)

    state.keyspaces[keyspace_name] = {
        "tables": {},
        "types": {},
        "views": {},
        "replication": replication,
        "durable_writes": True,
    }
    print(f"Created keyspace: {keyspace_name}")
    state.update_system_schema()
    return []


def handle_create_table(create_table_match, session, state):
    table_name_full, columns_str, options_str = create_table_match.groups()

    if "." in table_name_full:
        keyspace_name, table_name = table_name_full.split(".", 1)
    elif session.keyspace:
        keyspace_name, table_name = session.keyspace, table_name_full
    else:
        raise InvalidRequest("No keyspace specified for CREATE TABLE")

    if keyspace_name not in state.keyspaces:
        raise InvalidRequest(f"Keyspace '{keyspace_name}' does not exist")

    if table_name in state.keyspaces[keyspace_name]["tables"]:
        raise InvalidRequest(
            f"Table '{table_name}' already exists in keyspace '{keyspace_name}'"
        )

    primary_key = []
    pk_match = re.search(
        r"PRIMARY\s+KEY\s*\((.*?)\)", columns_str, re.IGNORECASE
    )
    if pk_match:
        pk_def = pk_match.group(1)

        pk_columns_str = pk_def.replace("(", "").replace(")", "")
        pk_cols = [c.strip() for c in pk_columns_str.split(",") if c.strip()]
        primary_key.extend(pk_cols)

        columns_str = (
            columns_str[: pk_match.start()] + columns_str[pk_match.end() :]
        )

    column_defs = _parse_column_defs(columns_str)

    columns = []
    for c in column_defs:
        parts = c.split(None, 1)
        if len(parts) == 2:
            name, type_ = parts

            if "PRIMARY KEY" in type_.upper():
                if name not in primary_key:
                    primary_key.append(name)
            type_ = re.sub(
                r"\s+PRIMARY\s+KEY", "", type_, flags=re.IGNORECASE
            ).strip()
            columns.append((name, type_))

    schema = {name: type_ for name, type_ in columns if name}

    options = {}
    if options_str:
        from mockylla.parser.utils import parse_with_options

        options = parse_with_options(options_str)

    state.keyspaces[keyspace_name]["tables"][table_name] = {
        "schema": schema,
        "primary_key": primary_key,
        "data": [],
        "indexes": [],
        "options": options,
    }
    print(
        f"Created table '{table_name}' in keyspace '{keyspace_name}' with schema: {schema}"
    )
    state.update_system_schema()
    return []


def _parse_replication(replication_str):
    try:
        replication_config = ast.literal_eval(replication_str)
        if isinstance(replication_config, dict):
            return {str(k): str(v) for k, v in replication_config.items()}
    except (ValueError, SyntaxError):
        pass
    return {
        "class": "SimpleStrategy",
        "replication_factor": "1",
    }
