import ast
import re

from cassandra import InvalidRequest


def _split_top_level(value):
    """Split a comma separated string while respecting nested parentheses."""

    parts = []
    current = ""
    depth = 0

    for char in value:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(current.strip())
            current = ""
            continue
        current += char

    parts.append(current.strip())
    return [part for part in parts if part]


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

    partition_keys = []
    clustering_keys = []
    pk_start = None
    paren_start = None
    for match in re.finditer(r"PRIMARY\s+KEY", columns_str, re.IGNORECASE):
        idx = match.end()
        while idx < len(columns_str) and columns_str[idx].isspace():
            idx += 1
        if idx < len(columns_str) and columns_str[idx] == "(":
            pk_start = match.start()
            paren_start = idx
            break

    if pk_start is not None and paren_start is not None:
        depth = 0
        idx = paren_start
        while idx < len(columns_str):
            if columns_str[idx] == "(":
                depth += 1
            elif columns_str[idx] == ")":
                depth -= 1
                if depth == 0:
                    break
            idx += 1

        if depth != 0:
            raise InvalidRequest(
                "Unbalanced parentheses in PRIMARY KEY definition"
            )

        pk_end = idx + 1
        pk_def = columns_str[paren_start + 1 : pk_end - 1]
        partition_keys, clustering_keys = _parse_primary_key(pk_def)

        columns_str = columns_str[:pk_start] + columns_str[pk_end:]

    column_defs = _parse_column_defs(columns_str)

    columns = []
    inline_primary_keys = []
    for c in column_defs:
        parts = c.split(None, 1)
        if len(parts) == 2:
            name, type_ = parts

            if "PRIMARY KEY" in type_.upper():
                inline_primary_keys.append(name)
            type_ = re.sub(
                r"\s+PRIMARY\s+KEY", "", type_, flags=re.IGNORECASE
            ).strip()
            columns.append((name, type_))

    schema = {name: type_ for name, type_ in columns if name}

    clustering_orders = {}
    options = {}
    if options_str:
        from mockylla.parser.utils import parse_with_options

        clustering_orders, options_str = _extract_clustering_orders(options_str)
        options = parse_with_options(options_str)

    if not partition_keys and inline_primary_keys:
        partition_keys = inline_primary_keys

    primary_key_info = {
        "partition": partition_keys,
        "clustering": clustering_keys,
        "all": partition_keys + clustering_keys,
    }

    state.keyspaces[keyspace_name]["tables"][table_name] = {
        "schema": schema,
        "primary_key": primary_key_info,
        "data": [],
        "indexes": [],
        "options": options,
        "clustering_orders": clustering_orders,
    }
    print(
        f"Created table '{table_name}' in keyspace '{keyspace_name}' with schema: {schema}"
    )
    state.update_system_schema()
    return []


def _parse_primary_key(pk_definition):
    """Parse PRIMARY KEY definition into partition and clustering components."""

    inner = pk_definition.strip()
    if inner.startswith("(") and inner.endswith(")"):
        inner = inner[1:-1].strip()

    components = _split_top_level(inner)
    if not components:
        return [], []

    first = components[0]
    if first.startswith("(") and first.endswith(")"):
        partition = [
            part.strip()
            for part in _split_top_level(first[1:-1].strip())
            if part.strip()
        ]
    else:
        partition = [first.strip()]

    clustering = [comp.strip() for comp in components[1:]]
    return partition, clustering


def _extract_clustering_orders(options_str):
    """Extract CLUSTERING ORDER BY clause from a WITH options string."""

    parts = re.split(r"\s+AND\s+", options_str.strip(), flags=re.IGNORECASE)
    remaining_parts = []
    clustering_orders = {}

    for part in parts:
        cleaned = part.strip().rstrip(";")
        if not cleaned:
            continue

        match = re.match(
            r"(?i)^CLUSTERING\s+ORDER\s+BY\s*\((.*)\)$",
            cleaned,
        )
        if match:
            clustering_orders = _parse_clustering_order(match.group(1))
        else:
            remaining_parts.append(cleaned)

    updated_options = " AND ".join(remaining_parts)
    return clustering_orders, updated_options


def _parse_clustering_order(order_clause):
    """Parse CLUSTERING ORDER BY clause content into a mapping."""

    orders = {}
    for token in _split_top_level(order_clause):
        if not token:
            continue
        pieces = token.strip().split()
        column = pieces[0]
        direction = pieces[1].upper() if len(pieces) > 1 else "ASC"
        orders[column] = direction
    return orders


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
