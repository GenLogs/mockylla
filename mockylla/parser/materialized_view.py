import re

from cassandra import InvalidRequest

from copy import deepcopy

from mockylla.parser.utils import (
    get_keyspace_and_name,
    get_table,
    parse_with_options,
)


def _split_top_level(value):
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
    return [p for p in parts if p]


def _parse_primary_key(primary_key_str):
    inner = primary_key_str.strip()
    if inner.startswith("(") and inner.endswith(")"):
        inner = inner[1:-1].strip()

    components = _split_top_level(inner)
    if not components:
        return {"partition": [], "clustering": [], "all": []}

    first = components[0]
    if first.startswith("(") and first.endswith(")"):
        partition = _split_top_level(first[1:-1].strip())
    else:
        partition = [first.strip()]

    clustering = [comp.strip() for comp in components[1:]]
    return {
        "partition": partition,
        "clustering": clustering,
        "all": partition + clustering,
    }


def _parse_select_columns(select_clause, base_schema):
    clause = select_clause.strip()
    if clause == "*":
        return list(base_schema.keys())
    columns = [part.strip() for part in clause.split(",") if part.strip()]
    return columns


def _parse_view_filters(where_clause):
    clause = (where_clause or "").strip()
    if not clause:
        return []
    filters = []
    for condition in re.split(r"\s+AND\s+", clause, flags=re.IGNORECASE):
        condition = condition.strip()
        if not condition:
            continue
        match_not_null = re.fullmatch(
            r"(\w+)\s+IS\s+NOT\s+NULL", condition, re.IGNORECASE
        )
        if match_not_null:
            filters.append(("not_null", match_not_null.group(1)))
            continue
        match_equals = re.fullmatch(
            r"(\w+)\s*=\s*(.+)", condition, re.IGNORECASE
        )
        if match_equals:
            column = match_equals.group(1)
            value = match_equals.group(2).strip().strip("'\"")
            filters.append(("equals", column, value))
            continue
        raise InvalidRequest(
            f"Unsupported materialized view WHERE condition: {condition}"
        )
    return filters


def _row_matches_filters(row, filters):
    for filter_def in filters:
        if filter_def[0] == "not_null":
            column = filter_def[1]
            if row.get(column) is None:
                return False
        elif filter_def[0] == "equals":
            column, expected = filter_def[1], filter_def[2]
            if str(row.get(column)) != expected:
                return False
    return True


def _project_row(row, columns):
    projected = {column: row.get(column) for column in columns}
    meta = row.get("__meta")
    if isinstance(meta, dict):
        projected["__meta"] = deepcopy(meta)
    return projected


def rebuild_materialized_views(state, keyspace_name, base_table_name):
    keyspace = state.keyspaces.get(keyspace_name)
    if not keyspace:
        return

    views = keyspace.get("views", {})
    base_table = keyspace.get("tables", {}).get(base_table_name)
    if not base_table:
        return

    base_rows = base_table.get("data", [])

    for view_name, view_info in views.items():
        if view_info.get("base_table") != base_table_name:
            continue
        view_table = keyspace["tables"].get(view_name)
        if view_table is None:
            continue

        filters = view_info.get("filters", [])
        columns = view_info.get(
            "select_columns", list(view_table.get("schema", {}).keys())
        )
        view_rows = []

        for row in base_rows:
            if not _row_matches_filters(row, filters):
                continue
            view_rows.append(_project_row(row, columns))

        view_table["data"] = view_rows


def handle_create_materialized_view(match, session, state):
    (
        view_name_full,
        select_clause,
        base_table_full,
        where_clause,
        primary_key_str,
        options_str,
    ) = match.groups()

    keyspace_name, view_name = get_keyspace_and_name(
        view_name_full, session.keyspace
    )
    base_keyspace_name, base_table_name, base_table_info = get_table(
        base_table_full, session, state
    )

    if keyspace_name not in state.keyspaces:
        raise InvalidRequest(f"Keyspace '{keyspace_name}' does not exist")

    keyspace = state.keyspaces[keyspace_name]
    views = keyspace.setdefault("views", {})
    if view_name in views:
        if "IF NOT EXISTS" in match.string.upper():
            return []
        raise InvalidRequest(
            f"Materialized view '{view_name_full}' already exists"
        )

    base_schema = base_table_info.get("schema", {})
    select_columns = _parse_select_columns(select_clause, base_schema)
    invalid_columns = [
        column for column in select_columns if column not in base_schema
    ]
    if invalid_columns:
        invalid = ", ".join(invalid_columns)
        raise InvalidRequest(
            f"Columns {invalid} do not exist on base table '{base_table_full}'"
        )

    primary_key = _parse_primary_key(primary_key_str)
    view_schema = {column: base_schema[column] for column in select_columns}
    filters = _parse_view_filters(where_clause)

    options = parse_with_options(options_str)

    views[view_name] = {
        "select": select_clause.strip(),
        "base_table": base_table_name,
        "base_keyspace": base_keyspace_name,
        "where_clause": where_clause.strip(),
        "primary_key": primary_key,
        "options": options,
        "select_columns": select_columns,
        "filters": filters,
    }

    keyspace["tables"][view_name] = {
        "schema": view_schema,
        "primary_key": primary_key,
        "data": [],
        "indexes": [],
        "options": options,
        "materialized_from": base_table_name,
    }

    rebuild_materialized_views(state, keyspace_name, base_table_name)

    state.update_system_schema()
    print(
        f"Created materialized view '{view_name}' on base table '{base_table_name}'"
    )
    return []


def handle_drop_materialized_view(match, session, state):
    view_name_full = match.group(1)
    keyspace_name, view_name = get_keyspace_and_name(
        view_name_full, session.keyspace
    )

    if keyspace_name not in state.keyspaces:
        if "IF EXISTS" in match.string.upper():
            return []
        raise InvalidRequest(f"Keyspace '{keyspace_name}' does not exist")

    keyspace = state.keyspaces[keyspace_name]
    views = keyspace.setdefault("views", {})
    if view_name not in views:
        if "IF EXISTS" in match.string.upper():
            return []
        raise InvalidRequest(
            f"Materialized view '{view_name_full}' does not exist"
        )

    del views[view_name]
    keyspace["tables"].pop(view_name, None)
    state.update_system_schema()
    print(
        f"Dropped materialized view '{view_name}' in keyspace '{keyspace_name}'"
    )
    return []
