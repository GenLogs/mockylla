import re
import time

from cassandra import InvalidRequest

from mockylla.parser.materialized_view import rebuild_materialized_views
from mockylla.parser.utils import (
    apply_write_metadata,
    build_lwt_result,
    cast_value,
    current_timestamp_microseconds,
    check_row_conditions,
    parse_lwt_clause,
    parse_using_options,
    purge_expired_rows,
    row_write_timestamp,
)


def _parse_udt_literal(literal):
    """Parses a UDT literal string like '{key1: val1, key2: val2}' into a dict."""
    if isinstance(literal, dict):
        return literal
    if not literal.startswith("{") or not literal.endswith("}"):
        return literal

    content = literal[1:-1].strip()
    udt_dict = {}

    for match in re.finditer(r"(\w+)\s*:\s*(?:'([^']*)'|([^,}\s]+))", content):
        key, val_quoted, val_unquoted = match.groups()
        val = val_quoted if val_quoted is not None else val_unquoted
        udt_dict[key.strip()] = val
    return udt_dict


def _parse_values(values_str):
    """
    Parses a string of CQL values, respecting parentheses, brackets, and braces.
    Example: "1, {key: 'val', key2: 'val2'}, [1, 2, 3]"
    """
    values = []
    current_val = ""
    level = 0
    in_string = False

    for char in values_str:
        if char == "'" and (len(current_val) == 0 or current_val[-1] != "\\\\"):
            in_string = not in_string
        elif char in "({[" and not in_string:
            level += 1
        elif char in ")}]" and not in_string:
            level -= 1
        elif char == "," and level == 0 and not in_string:
            values.append(current_val.strip())
            current_val = ""
            continue
        current_val += char

    values.append(current_val.strip())
    return values


def assign_row_data_value(val, cql_type, defined_types):
    if cql_type in defined_types:
        if isinstance(val, str):
            return _parse_udt_literal(val)
        else:
            return cast_value(val, cql_type)
    elif cql_type:
        if isinstance(val, str):
            return cast_value(val.strip("'\""), cql_type)
        else:
            return cast_value(val, cql_type)
    else:
        return val


def _determine_insert_target(table_name_full, session, state):
    if "." in table_name_full:
        keyspace_name, table_name = table_name_full.split(".", 1)
    elif session.keyspace:
        keyspace_name, table_name = session.keyspace, table_name_full
    else:
        raise InvalidRequest("No keyspace specified for INSERT")

    keyspace_info = state.keyspaces.get(keyspace_name)
    if keyspace_info is None:
        raise InvalidRequest(f"Keyspace '{keyspace_name}' does not exist")

    table_info = keyspace_info.get("tables", {}).get(table_name)
    if table_info is None:
        raise InvalidRequest(f"Table '{table_name_full}' does not exist")

    return keyspace_name, table_name, table_info


def _primary_key_columns(primary_key_info):
    if isinstance(primary_key_info, dict):
        pk_columns = primary_key_info.get("all")
        if pk_columns is None:
            pk_columns = primary_key_info.get(
                "partition", []
            ) + primary_key_info.get("clustering", [])
        return pk_columns
    return primary_key_info


def _resolve_write_directives(using_clause):
    ttl_value, ttl_provided, timestamp_value, timestamp_provided = (
        parse_using_options(using_clause)
    )
    if ttl_value is not None and ttl_value < 0:
        raise InvalidRequest("TTL value must be >= 0")
    write_timestamp = (
        timestamp_value
        if timestamp_provided
        else current_timestamp_microseconds()
    )
    now_seconds = (
        time.time() if ttl_provided and ttl_value and ttl_value > 0 else None
    )
    return ttl_value, ttl_provided, write_timestamp, now_seconds


def _coerce_values(columns_str, values_str, parameters):
    columns = [column.strip() for column in columns_str.split(",")]
    values = parameters if parameters else _parse_values(values_str)

    if len(columns) != len(values):
        raise InvalidRequest(
            "Number of columns does not match number of values"
        )
    return columns, values


def _build_row_data(columns, values, table_schema, defined_types):
    row_data = {}
    for column, value in zip(columns, values):
        cql_type = table_schema.get(column)
        row_data[column] = assign_row_data_value(value, cql_type, defined_types)
    return row_data


def _find_existing_row(table_rows, primary_key_cols, pk_values):
    if not primary_key_cols:
        return None
    for candidate in table_rows:
        if all(
            candidate.get(key) == pk_values.get(key) for key in primary_key_cols
        ):
            return candidate
    return None


def _append_new_row(
    table_info,
    new_row,
    write_timestamp,
    ttl_value,
    ttl_provided,
    now_seconds,
):
    apply_write_metadata(
        new_row,
        timestamp=write_timestamp,
        ttl_value=ttl_value,
        ttl_provided=ttl_provided,
        now=now_seconds,
    )
    table_info["data"].append(new_row)


def _overwrite_existing_row(
    existing,
    new_row,
    write_timestamp,
    ttl_value,
    ttl_provided,
    now_seconds,
):
    existing_ts = row_write_timestamp(existing)
    if write_timestamp < existing_ts:
        return False

    previous_meta = existing.get("__meta") if not ttl_provided else None
    existing.clear()
    existing.update(new_row)
    if previous_meta is not None:
        existing["__meta"] = previous_meta

    apply_write_metadata(
        existing,
        timestamp=write_timestamp,
        ttl_value=ttl_value,
        ttl_provided=ttl_provided,
        now=now_seconds,
    )
    return True


def _apply_lwt_insert(
    condition_type,
    condition_rows,
    existing,
    new_row,
    table_info,
    primary_key_cols,
    write_timestamp,
    ttl_value,
    ttl_provided,
    now_seconds,
):
    if condition_type == "if_not_exists":
        if existing is not None:
            return [build_lwt_result(False, existing)], False
        _append_new_row(
            table_info,
            new_row,
            write_timestamp,
            ttl_value,
            ttl_provided,
            now_seconds,
        )
        return [build_lwt_result(True)], True

    if condition_type == "if_exists":
        if existing is None:
            return [build_lwt_result(False)], False
        if not _overwrite_existing_row(
            existing,
            new_row,
            write_timestamp,
            ttl_value,
            ttl_provided,
            now_seconds,
        ):
            return [build_lwt_result(True)], False
        return [build_lwt_result(True)], True

    if condition_type == "conditions":
        if existing is None:
            return [build_lwt_result(False)], False
        if not check_row_conditions(existing, condition_rows):
            return [build_lwt_result(False, existing)], False
        if not _overwrite_existing_row(
            existing,
            new_row,
            write_timestamp,
            ttl_value,
            ttl_provided,
            now_seconds,
        ):
            return [build_lwt_result(True)], False
        return [build_lwt_result(True)], True

    return None, False


def handle_insert_into(insert_match, session, state, parameters=None):
    (
        table_name_full,
        columns_str,
        values_str,
        using_clause,
        if_clause,
    ) = insert_match.groups()

    keyspace_name, table_name, table_info = _determine_insert_target(
        table_name_full, session, state
    )
    purge_expired_rows(table_info)

    table_schema = table_info["schema"]
    primary_key_cols = _primary_key_columns(table_info.get("primary_key", []))
    defined_types = state.keyspaces[keyspace_name].get("types", {})

    (
        ttl_value,
        ttl_provided,
        write_timestamp,
        now_seconds,
    ) = _resolve_write_directives(using_clause)

    columns, values = _coerce_values(columns_str, values_str, parameters)
    row_data = _build_row_data(columns, values, table_schema, defined_types)

    pk_values = {key: row_data.get(key) for key in primary_key_cols or []}

    clause_info = parse_lwt_clause(if_clause, table_schema)
    condition_type = clause_info["type"]
    condition_rows = clause_info.get("conditions", [])

    existing = _find_existing_row(
        table_info["data"], primary_key_cols, pk_values
    )
    new_row = dict(row_data)

    result, mutated = _apply_lwt_insert(
        condition_type,
        condition_rows,
        existing,
        new_row,
        table_info,
        primary_key_cols,
        write_timestamp,
        ttl_value,
        ttl_provided,
        now_seconds,
    )

    if result is not None:
        if mutated:
            rebuild_materialized_views(state, keyspace_name, table_name)
        return result

    if existing is not None:
        if not _overwrite_existing_row(
            existing,
            new_row,
            write_timestamp,
            ttl_value,
            ttl_provided,
            now_seconds,
        ):
            return []
    else:
        _append_new_row(
            table_info,
            new_row,
            write_timestamp,
            ttl_value,
            ttl_provided,
            now_seconds,
        )

    rebuild_materialized_views(state, keyspace_name, table_name)
    print(f"Inserted row into '{table_name}': {row_data}")
    return []
