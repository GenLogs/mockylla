import re
import time

from cassandra import InvalidRequest

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


def handle_insert_into(insert_match, session, state, parameters=None):
    (
        table_name_full,
        columns_str,
        values_str,
        using_clause,
        if_clause,
    ) = insert_match.groups()

    if "." in table_name_full:
        keyspace_name, table_name = table_name_full.split(".", 1)
    elif session.keyspace:
        keyspace_name, table_name = session.keyspace, table_name_full
    else:
        raise InvalidRequest("No keyspace specified for INSERT")

    if keyspace_name not in state.keyspaces:
        raise InvalidRequest(f"Keyspace '{keyspace_name}' does not exist")

    tables = state.keyspaces[keyspace_name]["tables"]
    if table_name not in tables:
        raise InvalidRequest(f"Table '{table_name_full}' does not exist")

    table_info = tables[table_name]
    purge_expired_rows(table_info)
    table_schema = table_info["schema"]
    primary_key_info = table_info.get("primary_key", [])
    if isinstance(primary_key_info, dict):
        primary_key_cols = primary_key_info.get("all")
        if primary_key_cols is None:
            primary_key_cols = primary_key_info.get(
                "partition", []
            ) + primary_key_info.get("clustering", [])
    else:
        primary_key_cols = primary_key_info
    defined_types = state.keyspaces[keyspace_name].get("types", {})

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
    now_seconds = None
    if ttl_provided and ttl_value and ttl_value > 0:
        now_seconds = time.time()

    columns = [c.strip() for c in columns_str.split(",")]

    if parameters:
        values = parameters
    else:
        values = _parse_values(values_str)

    if len(columns) != len(values):
        raise InvalidRequest(
            "Number of columns does not match number of values"
        )

    row_data = {}
    for col, val in zip(columns, values):
        cql_type = table_schema.get(col)
        row_data[col] = assign_row_data_value(val, cql_type, defined_types)

    pk_values = {k: row_data.get(k) for k in primary_key_cols or []}

    clause_info = parse_lwt_clause(if_clause, table_schema)
    condition_type = clause_info["type"]
    condition_rows = clause_info.get("conditions", [])

    existing = None
    if primary_key_cols:
        for candidate in table_info["data"]:
            if all(
                candidate.get(k) == pk_values.get(k) for k in primary_key_cols
            ):
                existing = candidate
                break

    new_row = dict(row_data)

    if condition_type == "if_not_exists":
        if existing is not None:
            return [build_lwt_result(False, existing)]
        apply_write_metadata(
            new_row,
            timestamp=write_timestamp,
            ttl_value=ttl_value,
            ttl_provided=ttl_provided,
            now=now_seconds,
        )
        table_info["data"].append(new_row)
        return [build_lwt_result(True)]

    if condition_type == "if_exists":
        if existing is None:
            return [build_lwt_result(False)]
        existing_ts = row_write_timestamp(existing)
        if write_timestamp < existing_ts:
            return [build_lwt_result(True)]
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
        return [build_lwt_result(True)]

    if condition_type == "conditions":
        if existing is None:
            return [build_lwt_result(False)]
        if not check_row_conditions(existing, condition_rows):
            return [build_lwt_result(False, existing)]
        existing_ts = row_write_timestamp(existing)
        if write_timestamp < existing_ts:
            return [build_lwt_result(True)]
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
        return [build_lwt_result(True)]

    if existing is not None:
        existing_ts = row_write_timestamp(existing)
        if write_timestamp < existing_ts:
            return []
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
    else:
        apply_write_metadata(
            new_row,
            timestamp=write_timestamp,
            ttl_value=ttl_value,
            ttl_provided=ttl_provided,
            now=now_seconds,
        )
        table_info["data"].append(new_row)
    print(f"Inserted row into '{table_name}': {row_data}")
    return []
