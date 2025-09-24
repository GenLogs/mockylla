import re
import time

from cassandra import InvalidRequest

from mockylla.parser.utils import (
    apply_write_metadata,
    build_lwt_result,
    cast_value,
    check_row_conditions,
    current_timestamp_microseconds,
    get_table,
    parse_lwt_clause,
    parse_using_options,
    parse_where_clause,
    purge_expired_rows,
    row_write_timestamp,
)


def replace_placeholders(segment, params, start_idx):
    if not segment or "%s" not in segment:
        return segment, start_idx

    parts = segment.split("%s")
    if len(parts) - 1 + start_idx > len(params):
        raise ValueError(
            "Number of parameters does not match number of placeholders in UPDATE query"
        )

    new_segment = parts[0]
    idx = start_idx
    for i in range(len(parts) - 1):
        param = params[idx]
        param_str = f"'{param}'" if isinstance(param, str) else str(param)
        new_segment += param_str + parts[i + 1]
        idx += 1
    return new_segment, idx


def handle_update(update_match, session, state, parameters=None):
    """Handle UPDATE query by parsing and executing the update operation."""
    (
        table_name_full,
        using_clause,
        set_clause_str,
        where_clause_str,
        if_clause,
    ) = update_match.groups()

    ttl_value, ttl_provided, timestamp_value, timestamp_provided = (
        parse_using_options(using_clause)
    )
    if ttl_value is not None and ttl_value < 0:
        raise InvalidRequest("TTL value must be >= 0")

    if parameters and "%s" in (set_clause_str + where_clause_str):
        set_clause_str, next_idx = replace_placeholders(
            set_clause_str, parameters, 0
        )
        where_clause_str, _ = replace_placeholders(
            where_clause_str, parameters, next_idx
        )

    _, table_name, table = get_table(table_name_full, session, state)
    schema = table["schema"]
    purge_expired_rows(table)

    set_operations, counter_operations = __parse_set_clause(
        set_clause_str, schema
    )
    parsed_conditions = parse_where_clause(where_clause_str, schema)

    clause_info = parse_lwt_clause(if_clause, schema)
    condition_type = clause_info["type"]
    lwt_conditions = clause_info.get("conditions", [])

    matching_rows = [
        row
        for row in table["data"]
        if __handle_update_check_row(row, parsed_conditions)
    ]

    write_timestamp = (
        timestamp_value
        if timestamp_provided
        else current_timestamp_microseconds()
    )
    now_seconds = None
    if ttl_provided and ttl_value and ttl_value > 0:
        now_seconds = time.time()

    if condition_type == "if_not_exists":
        if matching_rows:
            return [build_lwt_result(False, matching_rows[0])]
        __handle_upsert(
            table,
            table_name,
            parsed_conditions,
            set_operations,
            counter_operations,
            write_timestamp,
            ttl_value,
            ttl_provided,
            now_seconds,
        )
        return [build_lwt_result(True)]

    if condition_type == "if_exists":
        if not matching_rows:
            return [build_lwt_result(False)]
        __update_existing_rows(
            table,
            parsed_conditions,
            set_operations,
            counter_operations,
            write_timestamp,
            ttl_value,
            ttl_provided,
            now_seconds,
        )
        return [build_lwt_result(True)]

    if condition_type == "conditions":
        if not matching_rows:
            return [build_lwt_result(False)]
        for row in matching_rows:
            if not check_row_conditions(row, lwt_conditions):
                return [build_lwt_result(False, row)]
        __update_existing_rows(
            table,
            parsed_conditions,
            set_operations,
            counter_operations,
            write_timestamp,
            ttl_value,
            ttl_provided,
            now_seconds,
        )
        return [build_lwt_result(True)]

    rows_updated = __update_existing_rows(
        table,
        parsed_conditions,
        set_operations,
        counter_operations,
        write_timestamp,
        ttl_value,
        ttl_provided,
        now_seconds,
    )

    if rows_updated > 0:
        print(f"Updated {rows_updated} rows in '{table_name}'")
        return []

    if not matching_rows:
        __handle_upsert(
            table,
            table_name,
            parsed_conditions,
            set_operations,
            counter_operations,
            write_timestamp,
            ttl_value,
            ttl_provided,
            now_seconds,
        )
    return []


def __parse_set_clause(set_clause_str, schema):
    """Parse SET clause into regular and counter operations."""
    set_operations = {}
    counter_operations = {}
    set_pairs = [s.strip() for s in set_clause_str.split(",")]

    for pair in set_pairs:
        counter_match = re.match(
            r"(\w+)\s*=\s*\1\s*([+-])\s*(\d+)", pair, re.IGNORECASE
        )
        if counter_match:
            col, op, val_str = counter_match.groups()
            val = int(val_str)
            if op == "-":
                val = -val
            counter_operations[col] = val
        else:
            col, val_str = [p.strip() for p in pair.split("=", 1)]
            val = val_str.strip("'\"")
            cql_type = schema.get(col)
            if cql_type:
                set_operations[col] = cast_value(val, cql_type)
            else:
                set_operations[col] = val

    return set_operations, counter_operations


def __update_existing_rows(
    table,
    parsed_conditions,
    set_operations,
    counter_operations,
    write_timestamp,
    ttl_value,
    ttl_provided,
    now_seconds,
):
    """Update existing rows that match conditions."""
    rows_updated = 0
    for row in table["data"]:
        if __handle_update_check_row(row, parsed_conditions):
            existing_ts = row_write_timestamp(row)
            if write_timestamp < existing_ts:
                continue
            row.update(set_operations)
            for col, val in counter_operations.items():
                row[col] = row.get(col, 0) + val
            apply_write_metadata(
                row,
                timestamp=write_timestamp,
                ttl_value=ttl_value,
                ttl_provided=ttl_provided,
                now=now_seconds,
            )
            rows_updated += 1
    return rows_updated


def __handle_upsert(
    table,
    table_name,
    parsed_conditions,
    set_operations,
    counter_operations,
    write_timestamp,
    ttl_value,
    ttl_provided,
    now_seconds,
):
    """Handle upsert case when no existing rows were updated."""
    new_row = {}
    is_upsert = False
    for col, op, val in parsed_conditions:
        if op == "=":
            new_row[col] = val
            is_upsert = True

    if not is_upsert:
        return

    new_row.update(set_operations)
    for col, val in counter_operations.items():
        new_row[col] = val

    apply_write_metadata(
        new_row,
        timestamp=write_timestamp,
        ttl_value=ttl_value,
        ttl_provided=ttl_provided,
        now=now_seconds,
    )
    table["data"].append(new_row)
    print(f"Upserted row in '{table_name}': {new_row}")


def __handle_update_check_row(row, parsed_conditions):
    """Check if a row matches the update conditions."""
    for col, op, val in parsed_conditions:
        row_val = row.get(col)
        if op == "=" and row_val != val:
            return False
    return True
