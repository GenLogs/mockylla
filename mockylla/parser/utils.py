import ast
import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from cassandra import InvalidRequest


def cast_value(value, cql_type):
    """Casts a value to a Python type based on CQL type."""

    if value is None:
        return None

    cql_type = (cql_type or "").lower()

    if isinstance(value, (list, tuple)):
        return value

    caster = _CASTERS.get(cql_type)
    if caster:
        return caster(value)

    if cql_type.startswith("list<"):
        return _cast_list(value, cql_type)
    if cql_type.startswith("set<"):
        return _cast_set(value, cql_type)
    if cql_type.startswith("map<"):
        return _cast_map(value, cql_type)

    return value


def get_keyspace_and_name(name_full, session_keyspace):
    """
    Splits a full name like 'keyspace.name' into its components.
    Uses the session keyspace if no keyspace is specified.
    """
    if "." in name_full:
        keyspace_name, name = name_full.split(".", 1)
    elif session_keyspace:
        keyspace_name, name = session_keyspace, name_full
    else:
        raise InvalidRequest(f"No keyspace specified for {name_full}")
    return keyspace_name, name


def get_table(table_name_full, session, state):
    """Get keyspace, table name and table data from state."""
    keyspace_name, table_name = get_keyspace_and_name(
        table_name_full, session.keyspace
    )

    if (
        keyspace_name not in state.keyspaces
        or table_name not in state.keyspaces[keyspace_name]["tables"]
    ):
        raise InvalidRequest(f"Table '{table_name_full}' does not exist")

    table_info = state.keyspaces[keyspace_name]["tables"][table_name]
    return keyspace_name, table_name, table_info


def parse_with_options(options_str):
    """Parse WITH options into a dictionary."""

    if not options_str:
        return {}

    options = {}
    # Normalize spacing and split by AND while respecting case-insensitivity
    parts = re.split(r"\s+AND\s+", options_str.strip(), flags=re.IGNORECASE)

    for part in parts:
        cleaned = part.strip().rstrip(";")
        if not cleaned:
            continue
        if "=" not in cleaned:
            continue
        key, value = cleaned.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        options[key] = value

    return options


def parse_where_clause(where_clause_str, schema):
    """Parse WHERE clause conditions into structured format."""
    where_clause_str = where_clause_str.rstrip(";")

    if not where_clause_str:
        return []

    conditions = [
        cond.strip()
        for cond in re.split(
            r"\s+AND\s+", where_clause_str, flags=re.IGNORECASE
        )
    ]

    return __parse_conditions(conditions, schema)


def __parse_conditions(conditions, schema):
    """Parse conditions into structured format."""
    parsed_conditions = []
    for cond in conditions:
        in_match = re.match(
            r"(\w+)\s+IN\s+\((.*)\)", cond.strip(), re.IGNORECASE
        )
        if in_match:
            parsed_conditions.append(__parse_in_condition(in_match, schema))
            continue

        match = re.match(
            r"(\w+)\s*([<>=]+)\s*(?:'([^']*)'|\"([^\"]*)\"|([\w\.-]+))",
            cond.strip(),
        )
        if match:
            parsed_conditions.append(
                __parse_comparison_condition(match, schema)
            )
    return parsed_conditions


def __parse_in_condition(in_match, schema):
    """Parse IN condition from regex match."""
    col, values_str = in_match.groups()
    values = [v.strip().strip("'\"") for v in values_str.split(",")]

    cql_type = schema.get(col)
    if cql_type:
        values = [cast_value(v, cql_type) for v in values]

    return (col, "IN", values)


def __parse_comparison_condition(match, schema):
    """Parse comparison condition from regex match."""
    col, op, v1, v2, v3 = match.groups()
    val = next((v for v in [v1, v2, v3] if v is not None), None)

    cql_type = schema.get(col)
    if cql_type:
        val = cast_value(val, cql_type)

    return (col, op, val)


def check_row_conditions(row, parsed_conditions):
    """Check if a row matches all parsed conditions."""
    for col, op, val in parsed_conditions:
        row_val = row.get(col)
        if row_val is None:
            return False

        if not __check_condition(row_val, op, val):
            return False
    return True


def __check_condition(row_val, op, val):
    """Check if a single condition is met."""
    if op == "=":
        return row_val == val
    elif op == ">":
        return row_val > val
    elif op == "<":
        return row_val < val
    elif op == ">=":
        return row_val >= val
    elif op == "<=":
        return row_val <= val
    elif op == "IN":
        return row_val in val
    return False


def _cast_int(value):
    return int(_strip_quotes(value))


def _cast_float(value):
    return float(_strip_quotes(value))


def _cast_decimal(value):
    val = _strip_quotes(value)
    return Decimal(str(val))


def _cast_text(value):
    return str(_strip_quotes(value))


def _cast_bool(value):
    if isinstance(value, bool):
        return value
    val = _strip_quotes(value).lower()
    if val in {"true", "1"}:
        return True
    if val in {"false", "0"}:
        return False
    raise ValueError(f"Cannot cast value '{value}' to boolean")


def _cast_uuid(value):
    if isinstance(value, uuid.UUID):
        return value
    val = _strip_quotes(value)
    try:
        return uuid.UUID(str(val))
    except (ValueError, AttributeError, TypeError):
        return value


def _cast_timestamp(value):
    if isinstance(value, datetime):
        return value
    val = _strip_quotes(value)
    if isinstance(val, (int, float)):
        return _timestamp_from_epoch(val)

    try:
        # Support ISO formats; fallback to epoch representation
        return datetime.fromisoformat(str(val))
    except ValueError:
        try:
            return _timestamp_from_epoch(float(val))
        except (ValueError, TypeError):
            raise ValueError(
                f"Cannot cast value '{value}' to timestamp"
            ) from None


def _timestamp_from_epoch(raw):
    # Cassandra represents timestamps in milliseconds when integers are used
    seconds = raw / 1000 if abs(raw) > 10**12 else raw
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def _cast_date(value):
    ts = _cast_timestamp(value)
    return ts.date()


def _cast_time(value):
    ts = _cast_timestamp(value)
    return ts.time()


def _cast_list(value, cql_type):
    inner_type = cql_type[cql_type.find("<") + 1 : -1].strip()
    parsed = _ensure_iterable(value)
    return [cast_value(item, inner_type) for item in parsed]


def _cast_set(value, cql_type):
    inner_type = cql_type[cql_type.find("<") + 1 : -1].strip()
    parsed = _ensure_iterable(value)
    return set(cast_value(item, inner_type) for item in parsed)


def _cast_map(value, cql_type):
    key_type, value_type = [
        part.strip() for part in cql_type[4:-1].split(",", 1)
    ]
    parsed = _ensure_mapping(value)
    return {
        cast_value(k, key_type): cast_value(v, value_type)
        for k, v in parsed.items()
    }


def _strip_quotes(value):
    if isinstance(value, str):
        stripped = value.strip()
        if (stripped.startswith("'") and stripped.endswith("'")) or (
            stripped.startswith('"') and stripped.endswith('"')
        ):
            return stripped[1:-1]
        return stripped
    return value


def _ensure_iterable(value):
    parsed = _maybe_parse_literal(value)
    if isinstance(parsed, (list, tuple, set)):
        return list(parsed)
    return [parsed]


def _ensure_mapping(value):
    parsed = _maybe_parse_literal(value)
    if isinstance(parsed, dict):
        return parsed
    raise ValueError(f"Cannot cast value '{value}' to map")


def _maybe_parse_literal(value):
    if isinstance(value, str):
        stripped = value.strip()
        try:
            return ast.literal_eval(_normalise_literal_booleans(stripped))
        except (ValueError, SyntaxError):
            return _strip_quotes(stripped)
    return value


def _normalise_literal_booleans(value):
    # Replace bare true/false/null with Python equivalents outside of quoted strings
    def replacer(match):
        token = match.group(0).lower()
        return {
            "true": "True",
            "false": "False",
            "null": "None",
        }[token]

    return re.sub(r"\b(true|false|null)\b", replacer, value)


_CASTERS = {
    "int": _cast_int,
    "bigint": _cast_int,
    "smallint": _cast_int,
    "tinyint": _cast_int,
    "integer": _cast_int,
    "counter": _cast_int,
    "varint": _cast_int,
    "double": _cast_float,
    "float": _cast_float,
    "decimal": _cast_decimal,
    "boolean": _cast_bool,
    "bool": _cast_bool,
    "text": _cast_text,
    "varchar": _cast_text,
    "ascii": _cast_text,
    "inet": _cast_text,
    "uuid": _cast_uuid,
    "timeuuid": _cast_uuid,
    "timestamp": _cast_timestamp,
    "date": _cast_date,
    "time": _cast_time,
}
