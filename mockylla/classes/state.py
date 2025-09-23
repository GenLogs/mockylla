from cassandra import InvalidRequest


class ScyllaState:
    """Manages the in-memory state of the mock ScyllaDB."""

    def __init__(self):
        self.keyspaces = {
            "system": {
                "tables": {
                    "local": {
                        "schema": {
                            "key": "text",
                            "rpc_address": "inet",
                            "data_center": "text",
                            "rack": "text",
                        },
                        "data": [
                            {
                                "key": "local",
                                "rpc_address": "127.0.0.1",
                                "data_center": "datacenter1",
                                "rack": "rack1",
                            }
                        ],
                        "indexes": [],
                    }
                },
                "types": {},
                "views": {},
                "replication": {
                    "class": "SimpleStrategy",
                    "replication_factor": "1",
                },
                "durable_writes": True,
            }
        }
        self._ensure_system_schema_structure()
        self.update_system_schema()

    def reset(self):
        """Resets the state to a clean slate."""
        self.__init__()

    def _ensure_system_schema_structure(self):
        if "system_schema" not in self.keyspaces:
            self.keyspaces["system_schema"] = {
                "tables": {
                    "keyspaces": {
                        "schema": {
                            "keyspace_name": "text",
                            "durable_writes": "boolean",
                            "replication": "map<text, text>",
                        },
                        "primary_key": ["keyspace_name"],
                        "data": [],
                    },
                    "tables": {
                        "schema": {
                            "keyspace_name": "text",
                            "table_name": "text",
                        },
                        "primary_key": ["keyspace_name", "table_name"],
                        "data": [],
                    },
                    "columns": {
                        "schema": {
                            "keyspace_name": "text",
                            "table_name": "text",
                            "column_name": "text",
                            "kind": "text",
                            "type": "text",
                        },
                        "primary_key": [
                            "keyspace_name",
                            "table_name",
                            "column_name",
                        ],
                        "data": [],
                    },
                    "indexes": {
                        "schema": {
                            "keyspace_name": "text",
                            "table_name": "text",
                            "index_name": "text",
                            "target": "text",
                        },
                        "primary_key": [
                            "keyspace_name",
                            "table_name",
                            "index_name",
                        ],
                        "data": [],
                    },
                    "views": {
                        "schema": {
                            "keyspace_name": "text",
                            "view_name": "text",
                            "base_table_name": "text",
                            "where_clause": "text",
                        },
                        "primary_key": [
                            "keyspace_name",
                            "view_name",
                        ],
                        "data": [],
                    },
                },
                "types": {},
                "replication": {
                    "class": "SimpleStrategy",
                    "replication_factor": "1",
                },
                "durable_writes": True,
            }

    def update_system_schema(self):
        self._ensure_system_schema_structure()

        system_schema_tables = self.keyspaces["system_schema"]["tables"]

        (
            keyspaces_rows,
            tables_rows,
            columns_rows,
            indexes_rows,
            views_rows,
        ) = self._collect_system_schema_rows()

        system_schema_tables["keyspaces"]["data"] = keyspaces_rows
        system_schema_tables["tables"]["data"] = tables_rows
        system_schema_tables["columns"]["data"] = columns_rows
        system_schema_tables["indexes"]["data"] = indexes_rows
        system_schema_tables["views"]["data"] = views_rows

    def _collect_system_schema_rows(self):
        keyspaces_rows = []
        tables_rows = []
        columns_rows = []
        indexes_rows = []
        views_rows = []

        for keyspace_name, keyspace_info in self.keyspaces.items():
            keyspaces_rows.append(
                self._build_keyspace_row(keyspace_name, keyspace_info)
            )
            table_rows, column_rows, index_rows = self._build_table_rows(
                keyspace_name, keyspace_info
            )
            tables_rows.extend(table_rows)
            columns_rows.extend(column_rows)
            indexes_rows.extend(index_rows)
            views_rows.extend(
                self._build_view_rows(keyspace_name, keyspace_info)
            )

        return (
            keyspaces_rows,
            tables_rows,
            columns_rows,
            indexes_rows,
            views_rows,
        )

    def _build_keyspace_row(self, keyspace_name, keyspace_info):
        replication = {
            str(k): str(v)
            for k, v in keyspace_info.get("replication", {}).items()
        }
        if not replication:
            replication = {
                "class": "SimpleStrategy",
                "replication_factor": "1",
            }

        return {
            "keyspace_name": keyspace_name,
            "durable_writes": keyspace_info.get("durable_writes", True),
            "replication": replication,
        }

    def _build_table_rows(self, keyspace_name, keyspace_info):
        tables_rows = []
        columns_rows = []
        indexes_rows = []

        for table_name, table_info in keyspace_info.get("tables", {}).items():
            tables_rows.append(
                {
                    "keyspace_name": keyspace_name,
                    "table_name": table_name,
                }
            )
            columns_rows.extend(
                self._build_column_rows(keyspace_name, table_name, table_info)
            )
            indexes_rows.extend(
                self._build_index_rows(keyspace_name, table_name, table_info)
            )

        return tables_rows, columns_rows, indexes_rows

    def _build_column_rows(self, keyspace_name, table_name, table_info):
        schema = table_info.get("schema", {})
        primary_key = table_info.get("primary_key", [])
        partition_keys = primary_key[:1]
        clustering_keys = primary_key[1:]

        column_rows = []
        for column_name, data_type in schema.items():
            column_rows.append(
                {
                    "keyspace_name": keyspace_name,
                    "table_name": table_name,
                    "column_name": column_name,
                    "kind": self._determine_column_kind(
                        column_name, partition_keys, clustering_keys
                    ),
                    "type": data_type,
                }
            )

        return column_rows

    def _determine_column_kind(
        self, column_name, partition_keys, clustering_keys
    ):
        if column_name in partition_keys:
            return "partition_key"
        if column_name in clustering_keys:
            return "clustering"
        return "regular"

    def _build_index_rows(self, keyspace_name, table_name, table_info):
        index_rows = []
        for index in table_info.get("indexes", []) or []:
            index_rows.append(
                {
                    "keyspace_name": keyspace_name,
                    "table_name": table_name,
                    "index_name": index.get("name"),
                    "target": index.get("column"),
                }
            )
        return index_rows

    def _build_view_rows(self, keyspace_name, keyspace_info):
        view_rows = []
        for view_name, view_info in keyspace_info.get("views", {}).items():
            view_rows.append(
                {
                    "keyspace_name": keyspace_name,
                    "view_name": view_name,
                    "base_table_name": view_info.get("base_table"),
                    "where_clause": view_info.get("where_clause", ""),
                }
            )
        return view_rows


_global_state = None


def _set_global_state(state):
    """Sets the global state for the mock."""
    global _global_state
    _global_state = state


def get_keyspaces():
    """Returns a dictionary of the created keyspaces in the mock state."""
    if _global_state is None:
        raise InvalidRequest("Mock is not active.")
    return _global_state.keyspaces


def get_tables(keyspace_name):
    """Returns a dictionary of the created tables for a given keyspace."""
    if _global_state is None:
        raise InvalidRequest("Mock is not active.")
    if keyspace_name not in _global_state.keyspaces:
        raise InvalidRequest(
            f"Keyspace '{keyspace_name}' does not exist in mock state."
        )
    return _global_state.keyspaces[keyspace_name]["tables"]


def get_table_rows(keyspace_name, table_name):
    """Returns a list of rows for a given table in a keyspace."""
    tables = get_tables(keyspace_name)
    if table_name not in tables:
        raise InvalidRequest(
            f"Table '{table_name}' does not exist in keyspace '{keyspace_name}'."
        )
    return tables[table_name]["data"]


def get_types(keyspace_name):
    """Returns a dictionary of the created types for a given keyspace."""
    if _global_state is None:
        raise InvalidRequest("Mock is not active.")
    if keyspace_name not in _global_state.keyspaces:
        raise InvalidRequest(
            f"Keyspace '{keyspace_name}' does not exist in mock state."
        )
    return _global_state.keyspaces[keyspace_name].get("types", {})


__all__ = [
    "ScyllaState",
    "_set_global_state",
    "get_keyspaces",
    "get_tables",
    "get_table_rows",
    "get_types",
]
