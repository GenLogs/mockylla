# Introspecting State

The helper functions expose the in-memory schema and data so your tests can assert on driver-visible state without round-tripping through CQL.

```python
from mockylla import (
    get_keyspaces,
    get_tables,
    get_table_rows,
    get_types,
    mock_scylladb,
)
from cassandra.cluster import Cluster

@mock_scylladb
def inspect_mock_state():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute("CREATE TYPE full_name (first_name text, last_name text)")
    session.execute(
        "CREATE TABLE users (id int PRIMARY KEY, name frozen<full_name>)"
    )

    session.execute(
        "INSERT INTO users (id, name) VALUES (1, {first_name: 'Ada', last_name: 'Lovelace'})"
    )

    keyspaces = get_keyspaces()
    assert "ks" in keyspaces

    tables = get_tables("ks")
    assert tables["users"]["schema"] == {
        "id": "int",
        "name": "frozen<full_name>",
    }

    udts = get_types("ks")
    assert udts["full_name"]["fields"] == {
        "first_name": "text",
        "last_name": "text",
    }

    rows = get_table_rows("ks", "users")
    assert rows == [
        {
            "id": 1,
            "name": {"first_name": "Ada", "last_name": "Lovelace"},
        }
    ]


if __name__ == "__main__":
    inspect_mock_state()
```
