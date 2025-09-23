# mockylla

![PyPI - Version](https://img.shields.io/pypi/v/mockylla)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/mockylla)
![License](https://img.shields.io/github/license/GenLogs/mockylla)

A lightweight, in-memory mock for the ScyllaDB **Python** driver.
`mockylla` allows you to run integration-style tests for code that depends on ScyllaDB **without** requiring a live cluster.

---

## ✨ Key Features

- **Drop-in replacement** &nbsp;|&nbsp; Patch the `scylla-driver` at runtime with a single decorator – no changes to your application code.
- **Fast & isolated** &nbsp;|&nbsp; All state lives in-memory and is reset between tests, ensuring perfect test isolation.
- **Inspectable** &nbsp;|&nbsp; Helper utilities expose the internal state so you can assert against keyspaces, tables, and rows.
- **Pythonic API** &nbsp;|&nbsp; Mirrors the real driver's public API to minimise cognitive load and surprises.
- **No network dependencies** &nbsp;|&nbsp; Works entirely offline; ideal for CI pipelines and contributor development environments.

---

## 📦 Installation

```bash
pip install mockylla
```

`mockylla` supports **Python 3.8 → 3.11** and is continuously tested against the latest **scylla-driver** release.

---

## 🚀 Quick Start

```python
from mockylla import mock_scylladb, get_keyspaces
from cassandra.cluster import Cluster

@mock_scylladb
def test_my_app_creates_a_keyspace():
    # Arrange – connect to the mocked cluster (no real network I/O!)
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    # Act – run application logic
    session.execute(
        """
        CREATE KEYSPACE my_app_keyspace \
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
    )

    # Assert – inspect mock state
    assert "my_app_keyspace" in get_keyspaces()
```

> **Tip**
> Place `@mock_scylladb` on individual tests **or** a session-scoped fixture to enable the mock for an entire module.

---

## 🏗️ Comprehensive Example

```python
from mockylla import mock_scylladb, get_table_rows
from cassandra.cluster import Cluster

@mock_scylladb
def test_crud():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")

    session.execute(
        """
        CREATE TABLE users (
            user_id int PRIMARY KEY,
            name text,
            email text
        )
        """
    )

    # INSERT
    session.execute("INSERT INTO users (user_id, name, email) VALUES (1, 'Alice', 'alice@example.com')")

    # SELECT
    assert session.execute("SELECT name FROM users WHERE user_id = 1").one().name == "Alice"

    # UPDATE
    session.execute("UPDATE users SET email = 'alice@new.com' WHERE user_id = 1")

    # DELETE
    session.execute("DELETE FROM users WHERE user_id = 1")

    # Final state check
    assert get_table_rows("ks", "users") == []
```

---

## 📚 Examples

Browse the `examples/` directory for more focused walkthroughs:

- `examples/00-basic-usage.md` – scaffold a keyspace, table, and CRUD flow with the decorator.
- `examples/01-prepared-statements.md` – prepare, bind, and execute statements with tuple or mapping parameters.
- `examples/02-batch-operations.md` – mix string `BEGIN BATCH` blocks with `BatchStatement` helpers and mock batches.
- `examples/03-async-queries.md` – exercise `execute_async` flows and callbacks without a real cluster.
- `examples/04-introspecting-state.md` – inspect keyspaces, tables, UDTs, and rows via the helper utilities.

---

## 🔍 Public API

| Function / Decorator              | Description                                                                                                |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `mock_scylladb`                   | Context-manages the mock driver by patching `cassandra.connection.Connection.factory` & `Cluster.connect`. |
| `get_keyspaces()`                 | Return a `dict` of keyspace names → definition.                                                            |
| `get_tables(keyspace)`            | Return a `dict` of table names → definition.                                                               |
| `get_table_rows(keyspace, table)` | Return the current rows for *table* as a `list[dict]`.                                                     |
| `get_types(keyspace)`             | Return user-defined types for the keyspace.                                                                |

---

## 🗂️ Package Structure

The implementation now lives in the `mockylla/classes/` package, split into focused modules:

- `state.py` – in-memory `ScyllaState` and helpers for inspecting tables, rows, and types.
- `statements.py` – prepared/bound/batch statement shims plus placeholder utilities.
- `session.py` – `MockSession`, `MockCluster`, and async response primitives.
- `metadata.py` – lightweight metadata facades mirroring the driver objects.
- `scylladb.py` – context manager + decorator that patch the driver and bootstrap state.

The top-level `mockylla/__init__.py` re-exports the public API unchanged for consumers.

---

## 📄 License

`mockylla` is distributed under the [MIT](LICENSE) license.

---

## 🙌 Acknowledgements

- Inspired by the fantastic [`moto`](https://github.com/getmoto/moto) project for AWS.
- Built on top of the official [`scylla-driver`](https://github.com/scylladb/python-driver) by ScyllaDB.
