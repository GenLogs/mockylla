# pyscylladb-mock

A lightweight and easy-to-use library for mocking ScyllaDB in your Python tests. Inspired by the simplicity of `moto` for AWS, `pyscylladb-mock` allows you to test your ScyllaDB-dependent code without needing a live database instance.

## The Goal

The primary goal of `pyscylladb-mock` is to provide a seamless testing experience for developers working with ScyllaDB. We aim to create a mock that is:

-   **Non-intrusive**: Use a simple decorator to mock all `scylla-driver` interactions. No changes to your application code are needed.
-   **Fast & Isolated**: All mocking happens in-memory, ensuring your tests are fast and completely isolated from each other.
-   **Inspectable**: Provides helper functions to let you check the state of the mock database during your tests, allowing you to assert that your code behaved as expected (e.g., "Was a specific row inserted into a table?").

## How It Works

`pyscylladb-mock` works by patching the underlying `scylla-driver` at runtime. When you decorate a test function with `@mock_scylladb`, the library intercepts any calls that would normally go to a ScyllaDB cluster and redirects them to an in-memory mock backend.

## Usage Example

Here's a quick look at how you can use `pyscylladb-mock` in your tests:

```python
from pyscylladb_mock import mock_scylladb, get_keyspaces
from cassandra.cluster import Cluster

@mock_scylladb
def test_my_app_creates_a_keyspace():
    # Arrange: Your application code that connects to ScyllaDB
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    keyspace_name = "my_app_keyspace"

    # Act: Your application logic that creates a keyspace
    session.execute(
        f"CREATE KEYSPACE {keyspace_name} "
        "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )

    # Assert: Use the inspection API to verify the result
    created_keyspaces = get_keyspaces()
    assert keyspace_name in created_keyspaces
```

## Project Scope & Development Roadmap

This library is currently in the early stages of development. Here is a summary of what is currently supported and what is planned for the future.

### Current Features:

*   `@mock_scylladb` decorator for easy test setup.
*   Intercepting database connections.
*   Mocking of `CREATE KEYSPACE` statements.
*   Mocking of `CREATE TABLE` statements.
*   Mocking of `DROP TABLE` statements.
*   Mocking of `INSERT` statements.
*   Mocking of `SELECT` statements (basic `WHERE` clauses with `AND` supported, including `=`, `>`, `<`, `>=`, `<=`).
*   Mocking of `UPDATE` and `DELETE` statements.
*   An inspection API to view created keyspaces (`get_keyspaces`) and table data (`get_table_rows`).

### Future Goals:

Our roadmap is focused on expanding the CQL support to cover the most common operations developers use. Here are some of the features we're planning to add:

*   **Expanded Query Support**:
    *   [x] `IN` clauses in `SELECT` statements.
    *   [x] `ORDER BY` and `LIMIT` clauses.
    *   [x] `TRUNCATE TABLE` statements.
    *   [x] `ALTER TABLE` for schema modifications.
*   **Advanced Data Types**:
    *   [ ] Collection types: `list`, `set`, and `map`.
    *   [ ] User-Defined Types (UDTs).
*   **Advanced DML**:
    *   [ ] `BATCH` statements for atomic operations.
    *   [ ] Lightweight Transactions (LWT) with `IF EXISTS` / `IF NOT EXISTS`.
*   **Improved Testability**:
    *   [ ] Simulating specific database errors.

We welcome contributions to help build out these features!
