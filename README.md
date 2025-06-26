# mockylla

A lightweight and easy-to-use library for mocking ScyllaDB in your Python tests. Inspired by the simplicity of `moto` for AWS, `mockylla` allows you to test your ScyllaDB-dependent code without needing a live database instance.

## The Goal

The primary goal of `mockylla` is to provide a seamless testing experience for developers working with ScyllaDB. We aim to create a mock that is:

-   **Non-intrusive**: Use a simple decorator to mock all `scylla-driver` interactions. No changes to your application code are needed.
-   **Fast & Isolated**: All mocking happens in-memory, ensuring your tests are fast and completely isolated from each other.
-   **Inspectable**: Provides helper functions to let you check the state of the mock database during your tests, allowing you to assert that your code behaved as expected (e.g., "Was a specific row inserted into a table?").

## How It Works

`mockylla` works by patching the underlying `scylla-driver` at runtime. When you decorate a test function with `@mock_scylladb`, the library intercepts any calls that would normally go to a ScyllaDB cluster and redirects them to an in-memory mock backend.

## Usage Example

Here's a quick look at how you can use `mockylla` in your tests:

```python
from mocylla import mock_scylladb, get_keyspaces
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

### More Examples

Below is a more comprehensive example that demonstrates creating a table, inserting, querying, updating, and deleting data.

```python
from mocylla import mock_scylladb, get_table_rows
from cassandra.cluster import Cluster

@mock_scylladb
def test_crud_operations():
    # 1. Arrange: Connect and set up the schema
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    keyspace_name = "crud_app"
    table_name = "users"

    session.execute(
        f"CREATE KEYSPACE {keyspace_name} "
        "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace(keyspace_name)

    session.execute(f"""
        CREATE TABLE {table_name} (
            user_id int PRIMARY KEY,
            name text,
            email text
        )
    """)

    # 2. Act & Assert: Insert data
    session.execute(
        f"INSERT INTO {table_name} (user_id, name, email) "
        "VALUES (1, 'Alice', 'alice@example.com')"
    )
    session.execute(
        f"INSERT INTO {table_name} (user_id, name, email) "
        "VALUES (2, 'Bob', 'bob@example.com')"
    )

    rows = get_table_rows(keyspace_name, table_name)
    assert len(rows) == 2
    assert rows[0]['name'] == 'Alice'

    # 3. Act & Assert: Select data
    selection_result = session.execute(f"SELECT * FROM {table_name} WHERE user_id = 1")
    selected_row = selection_result.one()
    assert selected_row.name == 'Alice'
    assert selected_row.email == 'alice@example.com'

    # 4. Act & Assert: Update data
    session.execute(
        f"UPDATE {table_name} SET email = 'alice_updated@example.com' "
        "WHERE user_id = 1"
    )

    updated_rows = get_table_rows(keyspace_name, table_name)
    assert updated_rows[0]['email'] == 'alice_updated@example.com'

    # 5. Act & Assert: Delete data
    session.execute(f"DELETE FROM {table_name} WHERE user_id = 2")

    final_rows = get_table_rows(keyspace_name, table_name)
    assert len(final_rows) == 1
    assert final_rows[0]['user_id'] == 1
```

## Project Scope & Development Roadmap

This library is currently in the early stages of development. Here is a summary of what is currently supported and what is planned for the future.

### Current Features:

*   `@mock_scylladb` decorator for easy test setup.
*   Intercepting database connections.
*   Mocking of `CREATE KEYSPACE` statements.
*   Mocking of `CREATE TABLE` statements.
*   Mocking of `CREATE TYPE` for user-defined types.
*   Mocking of `ALTER TABLE` statements (`ADD` only).
*   Mocking of `DROP TABLE` statements.
*   Mocking of `TRUNCATE TABLE` statements.
*   Mocking of `INSERT` statements.
*   Mocking of `SELECT` statements (with `WHERE`, `ORDER BY`, and `LIMIT` clauses).
*   Mocking of `UPDATE` and `DELETE` statements.
*   Mocking of `BATCH` statements.
*   An inspection API to view created keyspaces (`get_keyspaces`), tables (`get_tables`), types (`get_types`), and table data (`get_table_rows`).

We welcome contributions to help build out these features!
