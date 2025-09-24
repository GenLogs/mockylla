# Batch Operations

`mockylla` understands both string-based `BEGIN BATCH` blocks and the driver statement helpers. Use whichever form your production code relies on.

```python
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
from mockylla import MockBatchStatement, mock_scylladb


@mock_scylladb
def run_cql_batch_block():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute("CREATE TABLE items (id int PRIMARY KEY, value text)")
    session.execute("INSERT INTO items (id, value) VALUES (1, 'old')")

    session.execute(
        """
        BEGIN BATCH
            INSERT INTO items (id, value) VALUES (2, 'two');
            UPDATE items SET value = 'updated' WHERE id = 1;
            DELETE FROM items WHERE id = 3;
        APPLY BATCH;
        """
    )

    rows = session.execute("SELECT id, value FROM items").all()
    assert {(row.id, row.value) for row in rows} == {(1, "updated"), (2, "two")}


@mock_scylladb
def run_driver_batch_helpers():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute("CREATE TABLE users (id int PRIMARY KEY, name text, active boolean)")

    insert_stmt = SimpleStatement(
        "INSERT INTO users (id, name, active) VALUES (%s, %s, %s)"
    )
    update_stmt = SimpleStatement("UPDATE users SET name = %s WHERE id = %s")

    driver_batch = BatchStatement()
    driver_batch.add(insert_stmt, (1, "Alice", True))
    driver_batch.add(update_stmt, ("Alicia", 1))
    session.execute(driver_batch)

    mock_batch = MockBatchStatement()
    insert_ps = session.prepare(
        "INSERT INTO users (id, name, active) VALUES (?, ?, ?)"
    )
    mock_batch.add(insert_ps, (2, "Bob", False))
    mock_batch.add(session.prepare("DELETE FROM users WHERE id = ?"), (1,))
    session.execute(mock_batch)

    rows = session.execute("SELECT id, name, active FROM users").all()
    assert {(row.id, row.name, row.active) for row in rows} == {(2, "Bob", False)}


if __name__ == "__main__":
    run_cql_batch_block()
    run_driver_batch_helpers()
```
