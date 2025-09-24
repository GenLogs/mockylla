# Basic Usage

This example shows the decorator workflow most test suites rely on. The mock patches the Scylla driver for the duration of the function and resets all state on exit.

```python
from mockylla import mock_scylladb, get_table_rows
from cassandra.cluster import Cluster

@mock_scylladb
def create_and_query_users():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE demo_app WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("demo_app")

    session.execute(
        "CREATE TABLE users (id int PRIMARY KEY, email text, active boolean)"
    )

    session.execute(
        "INSERT INTO users (id, email, active) VALUES (1, 'alice@example.com', true)"
    )
    session.execute(
        "UPDATE users SET active = false WHERE id = 1"
    )

    row = session.execute("SELECT email, active FROM users WHERE id = 1").one()
    assert row.email == "alice@example.com"
    assert row.active is False

    # You can also inspect the in-memory rows directly when debugging tests.
    current_rows = get_table_rows("demo_app", "users")
    assert current_rows == [
        {"id": 1, "email": "alice@example.com", "active": False},
    ]

    print("Query result:", row.email, row.active)


if __name__ == "__main__":
    create_and_query_users()
```
