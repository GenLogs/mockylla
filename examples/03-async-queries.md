# Async Queries

`execute_async` returns a `MockResponseFuture` that mimics the real driver's future API. Callbacks run immediately against the in-memory result so you can keep exercising asynchronous code paths in tests.

```python
from mockylla import mock_scylladb
from cassandra.cluster import Cluster

@mock_scylladb
def fetch_users_async():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute("CREATE TABLE users (id int PRIMARY KEY, name text)")
    session.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")

    future = session.execute_async("SELECT name FROM users WHERE id = 1")

    names = []
    future.add_callback(lambda result: names.append(result.one().name))

    assert future.result().one().name == "Alice"
    assert names == ["Alice"]
    assert future.done() is True
    assert future.cancelled() is False


if __name__ == "__main__":
    fetch_users_async()
```
