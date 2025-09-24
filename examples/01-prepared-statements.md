# Prepared Statements

Prepared statements work the same way they do with a live cluster. You can bind positional tuples or named mappings, and the mock validates missing or unexpected parameters like the real driver.

```python
from mockylla import mock_scylladb
from cassandra.cluster import Cluster

@mock_scylladb
def use_prepared_statements():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute("CREATE TABLE users (id int PRIMARY KEY, name text, active boolean)")

    insert_ps = session.prepare(
        "INSERT INTO users (id, name, active) VALUES (?, ?, ?)"
    )
    session.execute(insert_ps.bind((1, "Alice", True)))

    update_ps = session.prepare("UPDATE users SET active = ? WHERE id = ?")
    session.execute(update_ps, (False, 1))

    select_ps = session.prepare("SELECT name, active FROM users WHERE id = ?")
    row = session.execute(select_ps, {"id": 1}).one()

    assert row.name == "Alice"
    assert row.active is False


if __name__ == "__main__":
    use_prepared_statements()
```
