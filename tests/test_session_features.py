from cassandra.cluster import Cluster

from mockylla import mock_scylladb


def _bootstrap_schema(session):
    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute("CREATE TABLE users (id int PRIMARY KEY, name text)")


@mock_scylladb
def test_prepare_and_bound_execute():
    cluster = Cluster()
    session = cluster.connect()
    _bootstrap_schema(session)

    insert_ps = session.prepare("INSERT INTO users (id, name) VALUES (?, ?)")
    session.execute(insert_ps.bind((1, "Alice")))

    select_ps = session.prepare("SELECT name FROM users WHERE id = ?")
    row = session.execute(select_ps, (1,)).one()

    assert row.name == "Alice"
    assert (
        insert_ps.query_string == "INSERT INTO users (id, name) VALUES (?, ?)"
    )


@mock_scylladb
def test_execute_async_returns_future():
    cluster = Cluster()
    session = cluster.connect()
    _bootstrap_schema(session)
    session.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")

    future = session.execute_async("SELECT name FROM users WHERE id = 1")

    callback_results = []
    future.add_callback(lambda result: callback_results.append(result[0].name))

    assert future.result().one().name == "Alice"
    assert callback_results == ["Alice"]
    assert future.done() is True
    assert future.cancelled() is False


@mock_scylladb
def test_session_attributes_exposed():
    cluster = Cluster()
    session = cluster.connect()

    assert session.cluster is not None

    session.row_factory = lambda cols, rows: rows
    session.default_timeout = 5

    assert session.row_factory is not None
    assert session.default_timeout == 5
