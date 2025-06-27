from uuid import UUID, uuid1

from cassandra.cluster import Cluster

from mockylla import mock_scylladb


@mock_scylladb
def test_uuid_type_casting_and_where_clause():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute("CREATE TABLE items (id timeuuid PRIMARY KEY, name text)")

    uid = uuid1()
    session.execute(
        "INSERT INTO items (id, name) VALUES (%s, %s)",
        (uid, "widget"),
    )

    rows = list(session.execute(f"SELECT * FROM items WHERE id = {uid}"))

    assert len(rows) == 1
    assert rows[0]["id"] == uid
    assert isinstance(rows[0]["id"], UUID)


@mock_scylladb
def test_uuid_delete():
    """Verify that a row identified by a UUID can be deleted and is no longer returned."""

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    # Setup schema
    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute("CREATE TABLE items (id timeuuid PRIMARY KEY, name text)")

    # Insert two rows
    uid_to_delete = uuid1()
    uid_to_keep = uuid1()

    session.execute(
        "INSERT INTO items (id, name) VALUES (%s, %s)",
        (uid_to_delete, "gadget"),
    )
    session.execute(
        "INSERT INTO items (id, name) VALUES (%s, %s)",
        (uid_to_keep, "widget"),
    )

    session.execute("DELETE FROM items WHERE id = %s", (uid_to_delete,))

    rows_deleted = list(
        session.execute(f"SELECT * FROM items WHERE id = {uid_to_delete}")
    )
    rows_kept = list(
        session.execute(f"SELECT * FROM items WHERE id = {uid_to_keep}")
    )

    assert rows_deleted == []
    assert len(rows_kept) == 1
    assert rows_kept[0]["id"] == uid_to_keep
    assert isinstance(rows_kept[0]["id"], UUID)


@mock_scylladb
def test_uuid_select_with_parameters():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute("CREATE TABLE items (id timeuuid PRIMARY KEY, name text)")

    uid = uuid1()
    session.execute(
        "INSERT INTO items (id, name) VALUES (%s, %s)",
        (uid, "widget"),
    )

    result_set = session.execute("SELECT * FROM items WHERE id = %s", (uid,))
    rows = list(result_set)

    assert len(rows) == 1
    assert rows[0]["id"] == uid
    assert rows[0]["name"] == "widget"
    assert isinstance(rows[0]["id"], UUID)


@mock_scylladb
def test_uuid_update_with_parameters():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute("CREATE TABLE items (id timeuuid PRIMARY KEY, name text)")

    uid = uuid1()
    session.execute(
        "INSERT INTO items (id, name) VALUES (%s, %s)",
        (uid, "widget"),
    )

    # Perform update with parameters
    session.execute(
        "UPDATE items SET name = %s WHERE id = %s",
        ("gizmo", uid),
    )

    row = session.execute("SELECT name FROM items WHERE id = %s", (uid,)).one()

    assert row.name == "gizmo"
