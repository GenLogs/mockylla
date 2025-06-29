import uuid
from mockylla import mock_scylladb
from cassandra.cluster import Cluster


@mock_scylladb
def test_semicolon_handling_in_loop():
    """Ensure that terminating semicolons do not break WHERE-clause filtering in a delete / update loop."""

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    keyspace = "test_keyspace"
    session.execute(
        f"CREATE KEYSPACE {keyspace} WITH replication = {{'class':'SimpleStrategy','replication_factor':1}};"
    )
    session.set_keyspace(keyspace)

    session.execute(
        """
        CREATE TABLE counter_table (
            group_id text PRIMARY KEY,
            counter_value counter
        );
        """
    )
    session.execute(
        """
        CREATE TABLE images_data (
            group_id text,
            id timeuuid,
            data text,
            PRIMARY KEY (group_id, id)
        ) WITH CLUSTERING ORDER BY (id DESC);
        """
    )

    payload_template = '{"purpose": "test"}'
    inserts = [("groupA", uuid.uuid1()) for _ in range(7)] + [
        ("groupB", uuid.uuid1()) for _ in range(5)
    ]
    for stack, uid in inserts:
        session.execute(
            """
            INSERT INTO images_data (
                group_id,
                id,
                data
            ) VALUES (%s, %s, %s);
            """,
            (stack, uid, payload_template),
        )
        session.execute(
            """
            UPDATE counter_table SET counter_value = counter_value + 1 WHERE group_id = %s;
            """,
            (stack,),
        )

    while rows := list(session.execute("SELECT * FROM images_data LIMIT 6;")):
        for row in rows:
            session.execute(
                "DELETE FROM images_data WHERE id = %s AND group_id = %s;",
                (row.id, row.group_id),
            )
            session.execute(
                "UPDATE counter_table SET counter_value = counter_value - 1 WHERE group_id = %s;",
                (row.group_id,),
            )

    assert list(session.execute("SELECT * FROM images_data;")) == []

    for row in session.execute("SELECT * FROM counter_table;"):
        assert row.counter_value == 0
