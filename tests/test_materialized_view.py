from cassandra.cluster import Cluster

from mockylla import get_table_rows, mock_scylladb


@mock_scylladb
def test_materialized_view_tracks_inserts_updates_and_deletes():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute(
        """
        CREATE TABLE users (
            user_id int PRIMARY KEY,
            name text,
            city text
        )
        """
    )

    session.execute(
        """
        CREATE MATERIALIZED VIEW users_by_city AS
            SELECT user_id, name, city FROM users
            WHERE city IS NOT NULL AND user_id IS NOT NULL
            PRIMARY KEY (city, user_id)
        """
    )

    session.execute(
        "INSERT INTO users (user_id, name, city) VALUES (1, 'Alice', 'Paris')"
    )
    session.execute(
        "INSERT INTO users (user_id, name, city) VALUES (2, 'Bob', 'Rome')"
    )
    session.execute(
        "INSERT INTO users (user_id, name, city) VALUES (3, 'Charlie', null)"
    )

    rows = session.execute(
        "SELECT user_id, name FROM users_by_city WHERE city = 'Paris'"
    ).all()
    assert len(rows) == 1
    assert rows[0].name == "Alice"

    rows = session.execute(
        "SELECT user_id FROM users_by_city WHERE city = 'Rome'"
    ).all()
    assert [row.user_id for row in rows] == [2]

    view_rows = get_table_rows("ks", "users_by_city")
    assert {row["user_id"] for row in view_rows} == {1, 2}

    session.execute("UPDATE users SET city = 'Berlin' WHERE user_id = 2")

    rows = session.execute(
        "SELECT user_id FROM users_by_city WHERE city = 'Rome'"
    ).all()
    assert rows == []
    rows = session.execute(
        "SELECT user_id FROM users_by_city WHERE city = 'Berlin'"
    ).all()
    assert [row.user_id for row in rows] == [2]

    session.execute("DELETE FROM users WHERE user_id = 1")

    rows = session.execute(
        "SELECT user_id FROM users_by_city WHERE city = 'Paris'"
    ).all()
    assert rows == []

    base_rows = get_table_rows("ks", "users")
    view_rows = get_table_rows("ks", "users_by_city")
    assert len(base_rows) == 2
    assert {row["user_id"] for row in view_rows} == {2}
