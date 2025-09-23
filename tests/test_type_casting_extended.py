from datetime import datetime
from decimal import Decimal

from cassandra.cluster import Cluster

from mockylla import mock_scylladb


@mock_scylladb
def test_complex_type_casting_and_filtering():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute(
        """
        CREATE TABLE metrics (
            id int PRIMARY KEY,
            active boolean,
            score double,
            ratio decimal,
            created timestamp,
            tags list<text>,
            roles set<text>,
            attrs map<text, int>
        )
        """
    )

    session.execute(
        """
        INSERT INTO metrics (id, active, score, ratio, created, tags, roles, attrs)
        VALUES (
            1,
            true,
            3.14,
            1.23,
            '2024-01-01T00:00:00',
            ['alpha', 'beta'],
            {'admin', 'user'},
            {'x': 1, 'y': 2}
        )
        """
    )

    row = session.execute("SELECT * FROM metrics WHERE id = 1").one()

    assert row.active is True
    assert row.score == 3.14
    assert row.ratio == Decimal("1.23")
    assert row.created == datetime(2024, 1, 1, 0, 0, 0)
    assert row.tags == ["alpha", "beta"]
    assert row.roles == {"admin", "user"}
    assert row.attrs == {"x": 1, "y": 2}

    filtered_row = session.execute(
        "SELECT id FROM metrics WHERE active = true"
    ).one()
    assert filtered_row.id == 1

    assert (
        session.execute(
            "SELECT id FROM metrics WHERE id = 1 AND created >= '2024-01-01T00:00:00'"
        )
        .one()
        .id
        == 1
    )
