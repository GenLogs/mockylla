from cassandra.cluster import Cluster

from mockylla import mock_scylladb, get_tables


@mock_scylladb
def test_cluster_metadata_reflects_schema():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute(
        "CREATE TABLE users (id int PRIMARY KEY, name text, email text)"
    )

    keyspace_meta = cluster.metadata.get_keyspace("ks")
    assert keyspace_meta is not None
    assert keyspace_meta.name == "ks"
    assert "users" in keyspace_meta.tables

    table_meta = keyspace_meta.tables["users"]
    assert [col.name for col in table_meta.primary_key] == ["id"]
    assert table_meta.column("name").cql_type == "text"


@mock_scylladb
def test_system_schema_tables_are_queryable():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute(
        "CREATE TABLE users (id int PRIMARY KEY, name text, email text)"
    )

    keyspaces = session.execute(
        "SELECT keyspace_name FROM system_schema.keyspaces"
    ).all()
    assert any(row.keyspace_name == "ks" for row in keyspaces)

    tables = session.execute(
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'ks'"
    ).all()
    assert any(row.table_name == "users" for row in tables)

    columns = session.execute(
        "SELECT column_name, kind FROM system_schema.columns WHERE keyspace_name = 'ks' AND table_name = 'users'"
    ).all()
    kinds = {row.column_name: row.kind for row in columns}
    assert kinds["id"] == "partition_key"
    assert kinds["name"] == "regular"


@mock_scylladb
def test_composite_primary_key_metadata():
    cluster = Cluster()
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("ks")
    session.execute(
        """
        CREATE TABLE events (
            user_id int,
            bucket int,
            created_at timestamp,
            payload text,
            PRIMARY KEY ((user_id, bucket), created_at)
        ) WITH CLUSTERING ORDER BY (created_at DESC)
        """
    )

    tables = get_tables("ks")
    primary_info = tables["events"]["primary_key"]
    assert primary_info["partition"] == ["user_id", "bucket"]
    assert primary_info["clustering"] == ["created_at"]

    keyspace_meta = cluster.metadata.get_keyspace("ks")
    table_meta = keyspace_meta.table("events")

    partition_names = [col.name for col in table_meta.partition_key]
    clustering_names = [col.name for col in table_meta.clustering_key]
    assert partition_names == ["user_id", "bucket"]
    assert clustering_names == ["created_at"]

    assert table_meta.columns["created_at"].clustering_order == "DESC"
    assert table_meta.clustering_orders["created_at"] == "DESC"

    column_rows = session.execute(
        """
        SELECT column_name, kind, clustering_order
        FROM system_schema.columns
        WHERE keyspace_name = 'ks' AND table_name = 'events'
        """
    ).all()
    columns_by_name = {row.column_name: row for row in column_rows}

    assert columns_by_name["user_id"].kind == "partition_key"
    assert columns_by_name["bucket"].kind == "partition_key"
    assert columns_by_name["created_at"].kind == "clustering"
    assert columns_by_name["created_at"].clustering_order == "desc"
