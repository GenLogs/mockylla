from mockylla import mock_scylladb
from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy


@mock_scylladb
def test_execution_profile_name():
    """Ensure that passing an execution_profile name works."""
    node1_profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"])
    )
    profiles = {"node1": node1_profile}

    cluster = Cluster(execution_profiles=profiles)
    session = cluster.connect()

    rows = session.execute(
        "SELECT * FROM system.local", execution_profile="node1"
    )

    assert len(rows) == 1

    assert rows[0].rpc_address == "127.0.0.1"


@mock_scylladb
def test_execution_profile_instance():
    """Ensure that passing an execution_profile instance directly works."""
    node1_profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"])
    )

    cluster = Cluster()
    session = cluster.connect()

    rows = session.execute(
        "SELECT * FROM system.local", execution_profile=node1_profile
    )

    assert len(rows) == 1
    assert rows[0].rpc_address == "127.0.0.1"
