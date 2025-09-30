DROP KEYSPACE IF EXISTS sensor;
CREATE KEYSPACE IF NOT EXISTS sensor WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
DROP KEYSPACE IF EXISTS coordinator;
CREATE KEYSPACE IF NOT EXISTS coordinator WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
DROP KEYSPACE IF EXISTS scalardb;
CREATE KEYSPACE IF NOT EXISTS scalardb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE IF NOT EXISTS sensor.tx_sensor (
    timestamp int,
    device_id int,
    revision int,
    before_revision int,
    before_tx_committed_at bigint,
    before_tx_id text,
    before_tx_prepared_at bigint,
    before_tx_state int,
    before_tx_version int,
    tx_committed_at bigint,
    tx_id text,
    tx_prepared_at bigint,
    tx_state int,
    tx_version int,
    PRIMARY KEY (timestamp, device_id)
) WITH compaction = { 'class' : 'LeveledCompactionStrategy' };

CREATE TABLE IF NOT EXISTS coordinator.state (
    tx_id text,
    tx_child_ids text,
    tx_state int,
    tx_created_at bigint,
    PRIMARY KEY (tx_id)
);

CREATE TABLE IF NOT EXISTS scalardb.namespaces (
    name text,
    PRIMARY KEY (name)
);

INSERT INTO scalardb.namespaces (name) VALUES ('sensor');
INSERT INTO scalardb.namespaces (name) VALUES ('coordinator');
INSERT INTO scalardb.namespaces (name) VALUES ('scalardb');
