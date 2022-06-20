Experimenting with postgres logical replication streaming.

Set `wal_level = logical` in `postgresql.conf`.

Run `CREATE PUBLICATION all_tables FOR ALL TABLES;` to create the publication we can subscribe to. The subscription name `all_tables` is passed as a parameter to the `START_REPLICATION` command.
