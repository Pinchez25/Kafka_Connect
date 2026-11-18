create table debezium_signals
(
    id   varchar(64) primary key,
    type varchar(32) not null,
    data varchar(2048)
);

-- Owned by the schema rather than auto-created by Debezium, so the captured table set is versioned with it.
create publication pesaflow_cdc for table accounts, merchants, transactions, debezium_signals;
