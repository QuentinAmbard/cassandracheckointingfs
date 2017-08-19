
CREATE KEYSPACE rtfap WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE rtfap.transactions (
    cc_no text,
    year int,
    month int,
    day int,
    txn_time timestamp,
    amount double,
    cc_provider text,
    country text,
    date_text text,
    hour int,
    items map<text, double>,
    location text,
    merchant text,
    min int,
    notes text,
    status text,
    tags set<text>,
    txn_id text,
    user_id text,
    PRIMARY KEY ((cc_no, year, month, day), txn_time)
);

CREATE TABLE rtfap.txn_count_min (
    year int,
    month int,
    day int,
    hour int,
    minute int,
    approved_rate_hr double,
    approved_rate_min double,
    approved_txn_hr int,
    approved_txn_min int,
    ttl_txn_hr int,
    ttl_txn_min int,
    PRIMARY KEY ((year, month, day, hour, minute))
);
