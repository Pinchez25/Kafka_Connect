create table transactions
(
    id              uuid primary key default uuidv7(),
    created_at      timestamptz    not null,
    from_account_id uuid references accounts (id),
    to_account_id   uuid references accounts (id),
    merchant_id     uuid references merchants (id),
    amount          numeric(19, 2) not null,
    reference       varchar(12)    not null,
    type            varchar(20)    not null,
    agent_number    varchar(10),
    currency        varchar(3)     not null,
    status          varchar(10)    not null,
    failure_reason  varchar(30),

    constraint transactions_reference_uk unique (reference),
    constraint transactions_amount_ck check (amount > 0),
    constraint transactions_status_ck check (
        (status = 'COMPLETED' and failure_reason is null)
            or (status = 'FAILED' and failure_reason in ('SENDER_FROZEN', 'RECIPIENT_FROZEN', 'DAILY_LIMIT_EXCEEDED'))
        ),
    constraint transactions_parties_ck check (
        (type = 'P2P' and from_account_id is not null and to_account_id is not null
            and from_account_id <> to_account_id and merchant_id is null and agent_number is null)
            or (type = 'TOPUP' and from_account_id is null and to_account_id is not null
            and merchant_id is null and agent_number is not null)
            or (type = 'MERCHANT_PAYMENT' and from_account_id is not null and to_account_id is null
            and merchant_id is not null and agent_number is null)
        )
);

create index transactions_from_account_idx on transactions (from_account_id, id);
create index transactions_to_account_idx on transactions (to_account_id, id);
create index transactions_merchant_idx on transactions (merchant_id);

-- Index-only scan for the daily-limit sum.
create index transactions_daily_outgoing_idx on transactions (from_account_id, created_at)
    include (amount)
    where status = 'COMPLETED';
