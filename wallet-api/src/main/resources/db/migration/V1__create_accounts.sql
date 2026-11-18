create table accounts
(
    id           uuid primary key default uuidv7(),
    created_at   timestamptz    not null,
    updated_at   timestamptz    not null,
    version      bigint         not null default 0,
    daily_limit  numeric(19, 2) not null,
    phone_number varchar(15)    not null,
    full_name    varchar(120)   not null,
    kyc_level    varchar(10)    not null,
    status       varchar(10)    not null,

    constraint accounts_phone_number_uk unique (phone_number),
    constraint accounts_kyc_level_ck check (kyc_level in ('BASIC', 'FULL')),
    constraint accounts_status_ck check (status in ('ACTIVE', 'FROZEN')),
    constraint accounts_daily_limit_ck check (daily_limit > 0)
);
