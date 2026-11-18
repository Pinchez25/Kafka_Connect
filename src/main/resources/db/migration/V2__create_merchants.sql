create table merchants
(
    id          uuid primary key default uuidv7(),
    created_at  timestamptz  not null,
    till_number varchar(10)  not null,
    name        varchar(120) not null,
    category    varchar(20)  not null,

    constraint merchants_till_number_uk unique (till_number),
    constraint merchants_category_ck check (category in
        ('GROCERY', 'BETTING', 'FUEL', 'RESTAURANT', 'TRANSPORT', 'UTILITIES', 'PHARMACY'))
);
