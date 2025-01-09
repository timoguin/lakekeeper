create type statistic_type as enum ('endpoint', 'entity_count');
create type schedule_type as enum ('statistics_collection');

create table cron_schedule
(
    schedule_id  uuid primary key,
    warehouse_id uuid          not null REFERENCES warehouse (warehouse_id),
    schedule     text          not null,
    typ          schedule_type not null
);


create table statistics
(
    statistics_id uuid primary key,
    warehouse_id  uuid not null REFERENCES warehouse (warehouse_id)
);

select trigger_updated_at('"statistics"');
call add_time_columns('statistics');

create table counters
(
    name         text    not null,
    statistic_id uuid REFERENCES statistics (statistics_id),
    value        decimal not null,
    PRIMARY KEY (name, statistic_id)
);

