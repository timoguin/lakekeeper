create type statistic_type as enum ('endpoint', 'entity_count');
create type task_source as enum ('system', 'user');
create type queue as enum ('stats', 'compact');


alter table task
    add column last_heartbeat_at timestamptz,
    add column schedule          text,
    add column source            task_source not null default 'system';

alter table task
    alter column source drop default;


-- task pool id -> bring your own compute?
-- priority weight


create table stats_job
(
    task_id uuid primary key references task (task_id)
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

