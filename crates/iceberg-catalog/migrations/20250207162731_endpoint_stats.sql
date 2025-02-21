create type api_endpoints as enum (
    'catalog-post-aws-s3-sign',
    'catalog-post-prefix-aws-s3-sign',
    'catalog-get-config',
    'catalog-get-namespaces',
    'catalog-post-namespaces',
    'catalog-get-namespace',
    'catalog-post-namespace',
    'catalog-delete-namespace',
    'catalog-post-namespace-properties',
    'catalog-get-namespace-tables',
    'catalog-post-namespace-tables',
    'catalog-get-namespace-table',
    'catalog-post-namespace-table',
    'catalog-delete-namespace-table',
    'catalog-head-namespace-table',
    'catalog-get-namespace-table-credentials',
    'catalog-post-tables-rename',
    'catalog-post-namespace-register',
    'catalog-post-namespace-table-metrics',
    'catalog-post-transactions-commit',
    'catalog-post-namespace-views',
    'catalog-get-namespace-views',
    'catalog-get-namespace-view',
    'catalog-post-namespace-view',
    'catalog-delete-namespace-view',
    'catalog-head-namespace-view',
    'catalog-post-views-rename',
    'management-get-info',
    'management-post-bootstrap',
    'management-post-role',
    'management-get-role',
    'management-post-role-id',
    'management-get-role-id',
    'management-delete-role-id',
    'management-post-search-role',
    'management-get-whoami',
    'management-post-search-user',
    'management-post-user-id',
    'management-get-user-id',
    'management-delete-user-id',
    'management-post-user',
    'management-get-user',
    'management-post-project',
    'management-get-default-project',
    'management-delete-default-project',
    'management-post-rename-project',
    'management-get-project-id',
    'management-delete-project-id',
    'management-post-warehouse',
    'management-get-warehouse',
    'management-get-project-list',
    'management-get-warehouse-id',
    'management-delete-warehouse-id',
    'management-post-warehouse-rename',
    'management-post-warehouse-deactivate',
    'management-post-warehouse-activate',
    'management-post-warehouse-storage',
    'management-post-warehouse-storage-credential',
    'management-get-warehouse-statistics',
    'management-get-warehouse-deleted-tabulars',
    'management-post-warehouse-deleted-tabulars-undrop1',
    'management-post-warehouse-deleted-tabulars-undrop2',
    'management-post-warehouse-delete-profile',
    'management-get-permissions',
    'management-post-permissions',
    'management-head-permissions',
    'management-delete-permissions');


create table endpoint_statistics
(
    endpoint_statistics_id int generated always as identity primary key,
    warehouse_id           uuid references warehouse (warehouse_id) on delete cascade,
    --  warehouse_name text collate "case_insensitive" references warehouse (warehouse_name) on update cascade on delete cascade,
    project_id             uuid references project (project_id) on delete cascade,
    matched_path           api_endpoints not null,
    status_code            int           not null,
    count                  bigint        not null default 0,
    -- we keep stats in hourly intervals, every hour we create a new row,
    valid_until            timestamptz   not null default get_stats_date_default() + interval '1 hour',
    unique nulls not distinct (project_id, warehouse_id, matched_path, status_code, valid_until)
);



select trigger_updated_at('endpoint_statistics');
call add_time_columns('endpoint_statistics');