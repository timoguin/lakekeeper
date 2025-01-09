use crate::api::ApiContext;
use crate::service::authz::Authorizer;
use crate::service::{Catalog, ListFlags, SecretStore, State, Transaction};
use crate::WarehouseIdent;
use apalis::prelude::{Data, Monitor, WorkerBuilder, WorkerFactory, WorkerFactoryFn};
use apalis_cron::{CronStream, Schedule};
use apalis_sql::postgres::PostgresStorage;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct StatsTask;

#[derive(Debug)]
pub struct WarehouseStatistics {
    warehouse_ident: WarehouseIdent,
    number_of_tables: i64, // silly but necessary due to sqlx wanting i64, not usize
    number_of_views: i64,
}

pub async fn collect_stats<C: Catalog, A: Authorizer, S: SecretStore>(
    task: StatsTask,
    ctx: Data<ApiContext<State<A, C, S>>>,
) {
    let whid = WarehouseIdent(Uuid::now_v7());
    let mut trx = C::Transaction::begin_write(ctx.v1_state.catalog.clone())
        .await
        .unwrap();
    let stats = C::update_warehouse_statistics(
        whid,
        ListFlags {
            include_active: true,
            include_staged: false,
            include_deleted: false,
        },
        trx.transaction(),
    )
    .await
    .unwrap();
    tracing::info!("Updated warehouse stats to: {:?}", stats);
}

pub async fn thing<C: Catalog, A: Authorizer, S: SecretStore>(
    pg_pool: PgPool,
    ctx: ApiContext<State<A, C, S>>,
) {
    let mut mon = Monitor::new();
    let cr = Schedule::from_str("1/1 * * * *").unwrap();
    let cron = CronStream::new(cr);
    PostgresStorage::setup(&pg_pool).await.unwrap();
    let pool = PostgresStorage::new(pg_pool);
    let backend = cron.pipe_to_storage(pool);
    let be = mon.register(
        WorkerBuilder::new("stats")
            .data(ctx)
            .backend(backend)
            .build_fn(collect_stats),
    );
}
