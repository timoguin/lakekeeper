use crate::api::ApiContext;
use crate::service::authz::Authorizer;
use crate::service::{Catalog, ListFlags, SecretStore, State, Transaction};
use crate::WarehouseIdent;
use apalis::prelude::{Data, Monitor, WorkerBuilder, WorkerFactory, WorkerFactoryFn};
use apalis_cron::{CronStream, Schedule};
use apalis_sql::postgres::PostgresStorage;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;
//
// #[derive(Debug, Serialize, Deserialize)]
// pub struct StatsTask;
//
#[derive(Debug)]
pub struct WarehouseStatistics {
    warehouse_ident: WarehouseIdent,
    number_of_tables: i64, // silly but necessary due to sqlx wanting i64, not usize
    number_of_views: i64,
}
//
// pub async fn collect_stats<C: Catalog, A: Authorizer, S: SecretStore>(
//     task: chrono::DateTime<Utc>,
//     ctx: Data<ApiContext<State<A, C, S>>>,
// ) {
//     let whid = WarehouseIdent(Uuid::now_v7());
//     let mut trx = C::Transaction::begin_write(ctx.v1_state.catalog.clone())
//         .await
//         .unwrap();
//     let stats = C::update_warehouse_statistics(
//         whid,
//         ListFlags {
//             include_active: true,
//             include_staged: false,
//             include_deleted: false,
//         },
//         trx.transaction(),
//     )
//     .await
//     .unwrap();
//     tracing::info!("Updated warehouse stats to: {:?}", stats);
// }
//
// pub async fn thing<C: Catalog, A: Authorizer, S: SecretStore>(
//     pg_pool: PgPool,
//     ctx: ApiContext<State<A, C, S>>,
// ) {
//     let mut mon = Monitor::new();
//     let cr = Schedule::from_str("1/1 * * * *").unwrap();
//     let cron = CronStream::new(cr);
//     PostgresStorage::setup(&pg_pool).await.unwrap();
//     let pool = PostgresStorage::new(pg_pool);
//     let backend = cron.pipe_to_storage(pool);
//     let be = mon.register(
//         WorkerBuilder::new("stats")
//             .data(ctx)
//             .backend(backend)
//             .build_fn(collect_stats),
//     );
// }

#[cfg(test)]
mod test {
    use apalis::prelude::{Data, Monitor, WorkerBuilder, WorkerFactoryFn};
    use apalis_cron::{CronStream, Schedule};
    use apalis_sql::postgres::PostgresStorage;
    use chrono::Utc;
    use serde::{Deserialize, Serialize};
    use sqlx::{Executor, PgPool};
    use std::str::FromStr;
    use uuid::Uuid;

    #[derive(Debug, Clone)]
    pub struct StatsTask {
        warehouse_ident: Uuid,
    }

    pub async fn collect_stats(
        now: chrono::DateTime<Utc>,
        task: Data<StatsTask>,
        pg_pool: Data<PgPool>,
    ) {
        eprintln!("Got task: {:?}", task);
        todo!()
    }

    #[sqlx::test]
    pub async fn test(pg_pool: PgPool) {
        let mut mon = Monitor::new();
        let cr = Schedule::from_str("1 * * * * *").unwrap();
        let cron = CronStream::new(cr);
        let p = pg_pool.clone();

        let mut mig = PostgresStorage::migrations();
        // TODO: either use extra DB for apalis or use a different schema
        mig.set_ignore_missing(true);
        mig.run(&p).await.unwrap();

        let store = PostgresStorage::new(pg_pool.clone());
        let backend = cron.pipe_to_storage(store);
        let be = mon
            .register(
                WorkerBuilder::new("stats")
                    .data(StatsTask {
                        warehouse_ident: Uuid::new_v4(),
                    })
                    .data(pg_pool)
                    .backend(backend)
                    .build_fn(collect_stats),
            )
            .run()
            .await
            .unwrap();
    }
}
