use crate::implementations::postgres::dbutils::DBErrorHandler;
// use crate::service::stats::endpoint::StatsSink;
use crate::service::stats::entities::WarehouseStatistics;
use crate::service::ListFlags;
use crate::WarehouseIdent;
use uuid::Uuid;

pub(crate) async fn update_stats<'e, E: sqlx::Executor<'e, Database = sqlx::Postgres>>(
    conn: E,
    warehouse_ident: WarehouseIdent,
    // TODO: use list_flags for filtering
    _list_flags: ListFlags,
) -> crate::api::Result<WarehouseStatistics> {
    // TODO: we could also pass the task idempotency key into here instead and use it as the stats id
    let statistics_id = Uuid::now_v7();

    Ok(sqlx::query_as!(
        WarehouseStatistics,
        r#"
        WITH update_tables AS (
            INSERT INTO scalars (name, statistic_id, value)
            VALUES ('tables', $1, (SELECT count(*) AS value FROM "table" t
                INNER JOIN tabular ti ON t.table_id = ti.tabular_id
                INNER JOIN namespace n ON ti.namespace_id = n.namespace_id
                INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
            WHERE w.warehouse_id = $1 AND w.status = 'active'))
            RETURNING value
        ),
        update_views AS (
            INSERT INTO scalars (name, statistic_id, value)
            VALUES ('views', $1, (SELECT count(*) AS value FROM "view" v
                INNER JOIN tabular vi ON v.view_id = vi.tabular_id
                INNER JOIN namespace n ON vi.namespace_id = n.namespace_id
                INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
            WHERE w.warehouse_id = $1 AND w.status = 'active'))
            RETURNING value
        )
        INSERT INTO statistics (statistics_id, warehouse_id) VALUES ($1, $2)
        RETURNING $1 as "warehouse_ident!", (select value from update_tables) as "number_of_tables!", (select value from update_views) as "number_of_views!"
        "#,
        statistics_id,
        warehouse_ident.0
    )
        .fetch_one(conn)
        .await
        .map_err(|e| e.into_error_model("failed to collect stats"))?)
}

// pub struct PostgresStatsSink {
//     pool: sqlx::PgPool,
// }
//
// impl PostgresStatsSink {
//     pub fn new(pool: sqlx::PgPool) -> Self {
//         Self { pool }
//     }
// }

// #[async_trait::async_trait]
// impl StatsSink for PostgresStatsSink {
//     async fn consume_endpoint_stats(
//         &self,
//         stats_id: Uuid,
//         stats: std::collections::HashMap<String, i64>,
//     ) {
//         let mut conn = self.pool.begin().await.unwrap();
//         let _ = sqlx::query!(
//             r#"
//             INSERT INTO statistics (statistics_id)
//             VALUES ($1)
//             "#,
//             stats_id
//         )
//         for (endpoint, count) in stats {
//             let _ = sqlx::query!(
//                 r#"
//                 INSERT INTO endpoint_stats (stats_id, endpoint, count)
//                 VALUES ($1, $2)
//                 ON CONFLICT (endpoint) DO UPDATE SET count = endpoint_stats.count + $2
//                 "#,
//                 endpoint,
//                 count
//             )
//             .execute(&mut conn)
//             .await;
//         }
//     }
// }
