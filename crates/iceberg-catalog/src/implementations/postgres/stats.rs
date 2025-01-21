use crate::implementations::postgres::dbutils::DBErrorHandler;
use sqlx::PgPool;
// use crate::service::stats::endpoint::StatsSink;
use crate::service::stats::entities::WarehouseStatistics;
use crate::service::ListFlags;
use crate::WarehouseIdent;
use uuid::Uuid;

pub(crate) async fn update_stats(
    conn: PgPool,
    warehouse_ident: WarehouseIdent,
    // TODO: use list_flags for filtering
    _list_flags: ListFlags,
) -> crate::api::Result<WarehouseStatistics> {
    // TODO: we could also pass the task idempotency key into here instead and use it as the stats id
    let mut t = conn
        .begin()
        .await
        .map_err(|e| e.into_error_model("failed to begin transaction collectin stats"))?;
    let statistics_id = Uuid::now_v7();

    sqlx::query!(
        r#"INSERT INTO statistics (statistics_id, warehouse_id) VALUES ($1, $2)"#,
        statistics_id,
        warehouse_ident.0
    )
    .execute(&mut *t)
    .await
    .map_err(|e| e.into_error_model("failed to collect stats"))?;

    let stats = sqlx::query_as!(
        WarehouseStatistics,
        r#"
        WITH update_tables AS (
            INSERT INTO scalars (name, statistic_id, value)
            VALUES ('tables', $1, (SELECT count(*) AS value FROM "table" t
                INNER JOIN tabular ti ON t.table_id = ti.tabular_id
                INNER JOIN namespace n ON ti.namespace_id = n.namespace_id
                INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
            WHERE w.warehouse_id = $2 AND w.status = 'active'))
            RETURNING value
        ),
        update_views AS (
            INSERT INTO scalars (name, statistic_id, value)
            VALUES ('views', $1, (SELECT count(*) AS value FROM "view" v
                INNER JOIN tabular vi ON v.view_id = vi.tabular_id
                INNER JOIN namespace n ON vi.namespace_id = n.namespace_id
                INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
            WHERE w.warehouse_id = $2 AND w.status = 'active'))
            RETURNING value
        )
        SELECT $1 as "statistics_id!", $2 as "warehouse_ident!", (SELECT value FROM update_tables) as "number_of_tables!", (SELECT value FROM update_views) as "number_of_views!"
        "#,
        statistics_id,
        warehouse_ident.0
    )
        .fetch_one(&mut *t)
        .await
        .map_err(|e| e.into_error_model("failed to collect stats"))?;
    t.commit()
        .await
        .map_err(|e| e.into_error_model("failed to commit transaction recording stats"))?;
    Ok(stats)
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
