use crate::api::management::v1::warehouse::{WarehouseStatistics, WarehouseStatsResponse};
use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::service::stats::endpoint::{EndpointIdentifier, StatsSink, WarehouseIdentOrPrefix};
use crate::service::ListFlags;
use crate::{ProjectIdent, WarehouseIdent};
use sqlx::PgPool;
use std::collections::HashMap;

pub(crate) async fn get_warehouse_stats(
    conn: PgPool,
    warehouse_ident: WarehouseIdent,
) -> crate::api::Result<WarehouseStatsResponse> {
    // TODO: pagination
    let stats = sqlx::query!(
        r#"
        SELECT number_of_views, number_of_tables, created_at
        FROM warehouse_statistics ws
        WHERE ws.warehouse_id = $1
        ORDER BY created_at DESC
        "#,
        warehouse_ident.0
    )
    .fetch_all(&conn)
    .await
    .map_err(|e| e.into_error_model("failed to get stats"))?;
    let stats = stats
        .into_iter()
        .map(|s| WarehouseStatistics {
            number_of_tables: s.number_of_tables,
            number_of_views: s.number_of_views,
            taken_at: s.created_at,
        })
        .collect();
    Ok(WarehouseStatsResponse {
        warehouse_ident: *warehouse_ident,
        stats,
    })
}

pub(crate) async fn update_stats(
    conn: PgPool,
    warehouse_ident: WarehouseIdent,
    // TODO: use list_flags for filtering
    _list_flags: ListFlags,
) -> crate::api::Result<WarehouseStatistics> {
    let stats = sqlx::query!(
        r#"
        WITH update_tables AS (
            SELECT count(*) AS value FROM "table" t
                INNER JOIN tabular ti ON t.table_id = ti.tabular_id
                INNER JOIN namespace n ON ti.namespace_id = n.namespace_id
                INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
            WHERE w.warehouse_id = $1 AND w.status = 'active'
        ),
        update_views AS (
            SELECT count(*) AS value FROM "view" v
                INNER JOIN tabular vi ON v.view_id = vi.tabular_id
                INNER JOIN namespace n ON vi.namespace_id = n.namespace_id
                INNER JOIN warehouse w ON n.warehouse_id = w.warehouse_id
            WHERE w.warehouse_id = $1 AND w.status = 'active'
        )
        INSERT INTO warehouse_statistics (warehouse_id, number_of_views, number_of_tables) VALUES ($1, (SELECT value FROM update_views), (SELECT value FROM update_tables))
        RETURNING (SELECT value as "number_of_views!" FROM update_views), (SELECT value as "number_of_tables!" FROM update_tables), created_at as "taken_at"
        "#,
        warehouse_ident.0
    )
        .fetch_one(&conn)
        .await
        .map_err(|e| e.into_error_model("failed to collect stats"))?;

    Ok(WarehouseStatistics {
        number_of_tables: stats.number_of_tables,
        number_of_views: stats.number_of_views,
        taken_at: stats.taken_at,
    })
}

#[derive(Debug)]
pub struct PostgresStatsSink {
    pool: sqlx::PgPool,
}

impl PostgresStatsSink {
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl StatsSink for PostgresStatsSink {
    async fn consume_endpoint_stats(
        &self,
        stats: HashMap<Option<ProjectIdent>, HashMap<EndpointIdentifier, i64>>,
    ) {
        // let mut conn = self.pool.begin().await.unwrap();

        for (project, endpoints) in stats {
            tracing::info!("Consuming stats for project: {project:?}, counts: {count:?}",);
            for (
                EndpointIdentifier {
                    uri,
                    status_code,
                    method,
                    warehouse,
                },
                count,
            ) in endpoints
            {
                tracing::info!("Consuming stats for endpoint: {endpoint:?}, count: {count:?}",);
                let (ident, prefix) = warehouse
                    .map(|w| match w {
                        WarehouseIdentOrPrefix::Ident(ident) => (Some(ident), None),
                        WarehouseIdentOrPrefix::Prefix(prefix) => (None, Some(prefix)),
                    })
                    .unzip();
                let ident = ident.flatten();
                let prefix = prefix.flatten();

                let (matched, non_matched) = uri.into_pair();
                let _ = sqlx::query!(
                    r#"
                    INSERT INTO endpoint_stats (project_id, warehouse_id, warehouse_name, uri, status_code, method, count)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (project_id, warehouse_id, uri, status_code, method)
                    DO UPDATE SET count = endpoint_stats.count + $6
                    "#,
                    project.map(|p| p.0),
                    ident,
                    prefix,

                )
                .execute(&mut conn)
                .await
                .map_err(|e| e.into_error_model("failed to consume stats"))?;
            }
        }
    }
}
