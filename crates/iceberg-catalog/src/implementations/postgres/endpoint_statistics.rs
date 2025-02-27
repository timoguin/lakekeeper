use chrono::Utc;
use http::StatusCode;
use itertools::izip;
use sqlx::{PgPool, Postgres, Transaction};
use std::{collections::HashMap, sync::Arc, time::Duration};
use uuid::Uuid;

use crate::api::management::v1::project::{
    EndpointStatistic, EndpointStatisticsResponse, WarehouseFilter,
};
use crate::{
    api::endpoints::Endpoints,
    implementations::postgres::dbutils::DBErrorHandler,
    service::endpoint_statistics::{EndpointIdentifier, EndpointStatisticsSink},
    ProjectIdent, WarehouseIdent,
};

#[derive(Debug)]
pub struct PostgresStatisticsSink {
    pool: sqlx::PgPool,
}

impl PostgresStatisticsSink {
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    async fn process_stats(
        &self,
        stats: Arc<HashMap<Option<ProjectIdent>, HashMap<EndpointIdentifier, i64>>>,
    ) -> crate::api::Result<()> {
        let mut conn = self.pool.begin().await.map_err(|e| {
            tracing::error!("Failed to start transaction: {e}, lost stats: {stats:?}");
            e.into_error_model("failed to start transaction")
        })?;

        for (project, endpoints) in stats.iter() {
            tracing::info!("Consuming stats for project: {project:?}, counts: {endpoints:?}",);
            for (
                EndpointIdentifier {
                    uri,
                    status_code,
                    warehouse,
                    warehouse_name,
                },
                count,
            ) in endpoints
            {
                insert_statistics(
                    &mut conn,
                    *project,
                    uri,
                    status_code,
                    *count,
                    *warehouse,
                    warehouse_name.as_deref(),
                )
                .await?;
            }
        }
        conn.commit().await.map_err(|e| {
            tracing::error!("Failed to commit: {e}");
            e.into_error_model("failed to commit")
        })?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl EndpointStatisticsSink for PostgresStatisticsSink {
    async fn consume_endpoint_statistics(
        &self,
        stats: HashMap<Option<ProjectIdent>, HashMap<EndpointIdentifier, i64>>,
    ) -> crate::api::Result<()> {
        let stats = Arc::new(stats);

        tryhard::retry_fn(async || {
            self.process_stats(stats.clone()).await.inspect_err(|e| {
                tracing::error!(
                    "Failed to consume stats: {}, will retry up to 5 times.",
                    e.error
                );
            })
        })
        .retries(5)
        .exponential_backoff(Duration::from_millis(125))
        .await
    }

    fn sink_id(&self) -> &'static str {
        "postgres"
    }
}

async fn list_statistics(
    conn: &PgPool,
    project: Option<ProjectIdent>,
    warehouse_filter: WarehouseFilter,
    status_codes: &[StatusCode],
    interval: Option<chrono::Duration>,
    end: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<EndpointStatisticsResponse, crate::api::ErrorModel> {
    let end = end.unwrap_or(Utc::now());
    let start = end - interval.unwrap_or_else(|| chrono::Duration::days(1));

    let get_all = matches!(warehouse_filter, WarehouseFilter::All);
    let warehouse_filter = match warehouse_filter {
        WarehouseFilter::Ident(ident) => Some(*ident),
        _ => None,
    };

    let row = sqlx::query!(
        r#"
        SELECT timestamp,
               array_agg(matched_path) as "matched_path!: Vec<Endpoints>",
               array_agg(status_code) as "status_code!",
               array_agg(count) as "count!",
               array_agg(created_at) as "created_at!",
               array_agg(updated_at) as "updated_at!"
        FROM endpoint_statistics
        WHERE project_id = $1
            AND (warehouse_id = $2 OR $3)
            AND status_code = ANY($4)
            AND timestamp >= $5
            AND timestamp <= $6
        group by timestamp
        order by timestamp desc
        "#,
        project.map(|p| *p),
        warehouse_filter,
        get_all,
        &status_codes.iter().map(|s| i32::from(s.as_u16())).collect(),
        start,
        end,
    )
    .fetch_all(conn)
    .await
    .map_err(|e| {
        tracing::error!("Failed to list stats: {e}");
        e.into_error_model("failed to list stats")
    })?;

    let (timestamps, stats): (Vec<_>, Vec<_>) = row
        .into_iter()
        .map(|r| {
            let row_stats: Vec<_> = izip!(
                &r.matched_path,
                &r.status_code,
                &r.count,
                &r.created_at,
                &r.updated_at
            )
            .map(
                |(uri, status_code, count, created_at, updated_at)| EndpointStatistic {
                    count: *count,
                    http_string: uri.to_http_string().to_string(),
                    status_code: *status_code as u16,
                    created_at: *created_at,
                    updated_at: *updated_at,
                },
            )
            .collect();

            (r.timestamp, row_stats)
        })
        .unzip();

    Ok(EndpointStatisticsResponse {
        timestamps,
        stats,
        previous_page_token: None,
        next_page_token: None,
    })
}

async fn insert_statistics(
    conn: &mut Transaction<'_, Postgres>,
    project: Option<ProjectIdent>,
    uri: &Endpoints,
    status_code: &StatusCode,
    count: i64,
    ident: Option<WarehouseIdent>,
    warehouse_name: Option<&str>,
) -> Result<(), crate::api::ErrorModel> {
    let _ = sqlx::query!(
                    r#"
                    WITH warehouse_id AS (SELECT CASE
                                 WHEN $2::uuid IS NULL
                                     THEN (SELECT warehouse_id FROM warehouse WHERE warehouse_name = $3)
                                 ELSE (SELECT warehouse_id FROM warehouse where warehouse.warehouse_id = $2::uuid)
                                 END AS warehouse_id)
                    INSERT
                    INTO endpoint_statistics (project_id, warehouse_id, matched_path, status_code, count)
                    SELECT $1,
                           (SELECT warehouse_id from warehouse_id),
                           $4,
                           $5,
                           COALESCE((SELECT count
                                     FROM endpoint_statistics
                                     WHERE project_id = $1
                                       AND warehouse_id = (select warehouse_id from warehouse_id)
                                       AND matched_path = $4
                                       AND status_code = $5
                                       AND timestamp = get_stats_date_default()), 0) + $6
                    ON CONFLICT (project_id, warehouse_id, matched_path, status_code, timestamp)
                        DO UPDATE SET count = EXCLUDED.count
                    "#,
        project.map(|p| *p),
        ident.as_deref().copied() as Option<Uuid>,
        warehouse_name,
        uri as _,
        i32::from(status_code.as_u16()),
        count
    )
    .execute(&mut **conn)
    .await
    .map_err(|e| {
        tracing::error!("Failed to insert stats: {e}");
        e.into_error_model("failed to insert stats")
    })
    ?;
    Ok(())
}

#[cfg(test)]
mod test {
    use strum::IntoEnumIterator;

    use crate::ProjectIdent;

    #[sqlx::test]
    async fn test_can_insert_all_variants(pool: sqlx::PgPool) {
        let mut conn = pool.begin().await.unwrap();

        let project = Some(ProjectIdent::default());
        let status_code = http::StatusCode::OK;
        let count = 1;
        let ident = None;
        let warehouse_name = None;
        for uri in crate::api::endpoints::Endpoints::iter() {
            super::insert_statistics(
                &mut conn,
                project,
                &uri,
                &status_code,
                count,
                ident,
                warehouse_name,
            )
            .await
            .unwrap();
        }

        conn.commit().await.unwrap();
    }
}
