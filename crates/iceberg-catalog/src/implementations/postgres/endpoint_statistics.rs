use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::Utc;
use itertools::{izip, Itertools};
use sqlx::PgPool;

use crate::{
    api::{
        endpoints::Endpoints,
        management::v1::project::{EndpointStatistic, EndpointStatisticsResponse, WarehouseFilter},
    },
    implementations::postgres::dbutils::DBErrorHandler,
    service::endpoint_statistics::{EndpointIdentifier, EndpointStatisticsSink},
    ProjectIdent,
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
        stats: Arc<HashMap<ProjectIdent, HashMap<EndpointIdentifier, i64>>>,
    ) -> crate::api::Result<()> {
        let mut conn = self.pool.begin().await.map_err(|e| {
            tracing::error!("Failed to start transaction: {e}, lost stats: {stats:?}");
            e.into_error_model("failed to start transaction")
        })?;

        for (project, endpoints) in stats.iter() {
            tracing::info!("Consuming stats for project: {project:?}, counts: {endpoints:?}",);
            let (uris, status_codes, warehouses, warehouse_names, counts): (
                Vec<Endpoints>,
                Vec<i32>,
                Vec<_>,
                Vec<_>,
                Vec<_>,
            ) = endpoints
                .iter()
                .map(
                    |(
                        EndpointIdentifier {
                            uri,
                            status_code,
                            warehouse,
                            warehouse_name,
                        },
                        count,
                    )| {
                        (
                            uri,
                            i32::from(status_code.as_u16()),
                            *warehouse,
                            warehouse_name,
                            count,
                        )
                    },
                )
                .multiunzip::<_>();
            let whn = warehouse_names
                .iter()
                .filter_map(|w| w.as_deref().map(ToString::to_string))
                .collect::<Vec<_>>();
            let warehouse_ids = sqlx::query!(
                r#"select warehouse_name, warehouse_id from warehouse where warehouse_name = any($1)"#,
                &whn
            ).fetch_all(&mut *conn).await.map_err(|e| {
                tracing::error!("Failed to fetch warehouse ids: {e}, lost stats: {stats:?}");
                e.into_error_model("failed to fetch warehouse ids")
            })?.into_iter().map(|w| (w.warehouse_name, w.warehouse_id)).collect::<HashMap<_, _>>();

            let warehouses = warehouses
                .into_iter()
                .zip(warehouse_names.iter())
                .map(|(w, wn)| {
                    let mut w = w.map(|w| *w);
                    if w.is_none() && wn.is_some() {
                        let wn = wn.as_ref().unwrap();
                        if let Some(warehouse_id) = warehouse_ids.get(wn) {
                            w.replace(*warehouse_id);
                        }
                    }
                    w
                })
                .collect::<Vec<_>>();

            sqlx::query!(
                        r#"INSERT INTO endpoint_statistics (project_id, warehouse_id, matched_path, status_code, count, timestamp)
                            SELECT
                                $1,
                                warehouse,
                                uri,
                                status_code,
                                cnt,
                                get_stats_date_default()
                            FROM (
                                SELECT
                                    unnest($2::UUID[]) as warehouse,
                                    unnest($3::api_endpoints[]) as uri,
                                    unnest($4::INT[]) as status_code,
                                    unnest($5::BIGINT[]) as cnt
                            ) t
                            ON CONFLICT (project_id, warehouse_id, matched_path, status_code, timestamp)
                                DO UPDATE SET count = endpoint_statistics.count + EXCLUDED.count"#,
                **project,
                warehouses.as_slice() as _,
                &uris as _,
                &status_codes,
                &counts
            ).execute(&mut *conn).await.map_err(|e| {
                tracing::error!("Failed to insert stats: {e}, lost stats: {stats:?}");
                e.into_error_model("failed to insert stats")
            })?;
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
        stats: HashMap<ProjectIdent, HashMap<EndpointIdentifier, i64>>,
    ) -> crate::api::Result<()> {
        let stats = Arc::new(stats);

        tryhard::retry_fn(async || {
            self.process_stats(stats.clone()).await.inspect_err(|e| {
                tracing::error!(
                    "Failed to consume stats: {:?}, will retry up to 5 times.",
                    e.error
                );
            })
        })
        .retries(5)
        .exponential_backoff(Duration::from_millis(125))
        .await
        .inspect(|()| {
            tracing::debug!("Successfully consumed stats");
        })
        .inspect_err(|e| {
            tracing::error!("Failed to consume stats: {:?}", e.error);
        })
    }

    fn sink_id(&self) -> &'static str {
        "postgres"
    }
}

pub(crate) async fn list_statistics(
    project: ProjectIdent,
    warehouse_filter: WarehouseFilter,
    status_codes: Option<&[u16]>,
    interval: chrono::Duration,
    end: chrono::DateTime<Utc>,
    conn: &PgPool,
) -> crate::api::Result<EndpointStatisticsResponse> {
    let start = end - interval;

    let get_all = matches!(warehouse_filter, WarehouseFilter::All);
    let warehouse_filter = match warehouse_filter {
        WarehouseFilter::Ident(ident) => Some(ident),
        _ => None,
    };
    let status_codes = status_codes.map(|s| s.iter().map(|i| i32::from(*i)).collect_vec());

    tracing::info!(
        "Listing stats for project: {project:?}, get_all: {get_all}, warehouse_filter: {warehouse_filter:?}, status_codes: {status_codes:?}, interval: {interval:?}, start: {start:?}, end: {end:?}",
        project = project,
        warehouse_filter = warehouse_filter,
        status_codes = status_codes,
        interval = interval,
        start = start,
        end = end,
    );

    let row = sqlx::query!(
        r#"
        SELECT timestamp,
               array_agg(matched_path) as "matched_path!: Vec<Endpoints>",
               array_agg(status_code) as "status_code!",
               array_agg(count) as "count!",
               array_agg(es.warehouse_id) as "warehouse_id!",
               array_agg(warehouse_name) as "warehouse_name!",
               array_agg(es.created_at) as "created_at!",
               array_agg(es.updated_at) as "updated_at!: Vec<Option<chrono::DateTime<Utc>>>"
        FROM endpoint_statistics es
        JOIN warehouse w ON es.warehouse_id = w.warehouse_id
        WHERE es.project_id = $1
            AND (es.warehouse_id = $2 OR $3)
            AND (status_code = ANY($4) OR $4 IS NULL)
            AND timestamp >= $5
            AND timestamp <= $6
        group by timestamp
        order by timestamp desc
        "#,
        *project,
        warehouse_filter,
        get_all,
        status_codes.as_deref(),
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
            let ts = r.timestamp;
            let row_stats: Vec<_> = izip!(
                r.matched_path,
                r.status_code,
                r.count,
                r.warehouse_id,
                r.warehouse_name,
                r.created_at,
                r.updated_at
            )
            .map(
                |(
                    uri,
                    status_code,
                    count,
                    warehouse_id,
                    warehouse_name,
                    created_at,
                    updated_at,
                )| EndpointStatistic {
                    count,
                    http_string: uri.to_http_string().to_string(),
                    status_code: status_code
                        .clamp(i32::from(u16::MIN), i32::from(u16::MAX))
                        .try_into()
                        .expect("status code is valid since we just clamped it"),
                    warehouse_id,
                    warehouse_name,
                    created_at,
                    updated_at,
                },
            )
            .collect();

            (ts, row_stats)
        })
        .unzip();

    Ok(EndpointStatisticsResponse {
        timestamps,
        stats,
        previous_page_token: None,
        next_page_token: None,
    })
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use strum::IntoEnumIterator;

    use crate::{
        api::management::v1::warehouse::TabularDeleteProfile,
        implementations::postgres::PostgresStatisticsSink, service::authz::AllowAllAuthorizer,
        DEFAULT_PROJECT_ID,
    };

    #[sqlx::test]
    async fn test_can_insert_all_variants(pool: sqlx::PgPool) {
        let conn = pool.begin().await.unwrap();
        let (_api, warehouse) = crate::tests::setup(
            pool.clone(),
            crate::tests::test_io_profile(),
            None,
            AllowAllAuthorizer,
            TabularDeleteProfile::Hard {},
            None,
            None,
        )
        .await;

        let sink = PostgresStatisticsSink::new(pool);

        let project = DEFAULT_PROJECT_ID.unwrap();
        let status_code = http::StatusCode::OK;
        let count = 1;
        let ident = None;
        let warehouse_name = Some(warehouse.warehouse_name);
        let mut stats = HashMap::default();
        stats.insert(project, HashMap::default());
        let s = stats.get_mut(&project).unwrap();
        for uri in crate::api::endpoints::Endpoints::iter() {
            s.insert(
                crate::service::endpoint_statistics::EndpointIdentifier {
                    uri,
                    status_code,
                    warehouse: ident,
                    warehouse_name: warehouse_name.clone(),
                },
                count,
            );
        }
        sink.process_stats(Arc::new(stats)).await.unwrap();
        conn.commit().await.unwrap();
    }
}
