use std::{sync::Arc, time::Duration};

use sqlx::PgPool;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

use crate::{
    api::{management::v1::warehouse::TabularDeleteProfile, ApiContext},
    implementations::postgres::{PostgresCatalog, PostgresStatisticsSink, SecretsState},
    service::{
        authz::AllowAllAuthorizer, endpoint_statistics::EndpointStatisticsTracker,
        task_queue::TaskQueueConfig, EndpointStatisticsTrackerTx, State, UserId,
    },
    tests::TestWarehouseResponse,
};

mod test {
    use std::{
        collections::{HashMap, HashSet},
        str::FromStr,
        sync::Arc,
        time::Duration,
    };

    use http::Method;
    use maplit::hashmap;
    use sqlx::PgPool;
    use strum::IntoEnumIterator;

    use crate::{
        api::{
            endpoints::Endpoints,
            management::v1::{
                project::{GetEndpointStatisticsRequest, Service, WarehouseFilter},
                ApiServer,
            },
        },
        request_metadata::RequestMetadata,
        service::{endpoint_statistics::EndpointStatisticsMessage, Actor},
        DEFAULT_PROJECT_ID,
    };

    #[sqlx::test]
    async fn test_stats_task_produces_correct_values(pool: PgPool) {
        let setup = super::setup_stats_test(pool).await;
        // tokio::time::pause();
        // send each endpoint once
        for ep in Endpoints::iter() {
            let (method, path) = ep.to_http_string().split_once(' ').unwrap();
            let method = Method::from_str(method).unwrap();
            let request_metadata = RequestMetadata::new_test(
                None,
                None,
                Actor::Anonymous,
                *DEFAULT_PROJECT_ID,
                Some(Arc::from(path)),
                method,
            );

            setup
                .tx
                .send(EndpointStatisticsMessage::EndpointCalled {
                    request_metadata,
                    response_status: http::StatusCode::OK,
                    path_params: hashmap! {
                        "warehouse_id".to_string() => setup.warehouse.warehouse_id.to_string(),
                    },
                    query_params: HashMap::default(),
                })
                .await
                .unwrap();
        }
        // tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::sleep(Duration::from_millis(2100)).await;
        let stats = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: None,
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(stats.timestamps.len(), 1);
        assert_eq!(stats.stats.len(), 1);
        assert_eq!(stats.stats[0].len(), Endpoints::iter().count());

        for s in &stats.stats[0] {
            assert_eq!(s.count, 1, "{s:?}");
        }

        let all = stats.stats[0]
            .iter()
            .map(|s| s.http_string.clone())
            .collect::<HashSet<_>>();
        let expected = Endpoints::iter()
            .map(|e| e.to_http_string().to_string())
            .collect::<HashSet<_>>();
        assert_eq!(
            all,
            expected,
            "symmetric diff: {:?}",
            all.symmetric_difference(&expected)
        );
        setup
            .tx
            .send(EndpointStatisticsMessage::Shutdown)
            .await
            .unwrap();
        setup.tracker_handle.await.unwrap();
    }
}

// TODO: test with multiple warehouses and projects

struct StatsSetup {
    ctx: ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
    tracker_handle: tokio::task::JoinHandle<()>,
    warehouse: TestWarehouseResponse,
    tx: EndpointStatisticsTrackerTx,
}

async fn configure_trigger(pool: &PgPool) {
    sqlx::query!(
        r#"CREATE OR REPLACE FUNCTION get_stats_interval_unit() RETURNS text AS
        $$
        BEGIN
            RETURN 'second';
        END;
        $$ LANGUAGE plpgsql;"#
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn setup_stats_test(pool: PgPool) -> StatsSetup {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::DEBUG.into())
                .from_env_lossy(),
        )
        .try_init()
        .ok();
    configure_trigger(&pool).await;

    let prof = crate::tests::test_io_profile();
    let (ctx, warehouse) = crate::tests::setup(
        pool.clone(),
        prof,
        None,
        AllowAllAuthorizer,
        TabularDeleteProfile::Hard {},
        Some(UserId::new_unchecked("oidc", "test-user-id")),
        Some(TaskQueueConfig {
            max_retries: 1,
            max_age: chrono::Duration::seconds(60),
            poll_interval: std::time::Duration::from_secs(10),
        }),
    )
    .await;

    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let tx = EndpointStatisticsTrackerTx::new(tx);
    let tracker = EndpointStatisticsTracker::new(
        rx,
        vec![Arc::new(PostgresStatisticsSink::new(pool.clone()))],
        Duration::from_secs(1),
    );
    let tracker_handle = tokio::task::spawn(tracker.run());

    StatsSetup {
        ctx,
        tracker_handle,
        warehouse,
        tx,
    }
}
