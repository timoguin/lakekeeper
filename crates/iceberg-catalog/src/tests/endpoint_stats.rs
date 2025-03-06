use std::{sync::Arc, time::Duration};

use sqlx::PgPool;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

use crate::{
    api::{management::v1::warehouse::TabularDeleteProfile, ApiContext},
    implementations::postgres::{PostgresCatalog, PostgresStatisticsSink, SecretsState},
    service::{
        authz::AllowAllAuthorizer,
        endpoint_statistics::{EndpointStatisticsTracker, FlushMode},
        task_queue::TaskQueueConfig,
        EndpointStatisticsTrackerTx, State, UserId,
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
                project::{GetEndpointStatisticsRequest, RangeSpecifier, Service, WarehouseFilter},
                ApiServer,
            },
        },
        implementations::postgres::{
            pagination,
            pagination::{PaginateToken, RoundTrippableDuration},
        },
        request_metadata::RequestMetadata,
        service::{
            endpoint_statistics::{EndpointStatisticsMessage, FlushMode},
            Actor,
        },
        tests::endpoint_stats::StatsSetup,
        DEFAULT_PROJECT_ID,
    };

    #[sqlx::test]
    async fn test_stats_task_produces_correct_values(pool: PgPool) {
        let setup = super::setup_stats_test(pool, FlushMode::Automatic).await;

        // send each endpoint once
        for ep in Endpoints::iter() {
            let (method, path) = ep.as_http_route().split_once(' ').unwrap();
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

        tokio::time::sleep(Duration::from_millis(1100)).await;

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
            .map(|s| s.http_route.clone())
            .collect::<HashSet<_>>();
        let expected = Endpoints::iter()
            .map(|e| e.as_http_route().to_string())
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

    #[sqlx::test]
    async fn test_endpoint_stats_pagination(pool: PgPool) {
        let setup = super::setup_stats_test(pool, FlushMode::Manual).await;

        send_all_endpoints(&setup).await;

        setup
            .tx
            .send(EndpointStatisticsMessage::Flush)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(950)).await;
        let stats = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: Some(RangeSpecifier::Range {
                    end_of_range: chrono::Utc::now(),
                    interval: chrono::Duration::seconds(1),
                }),
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        send_all_endpoints(&setup).await;

        setup
            .tx
            .send(EndpointStatisticsMessage::Flush)
            .await
            .unwrap();

        // give some time to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        let t: PaginateToken<RoundTrippableDuration> =
            pagination::PaginateToken::try_from(stats.next_page_token.as_str()).unwrap();
        tracing::error!("{t:?} {t}");
        let new_stats = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: Some(RangeSpecifier::PageToken {
                    token: stats.next_page_token,
                }),
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(new_stats.timestamps.len(), 1);
        assert_eq!(new_stats.stats.len(), 1);
        assert_eq!(new_stats.stats[0].len(), Endpoints::iter().count());
        assert!(new_stats.timestamps[0] > stats.timestamps[0]);

        for s in &new_stats.stats[0] {
            assert_eq!(s.count, 1, "{s:?}");
        }

        let two_items = ApiServer::get_endpoint_statistics(
            setup.ctx.clone(),
            GetEndpointStatisticsRequest {
                warehouse: WarehouseFilter::All,
                status_codes: None,
                range_specifier: Some(RangeSpecifier::Range {
                    end_of_range: new_stats.timestamps[0],
                    interval: chrono::Duration::seconds(2),
                }),
            },
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(two_items.timestamps.len(), 2);
        assert_eq!(two_items.stats.len(), 2);
        assert_eq!(two_items.stats[0].len(), Endpoints::iter().count());
        assert_eq!(two_items.stats[1].len(), Endpoints::iter().count());
        assert!(two_items.timestamps[0] > two_items.timestamps[1]);

        setup
            .tx
            .send(EndpointStatisticsMessage::Shutdown)
            .await
            .unwrap();
        setup.tracker_handle.await.unwrap();
    }

    async fn send_all_endpoints(setup: &StatsSetup) {
        // send each endpoint once
        for ep in Endpoints::iter() {
            let (method, path) = ep.as_http_route().split_once(' ').unwrap();
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

async fn setup_stats_test(pool: PgPool, flush_mode: FlushMode) -> StatsSetup {
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
        flush_mode,
    );
    let tracker_handle = tokio::task::spawn(tracker.run());

    StatsSetup {
        ctx,
        tracker_handle,
        warehouse,
        tx,
    }
}
