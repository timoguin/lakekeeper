use std::{str::FromStr, sync::Arc, time::Duration};

use http::Method;
use sqlx::PgPool;
use strum::IntoEnumIterator;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

use crate::{
    api::{
        endpoints::Endpoints,
        management::v1::warehouse::{CreateWarehouseResponse, TabularDeleteProfile},
        ApiContext,
    },
    implementations::postgres::{PostgresCatalog, PostgresStatisticsSink, SecretsState},
    request_metadata::RequestMetadata,
    service::{
        authz::AllowAllAuthorizer,
        endpoint_statistics::{EndpointStatisticsMessage, EndpointStatisticsTracker},
        task_queue::TaskQueueConfig,
        Actor, EndpointStatisticsTrackerTx, State, UserId,
    },
    tests::random_request_metadata,
    DEFAULT_PROJECT_ID,
};

mod test {
    use std::{str::FromStr, sync::Arc, time::Duration};

    use http::Method;
    use maplit::hashmap;
    use sqlx::PgPool;
    use strum::IntoEnumIterator;
    use uuid::Uuid;

    use crate::{
        api::{
            endpoints::Endpoints,
            management::v1::{project::Service, ApiServer},
        },
        request_metadata::RequestMetadata,
        service::{endpoint_statistics::EndpointStatisticsMessage, Actor},
        DEFAULT_PROJECT_ID,
    };

    #[sqlx::test]
    async fn test_stats_task_produces_correct_values(pool: PgPool) {
        let setup = super::setup_stats_test(pool).await;

        // send each endpoint once
        for ep in Endpoints::iter() {
            let (method, path) = ep.to_http_string().split_once(" ").unwrap();
            let method = Method::from_str(method).unwrap();
            let request_metadata = RequestMetadata::new_test(
                None,
                None,
                Actor::Anonymous,
                DEFAULT_PROJECT_ID.clone(),
                Some(Arc::from(path)),
                method,
            );

            setup
                .tx
                .send(EndpointStatisticsMessage::EndpointCalled {
                    request_metadata,
                    response_status: http::StatusCode::OK,
                    path_params: hashmap! {
                        "warehouse_id".to_string() => Uuid::new_v4().to_string(),
                    },
                    query_params: Default::default(),
                })
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(1100)).await;
        ApiServer::get_endpoint_statistics(setup.ctx.clone())
            .await
            .unwrap();
    }
}

// TODO: test with multiple warehouses and projects

struct StatsSetup {
    ctx: ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
    tracker_handle: tokio::task::JoinHandle<()>,
    warehouse: CreateWarehouseResponse,
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
                .with_default_directive(LevelFilter::INFO.into())
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
        warehouse,
        tracker_handle,
        tx,
    }
}
