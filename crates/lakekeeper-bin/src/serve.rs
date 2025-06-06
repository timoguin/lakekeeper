use std::sync::Arc;

use anyhow::anyhow;
#[cfg(feature = "ui")]
use axum::routing::get;
use lakekeeper::{
    api::router::{new_full_router, serve as service_serve, RouterArgs},
    implementations::{
        postgres::{endpoint_statistics::PostgresStatisticsSink, CatalogState, PostgresCatalog},
        Secrets,
    },
    service::{
        authz::{
            implementations::{get_default_authorizer_from_config, Authorizers},
            Authorizer,
        },
        contract_verification::ContractVerifiers,
        endpoint_hooks::EndpointHookCollection,
        endpoint_statistics::{EndpointStatisticsMessage, EndpointStatisticsTracker, FlushMode},
        event_publisher::{
            kafka::build_kafka_publisher_from_config, nats::build_nats_publisher_from_config,
            CloudEventBackend, CloudEventsMessage, CloudEventsPublisher,
            CloudEventsPublisherBackgroundTask, TracingPublisher,
        },
        health::ServiceHealthProvider,
        task_queue::TaskQueueRegistry,
        Catalog, EndpointStatisticsTrackerTx, StartupValidationData,
    },
    SecretBackend, CONFIG,
};
use limes::{Authenticator, AuthenticatorEnum};

const OIDC_IDP_ID: &str = "oidc";
const K8S_IDP_ID: &str = "kubernetes";

#[cfg(feature = "ui")]
use crate::ui;

pub(crate) async fn serve(bind_addr: std::net::SocketAddr) -> Result<(), anyhow::Error> {
    let read_pool = lakekeeper::implementations::postgres::get_reader_pool(
        CONFIG
            .to_pool_opts()
            .max_connections(CONFIG.pg_read_pool_connections),
    )
    .await?;
    let write_pool = lakekeeper::implementations::postgres::get_writer_pool(
        CONFIG
            .to_pool_opts()
            .max_connections(CONFIG.pg_write_pool_connections),
    )
    .await?;

    let catalog_state = CatalogState::from_pools(read_pool.clone(), write_pool.clone());

    let validation_data = PostgresCatalog::get_server_info(catalog_state.clone()).await?;
    match validation_data {
        StartupValidationData::NotBootstrapped => {
            tracing::info!("The catalog is not bootstrapped. Bootstrapping sets the initial administrator. Please open the Web-UI after startup or call the bootstrap endpoint directly.");
        }
        StartupValidationData::Bootstrapped {
            server_id,
            terms_accepted,
        } => {
            if !terms_accepted {
                return Err(anyhow!(
                    "The terms of service have not been accepted on bootstrap."
                ));
            }
            if server_id != CONFIG.server_id {
                return Err(anyhow!(
                    "The server ID during bootstrap {} does not match the server ID in the configuration {}.", server_id, CONFIG.server_id
                ));
            }
            tracing::info!("The catalog is bootstrapped. Server ID: {server_id}");
        }
    }

    let secrets_state: Secrets = match CONFIG.secret_backend {
        SecretBackend::KV2 => lakekeeper::implementations::kv2::SecretsState::from_config(
            CONFIG
                .kv2
                .as_ref()
                .ok_or_else(|| anyhow!("Need vault config to use vault as backend"))?,
        )
        .await?
        .into(),
        SecretBackend::Postgres => lakekeeper::implementations::postgres::SecretsState::from_pools(
            read_pool.clone(),
            write_pool.clone(),
        )
        .into(),
    };
    let authorizer = get_default_authorizer_from_config().await?;

    let health_provider = ServiceHealthProvider::new(
        vec![
            ("catalog", Arc::new(catalog_state.clone())),
            ("secrets", Arc::new(secrets_state.clone())),
            ("auth", Arc::new(authorizer.clone())),
        ],
        CONFIG.health_check_frequency_seconds,
        CONFIG.health_check_jitter_millis,
    );
    health_provider.spawn_health_checks().await;

    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|e| anyhow!(e).context(format!("Failed to bind to address: {bind_addr}")))?;
    match authorizer {
        Authorizers::AllowAll(a) => {
            serve_with_authn(a, catalog_state, secrets_state, health_provider, listener).await?
        }
        Authorizers::OpenFGA(a) => {
            serve_with_authn(a, catalog_state, secrets_state, health_provider, listener).await?
        }
    }

    Ok(())
}

async fn serve_with_authn<A: Authorizer>(
    authorizer: A,
    catalog_state: CatalogState,
    secrets_state: Secrets,
    health_provider: ServiceHealthProvider,
    listener: tokio::net::TcpListener,
) -> Result<(), anyhow::Error> {
    let authn_k8s_audience = if CONFIG.enable_kubernetes_authentication {
        Some(
            limes::kubernetes::KubernetesAuthenticator::try_new_with_default_client(
                Some(K8S_IDP_ID),
                CONFIG
                    .kubernetes_authentication_audience
                    .clone()
                    .unwrap_or_default(),
            )
            .await
            .inspect_err(|e| tracing::error!("Failed to create K8s authorizer: {e}"))
            .inspect(|v| tracing::info!("K8s authorizer created {:?}", v))?,
        )
    } else {
        tracing::info!("Running without Kubernetes authentication.");
        None
    };
    let authn_k8s_legacy = if CONFIG.enable_kubernetes_authentication
        && CONFIG.kubernetes_authentication_accept_legacy_serviceaccount
    {
        let mut authenticator =
            limes::kubernetes::KubernetesAuthenticator::try_new_with_default_client(
                Some(K8S_IDP_ID),
                vec![],
            )
            .await
            .inspect_err(|e| tracing::error!("Failed to create K8s authorizer: {e}"))?;
        authenticator.set_issuers(vec!["kubernetes/serviceaccount".to_string()]);
        tracing::info!(
            "K8s authorizer for legacy service account tokens created {:?}",
            authenticator
        );

        Some(authenticator)
    } else {
        tracing::info!("Running without Kubernetes authentication for legacy service accounts.");
        None
    };

    let authn_oidc = if let Some(uri) = CONFIG.openid_provider_uri.clone() {
        let mut authenticator = limes::jwks::JWKSWebAuthenticator::new(
            uri.as_ref(),
            Some(std::time::Duration::from_secs(3600)),
        )
        .await?
        .set_idp_id(OIDC_IDP_ID);
        if let Some(aud) = &CONFIG.openid_audience {
            tracing::debug!("Setting accepted audiences: {aud:?}");
            authenticator = authenticator.set_accepted_audiences(aud.clone());
        }
        if let Some(iss) = &CONFIG.openid_additional_issuers {
            tracing::debug!("Setting openid_additional_issuers: {iss:?}");
            authenticator = authenticator.add_additional_issuers(iss.clone());
        }
        if let Some(scope) = &CONFIG.openid_scope {
            tracing::debug!("Setting openid_scope: {}", scope);
            authenticator = authenticator.set_scope(scope.clone());
        }
        if let Some(subject_claim) = &CONFIG.openid_subject_claim {
            tracing::debug!("Setting openid_subject_claim: {}", subject_claim);
            authenticator = authenticator.with_subject_claim(subject_claim.clone());
        } else {
            // "oid" should be used for entra-id, as the `sub` is different between applications.
            // We prefer oid here by default as no other IdP sets this field (that we know of) and
            // we can provide an out-of-the-box experience for users.
            // Nevertheless, we document this behavior in the docs and recommend as part of the
            // `production` checklist to set the claim explicitly.
            tracing::debug!("Defaulting openid_subject_claim to: oid, sub");
            authenticator =
                authenticator.with_subject_claims(vec!["oid".to_string(), "sub".to_string()]);
        }
        tracing::info!("Running with OIDC authentication.");
        Some(authenticator)
    } else {
        tracing::info!("Running without OIDC authentication.");
        None
    };

    let authn_k8s = authn_k8s_audience.map(AuthenticatorEnum::from);
    let authn_k8s_legacy = authn_k8s_legacy.map(AuthenticatorEnum::from);
    let authn_oidc = authn_oidc.map(AuthenticatorEnum::from);
    match (authn_k8s, authn_oidc, authn_k8s_legacy) {
        (Some(k8s), Some(oidc), Some(authn_k8s_legacy)) => {
            let authenticator = limes::AuthenticatorChain::<AuthenticatorEnum>::builder()
                .add_authenticator(oidc)
                .add_authenticator(k8s)
                .add_authenticator(authn_k8s_legacy)
                .build();
            serve_inner(
                authorizer,
                Some(authenticator),
                catalog_state,
                secrets_state,
                health_provider,
                listener,
            )
            .await
        }
        (None, Some(auth1), Some(auth2))
        | (Some(auth1), None, Some(auth2))
        // OIDC has priority over k8s if specified
        | (Some(auth2), Some(auth1), None) => {
            let authenticator = limes::AuthenticatorChain::<AuthenticatorEnum>::builder()
                .add_authenticator(auth1)
                .add_authenticator(auth2)
                .build();
            serve_inner(
                authorizer,
                Some(authenticator),
                catalog_state,
                secrets_state,
                health_provider,
                listener,
            )
            .await
        }
        (Some(auth), None, None) | (None, Some(auth), None) | (None, None, Some(auth)) => {
            serve_inner(
                authorizer,
                Some(auth),
                catalog_state,
                secrets_state,
                health_provider,
                listener,
            )
            .await
        }
        (None, None, None) => {
            tracing::warn!("Authentication is disabled. This is not suitable for production!");
            serve_inner(
                authorizer,
                None::<AuthenticatorEnum>,
                catalog_state,
                secrets_state,
                health_provider,
                listener,
            )
            .await
        }
    }
}

/// Helper function to remove redundant code from matching different implementations
#[allow(clippy::too_many_arguments)]
async fn serve_inner<A: Authorizer, N: Authenticator + 'static>(
    authorizer: A,
    authenticator: Option<N>,
    catalog_state: CatalogState,
    secrets_state: Secrets,
    health_provider: ServiceHealthProvider,
    listener: tokio::net::TcpListener,
) -> Result<(), anyhow::Error> {
    let (cloud_events_tx, cloud_events_rx) = tokio::sync::mpsc::channel(1000);

    let mut cloud_event_sinks = vec![];

    if let Some(nats_publisher) = build_nats_publisher_from_config().await? {
        cloud_event_sinks
            .push(Arc::new(nats_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
    };
    if let Some(kafka_publisher) = build_kafka_publisher_from_config()? {
        cloud_event_sinks
            .push(Arc::new(kafka_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
    }

    if let Some(true) = &CONFIG.log_cloudevents {
        let tracing_publisher = TracingPublisher;
        cloud_event_sinks
            .push(Arc::new(tracing_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
        tracing::info!("Logging Cloudevents to Console.");
    } else {
        tracing::info!("Running without logging Cloudevents.");
    }

    if cloud_event_sinks.is_empty() {
        tracing::info!("Running without publisher.");
    }

    let x: CloudEventsPublisherBackgroundTask = CloudEventsPublisherBackgroundTask {
        source: cloud_events_rx,
        sinks: cloud_event_sinks,
    };

    let (layer, metrics_future) = lakekeeper::metrics::get_axum_layer_and_install_recorder(
        CONFIG.metrics_port,
    )
    .map_err(|e| {
        anyhow!(e).context(format!(
            "Failed to start metrics server on port: {}",
            CONFIG.metrics_port
        ))
    })?;

    let (endpoint_statistics_tx, endpoint_statistics_rx) = tokio::sync::mpsc::channel(1000);

    let tracker = EndpointStatisticsTracker::new(
        endpoint_statistics_rx,
        vec![Arc::new(PostgresStatisticsSink::new(
            catalog_state.write_pool(),
        ))],
        CONFIG.endpoint_stat_flush_interval,
        FlushMode::Automatic,
    );

    let endpoint_statistics_tracker_tx = EndpointStatisticsTrackerTx::new(endpoint_statistics_tx);
    let hooks = EndpointHookCollection::new(vec![Arc::new(CloudEventsPublisher::new(
        cloud_events_tx.clone(),
    ))]);
    let mut task_queue_registry = TaskQueueRegistry::new();
    task_queue_registry.register_built_in_queues::<PostgresCatalog, Secrets, A>(
        catalog_state.clone(),
        secrets_state.clone(),
        authorizer.clone(),
        CONFIG.task_poll_interval,
    );

    let router = new_full_router::<PostgresCatalog, _, Secrets, _>(RouterArgs {
        authenticator: authenticator.clone(),
        authorizer: authorizer.clone(),
        catalog_state: catalog_state.clone(),
        secrets_state: secrets_state.clone(),
        table_change_checkers: ContractVerifiers::new(vec![]),
        service_health_provider: health_provider,
        cors_origins: CONFIG.allow_origin.as_deref(),
        metrics_layer: Some(layer),
        endpoint_statistics_tracker_tx: endpoint_statistics_tracker_tx.clone(),
        hooks,
        registered_task_queues: task_queue_registry.registered_task_queues(),
    })?;

    #[cfg(feature = "ui")]
    let router = router
        .route(
            "/ui",
            get(|| async { axum::response::Redirect::permanent("/ui/") }),
        )
        .route(
            "/",
            get(|| async { axum::response::Redirect::permanent("/ui/") }),
        )
        .route(
            "/ui/index.html",
            get(|| async { axum::response::Redirect::permanent("/ui/") }),
        )
        .route("/ui/", get(ui::index_handler))
        .route("/ui/favicon.ico", get(ui::favicon_handler))
        .route("/ui/assets/{*file}", get(ui::static_handler))
        .route("/ui/{*file}", get(ui::index_handler));

    let publisher_handle = tokio::task::spawn(async move {
        match x.publish().await {
            Ok(_) => tracing::info!("Exiting publisher task"),
            Err(e) => tracing::error!("Publisher task failed: {e}"),
        };
    });
    let stats_handle = tokio::task::spawn(tracker.run());

    let task_runner = task_queue_registry.task_queues_runner();
    tokio::select!(
        _ = task_runner.run_queue_workers(true) => tracing::error!("Task queues failed."),
        err = service_serve(listener, router) => tracing::error!("Service failed: {err:?}"),
        _ = metrics_future => tracing::error!("Metrics server failed"),
    );

    tracing::debug!("Sending shutdown signal to event publisher.");
    endpoint_statistics_tracker_tx
        .send(EndpointStatisticsMessage::Shutdown)
        .await?;
    cloud_events_tx.send(CloudEventsMessage::Shutdown).await?;
    publisher_handle.await?;
    stats_handle.await?;
    Ok(())
}
