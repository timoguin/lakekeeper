use std::sync::Arc;

use anyhow::{anyhow, Error};
#[cfg(feature = "ui")]
use axum::routing::get;
use iceberg_catalog::{
    api::router::{new_full_router, serve as service_serve, RouterArgs},
    implementations::{
        postgres::{
            task_queues::{TabularExpirationQueue, TabularPurgeQueue},
            CatalogState, PostgresCatalog, PostgresStatsSink, ReadWrite,
        },
        Secrets,
    },
    service::{
        authz::{
            implementations::{get_default_authorizer_from_config, Authorizers},
            Authorizer,
        },
        contract_verification::ContractVerifiers,
        event_publisher::{
            CloudEventBackend, CloudEventsPublisher, CloudEventsPublisherBackgroundTask, Message,
            NatsBackend, TracingPublisher,
        },
        health::ServiceHealthProvider,
        stats::endpoint::Tracker,
        task_queue::TaskQueues,
        Catalog, StartupValidationData, TrackerTx,
    },
    SecretBackend, CONFIG,
};
use limes::{Authenticator, AuthenticatorEnum};
use reqwest::Url;

const OIDC_IDP_ID: &str = "oidc";
const K8S_IDP_ID: &str = "kubernetes";

#[cfg(feature = "ui")]
use crate::ui;

pub(crate) async fn serve(bind_addr: std::net::SocketAddr) -> Result<(), anyhow::Error> {
    let read_pool = iceberg_catalog::implementations::postgres::get_reader_pool(
        CONFIG
            .to_pool_opts()
            .max_connections(CONFIG.pg_read_pool_connections),
    )
    .await?;
    let write_pool = iceberg_catalog::implementations::postgres::get_writer_pool(
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
        SecretBackend::KV2 => iceberg_catalog::implementations::kv2::SecretsState::from_config(
            CONFIG
                .kv2
                .as_ref()
                .ok_or_else(|| anyhow!("Need vault config to use vault as backend"))?,
        )
        .await?
        .into(),
        SecretBackend::Postgres => {
            iceberg_catalog::implementations::postgres::SecretsState::from_pools(
                read_pool.clone(),
                write_pool.clone(),
            )
            .into()
        }
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

    let queues = TaskQueues::new(
        Arc::new(TabularExpirationQueue::from_config(
            ReadWrite::from_pools(read_pool.clone(), write_pool.clone()),
            CONFIG.queue_config.clone(),
        )?),
        Arc::new(TabularPurgeQueue::from_config(
            ReadWrite::from_pools(read_pool.clone(), write_pool.clone()),
            CONFIG.queue_config.clone(),
        )?),
    );

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;

    match authorizer {
        Authorizers::AllowAll(a) => {
            serve_with_authn(
                a,
                catalog_state,
                secrets_state,
                queues,
                health_provider,
                listener,
            )
            .await?
        }
        Authorizers::OpenFGA(a) => {
            serve_with_authn(
                a,
                catalog_state,
                secrets_state,
                queues,
                health_provider,
                listener,
            )
            .await?
        }
    }

    Ok(())
}

async fn serve_with_authn<A: Authorizer>(
    authorizer: A,
    catalog_state: CatalogState,
    secrets_state: Secrets,
    queues: TaskQueues,
    health_provider: ServiceHealthProvider,
    listener: tokio::net::TcpListener,
) -> Result<(), anyhow::Error> {
    let authn_k8s = if CONFIG.enable_kubernetes_authentication {
        Some(
            limes::kubernetes::KubernetesAuthenticator::try_new_with_default_client(
                Some(K8S_IDP_ID),
                vec![], // All audiences accepted
            )
            .await
            .inspect_err(|e| tracing::info!("Failed to create K8s authorizer: {e}"))
            .inspect(|v| tracing::info!("K8s authorizer created {:?}", v))?,
        )
    } else {
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
            authenticator = authenticator.set_accepted_audiences(aud.clone());
        }
        if let Some(iss) = &CONFIG.openid_additional_issuers {
            authenticator = authenticator.add_additional_issuers(iss.clone());
        }
        if let Some(scope) = &CONFIG.openid_scope {
            authenticator = authenticator.set_scope(scope.clone());
        }
        if let Some(subject_claim) = &CONFIG.openid_subject_claim {
            authenticator = authenticator.with_subject_claim(subject_claim.clone());
        } else {
            // "oid" should be used for entra-id, as the `sub` is different between applications.
            // We prefer oid here by default as no other IdP sets this field (that we know of) and
            // we can provide an out-of-the-box experience for users.
            // Nevertheless we document this behavior in the docs and recommend as part of the
            // `production` checklist to set the claim explicitly.
            authenticator =
                authenticator.with_subject_claims(vec!["oid".to_string(), "sub".to_string()]);
        }
        Some(authenticator)
    } else {
        None
    };

    if authn_k8s.is_none() && authn_oidc.is_none() {
        tracing::warn!("Authentication is disabled. This is not suitable for production!");
    }

    let authn_k8s = authn_k8s.map(AuthenticatorEnum::from);
    let authn_oidc = authn_oidc.map(AuthenticatorEnum::from);
    match (authn_k8s, authn_oidc) {
        (Some(k8s), Some(oidc)) => {
            let authenticator = limes::AuthenticatorChain::<AuthenticatorEnum>::builder()
                .add_authenticator(k8s)
                .add_authenticator(oidc)
                .build();
            serve_inner(
                authorizer,
                Some(authenticator),
                catalog_state,
                secrets_state,
                queues,
                health_provider,
                listener,
            )
            .await
        }
        (Some(auth), None) | (None, Some(auth)) => {
            serve_inner(
                authorizer,
                Some(auth),
                catalog_state,
                secrets_state,
                queues,
                health_provider,
                listener,
            )
            .await
        }
        (None, None) => {
            serve_inner(
                authorizer,
                None::<AuthenticatorEnum>,
                catalog_state,
                secrets_state,
                queues,
                health_provider,
                listener,
            )
            .await
        }
    }
}

/// Helper function to remove redundant code from matching different implementations
async fn serve_inner<A: Authorizer, N: Authenticator + 'static>(
    authorizer: A,
    authenticator: Option<N>,
    catalog_state: CatalogState,
    secrets_state: Secrets,
    queues: TaskQueues,
    health_provider: ServiceHealthProvider,
    listener: tokio::net::TcpListener,
) -> Result<(), anyhow::Error> {
    let (tx, rx) = tokio::sync::mpsc::channel(1000);

    let mut cloud_event_sinks = vec![];

    if let Some(nat_addr) = &CONFIG.nats_address {
        let nats_publisher = build_nats_client(nat_addr).await?;
        cloud_event_sinks
            .push(Arc::new(nats_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
    } else {
        tracing::info!("Running without NATS publisher.");
    };

    if let Some(true) = &CONFIG.log_cloudevents {
        let tracing_publisher = TracingPublisher;
        cloud_event_sinks
            .push(Arc::new(tracing_publisher) as Arc<dyn CloudEventBackend + Sync + Send>);
        tracing::info!("Logging Cloudevents.");
    } else {
        tracing::info!("Running without logging Cloudevents.");
    }

    let x: CloudEventsPublisherBackgroundTask = CloudEventsPublisherBackgroundTask {
        source: rx,
        sinks: cloud_event_sinks,
    };

    let (layer, metrics_future) =
        iceberg_catalog::metrics::get_axum_layer_and_install_recorder(CONFIG.metrics_port)?;

    let (tracker_tx, tracker_rx) = tokio::sync::mpsc::channel(1000);

    let tracker = Tracker::new(
        tracker_rx,
        vec![Arc::new(PostgresStatsSink::new(catalog_state.write_pool()))],
    );

    let tracker_tx = TrackerTx::new(tracker_tx);

    let router = new_full_router::<PostgresCatalog, _, Secrets, _>(RouterArgs {
        authenticator: authenticator.clone(),
        authorizer: authorizer.clone(),
        catalog_state: catalog_state.clone(),
        secrets_state: secrets_state.clone(),
        queues: queues.clone(),
        publisher: CloudEventsPublisher::new(tx.clone()),
        table_change_checkers: ContractVerifiers::new(vec![]),
        service_health_provider: health_provider,
        cors_origins: CONFIG.allow_origin.as_deref(),
        metrics_layer: Some(layer),
        tracker_tx,
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
        .route("/ui/assets/{*file}", get(ui::static_handler))
        .route("/ui/{*file}", get(ui::index_handler));

    let publisher_handle = tokio::task::spawn(async move {
        match x.publish().await {
            Ok(_) => tracing::info!("Exiting publisher task"),
            Err(e) => tracing::error!("Publisher task failed: {e}"),
        };
    });
    let stats_handle = tokio::task::spawn(tracker.run());
    tokio::select!(
        _ = queues.spawn_queues::<PostgresCatalog, _, _>(catalog_state, secrets_state, authorizer) => tracing::error!("Tabular queue task failed"),
        err = service_serve(listener, router) => tracing::error!("Service failed: {err:?}"),
        _ = metrics_future => tracing::error!("Metrics server failed"),
        _ = stats_handle => tracing::error!("Stats task failed"),
    );

    tracing::debug!("Sending shutdown signal to event publisher.");
    tx.send(Message::Shutdown).await?;
    publisher_handle.await?;

    Ok(())
}

async fn build_nats_client(nat_addr: &Url) -> Result<NatsBackend, Error> {
    tracing::info!("Running with nats publisher, connecting to: {nat_addr}");
    let builder = async_nats::ConnectOptions::new();

    let builder = if let Some(file) = &CONFIG.nats_creds_file {
        builder.credentials_file(file).await?
    } else {
        builder
    };

    let builder = if let (Some(user), Some(pw)) = (&CONFIG.nats_user, &CONFIG.nats_password) {
        builder.user_and_password(user.clone(), pw.clone())
    } else {
        builder
    };

    let builder = if let Some(token) = &CONFIG.nats_token {
        builder.token(token.clone())
    } else {
        builder
    };

    let nats_publisher = NatsBackend {
        client: builder.connect(nat_addr.to_string()).await?,
        topic: CONFIG
            .nats_topic
            .clone()
            .ok_or(anyhow::anyhow!("Missing nats topic."))?,
    };
    Ok(nats_publisher)
}
