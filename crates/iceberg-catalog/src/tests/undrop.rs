use crate::api::iceberg::types::Prefix;
use crate::api::iceberg::v1::tables::TablesService;
use crate::api::iceberg::v1::{DropParams, TableParameters};
use crate::api::management::v1::warehouse::{
    CreateWarehouseResponse, Service, TabularDeleteProfile, UndropTabularsRequest,
};
use crate::api::management::v1::ApiServer;
use crate::api::ApiContext;
use crate::catalog::CatalogServer;
use crate::implementations::postgres::{PostgresCatalog, SecretsState};
use crate::service::authz::AllowAllAuthorizer;
use crate::service::{State, TabularIdentUuid, UserId};
use crate::tests::random_request_metadata;

#[sqlx::test]
async fn test_undrop(pool: sqlx::PgPool) {
    let prof = crate::tests::test_io_profile();
    let authz = AllowAllAuthorizer::default();
    let (ctx, warehouse) = crate::tests::setup(
        pool.clone(),
        prof,
        None,
        authz,
        TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(1),
        },
        Some(UserId::OIDC("test-user-id".to_string())),
    )
    .await;
    let ns_name = "ns1";
    let tab_name = "tab1".to_string();
    let _ = crate::tests::create_ns(
        ctx.clone(),
        warehouse.warehouse_id.to_string(),
        ns_name.to_string(),
    )
    .await;

    let tab = super::create_table(
        ctx.clone(),
        &warehouse.warehouse_id.to_string(),
        &ns_name,
        &tab_name.clone(),
    )
    .await
    .unwrap();

    purge_table(&ctx, warehouse, &ns_name, &tab_name).await;

    spawn_drop_queues(&ctx);

    ApiServer::undrop_tabulars(
        random_request_metadata(),
        UndropTabularsRequest {
            targets: vec![TabularIdentUuid::Table(tab.metadata.uuid())],
        },
        ctx.clone(),
    )
    .unwrap()
}

async fn purge_table(
    ctx: &ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
    warehouse: CreateWarehouseResponse,
    ns_name: &str,
    tab_name: &str,
) {
    CatalogServer::drop_table(
        TableParameters::new(
            Some(Prefix(warehouse.warehouse_id.to_string())),
            &ns_name,
            &tab_name,
        ),
        DropParams {
            purge_requested: Some(true),
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
}

fn spawn_drop_queues(ctx: &ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>) {
    let ctx = ctx.clone();
    let handle = tokio::task::spawn(|| async move {
        ctx.clone()
            .v1_state
            .queues
            .spawn_queues(
                ctx.v1_state.catalog,
                ctx.v1_state.secrets,
                ctx.v1_state.authz,
            )
            .await
            .unwrap()
    });
}
