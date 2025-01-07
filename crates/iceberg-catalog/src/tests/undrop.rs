use crate::api::iceberg::types::Prefix;
use crate::api::iceberg::v1::tables::TablesService;
use crate::api::iceberg::v1::{DataAccess, DropParams, TableParameters};
use crate::api::management::v1::warehouse::{CreateWarehouseResponse, RescheduleSoftDeletionRequest, Service, TabularDeleteProfile, UndropTabularsRequest};
use crate::api::management::v1::ApiServer;
use crate::api::ApiContext;
use crate::catalog::CatalogServer;
use crate::implementations::postgres::{PostgresCatalog, SecretsState};
use crate::service::authz::{AllowAllAuthorizer, Authorizer};
use crate::service::{State, TabularIdentUuid, UserId};
use crate::tests::random_request_metadata;
use crate::WarehouseIdent;
use iceberg_ext::catalog::rest::LoadTableResult;
use sqlx::PgPool;

#[sqlx::test]
async fn test_undrop_makes_table_loadable_again(pool: sqlx::PgPool) {
    let DropSetup {
        ctx,
        warehouse,
        namespace_name,
        table_name,
        table,
    } = setup_drop_test(pool, chrono::Duration::seconds(360)).await;

    purge_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name).await;

    spawn_drop_queues(&ctx);
    let err = load_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name)
        .await
        .unwrap_err();
    assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());

    ApiServer::undrop_tabulars(
        random_request_metadata(),
        UndropTabularsRequest {
            targets: vec![TabularIdentUuid::Table(table.metadata.uuid())],
        },
        ctx.clone(),
    )
    .await
    .unwrap();

    let t2 = load_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name)
        .await
        .unwrap();

    assert_eq!(table.metadata.uuid(), t2.metadata.uuid());
}

#[sqlx::test]
async fn test_dropped_table_can_be_dropped_now(pool: PgPool) {
    figment::Jail::expect_with(|jail| async {
        jail.set_env("LAKEKEEPER__QUEUE_CONFIG__POLL_INTERVAL", "1");
        let DropSetup {
            ctx,
            warehouse,
            namespace_name,
            table_name,
            table,
        } = setup_drop_test(pool, chrono::Duration::seconds(10)).await;
        Ok(())
    });


    purge_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name).await;

    spawn_drop_queues(&ctx);

    let err = load_table(&ctx, warehouse.warehouse_id, &namespace_name, &table_name)
        .await
        .unwrap_err();
    assert_eq!(err.error.code, http::StatusCode::NOT_FOUND.as_u16());
    assert!(tokio::fs::try_exists(&table.metadata.location()).await.unwrap());

    ApiServer::reschedule_soft_deletions(
        random_request_metadata(),
        warehouse.warehouse_id,
        RescheduleSoftDeletionRequest {
            targets: vec![TabularIdentUuid::Table(table.metadata.uuid())],
            reschedule_to: chrono::Utc::now() + chrono::Duration::seconds(1),
        },
        ctx.clone(),
    ).unwrap()
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
}

struct DropSetup {
    ctx: ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
    warehouse: CreateWarehouseResponse,
    namespace_name: String,
    table_name: String,
    table: LoadTableResult,
}

async fn setup_drop_test(pool: PgPool, expiration_seconds: chrono::Duration) -> DropSetup {
    let prof = crate::tests::test_io_profile();
    let authz = AllowAllAuthorizer::default();
    let (ctx, warehouse) = crate::tests::setup(
        pool.clone(),
        prof,
        None,
        authz,
        TabularDeleteProfile::Soft { expiration_seconds },
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

    DropSetup {
        ctx,
        warehouse,
        namespace_name: ns_name.to_string(),
        table_name: tab_name,
        table: tab,
    }
}

async fn load_table<T: Authorizer>(
    ctx: &ApiContext<State<T, PostgresCatalog, SecretsState>>,
    warehouse: WarehouseIdent,
    ns_name: &str,
    tab_name: &str,
) -> crate::api::Result<LoadTableResult> {
    CatalogServer::load_table(
        TableParameters::new(Some(Prefix(warehouse.to_string())), &ns_name, &tab_name),
        DataAccess::none(),
        ctx.clone(),
        random_request_metadata(),
    )
    .await
}

async fn purge_table<T: Authorizer>(
    ctx: &ApiContext<State<T, PostgresCatalog, SecretsState>>,
    warehouse: WarehouseIdent,
    ns_name: &str,
    tab_name: &str,
) {
    CatalogServer::drop_table(
        TableParameters::new(Some(Prefix(warehouse.to_string())), &ns_name, &tab_name),
        DropParams {
            purge_requested: Some(true),
        },
        ctx.clone(),
        random_request_metadata(),
    )
    .await
    .unwrap();
}

fn spawn_drop_queues<T: Authorizer>(ctx: &ApiContext<State<T, PostgresCatalog, SecretsState>>) {
    let ctx = ctx.clone();
    let _ = tokio::task::spawn(async move {
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
