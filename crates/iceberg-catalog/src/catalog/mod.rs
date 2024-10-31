mod commit_tables;
pub(crate) mod compression_codec;
mod config;
pub(crate) mod io;
mod metrics;
pub(crate) mod namespace;
#[cfg(feature = "s3-signer")]
mod s3_signer;
mod tables;
mod views;

use iceberg::spec::{TableMetadata, ViewMetadata};
use iceberg_ext::catalog::rest::IcebergErrorResponse;
pub use namespace::{MAX_NAMESPACE_DEPTH, NAMESPACE_ID_PROPERTY, UNSUPPORTED_NAMESPACE_PROPERTIES};

use crate::api::iceberg::v1::{PageToken, MAX_PAGE_SIZE};
use crate::api::{iceberg::v1::Prefix, ErrorModel, Result};
use crate::service::storage::StorageCredential;
use crate::{
    service::{authz::Authorizer, secrets::SecretStore, Catalog},
    WarehouseIdent,
};
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;

pub trait CommonMetadata {
    fn properties(&self) -> &HashMap<String, String>;
}

impl CommonMetadata for TableMetadata {
    fn properties(&self) -> &HashMap<String, String> {
        TableMetadata::properties(self)
    }
}

impl CommonMetadata for ViewMetadata {
    fn properties(&self) -> &HashMap<String, String> {
        ViewMetadata::properties(self)
    }
}

#[derive(Clone, Debug)]

pub struct CatalogServer<C: Catalog, A: Authorizer + Clone, S: SecretStore> {
    auth_handler: PhantomData<A>,
    catalog_backend: PhantomData<C>,
    secret_store: PhantomData<S>,
}

fn require_warehouse_id(prefix: Option<Prefix>) -> Result<WarehouseIdent> {
    prefix
        .ok_or(
            ErrorModel::builder()
                .code(http::StatusCode::BAD_REQUEST.into())
                .message(
                    "No prefix specified. The warehouse-id must be provided as prefix in the URL."
                        .to_string(),
                )
                .r#type("NoPrefixProvided".to_string())
                .build(),
        )?
        .try_into()
}

pub(crate) async fn maybe_get_secret<S: SecretStore>(
    secret: Option<crate::SecretIdent>,
    state: &S,
) -> Result<Option<StorageCredential>, IcebergErrorResponse> {
    if let Some(secret_id) = &secret {
        Ok(Some(state.get_secret_by_id(secret_id).await?.secret))
    } else {
        Ok(None)
    }
}

pub const DEFAULT_PAGE_SIZE: i64 = 100;

// Helper fn that fetches data using `fetch_fn` and then filters the data using `filter_fn` until
// a full page is fetched. The `fetch_fn` is commonly a closure that fetches data from the catalog,
// `filter_fn` is a closure that filters the data based on some criteria, usually authz.
pub(crate) async fn fetch_until_full_page<
    'b,
    'd: 'b,
    Entity,
    EntityId,
    FetchFun,
    FilteredFuture,
    C: Catalog,
>(
    page_size: Option<i64>,
    page_token: PageToken,
    mut fetch_fn: FetchFun,
    mut filter_fn: impl FnMut((Vec<Entity>, Vec<EntityId>)) -> FilteredFuture,
    t: &'d mut C::Transaction,
) -> Result<(Vec<Entity>, Vec<EntityId>, Option<String>)>
where
    FetchFun: for<'c> FnMut(
        i64,
        Option<String>,
        &'c mut C::Transaction,
    )
        -> BoxFuture<'c, Result<(Vec<Entity>, Vec<EntityId>, Option<String>)>>,
    FilteredFuture: Future<Output = Result<(Vec<Entity>, Vec<EntityId>)>>,
{
    let page_size = page_size
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let usize_page_size = page_size.try_into().map_err(|e| {
        ErrorModel::internal(
            format!(
                "Encountered an impossible error: page size is larger than '{}' after clamping it to '1, {MAX_PAGE_SIZE}'",
                usize::MAX
            ),
            "InternalServerError",
            Some(Box::new(e)),
        )
    })?;
    let page_token = page_token.as_option().map(ToString::to_string);
    let (fetched_t, fetched_t2, mut next_page) = fetch_fn(page_size, page_token, t).await?;
    let (mut fetched_t, mut fetched_t2) = filter_fn((fetched_t, fetched_t2)).await?;

    while fetched_t.len() < usize_page_size {
        let (more_t, more_id, next_p) = fetch_fn(
            (usize_page_size - fetched_t.len())
                .try_into()
                .map_err(|e| {
                    ErrorModel::internal(
                        "Failed to convert usize to i64",
                        "InternalServerError",
                        Some(Box::new(e)),
                    )
                })?,
            next_page.clone(),
            t,
        )
        .await?;
        if more_t.is_empty() {
            break;
        }
        next_page = next_p;
        let (more_t, more_id) = filter_fn((more_t, more_id)).await?;
        fetched_t.extend(more_t);
        fetched_t2.extend(more_id);
    }

    Ok((fetched_t, fetched_t2, next_page))
}

#[cfg(test)]
mod test {
    use crate::api::iceberg::types::Prefix;
    use crate::api::iceberg::v1::namespace::Service;
    use crate::api::management::v1::project::{CreateProjectRequest, Service as _};
    use crate::api::management::v1::warehouse::{
        CreateWarehouseRequest, CreateWarehouseResponse, Service as _, TabularDeleteProfile,
    };
    use crate::api::management::v1::ApiServer;
    use crate::api::ApiContext;
    use crate::catalog::CatalogServer;
    use crate::implementations::postgres::{
        CatalogState, PostgresCatalog, ReadWrite, SecretsState,
    };
    use crate::request_metadata::RequestMetadata;
    use crate::service::authz::Authorizer;
    use crate::service::contract_verification::ContractVerifiers;
    use crate::service::event_publisher::CloudEventsPublisher;
    use crate::service::storage::{
        S3Credential, S3Flavor, S3Profile, StorageCredential, StorageProfile,
    };
    use crate::service::task_queue::TaskQueues;
    use crate::service::{AuthDetails, State};
    use crate::CONFIG;
    use iceberg::NamespaceIdent;
    use iceberg_ext::catalog::rest::{CreateNamespaceRequest, CreateNamespaceResponse};
    use sqlx::PgPool;
    use std::sync::Arc;
    use uuid::Uuid;

    pub(crate) fn minio_profile() -> (StorageProfile, StorageCredential) {
        let key_prefix = Some(format!("test_prefix-{}", Uuid::now_v7()));
        let bucket = std::env::var("LAKEKEEPER_TEST__S3_BUCKET").unwrap();
        let region = std::env::var("LAKEKEEPER_TEST__S3_REGION").unwrap_or("local".into());
        let aws_access_key_id = std::env::var("LAKEKEEPER_TEST__S3_ACCESS_KEY").unwrap();
        let aws_secret_access_key = std::env::var("LAKEKEEPER_TEST__S3_SECRET_KEY").unwrap();
        let endpoint = std::env::var("LAKEKEEPER_TEST__S3_ENDPOINT")
            .unwrap()
            .parse()
            .unwrap();

        let cred: StorageCredential = S3Credential::AccessKey {
            aws_access_key_id,
            aws_secret_access_key,
        }
        .into();

        let mut profile: StorageProfile = S3Profile {
            bucket,
            key_prefix,
            assume_role_arn: None,
            endpoint: Some(endpoint),
            region,
            path_style_access: Some(true),
            sts_role_arn: None,
            flavor: S3Flavor::Minio,
            sts_enabled: true,
        }
        .into();

        profile.normalize().unwrap();
        (profile, cred)
    }

    pub(crate) async fn create_ns<T: Authorizer>(
        api_context: ApiContext<State<T, PostgresCatalog, SecretsState>>,
        prefix: String,
        ns_name: String,
    ) -> CreateNamespaceResponse {
        CatalogServer::create_namespace(
            Some(Prefix(prefix)),
            CreateNamespaceRequest {
                namespace: NamespaceIdent::new(ns_name),
                properties: None,
            },
            api_context.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap()
    }

    pub(crate) async fn setup<T: Authorizer>(
        pool: PgPool,
        storage_profile: StorageProfile,
        storage_credential: Option<StorageCredential>,
        authorizer: T,
    ) -> (
        ApiContext<State<T, PostgresCatalog, SecretsState>>,
        CreateWarehouseResponse,
    ) {
        let api_context = get_api_context(pool, authorizer);
        let _state = api_context.v1_state.catalog.clone();
        let proj = ApiServer::create_project(
            CreateProjectRequest {
                project_name: format!("test-project-{}", Uuid::now_v7()),
                project_id: Some(Uuid::now_v7()),
            },
            api_context.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();
        let warehouse = ApiServer::create_warehouse(
            CreateWarehouseRequest {
                warehouse_name: format!("test-warehouse-{}", Uuid::now_v7()),
                project_id: Some(proj.project_id),
                storage_profile,
                storage_credential,
                delete_profile: TabularDeleteProfile::Hard {},
            },
            api_context.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        (api_context, warehouse)
    }

    pub(crate) fn get_api_context<T: Authorizer>(
        pool: PgPool,
        auth: T,
    ) -> ApiContext<State<T, PostgresCatalog, SecretsState>> {
        let (tx, _) = tokio::sync::mpsc::channel(1000);

        ApiContext {
            v1_state: State {
                authz: auth,
                catalog: CatalogState::from_pools(pool.clone(), pool.clone()),
                secrets: SecretsState::from_pools(pool.clone(), pool.clone()),
                publisher: CloudEventsPublisher::new(tx.clone()),
                contract_verifiers: ContractVerifiers::new(vec![]),
                queues: TaskQueues::new(
                    Arc::new(
                        crate::implementations::postgres::task_queues::TabularExpirationQueue::from_config(ReadWrite::from_pools(pool.clone(), pool.clone()), CONFIG.queue_config.clone()).unwrap(),
                    ),
                    Arc::new(
                        crate::implementations::postgres::task_queues::TabularPurgeQueue::from_config(ReadWrite::from_pools(pool.clone(), pool), CONFIG.queue_config.clone()).unwrap()
                    )
                )
            },
        }
    }

    pub(crate) fn random_request_metadata() -> RequestMetadata {
        RequestMetadata {
            request_id: Uuid::new_v4(),
            auth_details: AuthDetails::Unauthenticated,
        }
    }
}
