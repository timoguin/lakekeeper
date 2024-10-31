use crate::request_metadata::RequestMetadata;
use crate::rest::iceberg::v1::{ApiContext, Result, TableParameters};
use async_trait::async_trait;

use crate::modules::{authz::Authorizer, secrets::SecretStore, CatalogBackend, State};

use super::CatalogServer;

#[async_trait::async_trait]
impl<C: CatalogBackend, A: Authorizer + Clone, S: SecretStore>
    crate::rest::iceberg::v1::metrics::Service<State<A, C, S>> for CatalogServer<C, A, S>
{
    async fn report_metrics(
        _: TableParameters,
        _: serde_json::Value,
        _: ApiContext<State<A, C, S>>,
        _: RequestMetadata,
    ) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait Service<S: crate::rest::ThreadSafe>
where
    Self: Send + Sync + 'static,
{
    /// Send a metrics report to this endpoint to be processed by the backend
    async fn report_metrics(
        parameters: TableParameters,
        request: serde_json::Value,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;
}
