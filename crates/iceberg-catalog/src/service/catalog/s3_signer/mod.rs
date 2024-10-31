use crate::modules;
use crate::request_metadata::RequestMetadata;
use crate::rest::iceberg::types::Prefix;
use crate::rest::ApiContext;
use async_trait::async_trait;
use iceberg_ext::catalog::rest::{S3SignRequest, S3SignResponse};

pub(crate) mod error;
mod sign;

#[async_trait]
pub trait Service<S: crate::rest::ThreadSafe>
where
    Self: Send + Sync + 'static,
{
    /// Sign an S3 request.
    /// Requests should be send to `/:prefix/namespace/:namespace/table/:table/v1/aws/s3/sign`,
    /// where :namespace and :table can be any string. Typically these strings would be
    /// ids of the namespace and table, respectively - not their names.
    /// For clients to use this route, the server implementation should specify "s3.signer.uri"
    /// accordingly on `load_table` and other methods that require data access.
    ///
    /// If a request is recieved at `/aws/s3/sign`, table and namespace will be `None`.
    async fn sign(
        prefix: Option<Prefix>,
        namespace: Option<String>,
        table: Option<String>,
        request: S3SignRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> modules::Result<S3SignResponse>;
}
