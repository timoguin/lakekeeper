use super::{ApiContext, Prefix};
use crate::request_metadata::RequestMetadata;
use crate::service::catalog::s3_signer::Service;
use axum::extract::State;
use axum::routing::post;
use axum::{extract::Path, Extension, Json, Router};
use iceberg_ext::catalog::rest::S3SignRequest;

pub fn router<I: Service<S>, S: crate::rest::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new()
        .route(
            "/aws/s3/sign",
            post(
                |State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<S3SignRequest>| {
                    {
                        I::sign(None, None, None, request, api_context, metadata)
                    }
                },
            ),
        )
        .route(
            "/:prefix/v1/aws/s3/sign",
            post(
                |Path(prefix): Path<Prefix>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<S3SignRequest>| {
                    {
                        I::sign(Some(prefix), None, None, request, api_context, metadata)
                    }
                },
            ),
        )
}
