use super::namespace::NamespaceIdentUrl;
use crate::request_metadata::RequestMetadata;
use crate::rest::iceberg::types::Prefix;
use crate::rest::iceberg::v1::tables::TableParameters;
use crate::rest::ApiContext;
use crate::service::catalog::metrics::Service;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Extension, Json, Router};
use http::StatusCode;
use iceberg_ext::TableIdent;

pub fn router<I: Service<S>, S: crate::rest::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new()
        // /{prefix}/namespaces/{namespace}/tables/{table}/metrics
        .route(
            "/:prefix/namespaces/:namespace/tables/:table/metrics",
            post(
                |Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<serde_json::Value>| async {
                    {
                        I::report_metrics(
                            TableParameters {
                                prefix: Some(prefix),
                                table: TableIdent {
                                    namespace: namespace.into(),
                                    name: table,
                                },
                            },
                            request,
                            api_context,
                            metadata,
                        )
                    }
                    .await
                    .map(|()| StatusCode::NO_CONTENT.into_response())
                },
            ),
        )
}
