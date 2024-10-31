use super::ListTablesQuery;
use crate::request_metadata::RequestMetadata;
use crate::rest::iceberg::types::{DropParams, Prefix};
use crate::rest::iceberg::v1::namespace::{NamespaceIdentUrl, NamespaceParameters};
use crate::rest::{ApiContext, CommitViewRequest, CreateViewRequest, RenameTableRequest};
use crate::service::catalog::views::Service;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{
    extract::{Path, Query},
    routing::get,
    Extension, Json, Router,
};
use http::{HeaderMap, StatusCode};
use iceberg::TableIdent;

#[allow(clippy::too_many_lines)]
pub fn router<I: Service<S>, S: crate::rest::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new()
        // /{prefix}/namespaces/{namespace}/views
        .route(
            "/:prefix/namespaces/:namespace/views",
            get(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 Query(query): Query<ListTablesQuery>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| {
                    {
                        I::list_views(
                            NamespaceParameters {
                                prefix: Some(prefix),
                                namespace: namespace.into(),
                            },
                            query,
                            api_context,
                            metadata,
                        )
                    }
                },
            )
            // Create a view in the given namespace
            .post(
                |Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<CreateViewRequest>| {
                    {
                        I::create_view(
                            NamespaceParameters {
                                prefix: Some(prefix),
                                namespace: namespace.into(),
                            },
                            request,
                            api_context,
                            crate::rest::iceberg::v1::tables::parse_data_access(&headers),
                            metadata,
                        )
                    }
                },
            ),
        )
        // /{prefix}/namespaces/{namespace}/views/{view}
        .route(
            "/:prefix/namespaces/:namespace/views/:view",
            get(
                |Path((prefix, namespace, view)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Extension(metadata): Extension<RequestMetadata>| {
                    {
                        I::load_view(
                            ViewParameters {
                                prefix: Some(prefix),
                                view: TableIdent {
                                    namespace: namespace.into(),
                                    name: view,
                                },
                            },
                            api_context,
                            crate::rest::iceberg::v1::tables::parse_data_access(&headers),
                            metadata,
                        )
                    }
                },
            )
            .post(
                |Path((prefix, namespace, view)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 headers: HeaderMap,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<CommitViewRequest>| {
                    {
                        I::commit_view(
                            ViewParameters {
                                prefix: Some(prefix),
                                view: TableIdent {
                                    namespace: namespace.into(),
                                    name: view,
                                },
                            },
                            request,
                            api_context,
                            crate::rest::iceberg::v1::tables::parse_data_access(&headers),
                            metadata,
                        )
                    }
                },
            )
            .delete(
                |Path((prefix, namespace, view)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| async {
                    {
                        I::drop_view(
                            ViewParameters {
                                prefix: Some(prefix),
                                view: TableIdent {
                                    namespace: namespace.into(),
                                    name: view,
                                },
                            },
                            DropParams {
                                purge_requested: None,
                            },
                            api_context,
                            metadata,
                        )
                        .await
                        .map(|()| StatusCode::NO_CONTENT.into_response())
                    }
                },
            )
            .head(
                |Path((prefix, namespace, view)): Path<(Prefix, NamespaceIdentUrl, String)>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>| async {
                    {
                        I::view_exists(
                            ViewParameters {
                                prefix: Some(prefix),
                                view: TableIdent {
                                    namespace: namespace.into(),
                                    name: view,
                                },
                            },
                            api_context,
                            metadata,
                        )
                        .await
                        .map(|()| StatusCode::NO_CONTENT.into_response())
                    }
                },
            ),
        )
        // /{prefix}/views/rename
        .route(
            "/:prefix/views/rename",
            post(
                |Path(prefix): Path<Prefix>,
                 State(api_context): State<ApiContext<S>>,
                 Extension(metadata): Extension<RequestMetadata>,
                 Json(request): Json<RenameTableRequest>| async {
                    {
                        I::rename_view(Some(prefix), request, api_context, metadata)
                            .await
                            .map(|()| StatusCode::NO_CONTENT.into_response())
                    }
                },
            ),
        )
}

// Deliberately not ser / de so that it can't be used in the router directly
#[derive(Debug, Clone, PartialEq)]
pub struct ViewParameters {
    /// The prefix of the namespace
    pub prefix: Option<Prefix>,
    /// The table to load metadata for
    pub view: TableIdent,
}
