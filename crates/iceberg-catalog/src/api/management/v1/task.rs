use super::default_page_size;
use crate::api::iceberg::v1::{PageToken, PaginationQuery};
use crate::api::ApiContext;
use crate::request_metadata::RequestMetadata;
use crate::service::authz::Authorizer;
use crate::service::task_queue::{Task, TaskId, TaskInstance};
use crate::service::{Catalog, Result, SecretStore, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Deserialize, Serialize, IntoParams)]
pub struct ListTasksQuery {
    pub(crate) page_token: Option<String>,
    #[serde(default = "default_page_size")]
    pub(crate) page_size: i64,
}

impl ListTasksQuery {
    #[must_use]
    pub fn pagination_query(&self) -> PaginationQuery {
        PaginationQuery {
            page_token: self
                .page_token
                .clone()
                .map_or(PageToken::Empty, PageToken::Present),
            page_size: Some(self.page_size),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ListTasksResponse {
    pub(crate) tasks: Vec<Task>,
    pub(crate) continuation_token: Option<String>,
}

impl IntoResponse for ListTasksResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema, IntoParams)]
pub struct ListTaskInstancesQuery {
    pub task_id: Option<TaskId>,
    pub(crate) page_token: Option<String>,
    #[serde(default = "default_page_size")]
    pub(crate) page_size: i64,
}

impl ListTaskInstancesQuery {
    #[must_use]
    pub fn pagination_query(&self) -> PaginationQuery {
        PaginationQuery {
            page_token: self
                .page_token
                .clone()
                .map_or(PageToken::Empty, PageToken::Present),
            page_size: Some(self.page_size),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ListTaskInstancesResponse {
    pub(crate) tasks: Vec<TaskInstance>,
    pub(crate) continuation_token: Option<String>,
}

impl IntoResponse for ListTaskInstancesResponse {
    fn into_response(self) -> axum::response::Response {
        (http::StatusCode::OK, Json(self)).into_response()
    }
}

#[async_trait::async_trait]
impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> Service<C, A, S>
    for ApiContext<State<A, C, S>>
{
    async fn list_tasks(
        &self,
        _request_metadata: RequestMetadata,
        list_tasks_query: ListTasksQuery,
    ) -> Result<ListTasksResponse> {
        // TODO: authz + filtered pagination test
        C::list_tasks(
            list_tasks_query.pagination_query(),
            self.v1_state.catalog.clone(),
        )
        .await
    }

    async fn list_task_instances(
        &self,
        _request_metadata: RequestMetadata,
        list_task_instances_query: ListTaskInstancesQuery,
    ) -> Result<ListTaskInstancesResponse> {
        // TODO: authz + filtered pagination test
        C::list_task_instances(
            list_task_instances_query.task_id,
            list_task_instances_query.pagination_query(),
            self.v1_state.catalog.clone(),
        )
        .await
    }
}

#[async_trait::async_trait]
pub(crate) trait Service<C: Catalog, A: Authorizer, S: SecretStore> {
    async fn list_tasks(
        &self,
        request_metadata: RequestMetadata,
        list_tasks_query: ListTasksQuery,
    ) -> Result<ListTasksResponse>;

    async fn list_task_instances(
        &self,
        request_metadata: RequestMetadata,
        list_task_instances_query: ListTaskInstancesQuery,
    ) -> Result<ListTaskInstancesResponse>;
}
