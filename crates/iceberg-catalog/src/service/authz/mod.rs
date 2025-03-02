use std::collections::HashSet;

use axum::Router;
use strum::EnumIter;

use super::{
    health::HealthExt, Actor, Catalog, NamespaceIdentUuid, ProjectId, RoleId, SecretStore, State,
    TableIdentUuid, TabularDetails, ViewIdentUuid, WarehouseIdent,
};
use crate::{api::iceberg::v1::Result, request_metadata::RequestMetadata};

pub mod implementations;

use iceberg_ext::catalog::rest::ErrorModel;
pub use implementations::allow_all::AllowAllAuthorizer;

use crate::{api::ApiContext, service::authn::UserId};

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogUserAction {
    /// Can get all details of the user given its id
    CanRead,
    /// Can update the user.
    CanUpdate,
    /// Can delete this user
    CanDelete,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogServerAction {
    /// Can create items inside the server (can create Warehouses).
    CanCreateProject,
    /// Can update all users on this server.
    CanUpdateUsers,
    /// Can delete all users on this server.
    CanDeleteUsers,
    /// Can List all users on this server.
    CanListUsers,
    /// Can provision user
    CanProvisionUsers,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogProjectAction {
    CanCreateWarehouse,
    CanDelete,
    CanRename,
    CanGetMetadata,
    CanListWarehouses,
    CanIncludeInList,
    CanCreateRole,
    CanListRoles,
    CanSearchRoles,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogRoleAction {
    CanDelete,
    CanUpdate,
    CanRead,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogWarehouseAction {
    CanCreateNamespace,
    CanDelete,
    CanUpdateStorage,
    CanUpdateStorageCredential,
    CanGetMetadata,
    CanGetConfig,
    CanListNamespaces,
    CanUse,
    CanIncludeInList,
    CanDeactivate,
    CanActivate,
    CanRename,
    CanListDeletedTabulars,
    CanModifySoftDeletion,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogNamespaceAction {
    CanCreateTable,
    CanCreateView,
    CanCreateNamespace,
    CanDelete,
    CanUpdateProperties,
    CanGetMetadata,
    CanListTables,
    CanListViews,
    CanListNamespaces,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogTableAction {
    CanDrop,
    CanWriteData,
    CanReadData,
    CanGetMetadata,
    CanCommit,
    CanRename,
    CanIncludeInList,
    CanUndrop,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum CatalogViewAction {
    CanDrop,
    CanGetMetadata,
    CanCommit,
    CanIncludeInList,
    CanRename,
    CanUndrop,
}

pub trait TableUuid {
    fn table_uuid(&self) -> TableIdentUuid;
}

impl TableUuid for TableIdentUuid {
    fn table_uuid(&self) -> TableIdentUuid {
        *self
    }
}

impl TableUuid for TabularDetails {
    fn table_uuid(&self) -> TableIdentUuid {
        self.ident
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ListProjectsResponse {
    /// List of projects that the user is allowed to see.
    Projects(HashSet<ProjectId>),
    /// The user is allowed to see all projects.
    All,
}

#[derive(Debug, Clone)]
pub enum NamespaceParent {
    Warehouse(WarehouseIdent),
    Namespace(NamespaceIdentUuid),
}

#[async_trait::async_trait]
/// Interface to provide AuthZ functions to the catalog.
/// The provided `Actor` argument of all methods except `check_actor`
/// are assumed to be valid. Please ensure to call `check_actor` before, preferably
/// during Authentication.
/// `check_actor` ensures that the Actor itself is valid, especially that the principal
/// is allowed to assume the role.
pub trait Authorizer
where
    Self: Send + Sync + 'static + HealthExt + Clone,
{
    /// API Doc
    fn api_doc() -> utoipa::openapi::OpenApi;

    /// Router for the API
    fn new_router<C: Catalog, S: SecretStore>(&self) -> Router<ApiContext<State<Self, C, S>>>;

    /// Check if the requested actor combination is allowed - especially if the user
    /// is allowed to assume the specified role.
    async fn check_actor(&self, actor: &Actor) -> Result<()>;

    /// Check if this server can be bootstrapped.
    async fn can_bootstrap(&self, metadata: &RequestMetadata) -> Result<()>;

    /// Perform bootstrapping, including granting the provided user the highest level of access.
    async fn bootstrap(&self, metadata: &RequestMetadata, is_operator: bool) -> Result<()>;

    /// Return Err only for internal errors.
    async fn list_projects(&self, metadata: &RequestMetadata) -> Result<ListProjectsResponse>;

    /// Search users
    async fn can_search_users(&self, metadata: &RequestMetadata) -> Result<bool>;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_user_action(
        &self,
        metadata: &RequestMetadata,
        user_id: &UserId,
        action: &CatalogUserAction,
    ) -> Result<bool>;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_role_action(
        &self,
        metadata: &RequestMetadata,
        role_id: RoleId,
        action: &CatalogRoleAction,
    ) -> Result<bool>;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_server_action(
        &self,
        metadata: &RequestMetadata,
        action: &CatalogServerAction,
    ) -> Result<bool>;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: ProjectId,
        action: &CatalogProjectAction,
    ) -> Result<bool>;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        action: &CatalogWarehouseAction,
    ) -> Result<bool>;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_namespace_action<A>(
        &self,
        metadata: &RequestMetadata,
        namespace_id: NamespaceIdentUuid,
        action: A,
    ) -> Result<bool>
    where
        A: From<CatalogNamespaceAction> + std::fmt::Display + Send + 'static;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_table_action<A>(
        &self,
        metadata: &RequestMetadata,
        table_id: TableIdentUuid,
        action: A,
    ) -> Result<bool>
    where
        A: From<CatalogTableAction> + std::fmt::Display + Send + 'static;

    /// Return Ok(true) if the action is allowed, otherwise return Ok(false).
    /// Return Err for internal errors.
    async fn is_allowed_view_action<A>(
        &self,
        metadata: &RequestMetadata,
        view_id: ViewIdentUuid,
        action: A,
    ) -> Result<bool>
    where
        A: From<CatalogViewAction> + std::fmt::Display + Send + 'static;

    /// Hook that is called when a user is deleted.
    async fn delete_user(&self, metadata: &RequestMetadata, user_id: UserId) -> Result<()>;

    /// Hook that is called when a new project is created.
    /// This is used to set up the initial permissions for the project.
    async fn create_role(
        &self,
        metadata: &RequestMetadata,
        role_id: RoleId,
        parent_project_id: ProjectId,
    ) -> Result<()>;

    /// Hook that is called when a role is deleted.
    /// This is used to clean up permissions for the role.
    async fn delete_role(&self, metadata: &RequestMetadata, role_id: RoleId) -> Result<()>;

    /// Hook that is called when a new project is created.
    /// This is used to set up the initial permissions for the project.
    async fn create_project(&self, metadata: &RequestMetadata, project_id: ProjectId)
        -> Result<()>;

    /// Hook that is called when a project is deleted.
    /// This is used to clean up permissions for the project.
    async fn delete_project(&self, metadata: &RequestMetadata, project_id: ProjectId)
        -> Result<()>;

    /// Hook that is called when a new warehouse is created.
    /// This is used to set up the initial permissions for the warehouse.
    async fn create_warehouse(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        parent_project_id: ProjectId,
    ) -> Result<()>;

    /// Hook that is called when a warehouse is deleted.
    /// This is used to clean up permissions for the warehouse.
    async fn delete_warehouse(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
    ) -> Result<()>;

    /// Hook that is called when a new namespace is created.
    /// This is used to set up the initial permissions for the namespace.
    async fn create_namespace(
        &self,
        metadata: &RequestMetadata,
        namespace_id: NamespaceIdentUuid,
        parent: NamespaceParent,
    ) -> Result<()>;

    /// Hook that is called when a namespace is deleted.
    /// This is used to clean up permissions for the namespace.
    async fn delete_namespace(
        &self,
        metadata: &RequestMetadata,
        namespace_id: NamespaceIdentUuid,
    ) -> Result<()>;

    /// Hook that is called when a new table is created.
    /// This is used to set up the initial permissions for the table.
    async fn create_table(
        &self,
        metadata: &RequestMetadata,
        table_id: TableIdentUuid,
        parent: NamespaceIdentUuid,
    ) -> Result<()>;

    /// Hook that is called when a table is deleted.
    /// This is used to clean up permissions for the table.
    async fn delete_table(&self, table_id: TableIdentUuid) -> Result<()>;

    /// Hook that is called when a new view is created.
    /// This is used to set up the initial permissions for the view.
    async fn create_view(
        &self,
        metadata: &RequestMetadata,
        view_id: ViewIdentUuid,
        parent: NamespaceIdentUuid,
    ) -> Result<()>;

    /// Hook that is called when a view is deleted.
    /// This is used to clean up permissions for the view.
    async fn delete_view(&self, view_id: ViewIdentUuid) -> Result<()>;

    async fn require_search_users(&self, metadata: &RequestMetadata) -> Result<()> {
        if self.can_search_users(metadata).await? {
            Ok(())
        } else {
            Err(ErrorModel::forbidden(
                "Forbidden action search_users",
                "SearchUsersForbidden",
                None,
            )
            .into())
        }
    }

    async fn require_user_action(
        &self,
        metadata: &RequestMetadata,
        user_id: &UserId,
        action: &CatalogUserAction,
    ) -> Result<()> {
        if self
            .is_allowed_user_action(metadata, user_id, action)
            .await?
        {
            Ok(())
        } else {
            Err(ErrorModel::forbidden(
                format!("Forbidden action {action} on user {user_id}"),
                "UserActionForbidden",
                None,
            )
            .into())
        }
    }

    async fn require_role_action(
        &self,
        metadata: &RequestMetadata,
        role_id: RoleId,
        action: &CatalogRoleAction,
    ) -> Result<()> {
        if self
            .is_allowed_role_action(metadata, role_id, action)
            .await?
        {
            Ok(())
        } else {
            Err(ErrorModel::forbidden(
                format!("Forbidden action {action} on role {role_id}"),
                "RoleActionForbidden",
                None,
            )
            .into())
        }
    }

    async fn require_server_action(
        &self,
        metadata: &RequestMetadata,
        action: &CatalogServerAction,
    ) -> Result<()> {
        if self.is_allowed_server_action(metadata, action).await? {
            Ok(())
        } else {
            let actor = metadata.actor();
            Err(ErrorModel::forbidden(
                format!("Forbidden action {action} on server for {actor}"),
                "ServerActionForbidden",
                None,
            )
            .into())
        }
    }

    async fn require_project_action(
        &self,
        metadata: &RequestMetadata,
        project_id: ProjectId,
        action: &CatalogProjectAction,
    ) -> Result<()> {
        if self
            .is_allowed_project_action(metadata, project_id, action)
            .await?
        {
            Ok(())
        } else {
            let actor = metadata.actor();
            Err(ErrorModel::forbidden(
                format!("Forbidden action {action} on project {project_id} for {actor}"),
                "ProjectActionForbidden",
                None,
            )
            .into())
        }
    }

    async fn require_warehouse_action(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseIdent,
        action: &CatalogWarehouseAction,
    ) -> Result<()> {
        if self
            .is_allowed_warehouse_action(metadata, warehouse_id, action)
            .await?
        {
            Ok(())
        } else {
            let actor = metadata.actor();
            Err(ErrorModel::forbidden(
                format!("Forbidden action {action} on warehouse {warehouse_id} for {actor}"),
                "WarehouseActionForbidden",
                None,
            )
            .into())
        }
    }

    async fn require_namespace_action(
        &self,
        metadata: &RequestMetadata,
        // Outer error: Internal error that failed to fetch the namespace.
        // Ok(None): Namespace does not exist.
        // Ok(Some(namespace_id)): Namespace exists.
        namespace_id: Result<Option<NamespaceIdentUuid>>,
        action: impl From<CatalogNamespaceAction> + std::fmt::Display + Send + 'static,
    ) -> Result<NamespaceIdentUuid> {
        // It is important to throw the same error if the namespace does not exist (None) or if the action is not allowed,
        // to avoid leaking information about the existence of the namespace.
        let actor = metadata.actor();
        let msg = format!("Namespace action {action} forbidden for {actor}");
        let typ = "NamespaceActionForbidden";

        match namespace_id {
            Ok(None) => Err(ErrorModel::forbidden(msg, typ, None).into()),
            Ok(Some(namespace_id)) => {
                if self
                    .is_allowed_namespace_action(metadata, namespace_id, action)
                    .await?
                {
                    Ok(namespace_id)
                } else {
                    Err(ErrorModel::forbidden(msg, typ, None).into())
                }
            }
            Err(e) => Err(ErrorModel::internal(msg, typ, e.error.source)
                .append_detail(format!("Original Type: {}", e.error.r#type))
                .append_detail(e.error.message)
                .append_details(e.error.stack)
                .into()),
        }
    }

    async fn require_table_action<T: TableUuid + Send>(
        &self,
        metadata: &RequestMetadata,
        table_id: Result<Option<T>>,
        action: impl From<CatalogTableAction> + std::fmt::Display + Send + 'static,
    ) -> Result<T> {
        let actor = metadata.actor();
        let msg = format!("Table action {action} forbidden for {actor}");
        let typ = "TableActionForbidden";

        match table_id {
            Ok(None) => Err(ErrorModel::forbidden(msg, typ, None).into()),
            Ok(Some(table_id)) => {
                if self
                    .is_allowed_table_action(metadata, table_id.table_uuid(), action)
                    .await?
                {
                    Ok(table_id)
                } else {
                    Err(ErrorModel::forbidden(msg, typ, None).into())
                }
            }
            Err(e) => Err(ErrorModel::internal(msg, typ, e.error.source)
                .append_detail(format!("Original Type: {}", e.error.r#type))
                .append_detail(e.error.message)
                .append_details(e.error.stack)
                .into()),
        }
    }

    async fn require_view_action(
        &self,
        metadata: &RequestMetadata,
        view_id: Result<Option<ViewIdentUuid>>,
        action: impl From<CatalogViewAction> + std::fmt::Display + Send + 'static,
    ) -> Result<ViewIdentUuid> {
        let actor = metadata.actor();
        let msg = format!("View action {action} forbidden for {actor}");
        let typ = "ViewActionForbidden";

        match view_id {
            Ok(None) => Err(ErrorModel::forbidden(msg, typ, None).into()),
            Ok(Some(view_id)) => {
                if self
                    .is_allowed_view_action(metadata, view_id, action)
                    .await?
                {
                    Ok(view_id)
                } else {
                    Err(ErrorModel::forbidden(msg, typ, None).into())
                }
            }
            Err(e) => Err(ErrorModel::internal(msg, typ, e.error.source)
                .append_detail(format!("Original Type: {}", e.error.r#type))
                .append_detail(e.error.message)
                .append_details(e.error.stack)
                .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::service::health::Health;

    use super::*;

    #[test]
    fn test_namespace_action() {
        assert_eq!(
            CatalogNamespaceAction::CanCreateTable.to_string(),
            "can_create_table"
        );
    }

    mockall::mock! {
        pub AuthorizerMock {

        }
        impl Clone for AuthorizerMock {
            fn clone(&self) -> Self;
        }
        #[async_trait::async_trait]
        impl HealthExt for AuthorizerMock {
            async fn health(&self) -> Vec<Health>;
            async fn update_health(&self);
        }
        #[async_trait::async_trait]
        impl Authorizer for AuthorizerMock {
            fn api_doc() -> utoipa::openapi::OpenApi;
            fn new_router<C: Catalog, S: SecretStore>(&self) -> Router<ApiContext<State<Self, C, S>>>;

            async fn check_actor(&self, _actor: &Actor) -> Result<()> ;

            async fn can_bootstrap(&self, _metadata: &RequestMetadata) -> Result<()>;

            async fn bootstrap(&self, _metadata: &RequestMetadata, _is_operator: bool) -> Result<()>;

            async fn list_projects(&self, _metadata: &RequestMetadata) -> Result<ListProjectsResponse>;

            async fn can_search_users(&self, _metadata: &RequestMetadata) -> Result<bool>;

            async fn is_allowed_user_action(
                &self,
                _metadata: &RequestMetadata,
                _user_id: &UserId,
                _action: &CatalogUserAction,
            ) -> Result<bool>;

            async fn is_allowed_role_action(
                &self,
                _metadata: &RequestMetadata,
                _role_id: RoleId,
                _action: &CatalogRoleAction,
            ) -> Result<bool>;

            async fn is_allowed_server_action(
                &self,
                _metadata: &RequestMetadata,
                _action: &CatalogServerAction,
            ) -> Result<bool>;

            async fn is_allowed_project_action(
                &self,
                _metadata: &RequestMetadata,
                _project_id: ProjectId,
                _action: &CatalogProjectAction,
            ) -> Result<bool>;

            async fn is_allowed_warehouse_action(
                &self,
                _metadata: &RequestMetadata,
                _warehouse_id: WarehouseIdent,
                _action: &CatalogWarehouseAction,
            ) -> Result<bool>;
            async fn is_allowed_namespace_action<A>(
                &self,
                metadata: &RequestMetadata,
                namespace_id: NamespaceIdentUuid,
                action: A,
            ) -> Result<bool>
            where
                A: From<CatalogNamespaceAction> + std::fmt::Display + Send  + 'static;

            async fn is_allowed_table_action<A>(
                &self,
                metadata: &RequestMetadata,
                table_id: TableIdentUuid,
                action: A,
            ) -> Result<bool>
            where
                A: From<CatalogTableAction> + std::fmt::Display + Send+ 'static;

            async fn is_allowed_view_action<A>(
                &self,
                metadata: &RequestMetadata,
                view_id: ViewIdentUuid,
                action: A,
            ) -> Result<bool>
            where
                A: From<CatalogViewAction> + std::fmt::Display + Send+ 'static;

            async fn delete_user(&self, _metadata: &RequestMetadata, _user_id: UserId) -> Result<()>;

            async fn create_role(
                &self,
                _metadata: &RequestMetadata,
                _role_id: RoleId,
                _parent_project_id: ProjectId,
            ) -> Result<()>;

            async fn delete_role(&self, _metadata: &RequestMetadata, _role_id: RoleId) -> Result<()>;

            async fn create_project(
                &self,
                _metadata: &RequestMetadata,
                _project_id: ProjectId,
            ) -> Result<()>;

            async fn delete_project(
                &self,
                _metadata: &RequestMetadata,
                _project_id: ProjectId,
            ) -> Result<()>;

            async fn create_warehouse(
                &self,
                _metadata: &RequestMetadata,
                _warehouse_id: WarehouseIdent,
                _parent_project_id: ProjectId,
            ) -> Result<()>;

            async fn delete_warehouse(
                &self,
                _metadata: &RequestMetadata,
                _warehouse_id: WarehouseIdent,
            ) -> Result<()>;

            async fn create_namespace(
                &self,
                _metadata: &RequestMetadata,
                _namespace_id: NamespaceIdentUuid,
                _parent: NamespaceParent,
            ) -> Result<()>;

            async fn delete_namespace(
                &self,
                _metadata: &RequestMetadata,
                _namespace_id: NamespaceIdentUuid,
            ) -> Result<()>;

            async fn create_table(
                &self,
                _metadata: &RequestMetadata,
                _table_id: TableIdentUuid,
                _parent: NamespaceIdentUuid,
            ) -> Result<()>;

            async fn delete_table(&self, _table_id: TableIdentUuid) -> Result<()>;

            async fn create_view(
                &self,
                _metadata: &RequestMetadata,
                _view_id: ViewIdentUuid,
                _parent: NamespaceIdentUuid,
            ) -> Result<()>;

            async fn delete_view(&self, _view_id: ViewIdentUuid) -> Result<()>;
        }
    }

    // pub(crate) struct HidingAuthorizer {
    //     pub(crate) hidden: Arc<RwLock<HashSet<String>>>,
    // }

    // /// A mock for the `OpenFGA` client that allows to hide objects.
    // /// This is useful to test the behavior of the authorizer when objects are hidden.
    // ///
    // /// Create via `ObjectHidingMock::new()`, use `ObjectHidingMock::to_authorizer` to create an authorizer.
    // /// Hide objects via `ObjectHidingMock::hide`. Objects that have been hidden will return `allowed: false`
    // /// for any check request.
    // pub(crate) struct ObjectHidingMock {
    //     pub hidden: Arc<RwLock<HashSet<String>>>,
    //     pub mock: Arc<MockClient>,
    // }

    // impl ObjectHidingMock {
    //     pub(crate) fn new() -> Self {
    //         let hidden: Arc<RwLock<HashSet<String>>> = Arc::default();
    //         let hidden_clone = hidden.clone();
    //         let mut mock = MockClient::default();
    //         mock.expect_check().returning(move |r| {
    //             let hidden = hidden_clone.clone();
    //             let hidden = hidden.read().unwrap();

    //             if hidden.contains(&r.tuple_key.unwrap().object) {
    //                 return Ok(openfga_rs::tonic::Response::new(CheckResponse {
    //                     allowed: false,
    //                     resolution: String::new(),
    //                 }));
    //             }

    //             Ok(openfga_rs::tonic::Response::new(CheckResponse {
    //                 allowed: true,
    //                 resolution: String::new(),
    //             }))
    //         });
    //         mock.expect_read().returning(|_| {
    //             Ok(openfga_rs::tonic::Response::new(ReadResponse {
    //                 tuples: vec![],
    //                 continuation_token: String::new(),
    //             }))
    //         });
    //         mock.expect_write()
    //             .returning(|_| Ok(openfga_rs::tonic::Response::new(WriteResponse {})));

    //         Self {
    //             hidden,
    //             mock: Arc::new(mock),
    //         }
    //     }

    //     #[cfg(test)]
    //     pub(crate) fn hide(&self, object: &str) {
    //         self.hidden.write().unwrap().insert(object.to_string());
    //     }

    //     #[cfg(test)]
    //     pub(crate) fn to_authorizer(&self) -> OpenFGAAuthorizer {
    //         OpenFGAAuthorizer {
    //             client: self.mock.clone(),
    //             store_id: "test_store".to_string(),
    //             authorization_model_id: "test_model".to_string(),
    //             health: Arc::default(),
    //         }
    //     }
    // }
}
