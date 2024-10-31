pub use crate::modules::object_stores::{
    AzCredential, AzdlsProfile, GcsCredential, GcsProfile, GcsServiceKey, S3Credential, S3Profile,
    StorageCredential, StorageProfile,
};

pub use crate::modules::WarehouseStatus;
use crate::{ProjectIdent, CONFIG};
use serde::Deserialize;
use utoipa::ToSchema;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct CreateWarehouseRequest {
    /// Name of the warehouse to create. Must be unique
    /// within a project and may not contain "/"
    pub warehouse_name: String,
    /// Project ID in which to create the warehouse.
    /// If no default project is set for this server, this field is required.
    pub project_id: Option<uuid::Uuid>,
    /// Storage profile to use for the warehouse.
    pub storage_profile: StorageProfile,
    /// Optional storage credential to use for the warehouse.
    pub storage_credential: Option<StorageCredential>,
    /// Profile to determine behavior upon dropping of tabulars, defaults to soft-deletion with
    /// 7 days expiration.
    #[serde(default)]
    pub delete_profile: TabularDeleteProfile,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum TabularDeleteProfile {
    Hard {},

    Soft {
        #[serde(
            deserialize_with = "crate::config::seconds_to_duration",
            serialize_with = "crate::config::duration_to_seconds"
        )]
        expiration_seconds: chrono::Duration,
    },
}

impl TabularDeleteProfile {
    pub(crate) fn expiration_seconds(&self) -> Option<chrono::Duration> {
        match self {
            Self::Soft { expiration_seconds } => Some(*expiration_seconds),
            Self::Hard {} => None,
        }
    }
}

impl Default for TabularDeleteProfile {
    fn default() -> Self {
        Self::Soft {
            expiration_seconds: CONFIG.default_tabular_expiration_delay_seconds,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct CreateWarehouseResponse {
    /// ID of the created warehouse.
    pub warehouse_id: uuid::Uuid,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateWarehouseStorageRequest {
    /// Storage profile to use for the warehouse.
    /// The new profile must point to the same location as the existing profile
    /// to avoid data loss. For S3 this means that you may not change the
    /// bucket, key prefix, or region.
    pub storage_profile: StorageProfile,
    /// Optional storage credential to use for the warehouse.
    /// The existing credential is not re-used. If no credential is
    /// provided, we assume that this storage does not require credentials.
    #[serde(default)]
    pub storage_credential: Option<StorageCredential>,
}

#[derive(Debug, Deserialize, ToSchema, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ListWarehousesRequest {
    /// Optional filter to return only warehouses
    /// with the specified status.
    /// If not provided, only active warehouses are returned.
    #[serde(default)]
    pub warehouse_status: Option<Vec<WarehouseStatus>>,
    /// The project ID to list warehouses for.
    /// Setting a warehouse is required.
    #[serde(default)]
    #[param(value_type=Option::<uuid::Uuid>)]
    pub project_id: Option<ProjectIdent>,
}

#[derive(Debug, Clone, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct RenameWarehouseRequest {
    /// New name for the warehouse.
    pub new_name: String,
}

#[derive(Debug, Clone, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateWarehouseDeleteProfileRequest {
    pub delete_profile: TabularDeleteProfile,
}

#[derive(Debug, Clone, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct RenameProjectRequest {
    /// New name for the project.
    pub new_name: String,
}

#[derive(Debug, Clone, serde::Serialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct GetWarehouseResponse {
    /// ID of the warehouse.
    pub id: uuid::Uuid,
    /// Name of the warehouse.
    pub name: String,
    /// Project ID in which the warehouse is created.
    pub project_id: uuid::Uuid,
    /// Storage profile used for the warehouse.
    pub storage_profile: StorageProfile,
    /// Whether the warehouse is active.
    pub status: WarehouseStatus,
}

#[derive(Debug, Clone, serde::Serialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct ListWarehousesResponse {
    /// List of warehouses in the project.
    pub warehouses: Vec<GetWarehouseResponse>,
}

#[derive(Debug, Clone, serde::Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateWarehouseCredentialRequest {
    /// New storage credential to use for the warehouse.
    /// If not specified, the existing credential is removed.
    pub new_storage_credential: Option<StorageCredential>,
}

impl axum::response::IntoResponse for CreateWarehouseResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        (http::StatusCode::CREATED, axum::Json(self)).into_response()
    }
}

impl axum::response::IntoResponse for ListWarehousesResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        axum::Json(self).into_response()
    }
}

impl axum::response::IntoResponse for GetWarehouseResponse {
    fn into_response(self) -> axum::http::Response<axum::body::Body> {
        axum::Json(self).into_response()
    }
}

impl From<crate::modules::GetWarehouseResponse> for GetWarehouseResponse {
    fn from(warehouse: crate::modules::GetWarehouseResponse) -> Self {
        Self {
            id: warehouse.id.to_uuid(),
            name: warehouse.name,
            project_id: *warehouse.project_id,
            storage_profile: warehouse.storage_profile,
            status: warehouse.status,
        }
    }
}
