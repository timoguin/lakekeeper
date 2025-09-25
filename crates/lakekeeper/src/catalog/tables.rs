use std::{
    collections::{HashMap, HashSet},
    str::FromStr as _,
    sync::Arc,
};

use futures::FutureExt;
use fxhash::FxHashSet;
use http::StatusCode;
use iceberg::{
    spec::{
        FormatVersion, MetadataLog, SchemaId, SortOrder, TableMetadata, TableMetadataBuildResult,
        TableMetadataBuilder, UnboundPartitionSpec, PROPERTY_FORMAT_VERSION,
        PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX,
    },
    ErrorKind, NamespaceIdent, TableUpdate,
};
use iceberg_ext::{
    catalog::rest::{IcebergErrorResponse, LoadCredentialsResponse, StorageCredential},
    configs::{namespace::NamespaceProperties, ParseFromStr},
};
use itertools::Itertools;
use lakekeeper_io::{InvalidLocationError, Location};
use serde::Serialize;
use uuid::Uuid;

use super::{
    commit_tables::apply_commit,
    io::{delete_file, read_metadata_file, write_file},
    maybe_get_secret,
    namespace::{authorized_namespace_ident_to_id, validate_namespace_ident},
    require_warehouse_id, CatalogServer,
};
use crate::{
    api::{
        iceberg::{
            types::DropParams,
            v1::{
                tables::DataAccessMode, ApiContext, CommitTableRequest, CommitTableResponse,
                CommitTransactionRequest, CreateTableRequest, DataAccess, ErrorModel,
                ListTablesQuery, ListTablesResponse, LoadTableResult, NamespaceParameters, Prefix,
                RegisterTableRequest, RenameTableRequest, Result, TableIdent, TableParameters,
            },
        },
        management::v1::{warehouse::TabularDeleteProfile, DeleteKind, TabularType},
        set_not_found_status_code,
    },
    catalog::{self, compression_codec::CompressionCodec, tabular::list_entities},
    request_metadata::RequestMetadata,
    service::{
        authz::{Authorizer, CatalogNamespaceAction, CatalogTableAction, CatalogWarehouseAction},
        contract_verification::{ContractVerification, ContractVerificationOutcome},
        secrets::SecretStore,
        storage::{StorageLocations as _, StoragePermissions, StorageProfile, ValidationError},
        task_queue::{
            tabular_expiration_queue::{TabularExpirationPayload, TabularExpirationTask},
            tabular_purge_queue::{TabularPurgePayload, TabularPurgeTask},
            EntityId, TaskMetadata,
        },
        Catalog, CreateTableResponse, GetNamespaceResponse, ListFlags,
        LoadTableResponse as CatalogLoadTableResult, NamedEntity, State, TableCommit,
        TableCreation, TableId, TabularDetails, TabularId, Transaction, WarehouseStatus,
    },
    WarehouseId,
};

const PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED: &str =
    "write.metadata.delete-after-commit.enabled";
const PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT: bool = true;

pub(crate) const CONCURRENT_UPDATE_ERROR_TYPE: &str = "ConcurrentUpdateError";
pub(crate) const MAX_RETRIES_ON_CONCURRENT_UPDATE: usize = 2;

#[async_trait::async_trait]
impl<C: Catalog, A: Authorizer + Clone, S: SecretStore>
    crate::api::iceberg::v1::tables::TablesService<State<A, C, S>> for CatalogServer<C, A, S>
{
    #[allow(clippy::too_many_lines)]
    /// List all table identifiers underneath a given namespace
    async fn list_tables(
        parameters: NamespaceParameters,
        query: ListTablesQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListTablesResponse> {
        let return_uuids = query.return_uuids;
        // ------------------- VALIDATIONS -------------------
        let NamespaceParameters { namespace, prefix } = parameters;
        let warehouse_id = require_warehouse_id(prefix)?;
        validate_namespace_ident(&namespace)?;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
        let namespace_id = authorized_namespace_ident_to_id::<C, _>(
            authorizer.clone(),
            &request_metadata,
            &warehouse_id,
            &namespace,
            CatalogNamespaceAction::CanListTables,
            t.transaction(),
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let (identifiers, table_uuids, next_page_token) =
            catalog::fetch_until_full_page::<_, _, _, C>(
                query.page_size,
                query.page_token,
                list_entities!(
                    Table,
                    list_tables,
                    namespace,
                    namespace_id,
                    authorizer,
                    request_metadata,
                    warehouse_id
                ),
                &mut t,
            )
            .await?;
        t.commit().await?;
        let mut idents = Vec::with_capacity(identifiers.len());
        let mut protection_status = Vec::with_capacity(identifiers.len());
        for ident in identifiers {
            idents.push(ident.table_ident);
            protection_status.push(ident.protected);
        }

        Ok(ListTablesResponse {
            next_page_token,
            identifiers: idents,
            table_uuids: return_uuids.then_some(table_uuids.into_iter().map(|u| *u).collect()),
            protection_status: query.return_protection_status.then_some(protection_status),
        })
    }

    #[allow(clippy::too_many_lines)]
    /// Create a table in the given namespace
    async fn create_table(
        parameters: NamespaceParameters,
        // mut because we need to change location
        mut request: CreateTableRequest,
        data_access: impl Into<DataAccessMode> + Send,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        let data_access = data_access.into();
        // ------------------- VALIDATIONS -------------------
        let NamespaceParameters { namespace, prefix } = parameters.clone();
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        let table = TableIdent::new(namespace.clone(), request.name.clone());
        validate_table_or_view_ident(&table)?;

        if let Some(properties) = &request.properties {
            validate_table_properties(properties.keys())?;
        }

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let namespace_id = authorized_namespace_ident_to_id::<C, _>(
            authorizer.clone(),
            &request_metadata,
            &warehouse_id,
            &namespace,
            CatalogNamespaceAction::CanCreateTable,
            t.transaction(),
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let id = Uuid::now_v7();
        let tabular_id = TabularId::Table(id);
        let table_id = TableId::from(id);

        let namespace = C::get_namespace(warehouse_id, namespace_id, t.transaction()).await?;
        let warehouse = C::require_warehouse(warehouse_id, t.transaction()).await?;
        let storage_profile = &warehouse.storage_profile;
        require_active_warehouse(warehouse.status)?;

        let table_location = determine_tabular_location(
            &namespace,
            request.location.clone(),
            tabular_id,
            storage_profile,
        )?;

        // Update the request for event
        request.location = Some(table_location.to_string());
        let request = request; // Make it non-mutable again for our sanity

        // If stage-create is true, we should not create the metadata file
        let metadata_location = if request.stage_create.unwrap_or(false) {
            None
        } else {
            let metadata_id = Uuid::now_v7();
            Some(storage_profile.default_metadata_location(
                &table_location,
                &CompressionCodec::try_from_maybe_properties(request.properties.as_ref())?,
                metadata_id,
                0,
            ))
        };

        let table_metadata = create_table_request_into_table_metadata(table_id, request.clone())?;

        let CreateTableResponse {
            table_metadata,
            staged_table_id,
        } = C::create_table(
            TableCreation {
                warehouse_id: warehouse.id,
                namespace_id: namespace.namespace_id,
                table_ident: &table,
                table_metadata,
                metadata_location: metadata_location.as_ref(),
            },
            t.transaction(),
        )
        .await?;

        // We don't commit the transaction yet, first we need to write the metadata file.
        let storage_secret = if let Some(secret_id) = warehouse.storage_secret_id {
            let secret_state = state.v1_state.secrets;
            Some(secret_state.get_secret_by_id(secret_id).await?.secret)
        } else {
            None
        };

        let file_io = storage_profile.file_io(storage_secret.as_ref()).await?;
        if !crate::service::storage::is_empty(&file_io, &table_location).await? {
            return Err(ValidationError::from(InvalidLocationError::new(
                table_location.to_string(),
                "Unexpected files in location, tabular locations have to be empty",
            ))
            .into());
        }

        if let Some(metadata_location) = &metadata_location {
            let compression_codec = CompressionCodec::try_from_metadata(&table_metadata)?;
            write_file(
                &file_io,
                metadata_location,
                &table_metadata,
                compression_codec,
            )
            .await?;
        }

        // This requires the storage secret
        // because the table config might contain vended-credentials based
        // on the `data_access` parameter.
        let config = storage_profile
            .generate_table_config(
                data_access,
                storage_secret.as_ref(),
                &table_location,
                StoragePermissions::ReadWriteDelete,
                &request_metadata,
                warehouse_id,
                table_id.into(),
            )
            .await?;

        let storage_credentials = (!config.creds.inner().is_empty()).then(|| {
            vec![StorageCredential {
                prefix: table_location.to_string(),
                config: config.creds.into(),
            }]
        });

        let load_table_result = LoadTableResult {
            metadata_location: metadata_location.as_ref().map(ToString::to_string),
            metadata: table_metadata.clone(),
            config: Some(config.config.into()),
            storage_credentials,
        };

        authorizer
            .create_table(&request_metadata, warehouse_id, table_id, namespace_id)
            .await?;

        // Metadata file written, now we can commit the transaction
        t.commit().await?;

        // If a staged table was overwritten, delete it from authorizer
        if let Some(staged_table_id) = staged_table_id {
            authorizer
                .delete_table(warehouse_id, staged_table_id)
                .await
                .ok();
        }

        state
            .v1_state
            .hooks
            .create_table(
                warehouse_id,
                parameters,
                Arc::new(request),
                Arc::new(table_metadata),
                metadata_location.map(Arc::new),
                data_access,
                Arc::new(request_metadata),
            )
            .await;

        Ok(load_table_result)
    }

    /// Register a table in the given namespace using given metadata file location
    #[allow(clippy::too_many_lines)]
    async fn register_table(
        parameters: NamespaceParameters,
        request: RegisterTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        // ------------------- VALIDATIONS -------------------
        let NamespaceParameters { namespace, prefix } = &parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        let table = TableIdent::new(namespace.clone(), request.name.clone());
        validate_table_or_view_ident(&table)?;
        let metadata_location =
            parse_location(&request.metadata_location, StatusCode::BAD_REQUEST)?;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz.clone();
        let mut t_read = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
        let namespace_id = authorized_namespace_ident_to_id::<C, _>(
            authorizer.clone(),
            &request_metadata,
            &warehouse_id,
            namespace,
            CatalogNamespaceAction::CanCreateTable,
            t_read.transaction(),
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let warehouse = C::require_warehouse(warehouse_id, t_read.transaction()).await?;
        let storage_profile = &warehouse.storage_profile;

        require_active_warehouse(warehouse.status)?;
        storage_profile.require_allowed_location(&metadata_location)?;

        let storage_secret =
            maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
        let file_io = storage_profile.file_io(storage_secret.as_ref()).await?;
        let table_metadata = read_metadata_file(&file_io, &metadata_location).await?;
        let table_location = parse_location(table_metadata.location(), StatusCode::BAD_REQUEST)?;

        // Check if we need to handle overwrite
        let mut previous_table = None;

        let mut t_write = C::Transaction::begin_write(state.v1_state.catalog).await?;
        if request.overwrite {
            // Check if table exists
            previous_table = C::table_to_id(
                warehouse_id,
                &table,
                ListFlags {
                    include_active: true,
                    include_staged: true,
                    include_deleted: false,
                },
                t_read.transaction(),
            )
            .await?;

            if let Some(previous_table) = previous_table {
                tracing::debug!(
                    "Register Table: Dropping existing table '{}' in namespace '{:?}' of warehouse '{:?}' with id {previous_table} for overwrite operation",
                    table.name, table.namespace, warehouse.name
                );
                // Verify authorization to drop the table first
                authorizer
                    .require_table_action(
                        &request_metadata,
                        warehouse_id,
                        Ok(Some(previous_table)),
                        CatalogTableAction::CanDrop,
                    )
                    .await?;

                // Drop the existing table to overwrite it
                let _previous_table_location =
                    C::drop_table(warehouse_id, previous_table, false, t_write.transaction())
                        .await?;
                // We don't drop the files for the previous table on overwrite
            }
        }
        t_read.commit().await?;

        validate_table_properties(table_metadata.properties().keys())?;
        storage_profile.require_allowed_location(&table_location)?;

        let tabular_id = TableId::from(table_metadata.uuid());

        let CreateTableResponse {
            table_metadata,
            staged_table_id,
        } = C::create_table(
            TableCreation {
                warehouse_id: warehouse.id,
                namespace_id,
                table_ident: &table,
                table_metadata,
                metadata_location: Some(&metadata_location),
            },
            t_write.transaction(),
        )
        .await?;

        let config = storage_profile
            .generate_table_config(
                DataAccess::not_specified().into(),
                storage_secret.as_ref(),
                &table_location,
                StoragePermissions::ReadWriteDelete,
                &request_metadata,
                warehouse_id,
                tabular_id.into(),
            )
            .await?;

        let mut auth_needs_delete = false;
        // Delete the previous table from authorizer if it exists and differs from the new one
        if let Some(previous_table) = previous_table {
            if previous_table != tabular_id {
                auth_needs_delete = true;
                // Only create authorization for the new table if it's different
                authorizer
                    .create_table(&request_metadata, warehouse_id, tabular_id, namespace_id)
                    .await?;
            }
        } else {
            // No previous table, need to create authorization
            authorizer
                .create_table(&request_metadata, warehouse_id, tabular_id, namespace_id)
                .await?;
        }

        // Commit the transaction
        t_write.commit().await?;

        // If we need to delete the previous table from authorizer
        if auth_needs_delete {
            if let Some(previous_table) = previous_table {
                authorizer.delete_table(warehouse_id, previous_table).await.map_err({
                    |e| {
                        tracing::warn!(
                            "Failed to delete previous table {previous_table} from authorizer on overwrite via table register endpoint: {}",
                            e.error
                        );
                    }
                }).ok();
            }
        }

        // If a staged table was overwritten, delete it from authorizer
        if let Some(staged_table_id) = staged_table_id {
            authorizer
                .delete_table(warehouse_id, staged_table_id)
                .await
                .ok();
        }

        // Fire hooks
        state
            .v1_state
            .hooks
            .register_table(
                warehouse_id,
                parameters,
                Arc::new(request),
                Arc::new(table_metadata.clone()),
                Arc::new(metadata_location.clone()),
                Arc::new(request_metadata),
            )
            .await;

        Ok(LoadTableResult {
            metadata_location: Some(metadata_location.to_string()),
            metadata: table_metadata,
            config: Some(config.config.into()),
            storage_credentials: None,
        })
    }

    /// Load a table from the catalog
    #[allow(clippy::too_many_lines)]
    async fn load_table(
        parameters: TableParameters,
        data_access: impl Into<DataAccessMode> + Send,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        // ------------------- VALIDATIONS -------------------
        let TableParameters { prefix, table } = parameters;
        let warehouse_id = require_warehouse_id(prefix)?;
        // It is important to throw a 404 if a table cannot be found,
        // because spark might check if `table`.`branch` exists, which should return 404.
        // Only then will it treat it as a branch.
        if let Err(mut e) = validate_table_or_view_ident(&table) {
            if e.error.r#type == *"NamespaceDepthExceeded" {
                e.error.code = StatusCode::NOT_FOUND.into();
            }
            return Err(e);
        }

        let list_flags = ListFlags {
            include_active: true,
            include_staged: false,
            include_deleted: false,
        };

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        let catalog = state.v1_state.catalog;
        let mut t = C::Transaction::begin_read(catalog).await?;

        let (tabular_details, storage_permissions) = Self::resolve_and_authorize_table_access(
            &request_metadata,
            &table,
            warehouse_id,
            list_flags,
            authorizer,
            t.transaction(),
        )
        .await?;

        // ------------------- BUSINESS LOGIC -------------------
        let CatalogLoadTableResult {
            table_id,
            namespace_id: _,
            table_metadata,
            metadata_location,
            storage_secret_ident,
            storage_profile,
        } = load_table_inner::<C>(
            warehouse_id,
            tabular_details.table_id,
            &table,
            list_flags.include_deleted,
            &mut t,
        )
        .await?;
        t.commit().await?;

        let table_location =
            parse_location(table_metadata.location(), StatusCode::INTERNAL_SERVER_ERROR)?;

        // ToDo: This is a small inefficiency: We fetch the secret even if it might
        // not be required based on the `data_access` parameter.
        let storage_config = if let Some(storage_permissions) = storage_permissions {
            let storage_secret =
                maybe_get_secret(storage_secret_ident, &state.v1_state.secrets).await?;
            Some(
                storage_profile
                    .generate_table_config(
                        data_access.into(),
                        storage_secret.as_ref(),
                        &table_location,
                        storage_permissions,
                        &request_metadata,
                        warehouse_id,
                        table_id.into(),
                    )
                    .await?,
            )
        } else {
            None
        };

        let storage_credentials = storage_config.as_ref().and_then(|c| {
            (!c.creds.inner().is_empty()).then(|| {
                vec![StorageCredential {
                    prefix: table_location.to_string(),
                    config: c.creds.clone().into(),
                }]
            })
        });

        let load_table_result = LoadTableResult {
            metadata_location: metadata_location.as_ref().map(ToString::to_string),
            metadata: table_metadata,
            config: storage_config.map(|c| c.config.into()),
            storage_credentials,
        };

        Ok(load_table_result)
    }

    async fn load_table_credentials(
        parameters: TableParameters,
        data_access: DataAccess,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadCredentialsResponse> {
        // ------------------- VALIDATIONS -------------------
        let TableParameters { prefix, table } = parameters;
        let warehouse_id = require_warehouse_id(prefix)?;

        let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
        let (tabular_details, storage_permissions) = Self::resolve_and_authorize_table_access(
            &request_metadata,
            &table,
            warehouse_id,
            ListFlags {
                include_active: true,
                include_staged: false,
                include_deleted: false,
            },
            state.v1_state.authz,
            t.transaction(),
        )
        .await?;
        let storage_permission = storage_permissions.ok_or(ErrorModel::unauthorized(
            "No storage permissions for table",
            "NoStoragePermissions",
            None,
        ))?;

        let (storage_secret_ident, storage_profile) =
            C::load_storage_profile(warehouse_id, tabular_details.table_id, t.transaction())
                .await?;
        // DB work is done; release the read transaction before external IO/crypto work.
        t.commit().await?;

        let storage_secret =
            maybe_get_secret(storage_secret_ident, &state.v1_state.secrets).await?;
        let storage_config = storage_profile
            .generate_table_config(
                data_access.into(),
                storage_secret.as_ref(),
                &parse_location(
                    tabular_details.location.as_str(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )?,
                storage_permission,
                &request_metadata,
                warehouse_id,
                tabular_details.table_id.into(),
            )
            .await?;

        let storage_credentials = if storage_config.creds.inner().is_empty() {
            vec![]
        } else {
            vec![StorageCredential {
                prefix: tabular_details.location.clone(),
                config: storage_config.creds.into(),
            }]
        };

        Ok(LoadCredentialsResponse {
            storage_credentials,
        })
    }

    /// Commit updates to a table
    #[allow(clippy::too_many_lines)]
    async fn commit_table(
        parameters: TableParameters,
        mut request: CommitTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<CommitTableResponse> {
        request.identifier = Some(determine_table_ident(
            &parameters.table,
            request.identifier.as_ref(),
        )?);
        let t = commit_tables_with_authz(
            parameters.prefix,
            CommitTransactionRequest {
                table_changes: vec![request],
            },
            state,
            request_metadata,
        )
        .await?;
        let mut it = t.into_iter();
        let Some(item) = it.next() else {
            return Err(ErrorModel::internal(
                "No new metadata returned by backend",
                "NoNewMetadataReturned",
                None,
            )
            .into());
        };
        debug_assert!(
            it.next().is_none(),
            "commit_table must return exactly one CommitContext"
        );

        Ok(CommitTableResponse {
            metadata_location: item.new_metadata_location.to_string(),
            metadata: item.new_metadata,
            config: None,
        })
    }

    #[allow(clippy::too_many_lines)]
    /// Drop a table from the catalog
    async fn drop_table(
        parameters: TableParameters,
        DropParams {
            purge_requested,
            force,
        }: DropParams,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let TableParameters { prefix, table } = &parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        validate_table_or_view_ident(table)?;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        authorizer
            .require_warehouse_action(
                &request_metadata,
                warehouse_id,
                CatalogWarehouseAction::CanUse,
            )
            .await?;

        let include_staged = true;
        let include_deleted = false;
        let include_active = true;

        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let table_details = C::table_to_id(
            warehouse_id,
            table,
            ListFlags {
                include_active,
                include_staged,
                include_deleted,
            },
            t.transaction(),
        )
        .await; // We can't fail before AuthZ

        let table_id = authorizer
            .require_table_action(
                &request_metadata,
                warehouse_id,
                table_details,
                CatalogTableAction::CanDrop,
            )
            .await?;

        // ------------------- BUSINESS LOGIC -------------------

        let warehouse = C::require_warehouse(warehouse_id, t.transaction()).await?;

        state
            .v1_state
            .contract_verifiers
            .check_drop(TabularId::Table(*table_id))
            .await?
            .into_result()?;

        let delete_profile = if force {
            TabularDeleteProfile::Hard {}
        } else {
            warehouse.tabular_delete_profile
        };

        match delete_profile {
            TabularDeleteProfile::Hard {} => {
                let location =
                    C::drop_table(warehouse_id, table_id, force, t.transaction()).await?;

                if purge_requested {
                    TabularPurgeTask::schedule_task::<C>(
                        TaskMetadata {
                            warehouse_id,
                            entity_id: EntityId::Tabular(*table_id),
                            parent_task_id: None,
                            schedule_for: None,
                            entity_name: table.clone().into_name_parts(),
                        },
                        TabularPurgePayload {
                            tabular_location: location,
                            tabular_type: TabularType::Table,
                        },
                        t.transaction(),
                    )
                    .await?;

                    tracing::debug!("Queued purge task for dropped table '{table_id}'.");
                }
                t.commit().await?;
                authorizer
                    .delete_table(warehouse_id, table_id)
                    .await
                    .inspect_err(|e| {
                        tracing::error!(?e, "Failed to delete table from authorizer: {}", e.error);
                    })
                    .ok();
            }
            TabularDeleteProfile::Soft { expiration_seconds } => {
                let _ = TabularExpirationTask::schedule_task::<C>(
                    TaskMetadata {
                        entity_id: EntityId::Tabular(*table_id),
                        warehouse_id,
                        parent_task_id: None,
                        schedule_for: Some(chrono::Utc::now() + expiration_seconds),
                        entity_name: table.clone().into_name_parts(),
                    },
                    TabularExpirationPayload {
                        tabular_type: TabularType::Table,
                        deletion_kind: if purge_requested {
                            DeleteKind::Purge
                        } else {
                            DeleteKind::Default
                        },
                    },
                    t.transaction(),
                )
                .await?;

                C::mark_tabular_as_deleted(
                    warehouse_id,
                    TabularId::Table(*table_id),
                    force,
                    t.transaction(),
                )
                .await?;

                tracing::debug!("Queued expiration task for dropped table '{table_id}'.");
                t.commit().await?;
            }
        }

        state
            .v1_state
            .hooks
            .drop_table(
                warehouse_id,
                parameters,
                DropParams {
                    purge_requested,
                    force,
                },
                TableId::from(*table_id),
                Arc::new(request_metadata),
            )
            .await;

        Ok(())
    }

    /// Check if a table exists
    async fn table_exists(
        parameters: TableParameters,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let TableParameters { prefix, table } = parameters;
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        validate_table_or_view_ident(&table)?;

        // ------------------- AUTHZ -------------------
        let authorizer = state.v1_state.authz;
        let mut t = C::Transaction::begin_read(state.v1_state.catalog).await?;
        let list_flags = ListFlags {
            include_staged: false,
            include_deleted: false,
            include_active: true,
        };
        let _table_id = authorized_table_ident_to_id::<C, _>(
            authorizer,
            &request_metadata,
            warehouse_id,
            &table,
            list_flags,
            CatalogTableAction::CanGetMetadata,
            t.transaction(),
        )
        .await?;
        t.commit().await?;

        // ------------------- BUSINESS LOGIC -------------------
        Ok(())
    }

    /// Rename a table
    async fn rename_table(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        // ------------------- VALIDATIONS -------------------
        let warehouse_id = require_warehouse_id(prefix.clone())?;
        let RenameTableRequest {
            source,
            destination,
        } = &request;
        validate_table_or_view_ident(source)?;
        validate_table_or_view_ident(destination)?;

        // ------------------- AUTHZ -------------------
        // Authorization is required for:
        // * renaming the old table
        // * creating a table in the destination namespace
        let authorizer = state.v1_state.authz;
        let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
        let list_flags = ListFlags {
            include_staged: false,
            include_deleted: false,
            include_active: true,
        };
        let source_table_id = authorized_table_ident_to_id::<C, _>(
            authorizer.clone(),
            &request_metadata,
            warehouse_id,
            source,
            list_flags,
            CatalogTableAction::CanRename,
            t.transaction(),
        )
        .await?;

        let destination_namespace_id =
            C::namespace_to_id(warehouse_id, &destination.namespace, t.transaction())
                .await?
                .ok_or_else(|| {
                    ErrorModel::not_found(
                        "Destination namespace not found",
                        "NoSuchNamespaceException",
                        None,
                    )
                })?;

        authorizer
            .require_namespace_action(
                &request_metadata,
                Ok(Some(destination_namespace_id)),
                CatalogNamespaceAction::CanCreateTable,
            )
            .await?;

        // ------------------- BUSINESS LOGIC -------------------
        if source == destination {
            return Ok(());
        }

        C::rename_table(
            warehouse_id,
            source_table_id,
            source,
            destination,
            t.transaction(),
        )
        .await?;

        state
            .v1_state
            .contract_verifiers
            .check_rename(TabularId::Table(*source_table_id), destination)
            .await?
            .into_result()?;

        t.commit().await?;

        state
            .v1_state
            .hooks
            .rename_table(
                warehouse_id,
                source_table_id,
                Arc::new(request),
                Arc::new(request_metadata),
            )
            .await;

        Ok(())
    }

    /// Commit updates to multiple tables in an atomic operation
    #[allow(clippy::too_many_lines)]
    async fn commit_transaction(
        prefix: Option<Prefix>,
        request: CommitTransactionRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        let contexts = commit_tables_with_authz(prefix, request, state, request_metadata).await?;
        tracing::debug!("Successfully committed {} table(s)", contexts.len());
        Ok(())
    }
}

impl<C: Catalog, A: Authorizer + Clone, S: SecretStore> CatalogServer<C, A, S> {
    async fn resolve_and_authorize_table_access(
        request_metadata: &RequestMetadata,
        table: &TableIdent,
        warehouse_id: WarehouseId,
        list_flags: ListFlags,
        authorizer: A,
        transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
    ) -> Result<(TabularDetails, Option<StoragePermissions>)> {
        authorizer
            .require_warehouse_action(
                request_metadata,
                warehouse_id,
                CatalogWarehouseAction::CanUse,
            )
            .await?;

        // We can't fail before AuthZ.
        let tabular_details =
            C::resolve_table_ident(warehouse_id, table, list_flags, transaction).await;

        let tabular_details = authorizer
            .require_table_action(
                request_metadata,
                warehouse_id,
                tabular_details,
                CatalogTableAction::CanGetMetadata,
            )
            .await
            .map_err(set_not_found_status_code)?;

        let (read_access, write_access) = futures::try_join!(
            authorizer.is_allowed_table_action(
                request_metadata,
                warehouse_id,
                tabular_details.table_id,
                CatalogTableAction::CanReadData,
            ),
            authorizer.is_allowed_table_action(
                request_metadata,
                warehouse_id,
                tabular_details.table_id,
                CatalogTableAction::CanWriteData,
            ),
        )?;
        let can_read = read_access.into_inner();
        let can_write = write_access.into_inner();

        let storage_permissions = if can_write {
            Some(StoragePermissions::ReadWriteDelete)
        } else if can_read {
            Some(StoragePermissions::Read)
        } else {
            None
        };
        Ok((tabular_details, storage_permissions))
    }
}

/// Load a table from the catalog, ensuring that it is not staged
///
/// # Errors
/// Returns an error if the table is staged, if it cannot be found, or if a DB error occurs.
async fn load_table_inner<C: Catalog>(
    warehouse_id: WarehouseId,
    table_id: TableId,
    table_ident: &TableIdent,
    include_deleted: bool,
    t: &mut C::Transaction,
) -> Result<CatalogLoadTableResult> {
    let mut metadatas =
        C::load_tables(warehouse_id, [table_id], include_deleted, t.transaction()).await?;
    let result = take_table_metadata(&table_id, table_ident, &mut metadatas)?;
    require_not_staged(result.metadata_location.as_ref())?;
    Ok(result)
}

/// Validate commit table requests
///
/// # Errors
/// Returns an error if any validation fails.
fn commit_tables_validate(request: &CommitTransactionRequest) -> Result<()> {
    for change in &request.table_changes {
        validate_table_updates(&change.updates)?;
        change
            .identifier
            .as_ref()
            .map(validate_table_or_view_ident)
            .transpose()?;

        if change.identifier.is_none() {
            return Err(ErrorModel::bad_request(
                "Table identifier is required for each change in the CommitTransactionRequest (one of the changes was missing an identifier)",
                "TableIdentifierRequiredForCommitTransaction",
                None,
            )
            .into());
        }
    }

    // Check table identifier uniqueness
    let identifiers = request
        .table_changes
        .iter()
        .filter_map(|change| change.identifier.as_ref())
        .collect::<HashSet<_>>();
    let n_identifiers = identifiers.len();

    if n_identifiers != request.table_changes.len() {
        let mut counts = std::collections::HashMap::<&TableIdent, usize>::new();
        for ident in request
            .table_changes
            .iter()
            .filter_map(|c| c.identifier.as_ref())
        {
            *counts.entry(ident).or_default() += 1;
        }
        let dups = counts
            .into_iter()
            .filter(|&(_i, c)| c > 1)
            .map(|(i, _c)| i.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(ErrorModel::bad_request(
            format!("Table identifiers must be unique; duplicates: [{dups}]"),
            "UniqueTableIdentifiersRequiredForCommitTransaction",
            None,
        )
        .into());
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
/// Commit updates to multiple tables without authorization checks
///
/// # Errors
/// Returns an error if the commit fails or if a DB error occurs.
/// This function will retry on concurrent update errors up to a maximum number of retries.
async fn commit_tables_inner<
    C: Catalog,
    A: Authorizer,
    S: SecretStore,
    H: ::std::hash::BuildHasher + 'static + Send + Sync,
>(
    warehouse_id: WarehouseId,
    request: CommitTransactionRequest,
    table_ids: Arc<HashMap<TableIdent, TableId, H>>,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<Vec<CommitContext>> {
    let include_deleted = false;

    // Start the retry loop
    let mut attempt = 0;
    loop {
        let result = try_commit_tables::<C, A, S, _>(
            &request,
            warehouse_id,
            table_ids.clone(),
            &state,
            include_deleted,
        )
        .await;

        match result {
            Ok(commits) => {
                // Fire hooks
                state
                    .v1_state
                    .hooks
                    .commit_transaction(
                        warehouse_id,
                        Arc::new(request),
                        Arc::new(commits.clone()),
                        table_ids,
                        Arc::new(request_metadata),
                    )
                    .await;
                return Ok(commits);
            }
            Err(e)
                if e.error.r#type == CONCURRENT_UPDATE_ERROR_TYPE
                    && attempt < MAX_RETRIES_ON_CONCURRENT_UPDATE =>
            {
                attempt += 1;
                tracing::info!(
                    warehouse_id = %warehouse_id,
                    n_tables = %table_ids.len(),
                    attempt = attempt,
                    max_attempts = MAX_RETRIES_ON_CONCURRENT_UPDATE,
                    "Concurrent update detected, retrying commit operation"
                );
                // Short jittered exponential backoff to reduce contention
                // First delay: 50ms, then 100ms, 200ms, ..., up to 3200ms (50*2^6)
                let exp = u32::try_from(attempt.saturating_sub(1).min(6)).unwrap_or(6); // cap growth explicitly
                let base = 50u64.saturating_mul(1u64 << exp);
                let jitter = fastrand::u64(..base / 2);
                tracing::debug!(attempt, base, jitter, "Concurrent update backoff");
                tokio::time::sleep(std::time::Duration::from_millis(base + jitter)).await;
            }
            Err(e) => {
                if attempt > 0 {
                    tracing::warn!(
                        warehouse_id = %warehouse_id,
                        n_tables = %table_ids.len(),
                        attempt = attempt,
                        "Table commit operation failed after {} attempts. Operation was retried due to concurrent updates. {e}",
                        attempt + 1
                    );
                }
                return Err(e);
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
/// Commit updates to multiple tables in an atomic operation
///
/// # Errors
/// Returns an error if the commit fails, if the table identifiers are not unique,
/// or if the table identifiers are not provided for each change.
/// This function will retry on concurrent update errors up to a maximum number of retries.
async fn commit_tables_with_authz<C: Catalog, A: Authorizer + Clone, S: SecretStore>(
    prefix: Option<Prefix>,
    request: CommitTransactionRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<Vec<CommitContext>> {
    // ------------------- VALIDATIONS -------------------
    let warehouse_id = require_warehouse_id(prefix.clone())?;
    commit_tables_validate(&request)?;

    // ------------------- AUTHZ -------------------
    let authorizer = state.v1_state.authz.clone();
    authorizer
        .require_warehouse_action(
            &request_metadata,
            warehouse_id,
            CatalogWarehouseAction::CanUse,
        )
        .await?;

    let include_staged = true;
    let include_deleted = false;
    let include_active = true;

    let identifiers = request
        .table_changes
        .iter()
        .filter_map(|change| change.identifier.as_ref())
        .collect::<HashSet<_>>();
    let table_ids = C::table_idents_to_ids(
        warehouse_id,
        identifiers,
        ListFlags {
            include_active,
            include_staged,
            include_deleted,
        },
        state.v1_state.catalog.clone(),
    )
    .await
    .map_err(|e| {
        ErrorModel::internal("Error fetching table ids", "TableIdsFetchError", None)
            .append_detail(e.error.message)
            .append_details(e.error.stack)
    })?;

    // Build futures alongside their idents to preserve pairing
    let authz_checks = table_ids
        .iter()
        .map(|(ident, id)| {
            (
                ident.clone(),
                authorizer.require_table_action(
                    &request_metadata,
                    warehouse_id,
                    Ok(*id),
                    CatalogTableAction::CanCommit,
                ),
            )
        })
        .collect::<Vec<_>>();
    // Resolve and re-associate
    let resolved: Vec<(TableIdent, TableId)> = futures::future::try_join_all(
        authz_checks
            .into_iter()
            .map(|(ident, fut)| async move { fut.await.map(|id| (ident, id)) }),
    )
    .await?;
    let table_ids = Arc::new(resolved.into_iter().collect::<HashMap<_, _>>());

    // ------------------- BUSINESS LOGIC -------------------
    commit_tables_inner(warehouse_id, request, table_ids, state, request_metadata).await
}

// Extract the core commit logic to a separate function for retry purposes
#[allow(clippy::too_many_lines)]
async fn try_commit_tables<
    C: Catalog,
    A: Authorizer + Clone,
    S: SecretStore,
    H: ::std::hash::BuildHasher,
>(
    request: &CommitTransactionRequest,
    warehouse_id: WarehouseId,
    table_ids: Arc<HashMap<TableIdent, TableId, H>>,
    state: &ApiContext<State<A, C, S>>,
    include_deleted: bool,
) -> Result<Vec<CommitContext>> {
    let mut transaction = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
    let warehouse = C::require_warehouse(warehouse_id, transaction.transaction()).await?;

    // Load old metadata
    let mut previous_metadatas = C::load_tables(
        warehouse_id,
        table_ids.values().copied(),
        include_deleted,
        transaction.transaction(),
    )
    .await?;

    transaction.commit().await?;

    let mut expired_metadata_logs: Vec<MetadataLog> = vec![];

    // Apply changes
    let commits = request
        .table_changes
        .iter()
        .map(|change| {
            let table_ident = change.identifier.as_ref().ok_or_else(||
                    // This should never happen due to validation
                    ErrorModel::internal(
                        "Change without Identifier",
                        "ChangeWithoutIdentifier",
                        None,
                    ))?;
            let table_id = require_table_id(table_ident, table_ids.get(table_ident).copied())?;
            let previous_table_metadata =
                take_table_metadata(&table_id, table_ident, &mut previous_metadatas)?;
            let TableMetadataBuildResult {
                metadata: new_metadata,
                changes: _,
                expired_metadata_logs: mut this_expired,
            } = apply_commit(
                previous_table_metadata.table_metadata.clone(),
                previous_table_metadata.metadata_location.as_ref(),
                &change.requirements,
                change.updates.clone(),
            )?;

            let number_expired_metadata_log_entries = this_expired.len();

            if get_delete_after_commit_enabled(new_metadata.properties()) {
                expired_metadata_logs.extend(this_expired);
            } else {
                this_expired.clear();
            }

            let next_metadata_count = previous_table_metadata
                .metadata_location
                .as_ref()
                .and_then(extract_count_from_metadata_location)
                .map_or(0, |v| v + 1);

            let new_table_location =
                parse_location(new_metadata.location(), StatusCode::INTERNAL_SERVER_ERROR)?;
            let new_compression_codec = CompressionCodec::try_from_metadata(&new_metadata)?;
            let new_metadata_location = previous_table_metadata
                .storage_profile
                .default_metadata_location(
                    &new_table_location,
                    &new_compression_codec,
                    Uuid::now_v7(),
                    next_metadata_count,
                );

            let number_added_metadata_log_entries = (new_metadata.metadata_log().len()
                + number_expired_metadata_log_entries)
                .saturating_sub(previous_table_metadata.table_metadata.metadata_log().len());

            Ok(CommitContext {
                new_metadata,
                new_metadata_location,
                new_compression_codec,
                previous_metadata_location: previous_table_metadata.metadata_location,
                updates: change.updates.clone(),
                previous_metadata: previous_table_metadata.table_metadata,
                number_expired_metadata_log_entries,
                number_added_metadata_log_entries,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // Check contract verification
    let futures = commits.iter().map(|c| {
        state
            .v1_state
            .contract_verifiers
            .check_table_updates(&c.updates, &c.previous_metadata)
    });

    futures::future::try_join_all(futures)
        .await?
        .into_iter()
        .map(ContractVerificationOutcome::into_result)
        .collect::<Result<Vec<()>, ErrorModel>>()?;

    // We don't commit the transaction yet, first we need to write the metadata file.
    let storage_secret =
        maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;

    // Write metadata files
    let file_io = warehouse
        .storage_profile
        .file_io(storage_secret.as_ref())
        .await?;

    let write_futures: Vec<_> = commits
        .iter()
        .map(|commit| {
            write_file(
                &file_io,
                &commit.new_metadata_location,
                &commit.new_metadata,
                commit.new_compression_codec,
            )
        })
        .collect();
    futures::future::try_join_all(write_futures).await?;

    // Make changes in DB
    let transaction_result = async {
        let mut transaction = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
        C::commit_table_transaction(
            warehouse_id,
            commits.iter().map(CommitContext::commit),
            transaction.transaction(),
        )
        .await?;

        transaction.commit().await?;
        Result::<_, IcebergErrorResponse>::Ok(())
    }
    .await;

    // If transaction fails, delete the metadata files we just wrote (best-effort), then
    // return the original error.
    if let Err(e) = transaction_result {
        let delete_result = futures::future::join_all(
            commits
                .iter()
                .map(|commit| delete_file(&file_io, &commit.new_metadata_location))
                .collect::<Vec<_>>(),
        )
        .await;
        // Log any delete errors, but return the original error
        for r in delete_result {
            if let Err(e) = r {
                tracing::warn!("Failed to delete metadata file after failed commit: {e:?}");
            }
        }
        return Err(e);
    }

    // Delete files in parallel - if one delete fails, we still want to delete the rest
    let expired_locations = expired_metadata_logs
        .into_iter()
        .filter_map(|expired_metadata_log| {
            Location::parse_value(&expired_metadata_log.metadata_file)
                .map_err(|e| {
                    tracing::warn!(
                        "Failed to parse expired metadata file location {}: {:?}",
                        expired_metadata_log.metadata_file,
                        e
                    );
                })
                .ok()
        })
        .collect::<Vec<_>>();
    let _ = futures::future::join_all(
        expired_locations
            .iter()
            .map(|location| delete_file(&file_io, location))
            .collect::<Vec<_>>(),
    )
    .await
    .into_iter()
    .map(|r| {
        r.map_err(|e| tracing::warn!("Failed to delete expired metadata file: {:?}", e))
            .ok()
    });

    Ok(commits)
}

pub(crate) async fn authorized_table_ident_to_id<C: Catalog, A: Authorizer>(
    authorizer: A,
    metadata: &RequestMetadata,
    warehouse_id: WarehouseId,
    table_ident: &TableIdent,
    list_flags: ListFlags,
    action: impl From<CatalogTableAction> + std::fmt::Display + Send,
    transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
) -> Result<TableId> {
    authorizer
        .require_warehouse_action(metadata, warehouse_id, CatalogWarehouseAction::CanUse)
        .await?;
    let table_details = C::table_to_id(warehouse_id, table_ident, list_flags, transaction).await; // We can't fail before AuthZ
    authorizer
        .require_table_action(metadata, warehouse_id, table_details, action)
        .await
        .map_err(set_not_found_status_code)
}

pub(crate) fn extract_count_from_metadata_location(location: &Location) -> Option<usize> {
    let last_segment = location
        .as_str()
        .trim_end_matches('/')
        .split('/')
        .next_back()
        .unwrap_or(location.as_str());

    if let Some((_whole, version, _metadata_id)) = lazy_regex::regex_captures!(
        r"^(\d+)-([\w-]{36})(?:\.\w+)?\.metadata\.json",
        last_segment
    ) {
        version.parse().ok()
    } else {
        None
    }
}

#[derive(Clone, Debug)]
pub struct CommitContext {
    pub new_metadata: iceberg::spec::TableMetadata,
    pub new_metadata_location: Location,
    pub previous_metadata: iceberg::spec::TableMetadata,
    pub previous_metadata_location: Option<Location>,
    pub updates: Vec<TableUpdate>,
    pub new_compression_codec: CompressionCodec,
    pub number_expired_metadata_log_entries: usize,
    pub number_added_metadata_log_entries: usize,
}

impl CommitContext {
    fn commit(&self) -> TableCommit {
        let diffs = calculate_diffs(
            &self.new_metadata,
            &self.previous_metadata,
            self.number_added_metadata_log_entries,
            self.number_expired_metadata_log_entries,
        );

        TableCommit {
            diffs,
            new_metadata: self.new_metadata.clone(),
            new_metadata_location: self.new_metadata_location.clone(),
            previous_metadata_location: self.previous_metadata_location.clone(),
            updates: self.updates.clone(),
        }
    }
}

#[allow(clippy::too_many_lines)]
fn calculate_diffs(
    new_metadata: &TableMetadata,
    previous_metadata: &TableMetadata,
    added_metadata_log: usize,
    expired_metadata_logs: usize,
) -> TableMetadataDiffs {
    let new_snaps = new_metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect::<FxHashSet<i64>>();
    let old_snaps = previous_metadata
        .snapshots()
        .map(|s| s.snapshot_id())
        .collect::<FxHashSet<i64>>();
    let removed_snaps = old_snaps
        .difference(&new_snaps)
        .copied()
        .collect::<Vec<i64>>();
    let added_snapshots = new_snaps
        .difference(&old_snaps)
        .copied()
        .collect::<Vec<i64>>();

    let old_schemas = previous_metadata
        .schemas_iter()
        .map(|s| s.schema_id())
        .collect::<FxHashSet<SchemaId>>();
    let new_schemas = new_metadata
        .schemas_iter()
        .map(|s| s.schema_id())
        .collect::<FxHashSet<SchemaId>>();
    let removed_schemas = old_schemas
        .difference(&new_schemas)
        .copied()
        .collect::<Vec<SchemaId>>();
    let added_schemas = new_schemas
        .difference(&old_schemas)
        .copied()
        .collect::<Vec<SchemaId>>();
    let new_current_schema_id = (previous_metadata.current_schema_id()
        != new_metadata.current_schema_id())
    .then_some(new_metadata.current_schema_id());

    let old_specs = previous_metadata
        .partition_specs_iter()
        .map(|s| s.spec_id())
        .collect::<FxHashSet<i32>>();
    let new_specs = new_metadata
        .partition_specs_iter()
        .map(|s| s.spec_id())
        .collect::<FxHashSet<i32>>();
    let removed_specs = old_specs
        .difference(&new_specs)
        .copied()
        .collect::<Vec<i32>>();
    let added_partition_specs = new_specs
        .difference(&old_specs)
        .copied()
        .collect::<Vec<i32>>();
    let default_partition_spec_id = (previous_metadata.default_partition_spec_id()
        != new_metadata.default_partition_spec_id())
    .then_some(new_metadata.default_partition_spec_id());

    let old_sort_orders = previous_metadata
        .sort_orders_iter()
        .map(|s| s.order_id)
        .collect::<FxHashSet<i64>>();
    let new_sort_orders = new_metadata
        .sort_orders_iter()
        .map(|s| s.order_id)
        .collect::<FxHashSet<i64>>();
    let removed_sort_orders = old_sort_orders
        .difference(&new_sort_orders)
        .copied()
        .collect::<Vec<i64>>();
    let added_sort_orders = new_sort_orders
        .difference(&old_sort_orders)
        .copied()
        .collect::<Vec<i64>>();
    let default_sort_order_id = (previous_metadata.default_sort_order_id()
        != new_metadata.default_sort_order_id())
    .then_some(new_metadata.default_sort_order_id());

    let head_of_snapshot_log_changed =
        previous_metadata.history().last() != new_metadata.history().last();

    let n_removed_snapshot_log = previous_metadata.history().len().saturating_sub(
        new_metadata
            .history()
            .len()
            .saturating_sub(usize::from(head_of_snapshot_log_changed)),
    );

    let old_stats = previous_metadata
        .statistics_iter()
        .map(|s| s.snapshot_id)
        .collect::<FxHashSet<_>>();
    let new_stats = new_metadata
        .statistics_iter()
        .map(|s| s.snapshot_id)
        .collect::<FxHashSet<_>>();
    let removed_stats = old_stats
        .difference(&new_stats)
        .copied()
        .collect::<Vec<_>>();
    let added_stats = new_stats
        .difference(&old_stats)
        .copied()
        .collect::<Vec<_>>();

    let old_partition_stats = previous_metadata
        .partition_statistics_iter()
        .map(|s| s.snapshot_id)
        .collect::<FxHashSet<_>>();
    let new_partition_stats = new_metadata
        .partition_statistics_iter()
        .map(|s| s.snapshot_id)
        .collect::<FxHashSet<_>>();
    let removed_partition_stats = old_partition_stats
        .difference(&new_partition_stats)
        .copied()
        .collect::<Vec<_>>();
    let added_partition_stats = new_partition_stats
        .difference(&old_partition_stats)
        .copied()
        .collect::<Vec<_>>();

    let old_encryption_keys = previous_metadata
        .encryption_keys_iter()
        .map(|k| k.key_id().to_string())
        .collect::<FxHashSet<_>>();
    let new_encryption_keys = new_metadata
        .encryption_keys_iter()
        .map(|k| k.key_id().to_string())
        .collect::<FxHashSet<_>>();
    let removed_encryption_keys = old_encryption_keys
        .difference(&new_encryption_keys)
        .cloned()
        .collect::<Vec<_>>();
    let added_encryption_keys = new_encryption_keys
        .difference(&old_encryption_keys)
        .cloned()
        .collect::<Vec<_>>();

    TableMetadataDiffs {
        removed_snapshots: removed_snaps,
        added_snapshots,
        removed_schemas,
        added_schemas,
        new_current_schema_id,
        removed_partition_specs: removed_specs,
        added_partition_specs,
        default_partition_spec_id,
        removed_sort_orders,
        added_sort_orders,
        default_sort_order_id,
        head_of_snapshot_log_changed,
        n_removed_snapshot_log,
        expired_metadata_logs,
        added_metadata_log,
        added_stats,
        removed_stats,
        added_partition_stats,
        removed_partition_stats,
        removed_encryption_keys,
        added_encryption_keys,
    }
}

#[derive(Debug, Clone)]
pub struct TableMetadataDiffs {
    pub removed_snapshots: Vec<i64>,
    pub added_snapshots: Vec<i64>,
    pub removed_schemas: Vec<i32>,
    pub added_schemas: Vec<i32>,
    pub new_current_schema_id: Option<i32>,
    pub removed_partition_specs: Vec<i32>,
    pub added_partition_specs: Vec<i32>,
    pub default_partition_spec_id: Option<i32>,
    pub removed_sort_orders: Vec<i64>,
    pub added_sort_orders: Vec<i64>,
    pub default_sort_order_id: Option<i64>,
    pub head_of_snapshot_log_changed: bool,
    pub n_removed_snapshot_log: usize,
    pub expired_metadata_logs: usize,
    pub added_metadata_log: usize,
    pub added_stats: Vec<i64>,
    pub removed_stats: Vec<i64>,
    pub added_partition_stats: Vec<i64>,
    pub removed_partition_stats: Vec<i64>,
    pub added_encryption_keys: Vec<String>,
    pub removed_encryption_keys: Vec<String>,
}

pub(crate) fn determine_table_ident(
    parameters_ident: &TableIdent,
    request_ident: Option<&TableIdent>,
) -> Result<TableIdent> {
    let Some(identifier) = request_ident else {
        return Ok(parameters_ident.clone());
    };

    if identifier == parameters_ident {
        return Ok(identifier.clone());
    }

    // Below is for the tricky case: We have a conflict.
    // When querying a branch, spark sends something like the following as part of the `parameters`:
    // namespace: (<my>, <namespace>, <table_name>)
    // table_name: branch_<branch_name>
    let ns_parts = parameters_ident.namespace.clone().inner();
    let table_name_candidate = if ns_parts.len() >= 2 {
        NamespaceIdent::from_vec(ns_parts.iter().take(ns_parts.len() - 1).cloned().collect())
            .ok()
            .map(|n| TableIdent::new(n, ns_parts.last().cloned().unwrap_or_default()))
    } else {
        None
    };

    if table_name_candidate != Some(identifier.clone()) {
        return Err(ErrorModel::bad_request(
            "Table identifier in path does not match the one in the request body",
            "TableIdentifierMismatch",
            None,
        )
        .into());
    }

    Ok(identifier.clone())
}

pub(super) fn parse_location(location: &str, code: StatusCode) -> Result<Location> {
    Location::from_str(location)
        .map_err(|e| {
            ErrorModel::builder()
                .code(code.into())
                .message(format!("Invalid location: {e}"))
                .r#type("InvalidTableLocation".to_string())
                .build()
        })
        .map_err(Into::into)
}

pub(super) fn determine_tabular_location(
    namespace: &GetNamespaceResponse,
    request_table_location: Option<String>,
    table_id: TabularId,
    storage_profile: &StorageProfile,
) -> Result<Location> {
    let request_table_location = request_table_location
        .map(|l| parse_location(&l, StatusCode::BAD_REQUEST))
        .transpose()?;

    let mut location = if let Some(location) = request_table_location {
        storage_profile.require_allowed_location(&location)?;
        location
    } else {
        let namespace_props = NamespaceProperties::from_props_unchecked(
            namespace.properties.clone().unwrap_or_default(),
        );

        let namespace_location = match namespace_props.get_location() {
            Some(location) => location,
            None => storage_profile
                .default_namespace_location(namespace.namespace_id)
                .map_err(|e| {
                    ErrorModel::internal(
                        "Failed to generate default namespace location",
                        "InvalidDefaultNamespaceLocation",
                        Some(Box::new(e)),
                    )
                })?,
        };

        storage_profile.default_tabular_location(&namespace_location, table_id)
    };
    // all locations are without a trailing slash
    location.without_trailing_slash();
    Ok(location)
}

fn require_table_id(table_ident: &TableIdent, table_id: Option<TableId>) -> Result<TableId> {
    table_id.ok_or_else(|| {
        ErrorModel::not_found(
            format!(
                "Table '{}.{}' does not exist.",
                table_ident.namespace.to_url_string(),
                table_ident.name
            ),
            ErrorKind::TableNotFound.to_string(),
            None,
        )
        .into()
    })
}

fn require_not_staged<T>(metadata_location: Option<&T>) -> Result<()> {
    if metadata_location.is_none() {
        return Err(ErrorModel::not_found(
            "Table not found or staged.",
            ErrorKind::TableNotFound.to_string(),
            None,
        )
        .into());
    }

    Ok(())
}

fn take_table_metadata<T>(
    table_id: &TableId,
    table_ident: &TableIdent,
    metadatas: &mut HashMap<TableId, T>,
) -> Result<T> {
    metadatas
        .remove(table_id)
        .ok_or_else(|| {
            ErrorModel::not_found(
                format!(
                    "Table '{}.{}' does not exist.",
                    table_ident.namespace.to_url_string(),
                    table_ident.name
                ),
                ErrorKind::TableNotFound.to_string(),
                None,
            )
        })
        .map_err(Into::into)
}

pub(crate) fn require_active_warehouse(status: WarehouseStatus) -> Result<()> {
    if status != WarehouseStatus::Active {
        return Err(ErrorModel::builder()
            .code(StatusCode::NOT_FOUND.into())
            .message("Warehouse is not active".to_string())
            .r#type("WarehouseNotActive".to_string())
            .build()
            .into());
    }
    Ok(())
}

// Quick validation of properties for early fails.
// Full validation is performed when changes are applied.
fn validate_table_updates(updates: &[TableUpdate]) -> Result<()> {
    for update in updates {
        match update {
            TableUpdate::SetProperties { updates } => {
                validate_table_properties(updates.keys())?;
            }
            TableUpdate::RemoveProperties { removals } => {
                validate_table_properties(removals)?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn get_delete_after_commit_enabled(properties: &HashMap<String, String>) -> bool {
    properties
        .get(PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED)
        .map_or(PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT, |v| {
            matches!(v.to_lowercase().as_str(), "true" | "yes" | "1")
        })
}

pub(crate) fn validate_table_properties<'a, I>(properties: I) -> Result<()>
where
    I: IntoIterator<Item = &'a String>,
{
    for prop in properties {
        if (prop.starts_with("write.metadata")
            && ![
                PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX,
                PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED,
                "write.metadata.compression-codec",
            ]
            .contains(&prop.as_str()))
            || prop.starts_with("write.data.path")
        {
            return Err(ErrorModel::conflict(
                format!("Properties contain unsupported property: '{prop}'"),
                "FailedToSetProperties",
                None,
            )
            .into());
        }
    }

    Ok(())
}

pub(crate) fn validate_table_or_view_ident(table: &TableIdent) -> Result<()> {
    let TableIdent {
        ref namespace,
        ref name,
    } = &table;
    validate_namespace_ident(namespace)?;

    if name.is_empty() {
        return Err(ErrorModel::bad_request(
            "name of the identifier cannot be empty",
            "IdentifierNameEmpty",
            None,
        )
        .into());
    }
    Ok(())
}

// This function does not return a result but serde_json::Value::Null if serialization
// fails. This follows the rationale that we'll likely end up ignoring the error in the API handler
// anyway since we already effected the change and only the event emission about the change failed.
// Given that we are serializing stuff we've received as a json body and also successfully
// processed, it's unlikely to cause issues.
pub(crate) fn maybe_body_to_json(request: impl Serialize) -> serde_json::Value {
    if let Ok(body) = serde_json::to_value(&request) {
        body
    } else {
        tracing::warn!("Serializing the request body to json failed, this is very unexpected. It will not be part of any emitted Event.");
        serde_json::Value::Null
    }
}

pub(crate) fn create_table_request_into_table_metadata(
    table_id: TableId,
    request: CreateTableRequest,
) -> Result<TableMetadata> {
    let CreateTableRequest {
        name: _,
        location,
        schema,
        partition_spec,
        write_order,
        // Stage-create is already handled in the catalog service.
        // If stage-create is true, the metadata_location is None,
        // otherwise, it is the location of the metadata file.
        stage_create: _,
        mut properties,
    } = request;

    let location = location.ok_or_else(|| {
        ErrorModel::conflict(
            "Table location is required",
            "CreateTableLocationRequired",
            None,
        )
    })?;

    let format_version = properties
        .as_mut()
        .and_then(|props| props.remove(PROPERTY_FORMAT_VERSION))
        .map(|s| match s.as_str() {
            "v1" | "1" => Ok(FormatVersion::V1),
            "v2" | "2" => Ok(FormatVersion::V2),
            "v3" | "3" => Ok(FormatVersion::V3),
            _ => Err(ErrorModel::bad_request(
                format!("Invalid format version specified in table_properties: {s}"),
                "InvalidFormatVersion",
                None,
            )),
        })
        .transpose()?
        .unwrap_or(FormatVersion::V2);

    let table_metadata = TableMetadataBuilder::new(
        schema,
        partition_spec.unwrap_or(UnboundPartitionSpec::builder().build()),
        write_order.unwrap_or(SortOrder::unsorted_order()),
        location,
        format_version,
        properties.unwrap_or_default(),
    )
    .map_err(|e| {
        let msg = e.message().to_string();
        ErrorModel::bad_request(msg, "CreateTableMetadataError", Some(Box::new(e)))
    })?
    .assign_uuid(*table_id)
    .build()
    .map_err(|e| {
        let msg = e.message().to_string();
        ErrorModel::bad_request(msg, "BuildTableMetadataError", Some(Box::new(e)))
    })?
    .metadata;

    Ok(table_metadata)
}

#[cfg(test)]
pub(crate) mod test {
    use std::{collections::HashMap, str::FromStr};

    use http::StatusCode;
    use iceberg::{
        spec::{
            EncryptedKey, FormatVersion, NestedField, Operation, PrimitiveType, Schema, Snapshot,
            SnapshotReference, SnapshotRetention, Summary, TableMetadata, Transform, Type,
            UnboundPartitionField, UnboundPartitionSpec, MAIN_BRANCH, PROPERTY_FORMAT_VERSION,
            PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX,
        },
        NamespaceIdent, TableIdent, TableUpdate,
    };
    use iceberg_ext::catalog::rest::{
        CommitTableRequest, CreateNamespaceResponse, CreateTableRequest, LoadTableResult,
        RenameTableRequest,
    };
    use itertools::Itertools;
    use lakekeeper_io::Location;
    use sqlx::PgPool;
    use uuid::Uuid;

    use crate::{
        api::{
            iceberg::{
                types::{PageToken, Prefix},
                v1::{
                    tables::TablesService as _, DataAccess, DropParams, ListTablesQuery,
                    NamespaceParameters, TableParameters,
                },
            },
            management::v1::{
                table::TableManagementService, warehouse::TabularDeleteProfile,
                ApiServer as ManagementApiServer,
            },
            ApiContext,
        },
        catalog::{
            tables::validate_table_properties,
            test::{impl_pagination_tests, tabular_test_multi_warehouse_setup},
            Catalog, CatalogServer,
        },
        implementations::postgres::{
            tabular::table::tests::initialize_table, PostgresCatalog, SecretsState,
        },
        request_metadata::RequestMetadata,
        service::{
            authz::{
                tests::HidingAuthorizer, AllowAllAuthorizer, CatalogNamespaceAction,
                CatalogTableAction,
            },
            ListFlags, SecretStore, State, TableId, UserId,
        },
        tests::random_request_metadata,
        WarehouseId,
    };

    #[test]
    fn test_mixed_case_properties() {
        let properties = ["a".to_string(), "B".to_string()];
        assert!(validate_table_properties(properties.iter()).is_ok());
    }

    #[test]
    fn test_extract_count_from_metadata_location() {
        let location = Location::from_str("s3://path/to/table/metadata/00000-d0407fb2-1112-4944-bb88-c68ae697e2b4.gz.metadata.json").unwrap();
        let count = super::extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 0);

        let location = Location::from_str("s3://path/to/table/metadata/00010-d0407fb2-1112-4944-bb88-c68ae697e2b4.gz.metadata.json").unwrap();
        let count = super::extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 10);

        let location = Location::from_str(
            "s3://path/to/table/metadata/1-d0407fb2-1112-4944-bb88-c68ae697e2b4.gz.metadata.json",
        )
        .unwrap();
        let count = super::extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 1);

        let location = Location::from_str(
            "s3://path/to/table/metadata/10000010-d0407fb2-1112-4944-bb88-c68ae697e2b4.gz.metadata.json",
        )
            .unwrap();
        let count = super::extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 10_000_010);

        let location = Location::from_str(
            "s3://path/to/table/metadata/10000010-d0407fb2-1112-4944-bb88-c68ae697e2b4.metadata.json",
        )
            .unwrap();
        let count = super::extract_count_from_metadata_location(&location).unwrap();
        assert_eq!(count, 10_000_010);

        let location = Location::from_str(
            "s3://path/to/table/metadata/d0407fb2-1112-4944-bb88-c68ae697e2b4.metadata.json",
        )
        .unwrap();
        let count = super::extract_count_from_metadata_location(&location);
        assert!(count.is_none());
    }

    pub(crate) fn create_request(
        table_name: Option<String>,
        stage_create: Option<bool>,
    ) -> CreateTableRequest {
        CreateTableRequest {
            name: table_name.unwrap_or("my_table".to_string()),
            location: None,
            schema: Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "id",
                        iceberg::spec::Type::Primitive(PrimitiveType::Int),
                    )
                    .into(),
                    NestedField::required(
                        2,
                        "name",
                        iceberg::spec::Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            write_order: None,
            stage_create,
            properties: None,
        }
    }

    fn partition_spec() -> UnboundPartitionSpec {
        UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_field(2, "y", Transform::Identity)
            .unwrap()
            .build()
    }

    // Helper functions to reduce repetitive code in tests

    /// Creates a standard test schema with id and name fields
    fn create_test_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", iceberg::spec::Type::Primitive(PrimitiveType::Int))
                    .into(),
                NestedField::required(
                    2,
                    "name",
                    iceberg::spec::Type::Primitive(PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    /// Creates a `CreateTableRequest` with the given name and format version
    fn create_table_request(
        name: &str,
        format_version: Option<FormatVersion>,
    ) -> CreateTableRequest {
        let mut properties = None;
        if let Some(version) = format_version {
            properties = Some(HashMap::from([(
                PROPERTY_FORMAT_VERSION.to_string(),
                match version {
                    FormatVersion::V1 => "1".to_string(),
                    FormatVersion::V2 => "2".to_string(),
                    FormatVersion::V3 => "3".to_string(),
                },
            )]));
        }

        CreateTableRequest {
            name: name.to_string(),
            location: None,
            schema: create_test_schema(),
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            write_order: None,
            stage_create: Some(false),
            properties,
        }
    }

    /// Helper to load a table using `CatalogServer`
    async fn load_table(
        ctx: &ApiContext<
            State<impl crate::service::authz::Authorizer, impl Catalog, impl SecretStore>,
        >,
        ns_params: &NamespaceParameters,
        table_name: &str,
    ) -> LoadTableResult {
        let table_ident = TableIdent {
            namespace: ns_params.namespace.clone(),
            name: table_name.to_string(),
        };

        CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident,
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap()
    }

    /// Helper to commit table changes
    async fn commit_table_changes(
        ctx: &ApiContext<
            State<impl crate::service::authz::Authorizer, impl Catalog, impl SecretStore>,
        >,
        ns_params: &NamespaceParameters,
        table_ident: &TableIdent,
        updates: Vec<TableUpdate>,
    ) -> super::CommitContext {
        super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
    }

    /// Helper to create a standard snapshot for testing
    fn create_test_snapshot_v3(
        snapshot_id: i64,
        timestamp_ms: i64,
        sequence_number: i64,
        manifest_list: &str,
        row_range: Option<(u64, u64)>,
        added_records: u64,
        key_id: &str,
    ) -> Snapshot {
        let base_builder = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_timestamp_ms(timestamp_ms)
            .with_sequence_number(sequence_number)
            .with_schema_id(0)
            .with_manifest_list(manifest_list)
            .with_encryption_key_id(Some(key_id.to_string()))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    ("added-data-files".to_string(), "1".to_string()),
                    ("added-records".to_string(), added_records.to_string()),
                ]),
            });

        if let Some((first_row_id, added_rows_count)) = row_range {
            base_builder
                .with_row_range(first_row_id, added_rows_count)
                .build()
        } else {
            base_builder.build()
        }
    }

    /// Helper to create a snapshot reference
    fn create_snapshot_reference(snapshot_id: i64) -> SnapshotReference {
        SnapshotReference {
            snapshot_id,
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep: Some(10),
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        }
    }

    #[sqlx::test]
    async fn test_set_properties_commit_table(pool: sqlx::PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;

        let table_metadata = table
            .metadata
            .into_builder(table.metadata_location)
            .set_properties(HashMap::from([
                ("p1".into(), "v2".into()),
                ("p2".into(), "v2".into()),
            ]))
            .unwrap()
            .build()
            .unwrap();
        let updates = table_metadata.changes;
        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(TableIdent {
                        namespace: ns.namespace.clone(),
                        name: "tab-1".to_string(),
                    }),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .new_metadata;

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix,
                table: TableIdent {
                    namespace: ns.namespace.clone(),
                    name: "tab-1".to_string(),
                },
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_table_metadata_are_equal(&table_metadata.metadata, &tab.metadata);
    }

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "y", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(3, "z", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap()
    }

    fn assert_table_metadata_are_equal(expected: &TableMetadata, actual: &TableMetadata) {
        assert_eq!(actual.location(), expected.location());
        assert_eq!(actual.properties(), expected.properties());
        assert_eq!(
            actual
                .snapshots()
                .sorted_by_key(|s| s.snapshot_id())
                .collect_vec(),
            expected
                .snapshots()
                .sorted_by_key(|s| s.snapshot_id())
                .collect_vec()
        );
        assert_eq!(
            actual
                .partition_specs_iter()
                .sorted_by_key(|ps| ps.spec_id())
                .collect_vec(),
            expected
                .partition_specs_iter()
                .sorted_by_key(|ps| ps.spec_id())
                .collect_vec()
        );
        assert_eq!(
            actual
                .partition_statistics_iter()
                .sorted_by_key(|s| (s.snapshot_id, &s.statistics_path))
                .collect_vec(),
            expected
                .partition_statistics_iter()
                .sorted_by_key(|s| (s.snapshot_id, &s.statistics_path))
                .collect_vec()
        );
        assert_eq!(
            actual
                .sort_orders_iter()
                .sorted_by_key(|s| s.order_id)
                .collect_vec(),
            expected
                .sort_orders_iter()
                .sorted_by_key(|s| s.order_id)
                .collect_vec()
        );
        assert_eq!(
            actual
                .statistics_iter()
                .sorted_by_key(|s| (s.snapshot_id, &s.statistics_path))
                .collect_vec(),
            expected
                .statistics_iter()
                .sorted_by_key(|s| (s.snapshot_id, &s.statistics_path))
                .collect_vec()
        );
        assert_eq!(actual.history(), expected.history());
        assert_eq!(actual.current_schema_id(), expected.current_schema_id());
        assert_eq!(actual.current_snapshot_id(), expected.current_snapshot_id());
        assert_eq!(
            actual.default_partition_spec(),
            expected.default_partition_spec()
        );
        assert_eq!(actual.default_sort_order(), expected.default_sort_order());
        assert_eq!(actual.format_version(), expected.format_version());
        assert_eq!(actual.last_column_id(), expected.last_column_id());
        assert_eq!(
            actual.last_sequence_number(),
            expected.last_sequence_number()
        );
        assert_eq!(actual.last_partition_id(), expected.last_partition_id());
    }

    #[sqlx::test]
    async fn test_add_partition_spec_commit_table(pool: sqlx::PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;

        let added_spec = UnboundPartitionSpec::builder()
            .with_spec_id(10)
            .add_partition_fields(vec![
                UnboundPartitionField {
                    // The previous field - has field_id set
                    name: "y".to_string(),
                    transform: Transform::Identity,
                    source_id: 2,
                    field_id: Some(1000),
                },
                UnboundPartitionField {
                    // A new field without field id - should still be without field id in changes
                    name: "z".to_string(),
                    transform: Transform::Identity,
                    source_id: 3,
                    field_id: None,
                },
            ])
            .unwrap()
            .build();

        let table_metadata = table
            .metadata
            .into_builder(table.metadata_location)
            .add_schema(schema())
            .unwrap()
            .set_current_schema(-1)
            .unwrap()
            .add_partition_spec(partition_spec())
            .unwrap()
            .add_partition_spec(added_spec.clone())
            .unwrap()
            .build()
            .unwrap();

        let updates = table_metadata.changes;
        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(TableIdent {
                        namespace: ns.namespace.clone(),
                        name: "tab-1".to_string(),
                    }),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix,
                table: TableIdent {
                    namespace: ns.namespace.clone(),
                    name: "tab-1".to_string(),
                },
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_table_metadata_are_equal(&table_metadata.metadata, &tab.metadata);
    }

    #[sqlx::test]
    async fn test_set_default_partition_spec(pool: PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;

        let added_spec = UnboundPartitionSpec::builder()
            .with_spec_id(10)
            .add_partition_field(1, "y_bucket[2]", Transform::Bucket(2))
            .unwrap()
            .build();

        let table_metadata = table
            .metadata
            .into_builder(table.metadata_location)
            .add_partition_spec(added_spec)
            .unwrap()
            .set_default_partition_spec(-1)
            .unwrap()
            .build()
            .unwrap();
        let updates = table_metadata.changes;

        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(TableIdent {
                        namespace: ns.namespace.clone(),
                        name: "tab-1".to_string(),
                    }),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .new_metadata;

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix,
                table: TableIdent {
                    namespace: ns.namespace.clone(),
                    name: "tab-1".to_string(),
                },
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_table_metadata_are_equal(&table_metadata.metadata, &tab.metadata);
    }

    #[sqlx::test]
    async fn test_set_ref(pool: PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;
        let last_updated = table.metadata.last_updated_ms();
        let builder = table.metadata.into_builder(table.metadata_location);

        let snapshot = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(last_updated + 1)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    (
                        "spark.app.id".to_string(),
                        "local-1662532784305".to_string(),
                    ),
                    ("added-data-files".to_string(), "4".to_string()),
                    ("added-records".to_string(), "4".to_string()),
                    ("added-files-size".to_string(), "6001".to_string()),
                ]),
            })
            .build();

        let builder = builder
            .add_snapshot(snapshot.clone())
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference {
                    snapshot_id: 1,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: Some(10),
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            )
            .unwrap()
            .build()
            .unwrap();
        let updates = builder.changes;

        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(TableIdent {
                        namespace: ns.namespace.clone(),
                        name: "tab-1".to_string(),
                    }),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: TableIdent {
                    namespace: ns.namespace.clone(),
                    name: "tab-1".to_string(),
                },
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(tab.metadata, builder.metadata);
    }

    #[sqlx::test]
    async fn test_expire_metadata_log(pool: PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pool).await;
        let table_ident = TableIdent {
            namespace: ns.namespace.clone(),
            name: "tab-1".to_string(),
        };
        let builder = table
            .metadata
            .into_builder(table.metadata_location)
            .set_properties(HashMap::from_iter([(
                PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX.to_string(),
                "2".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();
        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_table_metadata_are_equal(&builder.metadata, &tab.metadata);

        let builder = builder
            .metadata
            .into_builder(tab.metadata_location)
            .set_properties(HashMap::from_iter(vec![(
                "change_nr".to_string(),
                "1".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();

        let committed = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_table_metadata_are_equal(&builder.metadata, &tab.metadata);

        let builder = committed
            .new_metadata
            .into_builder(tab.metadata_location)
            .set_properties(HashMap::from_iter(vec![(
                "change_nr".to_string(),
                "2".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap();

        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix,
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_table_metadata_are_equal(&builder.metadata, &tab.metadata);
    }

    #[sqlx::test]
    async fn test_default_format_version_is_v2(pg_pool: PgPool) {
        let (ctx, _ns, ns_params, _) = table_test_setup(pg_pool).await;
        let create_request = create_table_request("my_table", None);
        let table = CatalogServer::create_table(
            ns_params.clone(),
            create_request,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(table.metadata.format_version(), FormatVersion::V2);
    }

    #[sqlx::test]
    #[allow(clippy::too_many_lines)]
    async fn test_table_v3(pg_pool: PgPool) {
        let (ctx, ns, ns_params, _) = table_test_setup(pg_pool).await;
        let create_request = create_table_request("my_table", Some(FormatVersion::V3));
        let table = CatalogServer::create_table(
            ns_params.clone(),
            create_request,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(table.metadata.format_version(), FormatVersion::V3);
        assert_eq!(table.metadata.next_row_id(), 0);

        // Create table identifier for commits
        let table_ident = TableIdent {
            namespace: ns.namespace.clone(),
            name: "my_table".to_string(),
        };

        // Add a snapshot with row_range (0, 100)
        let last_updated = table.metadata.last_updated_ms();

        let snapshot1 = create_test_snapshot_v3(
            1,
            last_updated + 1,
            1,
            "/snap-1.avro",
            Some((0, 100)),
            100,
            "key-1",
        );

        // Commit using Catalog
        let encryption_key = EncryptedKey::builder()
            .key_id("key-1")
            .encrypted_key_metadata("key-metadata".as_bytes().to_vec())
            .encrypted_by_id("my-vault".to_string())
            .build();

        commit_table_changes(
            &ctx,
            &ns_params,
            &table_ident,
            vec![
                TableUpdate::AddSnapshot {
                    snapshot: snapshot1,
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: create_snapshot_reference(1),
                },
                TableUpdate::AddEncryptionKey {
                    encryption_key: encryption_key.clone(),
                },
            ],
        )
        .await;

        // Load using Catalog and assert next_row_id = 100
        let loaded_table = load_table(&ctx, &ns_params, "my_table").await;
        assert_eq!(loaded_table.metadata.next_row_id(), 100);
        let current_snapshot = loaded_table
            .metadata
            .current_snapshot()
            .expect("There should be a current snapshot");
        assert_eq!(current_snapshot.snapshot_id(), 1);
        assert_eq!(current_snapshot.row_range(), Some((0, 100)));
        assert_eq!(
            loaded_table.metadata.encryption_key("key-1"),
            Some(&encryption_key)
        );
        assert_eq!(current_snapshot.encryption_key_id(), Some("key-1"));

        let snapshot2_invalid = create_test_snapshot_v3(
            2,
            last_updated + 2,
            2,
            "/snap-2-invalid.avro",
            Some((50, 100)),
            100,
            "key-1",
        );

        // This commit should fail due to row range overlap
        let invalid_commit_result = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: vec![TableUpdate::AddSnapshot {
                        snapshot: snapshot2_invalid,
                    }],
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await;

        // Assert that the commit fails
        assert!(invalid_commit_result.is_err());
        let err_string = invalid_commit_result.as_ref().unwrap_err().to_string();
        assert!(
            err_string.contains("first-row-id is behind table next-row-id"),
            "The error message `{err_string}` did not contain the expected text",
        );

        // Add another snapshot with row_range (100, 50) - this should succeed
        // because it doesn't overlap (rows 100-149)
        let loaded_table2 = load_table(&ctx, &ns_params, "my_table").await;

        assert_eq!(loaded_table2.metadata.next_row_id(), 100);
        assert_eq!(loaded_table2.metadata.format_version(), FormatVersion::V3);

        let snapshot3_valid = create_test_snapshot_v3(
            3,
            last_updated + 3,
            2,
            "/snap-3-valid.avro",
            Some((100, 50)), // first_row_id: 100, added_rows_count: 50
            50,              // added_records: 50
            "key-1",
        );

        // This commit should succeed
        commit_table_changes(
            &ctx,
            &ns_params,
            &table_ident,
            vec![TableUpdate::AddSnapshot {
                snapshot: snapshot3_valid,
            }],
        )
        .await;

        // Load again and check next_row_id should now be 150
        let final_table = load_table(&ctx, &ns_params, "my_table").await;

        assert_eq!(final_table.metadata.next_row_id(), 150);
        println!(
            "Available snapshot ids: {:?}",
            final_table
                .metadata
                .snapshots()
                .map(|s| s.snapshot_id())
                .collect::<Vec<_>>()
        );
        let snapshot = final_table.metadata.snapshot_by_id(3).unwrap();
        assert_eq!(snapshot.row_range(), Some((100, 50)));
        assert_eq!(snapshot.manifest_list(), "/snap-3-valid.avro");
    }

    #[sqlx::test]
    async fn test_v2_to_v3_migration(pg_pool: PgPool) {
        let (ctx, ns, ns_params, _) = table_test_setup(pg_pool).await;

        // Create a v2 table (default version)
        let create_request = CreateTableRequest {
            name: "my_migration_table".to_string(),
            location: None,
            schema: Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "id",
                        iceberg::spec::Type::Primitive(PrimitiveType::Int),
                    )
                    .into(),
                    NestedField::required(
                        2,
                        "name",
                        iceberg::spec::Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
            partition_spec: Some(UnboundPartitionSpec::builder().build()),
            write_order: None,
            stage_create: Some(false),
            properties: None, // No format version specified, should default to V2
        };

        let table = CatalogServer::create_table(
            ns_params.clone(),
            create_request,
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Verify it's a V2 table
        assert_eq!(table.metadata.format_version(), FormatVersion::V2);

        // Create table identifier for commits
        let table_ident = TableIdent {
            namespace: ns.namespace.clone(),
            name: "my_migration_table".to_string(),
        };

        // Add a snapshot to the V2 table (without row_range)
        let last_updated = table.metadata.last_updated_ms();
        let builder = table.metadata.into_builder(table.metadata_location);

        let snapshot1 = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(last_updated + 1)
            .with_sequence_number(1)
            .with_schema_id(0)
            .with_manifest_list("/snap-1.avro")
            // No row_range for V2 table
            .with_row_range(0, 50) // row_range is ignored in V2
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    ("added-data-files".to_string(), "1".to_string()),
                    ("added-records".to_string(), "100".to_string()),
                ]),
            })
            .build();

        let builder = builder
            .add_snapshot(snapshot1)
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference {
                    snapshot_id: 1,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: Some(10),
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            )
            .unwrap()
            .build()
            .unwrap();

        // Commit the snapshot
        super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Load table and verify it's still V2
        let loaded_table_v2 = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(loaded_table_v2.metadata.format_version(), FormatVersion::V2);

        // Upgrade to V3 using TableUpdate::UpgradeFormatVersion
        super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: vec![TableUpdate::UpgradeFormatVersion {
                        format_version: FormatVersion::V3,
                    }],
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Load table again -> should be V3 and next_row_id should be 0 (NULL equivalent)
        let loaded_table_v3 = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(loaded_table_v3.metadata.format_version(), FormatVersion::V3);
        assert_eq!(loaded_table_v3.metadata.next_row_id(), 0); // Should be 0 after migration

        // Add a snapshot with row_range to the V3 table
        let snapshot2 = Snapshot::builder()
            .with_snapshot_id(2)
            .with_timestamp_ms(last_updated + 2)
            .with_sequence_number(2)
            .with_schema_id(0)
            .with_manifest_list("/snap-2.avro")
            .with_row_range(0, 50) // first_row_id: 0, added_rows_count: 50
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    ("added-data-files".to_string(), "1".to_string()),
                    ("added-records".to_string(), "50".to_string()),
                ]),
            })
            .build();

        super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: vec![TableUpdate::AddSnapshot {
                        snapshot: snapshot2,
                    }],
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Load table -> next_row_id should now be increased to 50
        let final_table = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(final_table.metadata.format_version(), FormatVersion::V3);
        assert_eq!(final_table.metadata.next_row_id(), 50);
    }

    #[sqlx::test]
    async fn test_remove_snapshot_commit(pg_pool: PgPool) {
        let (ctx, ns, ns_params, table) = commit_test_setup(pg_pool).await;
        let table_ident = TableIdent {
            namespace: ns.namespace.clone(),
            name: "tab-1".to_string(),
        };
        let last_updated = table.metadata.last_updated_ms();
        let builder = table.metadata.into_builder(table.metadata_location);

        let snap = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(last_updated + 1)
            .with_sequence_number(0)
            .with_schema_id(0)
            .with_manifest_list("/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    (
                        "spark.app.id".to_string(),
                        "local-1662532784305".to_string(),
                    ),
                    ("added-data-files".to_string(), "4".to_string()),
                    ("added-records".to_string(), "4".to_string()),
                    ("added-files-size".to_string(), "6001".to_string()),
                ]),
            })
            .build();

        let builder = builder
            .add_snapshot(snap)
            .unwrap()
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference {
                    snapshot_id: 1,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: Some(10),
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            )
            .unwrap()
            .build()
            .unwrap();

        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates: builder.changes,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(tab.metadata.history(), builder.metadata.history());
        assert_eq!(tab.metadata, builder.metadata);

        assert_json_diff::assert_json_eq!(
            serde_json::to_value(tab.metadata.clone()).unwrap(),
            serde_json::to_value(builder.metadata.clone()).unwrap()
        );

        let last_updated = tab.metadata.last_updated_ms();
        let builder = builder.metadata.into_builder(tab.metadata_location);

        let snap = Snapshot::builder()
            .with_snapshot_id(2)
            .with_parent_snapshot_id(Some(1))
            .with_timestamp_ms(last_updated + 1)
            .with_sequence_number(1)
            .with_schema_id(0)
            .with_manifest_list("/snap-2.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    (
                        "spark.app.id".to_string(),
                        "local-1662532784305".to_string(),
                    ),
                    ("added-data-files".to_string(), "4".to_string()),
                    ("added-records".to_string(), "4".to_string()),
                    ("added-files-size".to_string(), "6001".to_string()),
                ]),
            })
            .build();

        let builder = builder.add_snapshot(snap).unwrap().build().unwrap();

        let updates = builder.changes;

        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(tab.metadata, builder.metadata);

        let last_updated = tab.metadata.last_updated_ms();
        let builder = builder.metadata.into_builder(tab.metadata_location);

        let snap = Snapshot::builder()
            .with_snapshot_id(3)
            .with_timestamp_ms(last_updated + 1)
            .with_parent_snapshot_id(Some(2))
            .with_sequence_number(2)
            .with_schema_id(0)
            .with_manifest_list("/snap-2.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from_iter(vec![
                    (
                        "spark.app.id".to_string(),
                        "local-1662532784305".to_string(),
                    ),
                    ("added-data-files".to_string(), "4".to_string()),
                    ("added-records".to_string(), "4".to_string()),
                    ("added-files-size".to_string(), "6001".to_string()),
                ]),
            })
            .build();

        let builder = builder.add_snapshot(snap).unwrap().build().unwrap();

        let updates = builder.changes;

        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(tab.metadata, builder.metadata);

        let builder = builder
            .metadata
            .into_builder(tab.metadata_location)
            .remove_snapshots(&[2])
            .build()
            .unwrap();

        let updates = builder.changes;

        let _ = super::commit_tables_with_authz(
            ns_params.prefix.clone(),
            super::CommitTransactionRequest {
                table_changes: vec![CommitTableRequest {
                    identifier: Some(table_ident.clone()),
                    requirements: vec![],
                    updates,
                }],
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let tab = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(tab.metadata.history(), builder.metadata.history());
        assert_eq!(
            tab.metadata
                .snapshots()
                .sorted_by_key(|s| s.snapshot_id())
                .collect_vec(),
            builder
                .metadata
                .snapshots()
                .sorted_by_key(|s| s.snapshot_id())
                .collect_vec()
        );
        assert_table_metadata_are_equal(&builder.metadata, &tab.metadata);
    }

    async fn commit_test_setup(
        pool: PgPool,
    ) -> (
        ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
        CreateNamespaceResponse,
        NamespaceParameters,
        LoadTableResult,
    ) {
        let (ctx, ns, ns_params, _) = table_test_setup(pool).await;
        let table = CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some("tab-1".to_string()), Some(false)),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        (ctx, ns, ns_params, table)
    }

    async fn table_test_setup(
        pool: PgPool,
    ) -> (
        ApiContext<State<AllowAllAuthorizer, PostgresCatalog, SecretsState>>,
        CreateNamespaceResponse,
        NamespaceParameters,
        String,
    ) {
        let prof = crate::catalog::test::memory_io_profile();
        let base_loc = prof.base_location().unwrap().to_string();
        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            None,
        )
        .await;
        let ns = crate::catalog::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "ns1".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns.namespace.clone(),
        };
        (ctx, ns, ns_params, base_loc)
    }

    #[sqlx::test]
    async fn test_can_create_tables_with_same_prefix_1(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();
        let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/my-table-2"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/my-table"));

        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn test_can_create_tables_with_same_prefix_2(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();
        let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/my-table"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/my-table-2"));

        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn test_cannot_create_table_at_same_location(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();
        let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket"));

        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let e = CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect_err("Table was created at same location which should not be possible");
        assert_eq!(e.error.code, StatusCode::BAD_REQUEST, "{e:?}");
        assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
    }

    #[sqlx::test]
    async fn test_cannot_create_staged_tables_at_sublocations_1(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();
        let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
        create_request_1.stage_create = Some(true);
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket/inner"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
        create_request_2.stage_create = Some(true);
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket"));
        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let e = CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect_err("Staged table could be created at sublocation which should not be possible");
        assert_eq!(e.error.code, StatusCode::BAD_REQUEST, "{e:?}");
        assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
    }

    #[sqlx::test]
    async fn test_cannot_create_staged_tables_at_sublocations_2(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();
        let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
        create_request_1.stage_create = Some(true);
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
        create_request_2.stage_create = Some(true);
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket/inner"));
        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let e = CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect_err("Staged table could be created at sublocation which should not be possible");
        assert_eq!(e.error.code, StatusCode::BAD_REQUEST, "{e:?}");
        assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
    }

    #[sqlx::test]
    async fn test_cannot_create_tables_at_sublocations_1(pool: PgPool) {
        let (ctx, _, ns_params, base_location) = table_test_setup(pool).await;
        let tmp_id = Uuid::now_v7();

        let mut create_request_1 = create_request(Some("tab-1".to_string()), Some(false));
        create_request_1.location = Some(format!("{base_location}/{tmp_id}/bucket"));
        let mut create_request_2 = create_request(Some("tab-2".to_string()), Some(false));
        create_request_2.location = Some(format!("{base_location}/{tmp_id}/bucket/sublocation"));
        let _ = CatalogServer::create_table(
            ns_params.clone(),
            create_request_1,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        let e = CatalogServer::create_table(
            ns_params.clone(),
            create_request_2,
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect_err("Staged table could be created at sublocation which should not be possible");
        assert_eq!(e.error.code, StatusCode::BAD_REQUEST, "{e:?}");
        assert_eq!(e.error.r#type.as_str(), "LocationAlreadyTaken");
    }

    async fn pagination_test_setup(
        pool: PgPool,
        n_tables: usize,
        hidden_ranges: &[(usize, usize)],
    ) -> (
        ApiContext<State<HidingAuthorizer, PostgresCatalog, SecretsState>>,
        NamespaceParameters,
    ) {
        let prof = crate::catalog::test::memory_io_profile();
        let base_location = prof.base_location().unwrap();
        let authz = HidingAuthorizer::new();
        // Prevent hidden tables from becoming visible through `can_list_everything`.
        authz.block_can_list_everything();

        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        let ns = crate::catalog::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "ns1".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns.namespace.clone(),
        };
        for i in 0..n_tables {
            let mut create_request = create_request(Some(format!("{i}")), Some(false));
            create_request.location = Some(format!("{base_location}/bucket/{i}"));
            let tab = CatalogServer::create_table(
                ns_params.clone(),
                create_request,
                DataAccess::not_specified(),
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap();
            for (start, end) in hidden_ranges.iter().copied() {
                if i >= start && i < end {
                    authz.hide(&format!(
                        "table:{}/{}",
                        warehouse.warehouse_id,
                        tab.metadata.uuid()
                    ));
                }
            }
        }

        (ctx, ns_params)
    }

    impl_pagination_tests!(
        table,
        pagination_test_setup,
        CatalogServer,
        ListTablesQuery,
        identifiers,
        |tid| { tid.name }
    );

    #[sqlx::test]
    async fn test_table_pagination(pool: sqlx::PgPool) {
        let prof = crate::catalog::test::memory_io_profile();

        let authz = HidingAuthorizer::new();
        // Prevent hidden tables from becoming visible through `can_list_everything`.
        authz.block_can_list_everything();

        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        let ns = crate::catalog::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "ns1".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns.namespace.clone(),
        };
        // create 10 staged tables
        for i in 0..10 {
            let _ = CatalogServer::create_table(
                ns_params.clone(),
                create_request(Some(format!("tab-{i}")), Some(false)),
                DataAccess {
                    vended_credentials: true,
                    remote_signing: false,
                },
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap();
        }

        // list 1 more than existing tables
        let all = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(11),
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(all.identifiers.len(), 10);

        // list exactly amount of existing tables
        let all = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(10),
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(all.identifiers.len(), 10);

        // next page is empty
        let next = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::Present(all.next_page_token.unwrap()),
                page_size: Some(10),
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(next.identifiers.len(), 0);
        assert!(next.next_page_token.is_none());

        let first_six = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(6),
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(first_six.identifiers.len(), 6);
        assert!(first_six.next_page_token.is_some());
        let first_six_items = first_six
            .identifiers
            .iter()
            .map(|i| i.name.clone())
            .sorted()
            .collect::<Vec<_>>();

        for (i, item) in first_six_items.iter().enumerate().take(6) {
            assert_eq!(item, &format!("tab-{i}"));
        }

        let next_four = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::Present(first_six.next_page_token.unwrap()),
                page_size: Some(6),
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(next_four.identifiers.len(), 4);
        // page-size > number of items left -> no next page
        assert!(next_four.next_page_token.is_none());

        let next_four_items = next_four
            .identifiers
            .iter()
            .map(|i| i.name.clone())
            .sorted()
            .collect::<Vec<_>>();

        for (idx, i) in (6..10).enumerate() {
            assert_eq!(next_four_items[idx], format!("tab-{i}"));
        }

        let mut ids = all.table_uuids.unwrap();
        ids.sort();
        for t in ids.iter().take(6).skip(4) {
            authz.hide(&format!("table:{}/{t}", warehouse.warehouse_id));
        }

        let page = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(5),
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(page.identifiers.len(), 5);
        assert!(page.next_page_token.is_some());
        let page_items = page
            .identifiers
            .iter()
            .map(|i| i.name.clone())
            .sorted()
            .collect::<Vec<_>>();
        for (i, item) in page_items.iter().enumerate() {
            let tab_id = if i > 3 { i + 2 } else { i };
            assert_eq!(item, &format!("tab-{tab_id}"));
        }

        let next_page = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::Present(page.next_page_token.unwrap()),
                page_size: Some(6),
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        assert_eq!(next_page.identifiers.len(), 3);

        let next_page_items = next_page
            .identifiers
            .iter()
            .map(|i| i.name.clone())
            .sorted()
            .collect::<Vec<_>>();

        for (idx, i) in (7..10).enumerate() {
            assert_eq!(next_page_items[idx], format!("tab-{i}"));
        }
    }

    #[sqlx::test]
    async fn test_list_tables(pool: sqlx::PgPool) {
        let prof = crate::catalog::test::memory_io_profile();

        let authz = HidingAuthorizer::new();

        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;
        let ns = crate::catalog::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "ns1".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: ns.namespace.clone(),
        };
        // create 10 staged tables
        for i in 0..10 {
            let _ = CatalogServer::create_table(
                ns_params.clone(),
                create_request(Some(format!("tab-{i}")), Some(false)),
                DataAccess {
                    vended_credentials: true,
                    remote_signing: false,
                },
                ctx.clone(),
                RequestMetadata::new_unauthenticated(),
            )
            .await
            .unwrap();
        }

        // By default `HidingAuthorizer` allows everything, meaning the quick check path in
        // `list_tables` will be hit since `can_list_everything: true`.
        let all = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(11),
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(all.identifiers.len(), 10);

        // Block `can_list_everything` to hit alternative code path.
        ctx.v1_state.authz.block_can_list_everything();
        let all = CatalogServer::list_tables(
            ns_params.clone(),
            ListTablesQuery {
                page_token: PageToken::NotSpecified,
                page_size: Some(11),
                return_uuids: true,
                return_protection_status: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
        assert_eq!(all.identifiers.len(), 10);
    }

    #[sqlx::test]
    async fn test_cannot_drop_protected_table(pool: PgPool) {
        let (ctx, _, ns_params, _) = table_test_setup(pool).await;
        let table_ident = TableIdent {
            namespace: ns_params.namespace.clone(),
            name: "tab-1".to_string(),
        };
        let tab = CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some("tab-1".to_string()), Some(false)),
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        ManagementApiServer::set_table_protection(
            tab.metadata.uuid().into(),
            WarehouseId::from_str(ns_params.prefix.clone().unwrap().as_str()).unwrap(),
            true,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        let e = CatalogServer::drop_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DropParams {
                purge_requested: true,
                force: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect_err("Table was dropped which should not be possible");
        assert_eq!(e.error.code, StatusCode::CONFLICT, "{e:?}");

        ManagementApiServer::set_table_protection(
            tab.metadata.uuid().into(),
            WarehouseId::from_str(ns_params.prefix.clone().unwrap().as_str()).unwrap(),
            false,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        CatalogServer::drop_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DropParams {
                purge_requested: true,
                force: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();
    }

    #[sqlx::test]
    async fn test_can_force_drop_protected_table(pool: PgPool) {
        let (ctx, _, ns_params, _) = table_test_setup(pool).await;
        let table_ident = TableIdent {
            namespace: ns_params.namespace.clone(),
            name: "tab-1".to_string(),
        };
        let tab = CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some("tab-1".to_string()), Some(false)),
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        ManagementApiServer::set_table_protection(
            tab.metadata.uuid().into(),
            WarehouseId::from_str(ns_params.prefix.clone().unwrap().as_str()).unwrap(),
            true,
            ctx.clone(),
            random_request_metadata(),
        )
        .await
        .unwrap();

        CatalogServer::drop_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            DropParams {
                purge_requested: true,
                force: true,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect("Table couldn't be force dropped which should be possible");
    }

    #[sqlx::test]
    async fn test_rename_table_without_can_rename(pool: sqlx::PgPool) {
        let prof = crate::catalog::test::memory_io_profile();
        let authz = HidingAuthorizer::new();
        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;

        let from_ns = crate::catalog::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "from_ns".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: from_ns.namespace.clone(),
        };
        let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
        let table_name = "from_table".to_string();
        CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some(table_name.clone()), Some(false)),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Not authorized to rename the source table
        authz.block_action(format!("table:{}", CatalogTableAction::CanRename).as_str());
        let response = CatalogServer::rename_table(
            prefix,
            RenameTableRequest {
                source: TableIdent {
                    namespace: ns_params.namespace.clone(),
                    name: table_name.clone(),
                },
                destination: TableIdent {
                    namespace: NamespaceIdent::new("to_ns".to_string()),
                    name: table_name,
                },
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap_err();

        assert_eq!(response.error.code, StatusCode::NOT_FOUND);
        assert_eq!(response.error.r#type, "TableActionForbidden");
    }

    #[sqlx::test]
    async fn test_rename_table_without_can_create(pool: sqlx::PgPool) {
        let prof = crate::catalog::test::memory_io_profile();
        let authz = HidingAuthorizer::new();
        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            authz.clone(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;

        let from_ns = crate::catalog::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "from_ns".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: from_ns.namespace.clone(),
        };
        let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
        let table_name = "from_table".to_string();
        CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some(table_name.clone()), Some(false)),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Not authorized to create a table in the destination namepsace
        authz.block_action(format!("table:{}", CatalogNamespaceAction::CanCreateTable).as_str());
        let response = CatalogServer::rename_table(
            prefix,
            RenameTableRequest {
                source: TableIdent {
                    namespace: ns_params.namespace.clone(),
                    name: table_name.clone(),
                },
                destination: TableIdent {
                    namespace: NamespaceIdent::new("to_ns".to_string()),
                    name: table_name,
                },
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap_err();

        assert_eq!(response.error.code, StatusCode::NOT_FOUND);
        assert_eq!(response.error.r#type, "NoSuchNamespaceException");
    }

    #[sqlx::test]
    async fn test_rename_table_without_target_namespace(pool: sqlx::PgPool) {
        let prof = crate::catalog::test::memory_io_profile();
        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;

        let from_ns = crate::catalog::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "from_ns".to_string(),
        )
        .await;
        let ns_params = NamespaceParameters {
            prefix: Some(Prefix(warehouse.warehouse_id.to_string())),
            namespace: from_ns.namespace.clone(),
        };
        let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
        let table_name = "from_table".to_string();
        CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some(table_name.clone()), Some(false)),
            DataAccess {
                vended_credentials: true,
                remote_signing: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // All actions are allowed but the target namespace does not exist
        let response = CatalogServer::rename_table(
            prefix,
            RenameTableRequest {
                source: TableIdent {
                    namespace: ns_params.namespace.clone(),
                    name: table_name.clone(),
                },
                destination: TableIdent {
                    namespace: NamespaceIdent::new("to_ns".to_string()),
                    name: table_name,
                },
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap_err();

        assert_eq!(response.error.code, StatusCode::NOT_FOUND);
        assert_eq!(response.error.r#type, "NoSuchNamespaceException");
    }

    #[sqlx::test]
    async fn test_rename_table_without_source_table(pool: sqlx::PgPool) {
        let prof = crate::catalog::test::memory_io_profile();
        let (ctx, warehouse) = crate::catalog::test::setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer::default(),
            TabularDeleteProfile::Hard {},
            Some(UserId::new_unchecked("oidc", "test-user-id")),
        )
        .await;

        let from_ns = crate::catalog::test::create_ns(
            ctx.clone(),
            warehouse.warehouse_id.to_string(),
            "from_ns".to_string(),
        )
        .await;
        let prefix = Some(Prefix(warehouse.warehouse_id.to_string()));
        let table_name = "from_table".to_string();

        // All actions are allowed but the origin table does not exist
        let response = CatalogServer::rename_table(
            prefix,
            RenameTableRequest {
                source: TableIdent {
                    namespace: from_ns.namespace.clone(),
                    name: table_name.clone(),
                },
                destination: TableIdent {
                    namespace: NamespaceIdent::new("to_ns".to_string()),
                    name: table_name,
                },
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap_err();

        assert_eq!(response.error.code, StatusCode::NOT_FOUND);
        assert_eq!(response.error.r#type, "TableActionForbidden");
    }

    #[sqlx::test]
    async fn test_register_table_with_overwrite(pool: PgPool) {
        let (ctx, ns, ns_params, _) = table_test_setup(pool).await;

        // Create a table first
        let initial_table = CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some("test_overwrite".to_string()), Some(false)),
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Verify the table exists
        let table_ident = TableIdent {
            namespace: ns.namespace.clone(),
            name: "test_overwrite".to_string(),
        };

        CatalogServer::table_exists(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: table_ident.clone(),
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Now create a second table to use for the overwrite test
        let second_table = CatalogServer::create_table(
            ns_params.clone(),
            create_request(Some("second_table".to_string()), Some(false)),
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Read table metadata

        // Drop second table, keep data
        CatalogServer::drop_table(
            TableParameters {
                prefix: ns_params.prefix.clone(),
                table: TableIdent {
                    namespace: ns.namespace.clone(),
                    name: "second_table".to_string(),
                },
            },
            DropParams {
                purge_requested: false,
                force: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect("Failed to drop second table");

        // Test without overwrite flag - should fail
        let register_request = iceberg_ext::catalog::rest::RegisterTableRequest::builder()
            .name("test_overwrite".to_string())
            .metadata_location(second_table.metadata_location.as_ref().unwrap().to_string())
            .build();

        CatalogServer::register_table(
            ns_params.clone(),
            register_request.clone(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .expect_err("Registration should fail without overwrite flag");

        // Test with overwrite flag - should succeed
        let register_request_with_overwrite =
            iceberg_ext::catalog::rest::RegisterTableRequest::builder()
                .name("test_overwrite".to_string())
                .metadata_location(second_table.metadata_location.as_ref().unwrap().to_string())
                .overwrite(true)
                .build();

        let result = CatalogServer::register_table(
            ns_params.clone(),
            register_request_with_overwrite,
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await;

        assert!(
            result.is_ok(),
            "Registration with overwrite flag should succeed, but failed with: {:?}",
            result.err().map(|e| e.error.message)
        );

        // Verify the table exists and has the new metadata
        let loaded_table = CatalogServer::load_table(
            TableParameters {
                prefix: ns_params.prefix,
                table: table_ident,
            },
            DataAccess::not_specified(),
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // The loaded table should have the UUID and content of the second table
        assert_eq!(loaded_table.metadata.uuid(), second_table.metadata.uuid());
        assert_ne!(loaded_table.metadata.uuid(), initial_table.metadata.uuid());
    }

    // Reasons for using a mix of PostgresCatalog and CatalogServer:
    //
    // - PostgresCatalog: required for specifying id of table to be created
    // - CatalogServer: required for taking TabularDeleteProfile into account
    #[sqlx::test]
    async fn test_reuse_table_ids_hard_delete(pool: PgPool) {
        let delete_profile = TabularDeleteProfile::Hard {};
        let (ctx, mut wh_ns_data, _base_loc) =
            tabular_test_multi_warehouse_setup(pool.clone(), 3, delete_profile).await;

        let t_id = TableId::new_random();
        let t_name = "t1".to_string();
        let list_flags = ListFlags::all();

        // Create tables with the same table ID across different warehouses.
        for (wh_id, _ns_id, ns_params) in &wh_ns_data {
            let _inited_table = initialize_table(
                *wh_id,
                ctx.v1_state.catalog.clone(),
                false,
                Some(ns_params.namespace.clone()),
                Some(t_id),
                Some(t_name.clone()),
            )
            .await;

            // Verify table creation.
            let _meta = PostgresCatalog::get_table_metadata_by_id(
                *wh_id,
                t_id,
                list_flags,
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .expect("table and metadata should exist");
        }

        // Hard delete one of the tables.
        let deleted_table_data = wh_ns_data.pop().unwrap();
        CatalogServer::drop_table(
            TableParameters {
                prefix: deleted_table_data.2.prefix.clone(),
                table: TableIdent {
                    namespace: deleted_table_data.2.namespace.clone(),
                    name: t_name.clone(),
                },
            },
            DropParams {
                purge_requested: false,
                force: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Deleted table cannot be accessed anymore.
        let deleted_res = PostgresCatalog::get_table_metadata_by_id(
            deleted_table_data.0,
            t_id,
            list_flags,
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        assert!(deleted_res.is_none(), "Table should be deleted");

        // Tables in other warehouses are still there.
        assert!(!wh_ns_data.is_empty());
        for (wh_id, _ns_id, _ns_params) in &wh_ns_data {
            PostgresCatalog::get_table_metadata_by_id(
                *wh_id,
                t_id,
                list_flags,
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .expect("table and metadata should still exist");
        }

        // As the delete was hard, the table can be recreated in the warehouse.
        let _inited_table = initialize_table(
            deleted_table_data.0,
            ctx.v1_state.catalog.clone(),
            false,
            Some(deleted_table_data.2.namespace.clone()),
            Some(t_id),
            Some(t_name.clone()),
        )
        .await;
        let _meta = PostgresCatalog::get_table_metadata_by_id(
            deleted_table_data.0,
            t_id,
            list_flags,
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap()
        .expect("table and metadata should exist");
    }

    // Reasons for using a mix of PostgresCatalog and CatalogServer:
    //
    // - PostgresCatalog: required for specifying id of table to be created
    // - CatalogServer: required for taking TabularDeleteProfile into account
    #[sqlx::test]
    async fn test_reuse_table_ids_soft_delete(pool: PgPool) {
        let delete_profile = TabularDeleteProfile::Soft {
            expiration_seconds: chrono::Duration::seconds(10),
        };
        let (ctx, mut wh_ns_data, _base_loc) =
            tabular_test_multi_warehouse_setup(pool.clone(), 3, delete_profile).await;

        let t_id = TableId::new_random();
        let t_name = "t1".to_string();
        let list_flags_active = ListFlags::default();

        // Create tables with the same table ID across different warehouses.
        for (wh_id, _ns_id, ns_params) in &wh_ns_data {
            let _inited_table = initialize_table(
                *wh_id,
                ctx.v1_state.catalog.clone(),
                false,
                Some(ns_params.namespace.clone()),
                Some(t_id),
                Some(t_name.clone()),
            )
            .await;

            // Verify table creation.
            let _meta = PostgresCatalog::get_table_metadata_by_id(
                *wh_id,
                t_id,
                list_flags_active,
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .expect("table and metadata should exist");
        }

        // Soft delete one of the tables.
        let deleted_table_data = wh_ns_data.pop().unwrap();
        CatalogServer::drop_table(
            TableParameters {
                prefix: deleted_table_data.2.prefix.clone(),
                table: TableIdent {
                    namespace: deleted_table_data.2.namespace.clone(),
                    name: t_name.clone(),
                },
            },
            DropParams {
                purge_requested: false,
                force: false,
            },
            ctx.clone(),
            RequestMetadata::new_unauthenticated(),
        )
        .await
        .unwrap();

        // Check availability depending on list flags.
        let deleted_res = PostgresCatalog::get_table_metadata_by_id(
            deleted_table_data.0,
            t_id,
            list_flags_active,
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        assert!(deleted_res.is_none(), "Table should be soft deleted");
        let deleted_res = PostgresCatalog::get_table_metadata_by_id(
            deleted_table_data.0,
            t_id,
            ListFlags::all(), // include soft deleted
            ctx.v1_state.catalog.clone(),
        )
        .await
        .unwrap();
        assert!(deleted_res.is_some(), "Table should be only soft deleted");

        // Tables in other warehouses are still there.
        assert!(!wh_ns_data.is_empty());
        for (wh_id, _ns_id, _ns_params) in &wh_ns_data {
            PostgresCatalog::get_table_metadata_by_id(
                *wh_id,
                t_id,
                list_flags_active,
                ctx.v1_state.catalog.clone(),
            )
            .await
            .unwrap()
            .expect("table and metadata should still exist");
        }
    }
}
