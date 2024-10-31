import { ResponseContext, RequestContext, HttpFile, HttpInfo } from '../http/http';
import { Configuration} from '../configuration'

import { AuthZBackend } from '../models/AuthZBackend';
import { AzCredential } from '../models/AzCredential';
import { AzCredentialClientCredentials } from '../models/AzCredentialClientCredentials';
import { AzdlsProfile } from '../models/AzdlsProfile';
import { BootstrapRequest } from '../models/BootstrapRequest';
import { CreateProjectRequest } from '../models/CreateProjectRequest';
import { CreateProjectResponse } from '../models/CreateProjectResponse';
import { CreateRoleRequest } from '../models/CreateRoleRequest';
import { CreateUserRequest } from '../models/CreateUserRequest';
import { CreateWarehouseRequest } from '../models/CreateWarehouseRequest';
import { CreateWarehouseResponse } from '../models/CreateWarehouseResponse';
import { DeleteKind } from '../models/DeleteKind';
import { DeletedTabularResponse } from '../models/DeletedTabularResponse';
import { GcsCredential } from '../models/GcsCredential';
import { GcsCredentialServiceAccountKey } from '../models/GcsCredentialServiceAccountKey';
import { GcsProfile } from '../models/GcsProfile';
import { GcsServiceKey } from '../models/GcsServiceKey';
import { GetNamespaceAccessResponse } from '../models/GetNamespaceAccessResponse';
import { GetNamespaceAssignmentsResponse } from '../models/GetNamespaceAssignmentsResponse';
import { GetNamespaceResponse } from '../models/GetNamespaceResponse';
import { GetProjectAccessResponse } from '../models/GetProjectAccessResponse';
import { GetProjectAssignmentsResponse } from '../models/GetProjectAssignmentsResponse';
import { GetProjectResponse } from '../models/GetProjectResponse';
import { GetRoleAccessResponse } from '../models/GetRoleAccessResponse';
import { GetRoleAssignmentsResponse } from '../models/GetRoleAssignmentsResponse';
import { GetServerAccessResponse } from '../models/GetServerAccessResponse';
import { GetServerAssignmentsResponse } from '../models/GetServerAssignmentsResponse';
import { GetTableAccessResponse } from '../models/GetTableAccessResponse';
import { GetTableAssignmentsResponse } from '../models/GetTableAssignmentsResponse';
import { GetViewAccessResponse } from '../models/GetViewAccessResponse';
import { GetViewAssignmentsResponse } from '../models/GetViewAssignmentsResponse';
import { GetWarehouseAccessResponse } from '../models/GetWarehouseAccessResponse';
import { GetWarehouseAssignmentsResponse } from '../models/GetWarehouseAssignmentsResponse';
import { GetWarehouseResponse } from '../models/GetWarehouseResponse';
import { ListDeletedTabularsResponse } from '../models/ListDeletedTabularsResponse';
import { ListProjectsResponse } from '../models/ListProjectsResponse';
import { ListRolesResponse } from '../models/ListRolesResponse';
import { ListUsersResponse } from '../models/ListUsersResponse';
import { ListWarehousesRequest } from '../models/ListWarehousesRequest';
import { ListWarehousesResponse } from '../models/ListWarehousesResponse';
import { NamespaceAction } from '../models/NamespaceAction';
import { NamespaceAssignment } from '../models/NamespaceAssignment';
import { NamespaceAssignmentCreate } from '../models/NamespaceAssignmentCreate';
import { NamespaceAssignmentDescribe } from '../models/NamespaceAssignmentDescribe';
import { NamespaceAssignmentManageGrants } from '../models/NamespaceAssignmentManageGrants';
import { NamespaceAssignmentModify } from '../models/NamespaceAssignmentModify';
import { NamespaceAssignmentOwnership } from '../models/NamespaceAssignmentOwnership';
import { NamespaceAssignmentPassGrants } from '../models/NamespaceAssignmentPassGrants';
import { NamespaceAssignmentSelect } from '../models/NamespaceAssignmentSelect';
import { NamespaceRelation } from '../models/NamespaceRelation';
import { ProjectAction } from '../models/ProjectAction';
import { ProjectAssignment } from '../models/ProjectAssignment';
import { ProjectAssignmentCreate } from '../models/ProjectAssignmentCreate';
import { ProjectAssignmentDescribe } from '../models/ProjectAssignmentDescribe';
import { ProjectAssignmentModify } from '../models/ProjectAssignmentModify';
import { ProjectAssignmentProjectAdmin } from '../models/ProjectAssignmentProjectAdmin';
import { ProjectAssignmentRoleCreator } from '../models/ProjectAssignmentRoleCreator';
import { ProjectAssignmentSecurityAdmin } from '../models/ProjectAssignmentSecurityAdmin';
import { ProjectAssignmentSelect } from '../models/ProjectAssignmentSelect';
import { ProjectAssignmentWarehouseAdmin } from '../models/ProjectAssignmentWarehouseAdmin';
import { ProjectRelation } from '../models/ProjectRelation';
import { RenameProjectRequest } from '../models/RenameProjectRequest';
import { RenameWarehouseRequest } from '../models/RenameWarehouseRequest';
import { Role } from '../models/Role';
import { RoleAction } from '../models/RoleAction';
import { RoleAssignment } from '../models/RoleAssignment';
import { RoleAssignmentAssignee } from '../models/RoleAssignmentAssignee';
import { RoleAssignmentOwnership } from '../models/RoleAssignmentOwnership';
import { S3Credential } from '../models/S3Credential';
import { S3CredentialAccessKey } from '../models/S3CredentialAccessKey';
import { S3Flavor } from '../models/S3Flavor';
import { S3Profile } from '../models/S3Profile';
import { SearchRoleRequest } from '../models/SearchRoleRequest';
import { SearchRoleResponse } from '../models/SearchRoleResponse';
import { SearchUser } from '../models/SearchUser';
import { SearchUserRequest } from '../models/SearchUserRequest';
import { SearchUserResponse } from '../models/SearchUserResponse';
import { ServerAction } from '../models/ServerAction';
import { ServerAssignment } from '../models/ServerAssignment';
import { ServerAssignmentGlobalAdmin } from '../models/ServerAssignmentGlobalAdmin';
import { ServerInfo } from '../models/ServerInfo';
import { ServerRelation } from '../models/ServerRelation';
import { SetManagedAccessRequest } from '../models/SetManagedAccessRequest';
import { StorageCredential } from '../models/StorageCredential';
import { StorageCredentialAz } from '../models/StorageCredentialAz';
import { StorageCredentialGcs } from '../models/StorageCredentialGcs';
import { StorageCredentialS3 } from '../models/StorageCredentialS3';
import { StorageProfile } from '../models/StorageProfile';
import { StorageProfileAzdls } from '../models/StorageProfileAzdls';
import { StorageProfileGcs } from '../models/StorageProfileGcs';
import { StorageProfileS3 } from '../models/StorageProfileS3';
import { TableAction } from '../models/TableAction';
import { TableAssignment } from '../models/TableAssignment';
import { TableAssignmentCreate } from '../models/TableAssignmentCreate';
import { TableAssignmentDescribe } from '../models/TableAssignmentDescribe';
import { TableAssignmentManageGrants } from '../models/TableAssignmentManageGrants';
import { TableAssignmentOwnership } from '../models/TableAssignmentOwnership';
import { TableAssignmentPassGrants } from '../models/TableAssignmentPassGrants';
import { TableAssignmentSelect } from '../models/TableAssignmentSelect';
import { TableRelation } from '../models/TableRelation';
import { TabularDeleteProfile } from '../models/TabularDeleteProfile';
import { TabularDeleteProfileHard } from '../models/TabularDeleteProfileHard';
import { TabularDeleteProfileSoft } from '../models/TabularDeleteProfileSoft';
import { TabularType } from '../models/TabularType';
import { UpdateNamespaceAssignmentsRequest } from '../models/UpdateNamespaceAssignmentsRequest';
import { UpdateProjectAssignmentsRequest } from '../models/UpdateProjectAssignmentsRequest';
import { UpdateRoleAssignmentsRequest } from '../models/UpdateRoleAssignmentsRequest';
import { UpdateRoleRequest } from '../models/UpdateRoleRequest';
import { UpdateServerAssignmentsRequest } from '../models/UpdateServerAssignmentsRequest';
import { UpdateTableAssignmentsRequest } from '../models/UpdateTableAssignmentsRequest';
import { UpdateUserRequest } from '../models/UpdateUserRequest';
import { UpdateViewAssignmentsRequest } from '../models/UpdateViewAssignmentsRequest';
import { UpdateWarehouseAssignmentsRequest } from '../models/UpdateWarehouseAssignmentsRequest';
import { UpdateWarehouseCredentialRequest } from '../models/UpdateWarehouseCredentialRequest';
import { UpdateWarehouseDeleteProfileRequest } from '../models/UpdateWarehouseDeleteProfileRequest';
import { UpdateWarehouseStorageRequest } from '../models/UpdateWarehouseStorageRequest';
import { User } from '../models/User';
import { UserLastUpdatedWith } from '../models/UserLastUpdatedWith';
import { UserOrRole } from '../models/UserOrRole';
import { UserOrRoleRole } from '../models/UserOrRoleRole';
import { UserOrRoleUser } from '../models/UserOrRoleUser';
import { UserType } from '../models/UserType';
import { ViewAction } from '../models/ViewAction';
import { ViewAssignment } from '../models/ViewAssignment';
import { ViewAssignmentDescribe } from '../models/ViewAssignmentDescribe';
import { ViewAssignmentManageGrants } from '../models/ViewAssignmentManageGrants';
import { ViewAssignmentModify } from '../models/ViewAssignmentModify';
import { ViewAssignmentOwnership } from '../models/ViewAssignmentOwnership';
import { ViewAssignmentPassGrants } from '../models/ViewAssignmentPassGrants';
import { ViewRelation } from '../models/ViewRelation';
import { WarehouseAction } from '../models/WarehouseAction';
import { WarehouseAssignment } from '../models/WarehouseAssignment';
import { WarehouseAssignmentCreate } from '../models/WarehouseAssignmentCreate';
import { WarehouseAssignmentDescribe } from '../models/WarehouseAssignmentDescribe';
import { WarehouseAssignmentManageGrants } from '../models/WarehouseAssignmentManageGrants';
import { WarehouseAssignmentModify } from '../models/WarehouseAssignmentModify';
import { WarehouseAssignmentOwnership } from '../models/WarehouseAssignmentOwnership';
import { WarehouseAssignmentPassGrants } from '../models/WarehouseAssignmentPassGrants';
import { WarehouseAssignmentSelect } from '../models/WarehouseAssignmentSelect';
import { WarehouseRelation } from '../models/WarehouseRelation';
import { WarehouseStatus } from '../models/WarehouseStatus';
import { ObservablePermissionsApi } from './ObservableAPI';

import { PermissionsApiRequestFactory, PermissionsApiResponseProcessor} from "../apis/PermissionsApi";
export class PromisePermissionsApi {
    private api: ObservablePermissionsApi

    public constructor(
        configuration: Configuration,
        requestFactory?: PermissionsApiRequestFactory,
        responseProcessor?: PermissionsApiResponseProcessor
    ) {
        this.api = new ObservablePermissionsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Get my access to a namespace
     * @param namespaceId Namespace ID
     */
    public getNamespaceAccessByIdWithHttpInfo(namespaceId: string, _options?: Configuration): Promise<HttpInfo<Array<GetNamespaceAccessResponse>>> {
        const result = this.api.getNamespaceAccessByIdWithHttpInfo(namespaceId, _options);
        return result.toPromise();
    }

    /**
     * Get my access to a namespace
     * @param namespaceId Namespace ID
     */
    public getNamespaceAccessById(namespaceId: string, _options?: Configuration): Promise<Array<GetNamespaceAccessResponse>> {
        const result = this.api.getNamespaceAccessById(namespaceId, _options);
        return result.toPromise();
    }

    /**
     * Get user and role assignments for a namespace
     * @param namespaceId Namespace ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getNamespaceAssignmentsByIdWithHttpInfo(namespaceId: string, relations?: Array<NamespaceRelation>, _options?: Configuration): Promise<HttpInfo<Array<GetNamespaceAssignmentsResponse>>> {
        const result = this.api.getNamespaceAssignmentsByIdWithHttpInfo(namespaceId, relations, _options);
        return result.toPromise();
    }

    /**
     * Get user and role assignments for a namespace
     * @param namespaceId Namespace ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getNamespaceAssignmentsById(namespaceId: string, relations?: Array<NamespaceRelation>, _options?: Configuration): Promise<Array<GetNamespaceAssignmentsResponse>> {
        const result = this.api.getNamespaceAssignmentsById(namespaceId, relations, _options);
        return result.toPromise();
    }

    /**
     * Get Authorization properties of a namespace
     * @param namespaceId Namespace ID
     */
    public getNamespaceByIdWithHttpInfo(namespaceId: string, _options?: Configuration): Promise<HttpInfo<Array<GetNamespaceResponse>>> {
        const result = this.api.getNamespaceByIdWithHttpInfo(namespaceId, _options);
        return result.toPromise();
    }

    /**
     * Get Authorization properties of a namespace
     * @param namespaceId Namespace ID
     */
    public getNamespaceById(namespaceId: string, _options?: Configuration): Promise<Array<GetNamespaceResponse>> {
        const result = this.api.getNamespaceById(namespaceId, _options);
        return result.toPromise();
    }

    /**
     * Get my access to the default project
     */
    public getProjectAccessWithHttpInfo(_options?: Configuration): Promise<HttpInfo<Array<GetProjectAccessResponse>>> {
        const result = this.api.getProjectAccessWithHttpInfo(_options);
        return result.toPromise();
    }

    /**
     * Get my access to the default project
     */
    public getProjectAccess(_options?: Configuration): Promise<Array<GetProjectAccessResponse>> {
        const result = this.api.getProjectAccess(_options);
        return result.toPromise();
    }

    /**
     * Get my access to the default project
     * @param projectId Project ID
     */
    public getProjectAccessByIdWithHttpInfo(projectId: string, _options?: Configuration): Promise<HttpInfo<Array<GetProjectAccessResponse>>> {
        const result = this.api.getProjectAccessByIdWithHttpInfo(projectId, _options);
        return result.toPromise();
    }

    /**
     * Get my access to the default project
     * @param projectId Project ID
     */
    public getProjectAccessById(projectId: string, _options?: Configuration): Promise<Array<GetProjectAccessResponse>> {
        const result = this.api.getProjectAccessById(projectId, _options);
        return result.toPromise();
    }

    /**
     * Get user and role assignments to the current project
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getProjectAssignmentsWithHttpInfo(relations?: Array<ProjectRelation>, _options?: Configuration): Promise<HttpInfo<Array<GetProjectAssignmentsResponse>>> {
        const result = this.api.getProjectAssignmentsWithHttpInfo(relations, _options);
        return result.toPromise();
    }

    /**
     * Get user and role assignments to the current project
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getProjectAssignments(relations?: Array<ProjectRelation>, _options?: Configuration): Promise<Array<GetProjectAssignmentsResponse>> {
        const result = this.api.getProjectAssignments(relations, _options);
        return result.toPromise();
    }

    /**
     * Get user and role assignments to a project
     * @param projectId Project ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getProjectAssignmentsByIdWithHttpInfo(projectId: string, relations?: Array<ProjectRelation>, _options?: Configuration): Promise<HttpInfo<Array<GetProjectAssignmentsResponse>>> {
        const result = this.api.getProjectAssignmentsByIdWithHttpInfo(projectId, relations, _options);
        return result.toPromise();
    }

    /**
     * Get user and role assignments to a project
     * @param projectId Project ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getProjectAssignmentsById(projectId: string, relations?: Array<ProjectRelation>, _options?: Configuration): Promise<Array<GetProjectAssignmentsResponse>> {
        const result = this.api.getProjectAssignmentsById(projectId, relations, _options);
        return result.toPromise();
    }

    /**
     * Get my access to the default project
     * @param roleId Role ID
     */
    public getRoleAccessByIdWithHttpInfo(roleId: string, _options?: Configuration): Promise<HttpInfo<Array<GetRoleAccessResponse>>> {
        const result = this.api.getRoleAccessByIdWithHttpInfo(roleId, _options);
        return result.toPromise();
    }

    /**
     * Get my access to the default project
     * @param roleId Role ID
     */
    public getRoleAccessById(roleId: string, _options?: Configuration): Promise<Array<GetRoleAccessResponse>> {
        const result = this.api.getRoleAccessById(roleId, _options);
        return result.toPromise();
    }

    /**
     * Get user and role assignments to the current project
     * @param relations Relations to be loaded. If not specified, all relations are returned.
     * @param roleId Role ID
     */
    public getRoleAssignmentsByIdWithHttpInfo(relations: Array<ProjectRelation>, roleId: string, _options?: Configuration): Promise<HttpInfo<Array<GetRoleAssignmentsResponse>>> {
        const result = this.api.getRoleAssignmentsByIdWithHttpInfo(relations, roleId, _options);
        return result.toPromise();
    }

    /**
     * Get user and role assignments to the current project
     * @param relations Relations to be loaded. If not specified, all relations are returned.
     * @param roleId Role ID
     */
    public getRoleAssignmentsById(relations: Array<ProjectRelation>, roleId: string, _options?: Configuration): Promise<Array<GetRoleAssignmentsResponse>> {
        const result = this.api.getRoleAssignmentsById(relations, roleId, _options);
        return result.toPromise();
    }

    /**
     * Get my access to the server
     */
    public getServerAccessWithHttpInfo(_options?: Configuration): Promise<HttpInfo<Array<GetServerAccessResponse>>> {
        const result = this.api.getServerAccessWithHttpInfo(_options);
        return result.toPromise();
    }

    /**
     * Get my access to the server
     */
    public getServerAccess(_options?: Configuration): Promise<Array<GetServerAccessResponse>> {
        const result = this.api.getServerAccess(_options);
        return result.toPromise();
    }

    /**
     * Get user and role assignments to the current project
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getServerAssignmentsWithHttpInfo(relations?: Array<ServerRelation>, _options?: Configuration): Promise<HttpInfo<Array<GetServerAssignmentsResponse>>> {
        const result = this.api.getServerAssignmentsWithHttpInfo(relations, _options);
        return result.toPromise();
    }

    /**
     * Get user and role assignments to the current project
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getServerAssignments(relations?: Array<ServerRelation>, _options?: Configuration): Promise<Array<GetServerAssignmentsResponse>> {
        const result = this.api.getServerAssignments(relations, _options);
        return result.toPromise();
    }

    /**
     * Get my access to a table
     * @param tableId Table ID
     */
    public getTableAccessByIdWithHttpInfo(tableId: string, _options?: Configuration): Promise<HttpInfo<Array<GetTableAccessResponse>>> {
        const result = this.api.getTableAccessByIdWithHttpInfo(tableId, _options);
        return result.toPromise();
    }

    /**
     * Get my access to a table
     * @param tableId Table ID
     */
    public getTableAccessById(tableId: string, _options?: Configuration): Promise<Array<GetTableAccessResponse>> {
        const result = this.api.getTableAccessById(tableId, _options);
        return result.toPromise();
    }

    /**
     * Get my access to a view
     * @param viewId View ID
     */
    public getViewAccessByIdWithHttpInfo(viewId: string, _options?: Configuration): Promise<HttpInfo<Array<GetViewAccessResponse>>> {
        const result = this.api.getViewAccessByIdWithHttpInfo(viewId, _options);
        return result.toPromise();
    }

    /**
     * Get my access to a view
     * @param viewId View ID
     */
    public getViewAccessById(viewId: string, _options?: Configuration): Promise<Array<GetViewAccessResponse>> {
        const result = this.api.getViewAccessById(viewId, _options);
        return result.toPromise();
    }

    /**
     * Get user and role assignments for a view
     * @param namespaceId Namespace ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getViewAssignmentsByIdWithHttpInfo(namespaceId: string, relations?: Array<ViewRelation>, _options?: Configuration): Promise<HttpInfo<Array<GetViewAssignmentsResponse>>> {
        const result = this.api.getViewAssignmentsByIdWithHttpInfo(namespaceId, relations, _options);
        return result.toPromise();
    }

    /**
     * Get user and role assignments for a view
     * @param namespaceId Namespace ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getViewAssignmentsById(namespaceId: string, relations?: Array<ViewRelation>, _options?: Configuration): Promise<Array<GetViewAssignmentsResponse>> {
        const result = this.api.getViewAssignmentsById(namespaceId, relations, _options);
        return result.toPromise();
    }

    /**
     * Get my access to a warehouse
     * @param warehouseId Warehouse ID
     */
    public getWarehouseAccessByIdWithHttpInfo(warehouseId: string, _options?: Configuration): Promise<HttpInfo<Array<GetWarehouseAccessResponse>>> {
        const result = this.api.getWarehouseAccessByIdWithHttpInfo(warehouseId, _options);
        return result.toPromise();
    }

    /**
     * Get my access to a warehouse
     * @param warehouseId Warehouse ID
     */
    public getWarehouseAccessById(warehouseId: string, _options?: Configuration): Promise<Array<GetWarehouseAccessResponse>> {
        const result = this.api.getWarehouseAccessById(warehouseId, _options);
        return result.toPromise();
    }

    /**
     * Get user and role assignments for a warehouse
     * @param warehouseId Warehouse ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getWarehouseAssignmentsByIdWithHttpInfo(warehouseId: string, relations?: Array<WarehouseRelation>, _options?: Configuration): Promise<HttpInfo<Array<GetWarehouseAssignmentsResponse>>> {
        const result = this.api.getWarehouseAssignmentsByIdWithHttpInfo(warehouseId, relations, _options);
        return result.toPromise();
    }

    /**
     * Get user and role assignments for a warehouse
     * @param warehouseId Warehouse ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getWarehouseAssignmentsById(warehouseId: string, relations?: Array<WarehouseRelation>, _options?: Configuration): Promise<Array<GetWarehouseAssignmentsResponse>> {
        const result = this.api.getWarehouseAssignmentsById(warehouseId, relations, _options);
        return result.toPromise();
    }

    /**
     * Get Authorization properties of a warehouse
     * @param warehouseId Warehouse ID
     */
    public getWarehouseByIdWithHttpInfo(warehouseId: string, _options?: Configuration): Promise<HttpInfo<Array<GetWarehouseResponse>>> {
        const result = this.api.getWarehouseByIdWithHttpInfo(warehouseId, _options);
        return result.toPromise();
    }

    /**
     * Get Authorization properties of a warehouse
     * @param warehouseId Warehouse ID
     */
    public getWarehouseById(warehouseId: string, _options?: Configuration): Promise<Array<GetWarehouseResponse>> {
        const result = this.api.getWarehouseById(warehouseId, _options);
        return result.toPromise();
    }

    /**
     * Set managed access property of a namespace
     * @param namespaceId Namespace ID
     * @param setManagedAccessRequest 
     */
    public setNamespaceManagedAccessWithHttpInfo(namespaceId: string, setManagedAccessRequest: SetManagedAccessRequest, _options?: Configuration): Promise<HttpInfo<Array<any | null>>> {
        const result = this.api.setNamespaceManagedAccessWithHttpInfo(namespaceId, setManagedAccessRequest, _options);
        return result.toPromise();
    }

    /**
     * Set managed access property of a namespace
     * @param namespaceId Namespace ID
     * @param setManagedAccessRequest 
     */
    public setNamespaceManagedAccess(namespaceId: string, setManagedAccessRequest: SetManagedAccessRequest, _options?: Configuration): Promise<Array<any | null>> {
        const result = this.api.setNamespaceManagedAccess(namespaceId, setManagedAccessRequest, _options);
        return result.toPromise();
    }

    /**
     * Set managed access property of a warehouse
     * @param warehouseId Warehouse ID
     * @param setManagedAccessRequest 
     */
    public setWarehouseManagedAccessWithHttpInfo(warehouseId: string, setManagedAccessRequest: SetManagedAccessRequest, _options?: Configuration): Promise<HttpInfo<Array<any | null>>> {
        const result = this.api.setWarehouseManagedAccessWithHttpInfo(warehouseId, setManagedAccessRequest, _options);
        return result.toPromise();
    }

    /**
     * Set managed access property of a warehouse
     * @param warehouseId Warehouse ID
     * @param setManagedAccessRequest 
     */
    public setWarehouseManagedAccess(warehouseId: string, setManagedAccessRequest: SetManagedAccessRequest, _options?: Configuration): Promise<Array<any | null>> {
        const result = this.api.setWarehouseManagedAccess(warehouseId, setManagedAccessRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for a namespace
     * @param namespaceId Namespace ID
     * @param updateNamespaceAssignmentsRequest
     */
    public updateNamespaceAssignmentsByIdWithHttpInfo(namespaceId: string, updateNamespaceAssignmentsRequest: UpdateNamespaceAssignmentsRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.updateNamespaceAssignmentsByIdWithHttpInfo(namespaceId, updateNamespaceAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for a namespace
     * @param namespaceId Namespace ID
     * @param updateNamespaceAssignmentsRequest
     */
    public updateNamespaceAssignmentsById(namespaceId: string, updateNamespaceAssignmentsRequest: UpdateNamespaceAssignmentsRequest, _options?: Configuration): Promise<void> {
        const result = this.api.updateNamespaceAssignmentsById(namespaceId, updateNamespaceAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for the default project
     * @param updateProjectAssignmentsRequest
     */
    public updateProjectAssignmentsWithHttpInfo(updateProjectAssignmentsRequest: UpdateProjectAssignmentsRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.updateProjectAssignmentsWithHttpInfo(updateProjectAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for the default project
     * @param updateProjectAssignmentsRequest
     */
    public updateProjectAssignments(updateProjectAssignmentsRequest: UpdateProjectAssignmentsRequest, _options?: Configuration): Promise<void> {
        const result = this.api.updateProjectAssignments(updateProjectAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for a project
     * @param projectId Project ID
     * @param updateProjectAssignmentsRequest
     */
    public updateProjectAssignmentsByIdWithHttpInfo(projectId: string, updateProjectAssignmentsRequest: UpdateProjectAssignmentsRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.updateProjectAssignmentsByIdWithHttpInfo(projectId, updateProjectAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for a project
     * @param projectId Project ID
     * @param updateProjectAssignmentsRequest
     */
    public updateProjectAssignmentsById(projectId: string, updateProjectAssignmentsRequest: UpdateProjectAssignmentsRequest, _options?: Configuration): Promise<void> {
        const result = this.api.updateProjectAssignmentsById(projectId, updateProjectAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for a view
     * @param roleId Role ID
     * @param updateRoleAssignmentsRequest
     */
    public updateRoleAssignmentsByIdWithHttpInfo(roleId: string, updateRoleAssignmentsRequest: UpdateRoleAssignmentsRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.updateRoleAssignmentsByIdWithHttpInfo(roleId, updateRoleAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for a view
     * @param roleId Role ID
     * @param updateRoleAssignmentsRequest
     */
    public updateRoleAssignmentsById(roleId: string, updateRoleAssignmentsRequest: UpdateRoleAssignmentsRequest, _options?: Configuration): Promise<void> {
        const result = this.api.updateRoleAssignmentsById(roleId, updateRoleAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for this server
     * @param updateServerAssignmentsRequest
     */
    public updateServerAssignmentsWithHttpInfo(updateServerAssignmentsRequest: UpdateServerAssignmentsRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.updateServerAssignmentsWithHttpInfo(updateServerAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for this server
     * @param updateServerAssignmentsRequest
     */
    public updateServerAssignments(updateServerAssignmentsRequest: UpdateServerAssignmentsRequest, _options?: Configuration): Promise<void> {
        const result = this.api.updateServerAssignments(updateServerAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for a table
     * @param tableId Table ID
     * @param updateTableAssignmentsRequest
     */
    public updateTableAssignmentsByIdWithHttpInfo(tableId: string, updateTableAssignmentsRequest: UpdateTableAssignmentsRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.updateTableAssignmentsByIdWithHttpInfo(tableId, updateTableAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for a table
     * @param tableId Table ID
     * @param updateTableAssignmentsRequest
     */
    public updateTableAssignmentsById(tableId: string, updateTableAssignmentsRequest: UpdateTableAssignmentsRequest, _options?: Configuration): Promise<void> {
        const result = this.api.updateTableAssignmentsById(tableId, updateTableAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for a view
     * @param viewId View ID
     * @param updateViewAssignmentsRequest
     */
    public updateViewAssignmentsByIdWithHttpInfo(viewId: string, updateViewAssignmentsRequest: UpdateViewAssignmentsRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.updateViewAssignmentsByIdWithHttpInfo(viewId, updateViewAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for a view
     * @param viewId View ID
     * @param updateViewAssignmentsRequest
     */
    public updateViewAssignmentsById(viewId: string, updateViewAssignmentsRequest: UpdateViewAssignmentsRequest, _options?: Configuration): Promise<void> {
        const result = this.api.updateViewAssignmentsById(viewId, updateViewAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for a project
     * @param warehouseId Warehouse ID
     * @param updateWarehouseAssignmentsRequest
     */
    public updateWarehouseAssignmentsByIdWithHttpInfo(warehouseId: string, updateWarehouseAssignmentsRequest: UpdateWarehouseAssignmentsRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.updateWarehouseAssignmentsByIdWithHttpInfo(warehouseId, updateWarehouseAssignmentsRequest, _options);
        return result.toPromise();
    }

    /**
     * Update permissions for a project
     * @param warehouseId Warehouse ID
     * @param updateWarehouseAssignmentsRequest
     */
    public updateWarehouseAssignmentsById(warehouseId: string, updateWarehouseAssignmentsRequest: UpdateWarehouseAssignmentsRequest, _options?: Configuration): Promise<void> {
        const result = this.api.updateWarehouseAssignmentsById(warehouseId, updateWarehouseAssignmentsRequest, _options);
        return result.toPromise();
    }


}



import { ObservableProjectApi } from './ObservableAPI';

import { ProjectApiRequestFactory, ProjectApiResponseProcessor} from "../apis/ProjectApi";
export class PromiseProjectApi {
    private api: ObservableProjectApi

    public constructor(
        configuration: Configuration,
        requestFactory?: ProjectApiRequestFactory,
        responseProcessor?: ProjectApiResponseProcessor
    ) {
        this.api = new ObservableProjectApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Create a new project
     * @param createProjectRequest 
     */
    public createProjectWithHttpInfo(createProjectRequest: CreateProjectRequest, _options?: Configuration): Promise<HttpInfo<Array<CreateProjectResponse>>> {
        const result = this.api.createProjectWithHttpInfo(createProjectRequest, _options);
        return result.toPromise();
    }

    /**
     * Create a new project
     * @param createProjectRequest 
     */
    public createProject(createProjectRequest: CreateProjectRequest, _options?: Configuration): Promise<Array<CreateProjectResponse>> {
        const result = this.api.createProject(createProjectRequest, _options);
        return result.toPromise();
    }

    /**
     * Delete the default project
     */
    public deleteDefaultProjectWithHttpInfo(_options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.deleteDefaultProjectWithHttpInfo(_options);
        return result.toPromise();
    }

    /**
     * Delete the default project
     */
    public deleteDefaultProject(_options?: Configuration): Promise<void> {
        const result = this.api.deleteDefaultProject(_options);
        return result.toPromise();
    }

    /**
     * Delete the default project
     * @param projectId
     */
    public deleteProjectByIdWithHttpInfo(projectId: string, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.deleteProjectByIdWithHttpInfo(projectId, _options);
        return result.toPromise();
    }

    /**
     * Delete the default project
     * @param projectId
     */
    public deleteProjectById(projectId: string, _options?: Configuration): Promise<void> {
        const result = this.api.deleteProjectById(projectId, _options);
        return result.toPromise();
    }

    /**
     * Get the default project
     */
    public getDefaultProjectWithHttpInfo(_options?: Configuration): Promise<HttpInfo<Array<GetProjectResponse>>> {
        const result = this.api.getDefaultProjectWithHttpInfo(_options);
        return result.toPromise();
    }

    /**
     * Get the default project
     */
    public getDefaultProject(_options?: Configuration): Promise<Array<GetProjectResponse>> {
        const result = this.api.getDefaultProject(_options);
        return result.toPromise();
    }

    /**
     * Get a specific project by id
     * @param projectId
     */
    public getProjectByIdWithHttpInfo(projectId: string, _options?: Configuration): Promise<HttpInfo<Array<GetProjectResponse>>> {
        const result = this.api.getProjectByIdWithHttpInfo(projectId, _options);
        return result.toPromise();
    }

    /**
     * Get a specific project by id
     * @param projectId
     */
    public getProjectById(projectId: string, _options?: Configuration): Promise<Array<GetProjectResponse>> {
        const result = this.api.getProjectById(projectId, _options);
        return result.toPromise();
    }

    /**
     * List all projects the requesting user has access to
     */
    public listProjectsWithHttpInfo(_options?: Configuration): Promise<HttpInfo<Array<ListProjectsResponse>>> {
        const result = this.api.listProjectsWithHttpInfo(_options);
        return result.toPromise();
    }

    /**
     * List all projects the requesting user has access to
     */
    public listProjects(_options?: Configuration): Promise<Array<ListProjectsResponse>> {
        const result = this.api.listProjects(_options);
        return result.toPromise();
    }

    /**
     * Rename the default project
     * @param renameProjectRequest 
     */
    public renameDefaultProjectWithHttpInfo(renameProjectRequest: RenameProjectRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.renameDefaultProjectWithHttpInfo(renameProjectRequest, _options);
        return result.toPromise();
    }

    /**
     * Rename the default project
     * @param renameProjectRequest 
     */
    public renameDefaultProject(renameProjectRequest: RenameProjectRequest, _options?: Configuration): Promise<void> {
        const result = this.api.renameDefaultProject(renameProjectRequest, _options);
        return result.toPromise();
    }

    /**
     * Rename project by id
     * @param projectId
     * @param renameProjectRequest 
     */
    public renameProjectByIdWithHttpInfo(projectId: string, renameProjectRequest: RenameProjectRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.renameProjectByIdWithHttpInfo(projectId, renameProjectRequest, _options);
        return result.toPromise();
    }

    /**
     * Rename project by id
     * @param projectId
     * @param renameProjectRequest 
     */
    public renameProjectById(projectId: string, renameProjectRequest: RenameProjectRequest, _options?: Configuration): Promise<void> {
        const result = this.api.renameProjectById(projectId, renameProjectRequest, _options);
        return result.toPromise();
    }


}



import { ObservableRoleApi } from './ObservableAPI';

import { RoleApiRequestFactory, RoleApiResponseProcessor} from "../apis/RoleApi";
export class PromiseRoleApi {
    private api: ObservableRoleApi

    public constructor(
        configuration: Configuration,
        requestFactory?: RoleApiRequestFactory,
        responseProcessor?: RoleApiResponseProcessor
    ) {
        this.api = new ObservableRoleApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Create a new role
     * @param createRoleRequest
     */
    public createRoleWithHttpInfo(createRoleRequest: CreateRoleRequest, _options?: Configuration): Promise<HttpInfo<Array<Role>>> {
        const result = this.api.createRoleWithHttpInfo(createRoleRequest, _options);
        return result.toPromise();
    }

    /**
     * Create a new role
     * @param createRoleRequest
     */
    public createRole(createRoleRequest: CreateRoleRequest, _options?: Configuration): Promise<Array<Role>> {
        const result = this.api.createRole(createRoleRequest, _options);
        return result.toPromise();
    }

    /**
     * All permissions of the role are permanently removed.
     * Delete role
     * @param id
     */
    public deleteRoleWithHttpInfo(id: string, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.deleteRoleWithHttpInfo(id, _options);
        return result.toPromise();
    }

    /**
     * All permissions of the role are permanently removed.
     * Delete role
     * @param id
     */
    public deleteRole(id: string, _options?: Configuration): Promise<void> {
        const result = this.api.deleteRole(id, _options);
        return result.toPromise();
    }

    /**
     * Get a role
     * @param id
     */
    public getRoleWithHttpInfo(id: string, _options?: Configuration): Promise<HttpInfo<Array<Role>>> {
        const result = this.api.getRoleWithHttpInfo(id, _options);
        return result.toPromise();
    }

    /**
     * Get a role
     * @param id
     */
    public getRole(id: string, _options?: Configuration): Promise<Array<Role>> {
        const result = this.api.getRole(id, _options);
        return result.toPromise();
    }

    /**
     * List roles in a project
     * @param [name] Search for a specific role name
     * @param [pageToken] Next page token
     * @param [pageSize] Signals an upper bound of the number of results that a client will receive. Default: 100
     * @param [projectId] Project ID from which roles should be listed Only required if the project ID cannot be inferred from the users token and no default project is set.
     */
    public listRolesWithHttpInfo(name?: string, pageToken?: string, pageSize?: number, projectId?: string, _options?: Configuration): Promise<HttpInfo<Array<ListRolesResponse>>> {
        const result = this.api.listRolesWithHttpInfo(name, pageToken, pageSize, projectId, _options);
        return result.toPromise();
    }

    /**
     * List roles in a project
     * @param [name] Search for a specific role name
     * @param [pageToken] Next page token
     * @param [pageSize] Signals an upper bound of the number of results that a client will receive. Default: 100
     * @param [projectId] Project ID from which roles should be listed Only required if the project ID cannot be inferred from the users token and no default project is set.
     */
    public listRoles(name?: string, pageToken?: string, pageSize?: number, projectId?: string, _options?: Configuration): Promise<Array<ListRolesResponse>> {
        const result = this.api.listRoles(name, pageToken, pageSize, projectId, _options);
        return result.toPromise();
    }

    /**
     * Update a role
     * @param id
     * @param updateRoleRequest
     */
    public updateRoleWithHttpInfo(id: string, updateRoleRequest: UpdateRoleRequest, _options?: Configuration): Promise<HttpInfo<Array<Role>>> {
        const result = this.api.updateRoleWithHttpInfo(id, updateRoleRequest, _options);
        return result.toPromise();
    }

    /**
     * Update a role
     * @param id
     * @param updateRoleRequest
     */
    public updateRole(id: string, updateRoleRequest: UpdateRoleRequest, _options?: Configuration): Promise<Array<Role>> {
        const result = this.api.updateRole(id, updateRoleRequest, _options);
        return result.toPromise();
    }


}



import { ObservableServerApi } from './ObservableAPI';

import { ServerApiRequestFactory, ServerApiResponseProcessor} from "../apis/ServerApi";
export class PromiseServerApi {
    private api: ObservableServerApi

    public constructor(
        configuration: Configuration,
        requestFactory?: ServerApiRequestFactory,
        responseProcessor?: ServerApiResponseProcessor
    ) {
        this.api = new ObservableServerApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * If the user exists, it updates the users\' metadata from the token. The token sent to this endpoint should have \"profile\" and \"email\" scopes.
     * Creates the user in the catalog if it does not exist.
     * @param bootstrapRequest
     */
    public bootstrapWithHttpInfo(bootstrapRequest: BootstrapRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.bootstrapWithHttpInfo(bootstrapRequest, _options);
        return result.toPromise();
    }

    /**
     * If the user exists, it updates the users\' metadata from the token. The token sent to this endpoint should have \"profile\" and \"email\" scopes.
     * Creates the user in the catalog if it does not exist.
     * @param bootstrapRequest
     */
    public bootstrap(bootstrapRequest: BootstrapRequest, _options?: Configuration): Promise<void> {
        const result = this.api.bootstrap(bootstrapRequest, _options);
        return result.toPromise();
    }

    /**
     * Get information about the server
     */
    public getServerInfoWithHttpInfo(_options?: Configuration): Promise<HttpInfo<Array<ServerInfo>>> {
        const result = this.api.getServerInfoWithHttpInfo(_options);
        return result.toPromise();
    }

    /**
     * Get information about the server
     */
    public getServerInfo(_options?: Configuration): Promise<Array<ServerInfo>> {
        const result = this.api.getServerInfo(_options);
        return result.toPromise();
    }


}



import { ObservableUserApi } from './ObservableAPI';

import { UserApiRequestFactory, UserApiResponseProcessor} from "../apis/UserApi";
export class PromiseUserApi {
    private api: ObservableUserApi

    public constructor(
        configuration: Configuration,
        requestFactory?: UserApiRequestFactory,
        responseProcessor?: UserApiResponseProcessor
    ) {
        this.api = new ObservableUserApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * If the user exists, it updates the users\' metadata from the token. The token sent to this endpoint should have \"profile\" and \"email\" scopes.
     * Creates the user in the catalog if it does not exist.
     * @param createUserRequest
     */
    public createUserWithHttpInfo(createUserRequest: CreateUserRequest, _options?: Configuration): Promise<HttpInfo<Array<User>>> {
        const result = this.api.createUserWithHttpInfo(createUserRequest, _options);
        return result.toPromise();
    }

    /**
     * If the user exists, it updates the users\' metadata from the token. The token sent to this endpoint should have \"profile\" and \"email\" scopes.
     * Creates the user in the catalog if it does not exist.
     * @param createUserRequest
     */
    public createUser(createUserRequest: CreateUserRequest, _options?: Configuration): Promise<Array<User>> {
        const result = this.api.createUser(createUserRequest, _options);
        return result.toPromise();
    }

    /**
     * All permissions of the user are permanently removed and need to be re-added if the user is re-registered.
     * Delete user
     * @param id
     */
    public deleteUserWithHttpInfo(id: string, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.deleteUserWithHttpInfo(id, _options);
        return result.toPromise();
    }

    /**
     * All permissions of the user are permanently removed and need to be re-added if the user is re-registered.
     * Delete user
     * @param id
     */
    public deleteUser(id: string, _options?: Configuration): Promise<void> {
        const result = this.api.deleteUser(id, _options);
        return result.toPromise();
    }

    /**
     * Get a user by ID
     * @param id
     */
    public getUserWithHttpInfo(id: string, _options?: Configuration): Promise<HttpInfo<Array<User>>> {
        const result = this.api.getUserWithHttpInfo(id, _options);
        return result.toPromise();
    }

    /**
     * Get a user by ID
     * @param id
     */
    public getUser(id: string, _options?: Configuration): Promise<Array<User>> {
        const result = this.api.getUser(id, _options);
        return result.toPromise();
    }

    /**
     * List users
     * @param [name] Search for a specific username
     * @param [pageToken] Next page token
     * @param [pageSize] Signals an upper bound of the number of results that a client will receive. Default: 100
     */
    public listUserWithHttpInfo(name?: string, pageToken?: string, pageSize?: number, _options?: Configuration): Promise<HttpInfo<Array<ListUsersResponse>>> {
        const result = this.api.listUserWithHttpInfo(name, pageToken, pageSize, _options);
        return result.toPromise();
    }

    /**
     * List users
     * @param [name] Search for a specific username
     * @param [pageToken] Next page token
     * @param [pageSize] Signals an upper bound of the number of results that a client will receive. Default: 100
     */
    public listUser(name?: string, pageToken?: string, pageSize?: number, _options?: Configuration): Promise<Array<ListUsersResponse>> {
        const result = this.api.listUser(name, pageToken, pageSize, _options);
        return result.toPromise();
    }

    /**
     * Search for users (Fuzzy)
     * @param searchUserRequest
     */
    public searchUserWithHttpInfo(searchUserRequest: SearchUserRequest, _options?: Configuration): Promise<HttpInfo<Array<SearchUserResponse>>> {
        const result = this.api.searchUserWithHttpInfo(searchUserRequest, _options);
        return result.toPromise();
    }

    /**
     * Search for users (Fuzzy)
     * @param searchUserRequest
     */
    public searchUser(searchUserRequest: SearchUserRequest, _options?: Configuration): Promise<Array<SearchUserResponse>> {
        const result = this.api.searchUser(searchUserRequest, _options);
        return result.toPromise();
    }

    /**
     * If a field is not provided, it is set to `None`.
     * Update details of a user. Replaces the current details with the new details.
     * @param id
     * @param updateUserRequest
     */
    public updateUserWithHttpInfo(id: string, updateUserRequest: UpdateUserRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.updateUserWithHttpInfo(id, updateUserRequest, _options);
        return result.toPromise();
    }

    /**
     * If a field is not provided, it is set to `None`.
     * Update details of a user. Replaces the current details with the new details.
     * @param id
     * @param updateUserRequest
     */
    public updateUser(id: string, updateUserRequest: UpdateUserRequest, _options?: Configuration): Promise<void> {
        const result = this.api.updateUser(id, updateUserRequest, _options);
        return result.toPromise();
    }

    /**
     * Get the currently authenticated user
     */
    public whoamiWithHttpInfo(_options?: Configuration): Promise<HttpInfo<Array<User>>> {
        const result = this.api.whoamiWithHttpInfo(_options);
        return result.toPromise();
    }

    /**
     * Get the currently authenticated user
     */
    public whoami(_options?: Configuration): Promise<Array<User>> {
        const result = this.api.whoami(_options);
        return result.toPromise();
    }


}



import { ObservableWarehouseApi } from './ObservableAPI';

import { WarehouseApiRequestFactory, WarehouseApiResponseProcessor} from "../apis/WarehouseApi";
export class PromiseWarehouseApi {
    private api: ObservableWarehouseApi

    public constructor(
        configuration: Configuration,
        requestFactory?: WarehouseApiRequestFactory,
        responseProcessor?: WarehouseApiResponseProcessor
    ) {
        this.api = new ObservableWarehouseApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Activate a warehouse
     * @param warehouseId
     */
    public activateWarehouseWithHttpInfo(warehouseId: string, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.activateWarehouseWithHttpInfo(warehouseId, _options);
        return result.toPromise();
    }

    /**
     * Activate a warehouse
     * @param warehouseId
     */
    public activateWarehouse(warehouseId: string, _options?: Configuration): Promise<void> {
        const result = this.api.activateWarehouse(warehouseId, _options);
        return result.toPromise();
    }

    /**
     * Create a new warehouse in the given project. The project of a warehouse cannot be changed after creation. The storage configuration is validated by this method.
     * Create a new warehouse.
     * @param createWarehouseRequest
     */
    public createWarehouseWithHttpInfo(createWarehouseRequest: CreateWarehouseRequest, _options?: Configuration): Promise<HttpInfo<Array<CreateWarehouseResponse>>> {
        const result = this.api.createWarehouseWithHttpInfo(createWarehouseRequest, _options);
        return result.toPromise();
    }

    /**
     * Create a new warehouse in the given project. The project of a warehouse cannot be changed after creation. The storage configuration is validated by this method.
     * Create a new warehouse.
     * @param createWarehouseRequest
     */
    public createWarehouse(createWarehouseRequest: CreateWarehouseRequest, _options?: Configuration): Promise<Array<CreateWarehouseResponse>> {
        const result = this.api.createWarehouse(createWarehouseRequest, _options);
        return result.toPromise();
    }

    /**
     * Deactivate a warehouse
     * @param warehouseId
     */
    public deactivateWarehouseWithHttpInfo(warehouseId: string, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.deactivateWarehouseWithHttpInfo(warehouseId, _options);
        return result.toPromise();
    }

    /**
     * Deactivate a warehouse
     * @param warehouseId
     */
    public deactivateWarehouse(warehouseId: string, _options?: Configuration): Promise<void> {
        const result = this.api.deactivateWarehouse(warehouseId, _options);
        return result.toPromise();
    }

    /**
     * Delete a warehouse by ID
     * @param warehouseId
     */
    public deleteWarehouseWithHttpInfo(warehouseId: string, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.deleteWarehouseWithHttpInfo(warehouseId, _options);
        return result.toPromise();
    }

    /**
     * Delete a warehouse by ID
     * @param warehouseId
     */
    public deleteWarehouse(warehouseId: string, _options?: Configuration): Promise<void> {
        const result = this.api.deleteWarehouse(warehouseId, _options);
        return result.toPromise();
    }

    /**
     * Get a warehouse by ID
     * @param warehouseId
     */
    public getWarehouseWithHttpInfo(warehouseId: string, _options?: Configuration): Promise<HttpInfo<Array<GetWarehouseResponse>>> {
        const result = this.api.getWarehouseWithHttpInfo(warehouseId, _options);
        return result.toPromise();
    }

    /**
     * Get a warehouse by ID
     * @param warehouseId
     */
    public getWarehouse(warehouseId: string, _options?: Configuration): Promise<Array<GetWarehouseResponse>> {
        const result = this.api.getWarehouse(warehouseId, _options);
        return result.toPromise();
    }

    /**
     * List all soft-deleted tabulars in the warehouse that are visible to you.
     * List soft-deleted tabulars
     * @param warehouseId
     * @param [pageToken] Next page token
     * @param [pageSize] Signals an upper bound of the number of results that a client will receive.
     */
    public listDeletedTabularsWithHttpInfo(warehouseId: string, pageToken?: string, pageSize?: number, _options?: Configuration): Promise<HttpInfo<Array<ListDeletedTabularsResponse>>> {
        const result = this.api.listDeletedTabularsWithHttpInfo(warehouseId, pageToken, pageSize, _options);
        return result.toPromise();
    }

    /**
     * List all soft-deleted tabulars in the warehouse that are visible to you.
     * List soft-deleted tabulars
     * @param warehouseId
     * @param [pageToken] Next page token
     * @param [pageSize] Signals an upper bound of the number of results that a client will receive.
     */
    public listDeletedTabulars(warehouseId: string, pageToken?: string, pageSize?: number, _options?: Configuration): Promise<Array<ListDeletedTabularsResponse>> {
        const result = this.api.listDeletedTabulars(warehouseId, pageToken, pageSize, _options);
        return result.toPromise();
    }

    /**
     * By default, this endpoint does not return deactivated warehouses. To include deactivated warehouses, set the `include_deactivated` query parameter to `true`.
     * List all warehouses in a project
     * @param [warehouseStatus] Optional filter to return only warehouses with the specified status. If not provided, only active warehouses are returned.
     * @param [projectId] The project ID to list warehouses for. Setting a warehouse is required.
     */
    public listWarehousesWithHttpInfo(warehouseStatus?: Array<WarehouseStatus>, projectId?: string, _options?: Configuration): Promise<HttpInfo<Array<ListWarehousesResponse>>> {
        const result = this.api.listWarehousesWithHttpInfo(warehouseStatus, projectId, _options);
        return result.toPromise();
    }

    /**
     * By default, this endpoint does not return deactivated warehouses. To include deactivated warehouses, set the `include_deactivated` query parameter to `true`.
     * List all warehouses in a project
     * @param [warehouseStatus] Optional filter to return only warehouses with the specified status. If not provided, only active warehouses are returned.
     * @param [projectId] The project ID to list warehouses for. Setting a warehouse is required.
     */
    public listWarehouses(warehouseStatus?: Array<WarehouseStatus>, projectId?: string, _options?: Configuration): Promise<Array<ListWarehousesResponse>> {
        const result = this.api.listWarehouses(warehouseStatus, projectId, _options);
        return result.toPromise();
    }

    /**
     * Rename a warehouse
     * @param warehouseId
     * @param renameWarehouseRequest
     */
    public renameWarehouseWithHttpInfo(warehouseId: string, renameWarehouseRequest: RenameWarehouseRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.renameWarehouseWithHttpInfo(warehouseId, renameWarehouseRequest, _options);
        return result.toPromise();
    }

    /**
     * Rename a warehouse
     * @param warehouseId
     * @param renameWarehouseRequest
     */
    public renameWarehouse(warehouseId: string, renameWarehouseRequest: RenameWarehouseRequest, _options?: Configuration): Promise<void> {
        const result = this.api.renameWarehouse(warehouseId, renameWarehouseRequest, _options);
        return result.toPromise();
    }

    /**
     * This can be used to update credentials before expiration.
     * Update the storage credential of a warehouse. The storage profile is not modified.
     * @param warehouseId
     * @param updateWarehouseCredentialRequest
     */
    public updateStorageCredentialWithHttpInfo(warehouseId: string, updateWarehouseCredentialRequest: UpdateWarehouseCredentialRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.updateStorageCredentialWithHttpInfo(warehouseId, updateWarehouseCredentialRequest, _options);
        return result.toPromise();
    }

    /**
     * This can be used to update credentials before expiration.
     * Update the storage credential of a warehouse. The storage profile is not modified.
     * @param warehouseId
     * @param updateWarehouseCredentialRequest
     */
    public updateStorageCredential(warehouseId: string, updateWarehouseCredentialRequest: UpdateWarehouseCredentialRequest, _options?: Configuration): Promise<void> {
        const result = this.api.updateStorageCredential(warehouseId, updateWarehouseCredentialRequest, _options);
        return result.toPromise();
    }

    /**
     * Update the storage profile of a warehouse including its storage credential.
     * @param warehouseId
     * @param updateWarehouseStorageRequest
     */
    public updateStorageProfileWithHttpInfo(warehouseId: string, updateWarehouseStorageRequest: UpdateWarehouseStorageRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.updateStorageProfileWithHttpInfo(warehouseId, updateWarehouseStorageRequest, _options);
        return result.toPromise();
    }

    /**
     * Update the storage profile of a warehouse including its storage credential.
     * @param warehouseId
     * @param updateWarehouseStorageRequest
     */
    public updateStorageProfile(warehouseId: string, updateWarehouseStorageRequest: UpdateWarehouseStorageRequest, _options?: Configuration): Promise<void> {
        const result = this.api.updateStorageProfile(warehouseId, updateWarehouseStorageRequest, _options);
        return result.toPromise();
    }

    /**
     * Update the Deletion Profile (soft-delete) of a warehouse.
     * @param warehouseId
     * @param updateWarehouseDeleteProfileRequest
     */
    public updateWarehouseDeleteProfileWithHttpInfo(warehouseId: string, updateWarehouseDeleteProfileRequest: UpdateWarehouseDeleteProfileRequest, _options?: Configuration): Promise<HttpInfo<void>> {
        const result = this.api.updateWarehouseDeleteProfileWithHttpInfo(warehouseId, updateWarehouseDeleteProfileRequest, _options);
        return result.toPromise();
    }

    /**
     * Update the Deletion Profile (soft-delete) of a warehouse.
     * @param warehouseId
     * @param updateWarehouseDeleteProfileRequest
     */
    public updateWarehouseDeleteProfile(warehouseId: string, updateWarehouseDeleteProfileRequest: UpdateWarehouseDeleteProfileRequest, _options?: Configuration): Promise<void> {
        const result = this.api.updateWarehouseDeleteProfile(warehouseId, updateWarehouseDeleteProfileRequest, _options);
        return result.toPromise();
    }


}



