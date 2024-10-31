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

import { ObservablePermissionsApi } from "./ObservableAPI";
import { PermissionsApiRequestFactory, PermissionsApiResponseProcessor} from "../apis/PermissionsApi";

export interface PermissionsApiGetNamespaceAccessByIdRequest {
    /**
     * Namespace ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApigetNamespaceAccessById
     */
    namespaceId: string
}

export interface PermissionsApiGetNamespaceAssignmentsByIdRequest {
    /**
     * Namespace ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApigetNamespaceAssignmentsById
     */
    namespaceId: string
    /**
     * Relations to be loaded. If not specified, all relations are returned.
     * Defaults to: undefined
     * @type Array&lt;NamespaceRelation&gt;
     * @memberof PermissionsApigetNamespaceAssignmentsById
     */
    relations?: Array<NamespaceRelation>
}

export interface PermissionsApiGetNamespaceByIdRequest {
    /**
     * Namespace ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApigetNamespaceById
     */
    namespaceId: string
}

export interface PermissionsApiGetProjectAccessRequest {
}

export interface PermissionsApiGetProjectAccessByIdRequest {
    /**
     * Project ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApigetProjectAccessById
     */
    projectId: string
}

export interface PermissionsApiGetProjectAssignmentsRequest {
    /**
     * Relations to be loaded. If not specified, all relations are returned.
     * Defaults to: undefined
     * @type Array&lt;ProjectRelation&gt;
     * @memberof PermissionsApigetProjectAssignments
     */
    relations?: Array<ProjectRelation>
}

export interface PermissionsApiGetProjectAssignmentsByIdRequest {
    /**
     * Project ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApigetProjectAssignmentsById
     */
    projectId: string
    /**
     * Relations to be loaded. If not specified, all relations are returned.
     * Defaults to: undefined
     * @type Array&lt;ProjectRelation&gt;
     * @memberof PermissionsApigetProjectAssignmentsById
     */
    relations?: Array<ProjectRelation>
}

export interface PermissionsApiGetRoleAccessByIdRequest {
    /**
     * Role ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApigetRoleAccessById
     */
    roleId: string
}

export interface PermissionsApiGetRoleAssignmentsByIdRequest {
    /**
     * Relations to be loaded. If not specified, all relations are returned.
     * Defaults to: undefined
     * @type Array&lt;ProjectRelation&gt;
     * @memberof PermissionsApigetRoleAssignmentsById
     */
    relations: Array<ProjectRelation>
    /**
     * Role ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApigetRoleAssignmentsById
     */
    roleId: string
}

export interface PermissionsApiGetServerAccessRequest {
}

export interface PermissionsApiGetServerAssignmentsRequest {
    /**
     * Relations to be loaded. If not specified, all relations are returned.
     * Defaults to: undefined
     * @type Array&lt;ServerRelation&gt;
     * @memberof PermissionsApigetServerAssignments
     */
    relations?: Array<ServerRelation>
}

export interface PermissionsApiGetTableAccessByIdRequest {
    /**
     * Table ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApigetTableAccessById
     */
    tableId: string
}

export interface PermissionsApiGetViewAccessByIdRequest {
    /**
     * View ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApigetViewAccessById
     */
    viewId: string
}

export interface PermissionsApiGetViewAssignmentsByIdRequest {
    /**
     * Namespace ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApigetViewAssignmentsById
     */
    namespaceId: string
    /**
     * Relations to be loaded. If not specified, all relations are returned.
     * Defaults to: undefined
     * @type Array&lt;ViewRelation&gt;
     * @memberof PermissionsApigetViewAssignmentsById
     */
    relations?: Array<ViewRelation>
}

export interface PermissionsApiGetWarehouseAccessByIdRequest {
    /**
     * Warehouse ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApigetWarehouseAccessById
     */
    warehouseId: string
}

export interface PermissionsApiGetWarehouseAssignmentsByIdRequest {
    /**
     * Warehouse ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApigetWarehouseAssignmentsById
     */
    warehouseId: string
    /**
     * Relations to be loaded. If not specified, all relations are returned.
     * Defaults to: undefined
     * @type Array&lt;WarehouseRelation&gt;
     * @memberof PermissionsApigetWarehouseAssignmentsById
     */
    relations?: Array<WarehouseRelation>
}

export interface PermissionsApiGetWarehouseByIdRequest {
    /**
     * Warehouse ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApigetWarehouseById
     */
    warehouseId: string
}

export interface PermissionsApiSetNamespaceManagedAccessRequest {
    /**
     * Namespace ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApisetNamespaceManagedAccess
     */
    namespaceId: string
    /**
     * 
     * @type SetManagedAccessRequest
     * @memberof PermissionsApisetNamespaceManagedAccess
     */
    setManagedAccessRequest: SetManagedAccessRequest
}

export interface PermissionsApiSetWarehouseManagedAccessRequest {
    /**
     * Warehouse ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApisetWarehouseManagedAccess
     */
    warehouseId: string
    /**
     * 
     * @type SetManagedAccessRequest
     * @memberof PermissionsApisetWarehouseManagedAccess
     */
    setManagedAccessRequest: SetManagedAccessRequest
}

export interface PermissionsApiUpdateNamespaceAssignmentsByIdRequest {
    /**
     * Namespace ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApiupdateNamespaceAssignmentsById
     */
    namespaceId: string
    /**
     * 
     * @type UpdateNamespaceAssignmentsRequest
     * @memberof PermissionsApiupdateNamespaceAssignmentsById
     */
    updateNamespaceAssignmentsRequest: UpdateNamespaceAssignmentsRequest
}

export interface PermissionsApiUpdateProjectAssignmentsRequest {
    /**
     * 
     * @type UpdateProjectAssignmentsRequest
     * @memberof PermissionsApiupdateProjectAssignments
     */
    updateProjectAssignmentsRequest: UpdateProjectAssignmentsRequest
}

export interface PermissionsApiUpdateProjectAssignmentsByIdRequest {
    /**
     * Project ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApiupdateProjectAssignmentsById
     */
    projectId: string
    /**
     * 
     * @type UpdateProjectAssignmentsRequest
     * @memberof PermissionsApiupdateProjectAssignmentsById
     */
    updateProjectAssignmentsRequest: UpdateProjectAssignmentsRequest
}

export interface PermissionsApiUpdateRoleAssignmentsByIdRequest {
    /**
     * Role ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApiupdateRoleAssignmentsById
     */
    roleId: string
    /**
     * 
     * @type UpdateRoleAssignmentsRequest
     * @memberof PermissionsApiupdateRoleAssignmentsById
     */
    updateRoleAssignmentsRequest: UpdateRoleAssignmentsRequest
}

export interface PermissionsApiUpdateServerAssignmentsRequest {
    /**
     * 
     * @type UpdateServerAssignmentsRequest
     * @memberof PermissionsApiupdateServerAssignments
     */
    updateServerAssignmentsRequest: UpdateServerAssignmentsRequest
}

export interface PermissionsApiUpdateTableAssignmentsByIdRequest {
    /**
     * Table ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApiupdateTableAssignmentsById
     */
    tableId: string
    /**
     * 
     * @type UpdateTableAssignmentsRequest
     * @memberof PermissionsApiupdateTableAssignmentsById
     */
    updateTableAssignmentsRequest: UpdateTableAssignmentsRequest
}

export interface PermissionsApiUpdateViewAssignmentsByIdRequest {
    /**
     * View ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApiupdateViewAssignmentsById
     */
    viewId: string
    /**
     * 
     * @type UpdateViewAssignmentsRequest
     * @memberof PermissionsApiupdateViewAssignmentsById
     */
    updateViewAssignmentsRequest: UpdateViewAssignmentsRequest
}

export interface PermissionsApiUpdateWarehouseAssignmentsByIdRequest {
    /**
     * Warehouse ID
     * Defaults to: undefined
     * @type string
     * @memberof PermissionsApiupdateWarehouseAssignmentsById
     */
    warehouseId: string
    /**
     * 
     * @type UpdateWarehouseAssignmentsRequest
     * @memberof PermissionsApiupdateWarehouseAssignmentsById
     */
    updateWarehouseAssignmentsRequest: UpdateWarehouseAssignmentsRequest
}

export class ObjectPermissionsApi {
    private api: ObservablePermissionsApi

    public constructor(configuration: Configuration, requestFactory?: PermissionsApiRequestFactory, responseProcessor?: PermissionsApiResponseProcessor) {
        this.api = new ObservablePermissionsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Get my access to a namespace
     * @param param the request object
     */
    public getNamespaceAccessByIdWithHttpInfo(param: PermissionsApiGetNamespaceAccessByIdRequest, options?: Configuration): Promise<HttpInfo<Array<GetNamespaceAccessResponse>>> {
        return this.api.getNamespaceAccessByIdWithHttpInfo(param.namespaceId,  options).toPromise();
    }

    /**
     * Get my access to a namespace
     * @param param the request object
     */
    public getNamespaceAccessById(param: PermissionsApiGetNamespaceAccessByIdRequest, options?: Configuration): Promise<Array<GetNamespaceAccessResponse>> {
        return this.api.getNamespaceAccessById(param.namespaceId,  options).toPromise();
    }

    /**
     * Get user and role assignments for a namespace
     * @param param the request object
     */
    public getNamespaceAssignmentsByIdWithHttpInfo(param: PermissionsApiGetNamespaceAssignmentsByIdRequest, options?: Configuration): Promise<HttpInfo<Array<GetNamespaceAssignmentsResponse>>> {
        return this.api.getNamespaceAssignmentsByIdWithHttpInfo(param.namespaceId, param.relations,  options).toPromise();
    }

    /**
     * Get user and role assignments for a namespace
     * @param param the request object
     */
    public getNamespaceAssignmentsById(param: PermissionsApiGetNamespaceAssignmentsByIdRequest, options?: Configuration): Promise<Array<GetNamespaceAssignmentsResponse>> {
        return this.api.getNamespaceAssignmentsById(param.namespaceId, param.relations,  options).toPromise();
    }

    /**
     * Get Authorization properties of a namespace
     * @param param the request object
     */
    public getNamespaceByIdWithHttpInfo(param: PermissionsApiGetNamespaceByIdRequest, options?: Configuration): Promise<HttpInfo<Array<GetNamespaceResponse>>> {
        return this.api.getNamespaceByIdWithHttpInfo(param.namespaceId,  options).toPromise();
    }

    /**
     * Get Authorization properties of a namespace
     * @param param the request object
     */
    public getNamespaceById(param: PermissionsApiGetNamespaceByIdRequest, options?: Configuration): Promise<Array<GetNamespaceResponse>> {
        return this.api.getNamespaceById(param.namespaceId,  options).toPromise();
    }

    /**
     * Get my access to the default project
     * @param param the request object
     */
    public getProjectAccessWithHttpInfo(param: PermissionsApiGetProjectAccessRequest = {}, options?: Configuration): Promise<HttpInfo<Array<GetProjectAccessResponse>>> {
        return this.api.getProjectAccessWithHttpInfo( options).toPromise();
    }

    /**
     * Get my access to the default project
     * @param param the request object
     */
    public getProjectAccess(param: PermissionsApiGetProjectAccessRequest = {}, options?: Configuration): Promise<Array<GetProjectAccessResponse>> {
        return this.api.getProjectAccess( options).toPromise();
    }

    /**
     * Get my access to the default project
     * @param param the request object
     */
    public getProjectAccessByIdWithHttpInfo(param: PermissionsApiGetProjectAccessByIdRequest, options?: Configuration): Promise<HttpInfo<Array<GetProjectAccessResponse>>> {
        return this.api.getProjectAccessByIdWithHttpInfo(param.projectId,  options).toPromise();
    }

    /**
     * Get my access to the default project
     * @param param the request object
     */
    public getProjectAccessById(param: PermissionsApiGetProjectAccessByIdRequest, options?: Configuration): Promise<Array<GetProjectAccessResponse>> {
        return this.api.getProjectAccessById(param.projectId,  options).toPromise();
    }

    /**
     * Get user and role assignments to the current project
     * @param param the request object
     */
    public getProjectAssignmentsWithHttpInfo(param: PermissionsApiGetProjectAssignmentsRequest = {}, options?: Configuration): Promise<HttpInfo<Array<GetProjectAssignmentsResponse>>> {
        return this.api.getProjectAssignmentsWithHttpInfo(param.relations,  options).toPromise();
    }

    /**
     * Get user and role assignments to the current project
     * @param param the request object
     */
    public getProjectAssignments(param: PermissionsApiGetProjectAssignmentsRequest = {}, options?: Configuration): Promise<Array<GetProjectAssignmentsResponse>> {
        return this.api.getProjectAssignments(param.relations,  options).toPromise();
    }

    /**
     * Get user and role assignments to a project
     * @param param the request object
     */
    public getProjectAssignmentsByIdWithHttpInfo(param: PermissionsApiGetProjectAssignmentsByIdRequest, options?: Configuration): Promise<HttpInfo<Array<GetProjectAssignmentsResponse>>> {
        return this.api.getProjectAssignmentsByIdWithHttpInfo(param.projectId, param.relations,  options).toPromise();
    }

    /**
     * Get user and role assignments to a project
     * @param param the request object
     */
    public getProjectAssignmentsById(param: PermissionsApiGetProjectAssignmentsByIdRequest, options?: Configuration): Promise<Array<GetProjectAssignmentsResponse>> {
        return this.api.getProjectAssignmentsById(param.projectId, param.relations,  options).toPromise();
    }

    /**
     * Get my access to the default project
     * @param param the request object
     */
    public getRoleAccessByIdWithHttpInfo(param: PermissionsApiGetRoleAccessByIdRequest, options?: Configuration): Promise<HttpInfo<Array<GetRoleAccessResponse>>> {
        return this.api.getRoleAccessByIdWithHttpInfo(param.roleId,  options).toPromise();
    }

    /**
     * Get my access to the default project
     * @param param the request object
     */
    public getRoleAccessById(param: PermissionsApiGetRoleAccessByIdRequest, options?: Configuration): Promise<Array<GetRoleAccessResponse>> {
        return this.api.getRoleAccessById(param.roleId,  options).toPromise();
    }

    /**
     * Get user and role assignments to the current project
     * @param param the request object
     */
    public getRoleAssignmentsByIdWithHttpInfo(param: PermissionsApiGetRoleAssignmentsByIdRequest, options?: Configuration): Promise<HttpInfo<Array<GetRoleAssignmentsResponse>>> {
        return this.api.getRoleAssignmentsByIdWithHttpInfo(param.relations, param.roleId,  options).toPromise();
    }

    /**
     * Get user and role assignments to the current project
     * @param param the request object
     */
    public getRoleAssignmentsById(param: PermissionsApiGetRoleAssignmentsByIdRequest, options?: Configuration): Promise<Array<GetRoleAssignmentsResponse>> {
        return this.api.getRoleAssignmentsById(param.relations, param.roleId,  options).toPromise();
    }

    /**
     * Get my access to the server
     * @param param the request object
     */
    public getServerAccessWithHttpInfo(param: PermissionsApiGetServerAccessRequest = {}, options?: Configuration): Promise<HttpInfo<Array<GetServerAccessResponse>>> {
        return this.api.getServerAccessWithHttpInfo( options).toPromise();
    }

    /**
     * Get my access to the server
     * @param param the request object
     */
    public getServerAccess(param: PermissionsApiGetServerAccessRequest = {}, options?: Configuration): Promise<Array<GetServerAccessResponse>> {
        return this.api.getServerAccess( options).toPromise();
    }

    /**
     * Get user and role assignments to the current project
     * @param param the request object
     */
    public getServerAssignmentsWithHttpInfo(param: PermissionsApiGetServerAssignmentsRequest = {}, options?: Configuration): Promise<HttpInfo<Array<GetServerAssignmentsResponse>>> {
        return this.api.getServerAssignmentsWithHttpInfo(param.relations,  options).toPromise();
    }

    /**
     * Get user and role assignments to the current project
     * @param param the request object
     */
    public getServerAssignments(param: PermissionsApiGetServerAssignmentsRequest = {}, options?: Configuration): Promise<Array<GetServerAssignmentsResponse>> {
        return this.api.getServerAssignments(param.relations,  options).toPromise();
    }

    /**
     * Get my access to a table
     * @param param the request object
     */
    public getTableAccessByIdWithHttpInfo(param: PermissionsApiGetTableAccessByIdRequest, options?: Configuration): Promise<HttpInfo<Array<GetTableAccessResponse>>> {
        return this.api.getTableAccessByIdWithHttpInfo(param.tableId,  options).toPromise();
    }

    /**
     * Get my access to a table
     * @param param the request object
     */
    public getTableAccessById(param: PermissionsApiGetTableAccessByIdRequest, options?: Configuration): Promise<Array<GetTableAccessResponse>> {
        return this.api.getTableAccessById(param.tableId,  options).toPromise();
    }

    /**
     * Get my access to a view
     * @param param the request object
     */
    public getViewAccessByIdWithHttpInfo(param: PermissionsApiGetViewAccessByIdRequest, options?: Configuration): Promise<HttpInfo<Array<GetViewAccessResponse>>> {
        return this.api.getViewAccessByIdWithHttpInfo(param.viewId,  options).toPromise();
    }

    /**
     * Get my access to a view
     * @param param the request object
     */
    public getViewAccessById(param: PermissionsApiGetViewAccessByIdRequest, options?: Configuration): Promise<Array<GetViewAccessResponse>> {
        return this.api.getViewAccessById(param.viewId,  options).toPromise();
    }

    /**
     * Get user and role assignments for a view
     * @param param the request object
     */
    public getViewAssignmentsByIdWithHttpInfo(param: PermissionsApiGetViewAssignmentsByIdRequest, options?: Configuration): Promise<HttpInfo<Array<GetViewAssignmentsResponse>>> {
        return this.api.getViewAssignmentsByIdWithHttpInfo(param.namespaceId, param.relations,  options).toPromise();
    }

    /**
     * Get user and role assignments for a view
     * @param param the request object
     */
    public getViewAssignmentsById(param: PermissionsApiGetViewAssignmentsByIdRequest, options?: Configuration): Promise<Array<GetViewAssignmentsResponse>> {
        return this.api.getViewAssignmentsById(param.namespaceId, param.relations,  options).toPromise();
    }

    /**
     * Get my access to a warehouse
     * @param param the request object
     */
    public getWarehouseAccessByIdWithHttpInfo(param: PermissionsApiGetWarehouseAccessByIdRequest, options?: Configuration): Promise<HttpInfo<Array<GetWarehouseAccessResponse>>> {
        return this.api.getWarehouseAccessByIdWithHttpInfo(param.warehouseId,  options).toPromise();
    }

    /**
     * Get my access to a warehouse
     * @param param the request object
     */
    public getWarehouseAccessById(param: PermissionsApiGetWarehouseAccessByIdRequest, options?: Configuration): Promise<Array<GetWarehouseAccessResponse>> {
        return this.api.getWarehouseAccessById(param.warehouseId,  options).toPromise();
    }

    /**
     * Get user and role assignments for a warehouse
     * @param param the request object
     */
    public getWarehouseAssignmentsByIdWithHttpInfo(param: PermissionsApiGetWarehouseAssignmentsByIdRequest, options?: Configuration): Promise<HttpInfo<Array<GetWarehouseAssignmentsResponse>>> {
        return this.api.getWarehouseAssignmentsByIdWithHttpInfo(param.warehouseId, param.relations,  options).toPromise();
    }

    /**
     * Get user and role assignments for a warehouse
     * @param param the request object
     */
    public getWarehouseAssignmentsById(param: PermissionsApiGetWarehouseAssignmentsByIdRequest, options?: Configuration): Promise<Array<GetWarehouseAssignmentsResponse>> {
        return this.api.getWarehouseAssignmentsById(param.warehouseId, param.relations,  options).toPromise();
    }

    /**
     * Get Authorization properties of a warehouse
     * @param param the request object
     */
    public getWarehouseByIdWithHttpInfo(param: PermissionsApiGetWarehouseByIdRequest, options?: Configuration): Promise<HttpInfo<Array<GetWarehouseResponse>>> {
        return this.api.getWarehouseByIdWithHttpInfo(param.warehouseId,  options).toPromise();
    }

    /**
     * Get Authorization properties of a warehouse
     * @param param the request object
     */
    public getWarehouseById(param: PermissionsApiGetWarehouseByIdRequest, options?: Configuration): Promise<Array<GetWarehouseResponse>> {
        return this.api.getWarehouseById(param.warehouseId,  options).toPromise();
    }

    /**
     * Set managed access property of a namespace
     * @param param the request object
     */
    public setNamespaceManagedAccessWithHttpInfo(param: PermissionsApiSetNamespaceManagedAccessRequest, options?: Configuration): Promise<HttpInfo<Array<any | null>>> {
        return this.api.setNamespaceManagedAccessWithHttpInfo(param.namespaceId, param.setManagedAccessRequest,  options).toPromise();
    }

    /**
     * Set managed access property of a namespace
     * @param param the request object
     */
    public setNamespaceManagedAccess(param: PermissionsApiSetNamespaceManagedAccessRequest, options?: Configuration): Promise<Array<any | null>> {
        return this.api.setNamespaceManagedAccess(param.namespaceId, param.setManagedAccessRequest,  options).toPromise();
    }

    /**
     * Set managed access property of a warehouse
     * @param param the request object
     */
    public setWarehouseManagedAccessWithHttpInfo(param: PermissionsApiSetWarehouseManagedAccessRequest, options?: Configuration): Promise<HttpInfo<Array<any | null>>> {
        return this.api.setWarehouseManagedAccessWithHttpInfo(param.warehouseId, param.setManagedAccessRequest,  options).toPromise();
    }

    /**
     * Set managed access property of a warehouse
     * @param param the request object
     */
    public setWarehouseManagedAccess(param: PermissionsApiSetWarehouseManagedAccessRequest, options?: Configuration): Promise<Array<any | null>> {
        return this.api.setWarehouseManagedAccess(param.warehouseId, param.setManagedAccessRequest,  options).toPromise();
    }

    /**
     * Update permissions for a namespace
     * @param param the request object
     */
    public updateNamespaceAssignmentsByIdWithHttpInfo(param: PermissionsApiUpdateNamespaceAssignmentsByIdRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.updateNamespaceAssignmentsByIdWithHttpInfo(param.namespaceId, param.updateNamespaceAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for a namespace
     * @param param the request object
     */
    public updateNamespaceAssignmentsById(param: PermissionsApiUpdateNamespaceAssignmentsByIdRequest, options?: Configuration): Promise<void> {
        return this.api.updateNamespaceAssignmentsById(param.namespaceId, param.updateNamespaceAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for the default project
     * @param param the request object
     */
    public updateProjectAssignmentsWithHttpInfo(param: PermissionsApiUpdateProjectAssignmentsRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.updateProjectAssignmentsWithHttpInfo(param.updateProjectAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for the default project
     * @param param the request object
     */
    public updateProjectAssignments(param: PermissionsApiUpdateProjectAssignmentsRequest, options?: Configuration): Promise<void> {
        return this.api.updateProjectAssignments(param.updateProjectAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for a project
     * @param param the request object
     */
    public updateProjectAssignmentsByIdWithHttpInfo(param: PermissionsApiUpdateProjectAssignmentsByIdRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.updateProjectAssignmentsByIdWithHttpInfo(param.projectId, param.updateProjectAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for a project
     * @param param the request object
     */
    public updateProjectAssignmentsById(param: PermissionsApiUpdateProjectAssignmentsByIdRequest, options?: Configuration): Promise<void> {
        return this.api.updateProjectAssignmentsById(param.projectId, param.updateProjectAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for a view
     * @param param the request object
     */
    public updateRoleAssignmentsByIdWithHttpInfo(param: PermissionsApiUpdateRoleAssignmentsByIdRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.updateRoleAssignmentsByIdWithHttpInfo(param.roleId, param.updateRoleAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for a view
     * @param param the request object
     */
    public updateRoleAssignmentsById(param: PermissionsApiUpdateRoleAssignmentsByIdRequest, options?: Configuration): Promise<void> {
        return this.api.updateRoleAssignmentsById(param.roleId, param.updateRoleAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for this server
     * @param param the request object
     */
    public updateServerAssignmentsWithHttpInfo(param: PermissionsApiUpdateServerAssignmentsRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.updateServerAssignmentsWithHttpInfo(param.updateServerAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for this server
     * @param param the request object
     */
    public updateServerAssignments(param: PermissionsApiUpdateServerAssignmentsRequest, options?: Configuration): Promise<void> {
        return this.api.updateServerAssignments(param.updateServerAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for a table
     * @param param the request object
     */
    public updateTableAssignmentsByIdWithHttpInfo(param: PermissionsApiUpdateTableAssignmentsByIdRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.updateTableAssignmentsByIdWithHttpInfo(param.tableId, param.updateTableAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for a table
     * @param param the request object
     */
    public updateTableAssignmentsById(param: PermissionsApiUpdateTableAssignmentsByIdRequest, options?: Configuration): Promise<void> {
        return this.api.updateTableAssignmentsById(param.tableId, param.updateTableAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for a view
     * @param param the request object
     */
    public updateViewAssignmentsByIdWithHttpInfo(param: PermissionsApiUpdateViewAssignmentsByIdRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.updateViewAssignmentsByIdWithHttpInfo(param.viewId, param.updateViewAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for a view
     * @param param the request object
     */
    public updateViewAssignmentsById(param: PermissionsApiUpdateViewAssignmentsByIdRequest, options?: Configuration): Promise<void> {
        return this.api.updateViewAssignmentsById(param.viewId, param.updateViewAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for a project
     * @param param the request object
     */
    public updateWarehouseAssignmentsByIdWithHttpInfo(param: PermissionsApiUpdateWarehouseAssignmentsByIdRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.updateWarehouseAssignmentsByIdWithHttpInfo(param.warehouseId, param.updateWarehouseAssignmentsRequest,  options).toPromise();
    }

    /**
     * Update permissions for a project
     * @param param the request object
     */
    public updateWarehouseAssignmentsById(param: PermissionsApiUpdateWarehouseAssignmentsByIdRequest, options?: Configuration): Promise<void> {
        return this.api.updateWarehouseAssignmentsById(param.warehouseId, param.updateWarehouseAssignmentsRequest,  options).toPromise();
    }

}

import { ObservableProjectApi } from "./ObservableAPI";
import { ProjectApiRequestFactory, ProjectApiResponseProcessor} from "../apis/ProjectApi";

export interface ProjectApiCreateProjectRequest {
    /**
     * 
     * @type CreateProjectRequest
     * @memberof ProjectApicreateProject
     */
    createProjectRequest: CreateProjectRequest
}

export interface ProjectApiDeleteDefaultProjectRequest {
}

export interface ProjectApiDeleteProjectByIdRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof ProjectApideleteProjectById
     */
    projectId: string
}

export interface ProjectApiGetDefaultProjectRequest {
}

export interface ProjectApiGetProjectByIdRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof ProjectApigetProjectById
     */
    projectId: string
}

export interface ProjectApiListProjectsRequest {
}

export interface ProjectApiRenameDefaultProjectRequest {
    /**
     * 
     * @type RenameProjectRequest
     * @memberof ProjectApirenameDefaultProject
     */
    renameProjectRequest: RenameProjectRequest
}

export interface ProjectApiRenameProjectByIdRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof ProjectApirenameProjectById
     */
    projectId: string
    /**
     * 
     * @type RenameProjectRequest
     * @memberof ProjectApirenameProjectById
     */
    renameProjectRequest: RenameProjectRequest
}

export class ObjectProjectApi {
    private api: ObservableProjectApi

    public constructor(configuration: Configuration, requestFactory?: ProjectApiRequestFactory, responseProcessor?: ProjectApiResponseProcessor) {
        this.api = new ObservableProjectApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Create a new project
     * @param param the request object
     */
    public createProjectWithHttpInfo(param: ProjectApiCreateProjectRequest, options?: Configuration): Promise<HttpInfo<Array<CreateProjectResponse>>> {
        return this.api.createProjectWithHttpInfo(param.createProjectRequest,  options).toPromise();
    }

    /**
     * Create a new project
     * @param param the request object
     */
    public createProject(param: ProjectApiCreateProjectRequest, options?: Configuration): Promise<Array<CreateProjectResponse>> {
        return this.api.createProject(param.createProjectRequest,  options).toPromise();
    }

    /**
     * Delete the default project
     * @param param the request object
     */
    public deleteDefaultProjectWithHttpInfo(param: ProjectApiDeleteDefaultProjectRequest = {}, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.deleteDefaultProjectWithHttpInfo( options).toPromise();
    }

    /**
     * Delete the default project
     * @param param the request object
     */
    public deleteDefaultProject(param: ProjectApiDeleteDefaultProjectRequest = {}, options?: Configuration): Promise<void> {
        return this.api.deleteDefaultProject( options).toPromise();
    }

    /**
     * Delete the default project
     * @param param the request object
     */
    public deleteProjectByIdWithHttpInfo(param: ProjectApiDeleteProjectByIdRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.deleteProjectByIdWithHttpInfo(param.projectId,  options).toPromise();
    }

    /**
     * Delete the default project
     * @param param the request object
     */
    public deleteProjectById(param: ProjectApiDeleteProjectByIdRequest, options?: Configuration): Promise<void> {
        return this.api.deleteProjectById(param.projectId,  options).toPromise();
    }

    /**
     * Get the default project
     * @param param the request object
     */
    public getDefaultProjectWithHttpInfo(param: ProjectApiGetDefaultProjectRequest = {}, options?: Configuration): Promise<HttpInfo<Array<GetProjectResponse>>> {
        return this.api.getDefaultProjectWithHttpInfo( options).toPromise();
    }

    /**
     * Get the default project
     * @param param the request object
     */
    public getDefaultProject(param: ProjectApiGetDefaultProjectRequest = {}, options?: Configuration): Promise<Array<GetProjectResponse>> {
        return this.api.getDefaultProject( options).toPromise();
    }

    /**
     * Get a specific project by id
     * @param param the request object
     */
    public getProjectByIdWithHttpInfo(param: ProjectApiGetProjectByIdRequest, options?: Configuration): Promise<HttpInfo<Array<GetProjectResponse>>> {
        return this.api.getProjectByIdWithHttpInfo(param.projectId,  options).toPromise();
    }

    /**
     * Get a specific project by id
     * @param param the request object
     */
    public getProjectById(param: ProjectApiGetProjectByIdRequest, options?: Configuration): Promise<Array<GetProjectResponse>> {
        return this.api.getProjectById(param.projectId,  options).toPromise();
    }

    /**
     * List all projects the requesting user has access to
     * @param param the request object
     */
    public listProjectsWithHttpInfo(param: ProjectApiListProjectsRequest = {}, options?: Configuration): Promise<HttpInfo<Array<ListProjectsResponse>>> {
        return this.api.listProjectsWithHttpInfo( options).toPromise();
    }

    /**
     * List all projects the requesting user has access to
     * @param param the request object
     */
    public listProjects(param: ProjectApiListProjectsRequest = {}, options?: Configuration): Promise<Array<ListProjectsResponse>> {
        return this.api.listProjects( options).toPromise();
    }

    /**
     * Rename the default project
     * @param param the request object
     */
    public renameDefaultProjectWithHttpInfo(param: ProjectApiRenameDefaultProjectRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.renameDefaultProjectWithHttpInfo(param.renameProjectRequest,  options).toPromise();
    }

    /**
     * Rename the default project
     * @param param the request object
     */
    public renameDefaultProject(param: ProjectApiRenameDefaultProjectRequest, options?: Configuration): Promise<void> {
        return this.api.renameDefaultProject(param.renameProjectRequest,  options).toPromise();
    }

    /**
     * Rename project by id
     * @param param the request object
     */
    public renameProjectByIdWithHttpInfo(param: ProjectApiRenameProjectByIdRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.renameProjectByIdWithHttpInfo(param.projectId, param.renameProjectRequest,  options).toPromise();
    }

    /**
     * Rename project by id
     * @param param the request object
     */
    public renameProjectById(param: ProjectApiRenameProjectByIdRequest, options?: Configuration): Promise<void> {
        return this.api.renameProjectById(param.projectId, param.renameProjectRequest,  options).toPromise();
    }

}

import { ObservableRoleApi } from "./ObservableAPI";
import { RoleApiRequestFactory, RoleApiResponseProcessor} from "../apis/RoleApi";

export interface RoleApiCreateRoleRequest {
    /**
     * 
     * @type CreateRoleRequest
     * @memberof RoleApicreateRole
     */
    createRoleRequest: CreateRoleRequest
}

export interface RoleApiDeleteRoleRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof RoleApideleteRole
     */
    id: string
}

export interface RoleApiGetRoleRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof RoleApigetRole
     */
    id: string
}

export interface RoleApiListRolesRequest {
    /**
     * Search for a specific role name
     * Defaults to: undefined
     * @type string
     * @memberof RoleApilistRoles
     */
    name?: string
    /**
     * Next page token
     * Defaults to: undefined
     * @type string
     * @memberof RoleApilistRoles
     */
    pageToken?: string
    /**
     * Signals an upper bound of the number of results that a client will receive. Default: 100
     * Defaults to: undefined
     * @type number
     * @memberof RoleApilistRoles
     */
    pageSize?: number
    /**
     * Project ID from which roles should be listed Only required if the project ID cannot be inferred from the users token and no default project is set.
     * Defaults to: undefined
     * @type string
     * @memberof RoleApilistRoles
     */
    projectId?: string
}

export interface RoleApiUpdateRoleRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof RoleApiupdateRole
     */
    id: string
    /**
     * 
     * @type UpdateRoleRequest
     * @memberof RoleApiupdateRole
     */
    updateRoleRequest: UpdateRoleRequest
}

export class ObjectRoleApi {
    private api: ObservableRoleApi

    public constructor(configuration: Configuration, requestFactory?: RoleApiRequestFactory, responseProcessor?: RoleApiResponseProcessor) {
        this.api = new ObservableRoleApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Create a new role
     * @param param the request object
     */
    public createRoleWithHttpInfo(param: RoleApiCreateRoleRequest, options?: Configuration): Promise<HttpInfo<Array<Role>>> {
        return this.api.createRoleWithHttpInfo(param.createRoleRequest,  options).toPromise();
    }

    /**
     * Create a new role
     * @param param the request object
     */
    public createRole(param: RoleApiCreateRoleRequest, options?: Configuration): Promise<Array<Role>> {
        return this.api.createRole(param.createRoleRequest,  options).toPromise();
    }

    /**
     * All permissions of the role are permanently removed.
     * Delete role
     * @param param the request object
     */
    public deleteRoleWithHttpInfo(param: RoleApiDeleteRoleRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.deleteRoleWithHttpInfo(param.id,  options).toPromise();
    }

    /**
     * All permissions of the role are permanently removed.
     * Delete role
     * @param param the request object
     */
    public deleteRole(param: RoleApiDeleteRoleRequest, options?: Configuration): Promise<void> {
        return this.api.deleteRole(param.id,  options).toPromise();
    }

    /**
     * Get a role
     * @param param the request object
     */
    public getRoleWithHttpInfo(param: RoleApiGetRoleRequest, options?: Configuration): Promise<HttpInfo<Array<Role>>> {
        return this.api.getRoleWithHttpInfo(param.id,  options).toPromise();
    }

    /**
     * Get a role
     * @param param the request object
     */
    public getRole(param: RoleApiGetRoleRequest, options?: Configuration): Promise<Array<Role>> {
        return this.api.getRole(param.id,  options).toPromise();
    }

    /**
     * List roles in a project
     * @param param the request object
     */
    public listRolesWithHttpInfo(param: RoleApiListRolesRequest = {}, options?: Configuration): Promise<HttpInfo<Array<ListRolesResponse>>> {
        return this.api.listRolesWithHttpInfo(param.name, param.pageToken, param.pageSize, param.projectId,  options).toPromise();
    }

    /**
     * List roles in a project
     * @param param the request object
     */
    public listRoles(param: RoleApiListRolesRequest = {}, options?: Configuration): Promise<Array<ListRolesResponse>> {
        return this.api.listRoles(param.name, param.pageToken, param.pageSize, param.projectId,  options).toPromise();
    }

    /**
     * Update a role
     * @param param the request object
     */
    public updateRoleWithHttpInfo(param: RoleApiUpdateRoleRequest, options?: Configuration): Promise<HttpInfo<Array<Role>>> {
        return this.api.updateRoleWithHttpInfo(param.id, param.updateRoleRequest,  options).toPromise();
    }

    /**
     * Update a role
     * @param param the request object
     */
    public updateRole(param: RoleApiUpdateRoleRequest, options?: Configuration): Promise<Array<Role>> {
        return this.api.updateRole(param.id, param.updateRoleRequest,  options).toPromise();
    }

}

import { ObservableServerApi } from "./ObservableAPI";
import { ServerApiRequestFactory, ServerApiResponseProcessor} from "../apis/ServerApi";

export interface ServerApiBootstrapRequest {
    /**
     * 
     * @type BootstrapRequest
     * @memberof ServerApibootstrap
     */
    bootstrapRequest: BootstrapRequest
}

export interface ServerApiGetServerInfoRequest {
}

export class ObjectServerApi {
    private api: ObservableServerApi

    public constructor(configuration: Configuration, requestFactory?: ServerApiRequestFactory, responseProcessor?: ServerApiResponseProcessor) {
        this.api = new ObservableServerApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * If the user exists, it updates the users\' metadata from the token. The token sent to this endpoint should have \"profile\" and \"email\" scopes.
     * Creates the user in the catalog if it does not exist.
     * @param param the request object
     */
    public bootstrapWithHttpInfo(param: ServerApiBootstrapRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.bootstrapWithHttpInfo(param.bootstrapRequest,  options).toPromise();
    }

    /**
     * If the user exists, it updates the users\' metadata from the token. The token sent to this endpoint should have \"profile\" and \"email\" scopes.
     * Creates the user in the catalog if it does not exist.
     * @param param the request object
     */
    public bootstrap(param: ServerApiBootstrapRequest, options?: Configuration): Promise<void> {
        return this.api.bootstrap(param.bootstrapRequest,  options).toPromise();
    }

    /**
     * Get information about the server
     * @param param the request object
     */
    public getServerInfoWithHttpInfo(param: ServerApiGetServerInfoRequest = {}, options?: Configuration): Promise<HttpInfo<Array<ServerInfo>>> {
        return this.api.getServerInfoWithHttpInfo( options).toPromise();
    }

    /**
     * Get information about the server
     * @param param the request object
     */
    public getServerInfo(param: ServerApiGetServerInfoRequest = {}, options?: Configuration): Promise<Array<ServerInfo>> {
        return this.api.getServerInfo( options).toPromise();
    }

}

import { ObservableUserApi } from "./ObservableAPI";
import { UserApiRequestFactory, UserApiResponseProcessor} from "../apis/UserApi";

export interface UserApiCreateUserRequest {
    /**
     * 
     * @type CreateUserRequest
     * @memberof UserApicreateUser
     */
    createUserRequest: CreateUserRequest
}

export interface UserApiDeleteUserRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof UserApideleteUser
     */
    id: string
}

export interface UserApiGetUserRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof UserApigetUser
     */
    id: string
}

export interface UserApiListUserRequest {
    /**
     * Search for a specific username
     * Defaults to: undefined
     * @type string
     * @memberof UserApilistUser
     */
    name?: string
    /**
     * Next page token
     * Defaults to: undefined
     * @type string
     * @memberof UserApilistUser
     */
    pageToken?: string
    /**
     * Signals an upper bound of the number of results that a client will receive. Default: 100
     * Defaults to: undefined
     * @type number
     * @memberof UserApilistUser
     */
    pageSize?: number
}

export interface UserApiSearchUserRequest {
    /**
     * 
     * @type SearchUserRequest
     * @memberof UserApisearchUser
     */
    searchUserRequest: SearchUserRequest
}

export interface UserApiUpdateUserRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof UserApiupdateUser
     */
    id: string
    /**
     * 
     * @type UpdateUserRequest
     * @memberof UserApiupdateUser
     */
    updateUserRequest: UpdateUserRequest
}

export interface UserApiWhoamiRequest {
}

export class ObjectUserApi {
    private api: ObservableUserApi

    public constructor(configuration: Configuration, requestFactory?: UserApiRequestFactory, responseProcessor?: UserApiResponseProcessor) {
        this.api = new ObservableUserApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * If the user exists, it updates the users\' metadata from the token. The token sent to this endpoint should have \"profile\" and \"email\" scopes.
     * Creates the user in the catalog if it does not exist.
     * @param param the request object
     */
    public createUserWithHttpInfo(param: UserApiCreateUserRequest, options?: Configuration): Promise<HttpInfo<Array<User>>> {
        return this.api.createUserWithHttpInfo(param.createUserRequest,  options).toPromise();
    }

    /**
     * If the user exists, it updates the users\' metadata from the token. The token sent to this endpoint should have \"profile\" and \"email\" scopes.
     * Creates the user in the catalog if it does not exist.
     * @param param the request object
     */
    public createUser(param: UserApiCreateUserRequest, options?: Configuration): Promise<Array<User>> {
        return this.api.createUser(param.createUserRequest,  options).toPromise();
    }

    /**
     * All permissions of the user are permanently removed and need to be re-added if the user is re-registered.
     * Delete user
     * @param param the request object
     */
    public deleteUserWithHttpInfo(param: UserApiDeleteUserRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.deleteUserWithHttpInfo(param.id,  options).toPromise();
    }

    /**
     * All permissions of the user are permanently removed and need to be re-added if the user is re-registered.
     * Delete user
     * @param param the request object
     */
    public deleteUser(param: UserApiDeleteUserRequest, options?: Configuration): Promise<void> {
        return this.api.deleteUser(param.id,  options).toPromise();
    }

    /**
     * Get a user by ID
     * @param param the request object
     */
    public getUserWithHttpInfo(param: UserApiGetUserRequest, options?: Configuration): Promise<HttpInfo<Array<User>>> {
        return this.api.getUserWithHttpInfo(param.id,  options).toPromise();
    }

    /**
     * Get a user by ID
     * @param param the request object
     */
    public getUser(param: UserApiGetUserRequest, options?: Configuration): Promise<Array<User>> {
        return this.api.getUser(param.id,  options).toPromise();
    }

    /**
     * List users
     * @param param the request object
     */
    public listUserWithHttpInfo(param: UserApiListUserRequest = {}, options?: Configuration): Promise<HttpInfo<Array<ListUsersResponse>>> {
        return this.api.listUserWithHttpInfo(param.name, param.pageToken, param.pageSize,  options).toPromise();
    }

    /**
     * List users
     * @param param the request object
     */
    public listUser(param: UserApiListUserRequest = {}, options?: Configuration): Promise<Array<ListUsersResponse>> {
        return this.api.listUser(param.name, param.pageToken, param.pageSize,  options).toPromise();
    }

    /**
     * Search for users (Fuzzy)
     * @param param the request object
     */
    public searchUserWithHttpInfo(param: UserApiSearchUserRequest, options?: Configuration): Promise<HttpInfo<Array<SearchUserResponse>>> {
        return this.api.searchUserWithHttpInfo(param.searchUserRequest,  options).toPromise();
    }

    /**
     * Search for users (Fuzzy)
     * @param param the request object
     */
    public searchUser(param: UserApiSearchUserRequest, options?: Configuration): Promise<Array<SearchUserResponse>> {
        return this.api.searchUser(param.searchUserRequest,  options).toPromise();
    }

    /**
     * If a field is not provided, it is set to `None`.
     * Update details of a user. Replaces the current details with the new details.
     * @param param the request object
     */
    public updateUserWithHttpInfo(param: UserApiUpdateUserRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.updateUserWithHttpInfo(param.id, param.updateUserRequest,  options).toPromise();
    }

    /**
     * If a field is not provided, it is set to `None`.
     * Update details of a user. Replaces the current details with the new details.
     * @param param the request object
     */
    public updateUser(param: UserApiUpdateUserRequest, options?: Configuration): Promise<void> {
        return this.api.updateUser(param.id, param.updateUserRequest,  options).toPromise();
    }

    /**
     * Get the currently authenticated user
     * @param param the request object
     */
    public whoamiWithHttpInfo(param: UserApiWhoamiRequest = {}, options?: Configuration): Promise<HttpInfo<Array<User>>> {
        return this.api.whoamiWithHttpInfo( options).toPromise();
    }

    /**
     * Get the currently authenticated user
     * @param param the request object
     */
    public whoami(param: UserApiWhoamiRequest = {}, options?: Configuration): Promise<Array<User>> {
        return this.api.whoami( options).toPromise();
    }

}

import { ObservableWarehouseApi } from "./ObservableAPI";
import { WarehouseApiRequestFactory, WarehouseApiResponseProcessor} from "../apis/WarehouseApi";

export interface WarehouseApiActivateWarehouseRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof WarehouseApiactivateWarehouse
     */
    warehouseId: string
}

export interface WarehouseApiCreateWarehouseRequest {
    /**
     * 
     * @type CreateWarehouseRequest
     * @memberof WarehouseApicreateWarehouse
     */
    createWarehouseRequest: CreateWarehouseRequest
}

export interface WarehouseApiDeactivateWarehouseRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof WarehouseApideactivateWarehouse
     */
    warehouseId: string
}

export interface WarehouseApiDeleteWarehouseRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof WarehouseApideleteWarehouse
     */
    warehouseId: string
}

export interface WarehouseApiGetWarehouseRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof WarehouseApigetWarehouse
     */
    warehouseId: string
}

export interface WarehouseApiListDeletedTabularsRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof WarehouseApilistDeletedTabulars
     */
    warehouseId: string
    /**
     * Next page token
     * Defaults to: undefined
     * @type string
     * @memberof WarehouseApilistDeletedTabulars
     */
    pageToken?: string
    /**
     * Signals an upper bound of the number of results that a client will receive.
     * Defaults to: undefined
     * @type number
     * @memberof WarehouseApilistDeletedTabulars
     */
    pageSize?: number
}

export interface WarehouseApiListWarehousesRequest {
    /**
     * Optional filter to return only warehouses with the specified status. If not provided, only active warehouses are returned.
     * Defaults to: undefined
     * @type Array&lt;WarehouseStatus&gt;
     * @memberof WarehouseApilistWarehouses
     */
    warehouseStatus?: Array<WarehouseStatus>
    /**
     * The project ID to list warehouses for. Setting a warehouse is required.
     * Defaults to: undefined
     * @type string
     * @memberof WarehouseApilistWarehouses
     */
    projectId?: string
}

export interface WarehouseApiRenameWarehouseRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof WarehouseApirenameWarehouse
     */
    warehouseId: string
    /**
     * 
     * @type RenameWarehouseRequest
     * @memberof WarehouseApirenameWarehouse
     */
    renameWarehouseRequest: RenameWarehouseRequest
}

export interface WarehouseApiUpdateStorageCredentialRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof WarehouseApiupdateStorageCredential
     */
    warehouseId: string
    /**
     * 
     * @type UpdateWarehouseCredentialRequest
     * @memberof WarehouseApiupdateStorageCredential
     */
    updateWarehouseCredentialRequest: UpdateWarehouseCredentialRequest
}

export interface WarehouseApiUpdateStorageProfileRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof WarehouseApiupdateStorageProfile
     */
    warehouseId: string
    /**
     * 
     * @type UpdateWarehouseStorageRequest
     * @memberof WarehouseApiupdateStorageProfile
     */
    updateWarehouseStorageRequest: UpdateWarehouseStorageRequest
}

export interface WarehouseApiUpdateWarehouseDeleteProfileRequest {
    /**
     * 
     * Defaults to: undefined
     * @type string
     * @memberof WarehouseApiupdateWarehouseDeleteProfile
     */
    warehouseId: string
    /**
     * 
     * @type UpdateWarehouseDeleteProfileRequest
     * @memberof WarehouseApiupdateWarehouseDeleteProfile
     */
    updateWarehouseDeleteProfileRequest: UpdateWarehouseDeleteProfileRequest
}

export class ObjectWarehouseApi {
    private api: ObservableWarehouseApi

    public constructor(configuration: Configuration, requestFactory?: WarehouseApiRequestFactory, responseProcessor?: WarehouseApiResponseProcessor) {
        this.api = new ObservableWarehouseApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Activate a warehouse
     * @param param the request object
     */
    public activateWarehouseWithHttpInfo(param: WarehouseApiActivateWarehouseRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.activateWarehouseWithHttpInfo(param.warehouseId,  options).toPromise();
    }

    /**
     * Activate a warehouse
     * @param param the request object
     */
    public activateWarehouse(param: WarehouseApiActivateWarehouseRequest, options?: Configuration): Promise<void> {
        return this.api.activateWarehouse(param.warehouseId,  options).toPromise();
    }

    /**
     * Create a new warehouse in the given project. The project of a warehouse cannot be changed after creation. The storage configuration is validated by this method.
     * Create a new warehouse.
     * @param param the request object
     */
    public createWarehouseWithHttpInfo(param: WarehouseApiCreateWarehouseRequest, options?: Configuration): Promise<HttpInfo<Array<CreateWarehouseResponse>>> {
        return this.api.createWarehouseWithHttpInfo(param.createWarehouseRequest,  options).toPromise();
    }

    /**
     * Create a new warehouse in the given project. The project of a warehouse cannot be changed after creation. The storage configuration is validated by this method.
     * Create a new warehouse.
     * @param param the request object
     */
    public createWarehouse(param: WarehouseApiCreateWarehouseRequest, options?: Configuration): Promise<Array<CreateWarehouseResponse>> {
        return this.api.createWarehouse(param.createWarehouseRequest,  options).toPromise();
    }

    /**
     * Deactivate a warehouse
     * @param param the request object
     */
    public deactivateWarehouseWithHttpInfo(param: WarehouseApiDeactivateWarehouseRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.deactivateWarehouseWithHttpInfo(param.warehouseId,  options).toPromise();
    }

    /**
     * Deactivate a warehouse
     * @param param the request object
     */
    public deactivateWarehouse(param: WarehouseApiDeactivateWarehouseRequest, options?: Configuration): Promise<void> {
        return this.api.deactivateWarehouse(param.warehouseId,  options).toPromise();
    }

    /**
     * Delete a warehouse by ID
     * @param param the request object
     */
    public deleteWarehouseWithHttpInfo(param: WarehouseApiDeleteWarehouseRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.deleteWarehouseWithHttpInfo(param.warehouseId,  options).toPromise();
    }

    /**
     * Delete a warehouse by ID
     * @param param the request object
     */
    public deleteWarehouse(param: WarehouseApiDeleteWarehouseRequest, options?: Configuration): Promise<void> {
        return this.api.deleteWarehouse(param.warehouseId,  options).toPromise();
    }

    /**
     * Get a warehouse by ID
     * @param param the request object
     */
    public getWarehouseWithHttpInfo(param: WarehouseApiGetWarehouseRequest, options?: Configuration): Promise<HttpInfo<Array<GetWarehouseResponse>>> {
        return this.api.getWarehouseWithHttpInfo(param.warehouseId,  options).toPromise();
    }

    /**
     * Get a warehouse by ID
     * @param param the request object
     */
    public getWarehouse(param: WarehouseApiGetWarehouseRequest, options?: Configuration): Promise<Array<GetWarehouseResponse>> {
        return this.api.getWarehouse(param.warehouseId,  options).toPromise();
    }

    /**
     * List all soft-deleted tabulars in the warehouse that are visible to you.
     * List soft-deleted tabulars
     * @param param the request object
     */
    public listDeletedTabularsWithHttpInfo(param: WarehouseApiListDeletedTabularsRequest, options?: Configuration): Promise<HttpInfo<Array<ListDeletedTabularsResponse>>> {
        return this.api.listDeletedTabularsWithHttpInfo(param.warehouseId, param.pageToken, param.pageSize,  options).toPromise();
    }

    /**
     * List all soft-deleted tabulars in the warehouse that are visible to you.
     * List soft-deleted tabulars
     * @param param the request object
     */
    public listDeletedTabulars(param: WarehouseApiListDeletedTabularsRequest, options?: Configuration): Promise<Array<ListDeletedTabularsResponse>> {
        return this.api.listDeletedTabulars(param.warehouseId, param.pageToken, param.pageSize,  options).toPromise();
    }

    /**
     * By default, this endpoint does not return deactivated warehouses. To include deactivated warehouses, set the `include_deactivated` query parameter to `true`.
     * List all warehouses in a project
     * @param param the request object
     */
    public listWarehousesWithHttpInfo(param: WarehouseApiListWarehousesRequest = {}, options?: Configuration): Promise<HttpInfo<Array<ListWarehousesResponse>>> {
        return this.api.listWarehousesWithHttpInfo(param.warehouseStatus, param.projectId,  options).toPromise();
    }

    /**
     * By default, this endpoint does not return deactivated warehouses. To include deactivated warehouses, set the `include_deactivated` query parameter to `true`.
     * List all warehouses in a project
     * @param param the request object
     */
    public listWarehouses(param: WarehouseApiListWarehousesRequest = {}, options?: Configuration): Promise<Array<ListWarehousesResponse>> {
        return this.api.listWarehouses(param.warehouseStatus, param.projectId,  options).toPromise();
    }

    /**
     * Rename a warehouse
     * @param param the request object
     */
    public renameWarehouseWithHttpInfo(param: WarehouseApiRenameWarehouseRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.renameWarehouseWithHttpInfo(param.warehouseId, param.renameWarehouseRequest,  options).toPromise();
    }

    /**
     * Rename a warehouse
     * @param param the request object
     */
    public renameWarehouse(param: WarehouseApiRenameWarehouseRequest, options?: Configuration): Promise<void> {
        return this.api.renameWarehouse(param.warehouseId, param.renameWarehouseRequest,  options).toPromise();
    }

    /**
     * This can be used to update credentials before expiration.
     * Update the storage credential of a warehouse. The storage profile is not modified.
     * @param param the request object
     */
    public updateStorageCredentialWithHttpInfo(param: WarehouseApiUpdateStorageCredentialRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.updateStorageCredentialWithHttpInfo(param.warehouseId, param.updateWarehouseCredentialRequest,  options).toPromise();
    }

    /**
     * This can be used to update credentials before expiration.
     * Update the storage credential of a warehouse. The storage profile is not modified.
     * @param param the request object
     */
    public updateStorageCredential(param: WarehouseApiUpdateStorageCredentialRequest, options?: Configuration): Promise<void> {
        return this.api.updateStorageCredential(param.warehouseId, param.updateWarehouseCredentialRequest,  options).toPromise();
    }

    /**
     * Update the storage profile of a warehouse including its storage credential.
     * @param param the request object
     */
    public updateStorageProfileWithHttpInfo(param: WarehouseApiUpdateStorageProfileRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.updateStorageProfileWithHttpInfo(param.warehouseId, param.updateWarehouseStorageRequest,  options).toPromise();
    }

    /**
     * Update the storage profile of a warehouse including its storage credential.
     * @param param the request object
     */
    public updateStorageProfile(param: WarehouseApiUpdateStorageProfileRequest, options?: Configuration): Promise<void> {
        return this.api.updateStorageProfile(param.warehouseId, param.updateWarehouseStorageRequest,  options).toPromise();
    }

    /**
     * Update the Deletion Profile (soft-delete) of a warehouse.
     * @param param the request object
     */
    public updateWarehouseDeleteProfileWithHttpInfo(param: WarehouseApiUpdateWarehouseDeleteProfileRequest, options?: Configuration): Promise<HttpInfo<void>> {
        return this.api.updateWarehouseDeleteProfileWithHttpInfo(param.warehouseId, param.updateWarehouseDeleteProfileRequest,  options).toPromise();
    }

    /**
     * Update the Deletion Profile (soft-delete) of a warehouse.
     * @param param the request object
     */
    public updateWarehouseDeleteProfile(param: WarehouseApiUpdateWarehouseDeleteProfileRequest, options?: Configuration): Promise<void> {
        return this.api.updateWarehouseDeleteProfile(param.warehouseId, param.updateWarehouseDeleteProfileRequest,  options).toPromise();
    }

}
