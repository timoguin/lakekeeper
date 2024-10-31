import { ResponseContext, RequestContext, HttpFile, HttpInfo } from '../http/http';
import { Configuration} from '../configuration'
import { Observable, of, from } from '../rxjsStub';
import {mergeMap, map} from  '../rxjsStub';
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

import { PermissionsApiRequestFactory, PermissionsApiResponseProcessor} from "../apis/PermissionsApi";
export class ObservablePermissionsApi {
    private requestFactory: PermissionsApiRequestFactory;
    private responseProcessor: PermissionsApiResponseProcessor;
    private configuration: Configuration;

    public constructor(
        configuration: Configuration,
        requestFactory?: PermissionsApiRequestFactory,
        responseProcessor?: PermissionsApiResponseProcessor
    ) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new PermissionsApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new PermissionsApiResponseProcessor();
    }

    /**
     * Get my access to a namespace
     * @param namespaceId Namespace ID
     */
    public getNamespaceAccessByIdWithHttpInfo(namespaceId: string, _options?: Configuration): Observable<HttpInfo<Array<GetNamespaceAccessResponse>>> {
        const requestContextPromise = this.requestFactory.getNamespaceAccessById(namespaceId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getNamespaceAccessByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get my access to a namespace
     * @param namespaceId Namespace ID
     */
    public getNamespaceAccessById(namespaceId: string, _options?: Configuration): Observable<Array<GetNamespaceAccessResponse>> {
        return this.getNamespaceAccessByIdWithHttpInfo(namespaceId, _options).pipe(map((apiResponse: HttpInfo<Array<GetNamespaceAccessResponse>>) => apiResponse.data));
    }

    /**
     * Get user and role assignments for a namespace
     * @param namespaceId Namespace ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getNamespaceAssignmentsByIdWithHttpInfo(namespaceId: string, relations?: Array<NamespaceRelation>, _options?: Configuration): Observable<HttpInfo<Array<GetNamespaceAssignmentsResponse>>> {
        const requestContextPromise = this.requestFactory.getNamespaceAssignmentsById(namespaceId, relations, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getNamespaceAssignmentsByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get user and role assignments for a namespace
     * @param namespaceId Namespace ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getNamespaceAssignmentsById(namespaceId: string, relations?: Array<NamespaceRelation>, _options?: Configuration): Observable<Array<GetNamespaceAssignmentsResponse>> {
        return this.getNamespaceAssignmentsByIdWithHttpInfo(namespaceId, relations, _options).pipe(map((apiResponse: HttpInfo<Array<GetNamespaceAssignmentsResponse>>) => apiResponse.data));
    }

    /**
     * Get Authorization properties of a namespace
     * @param namespaceId Namespace ID
     */
    public getNamespaceByIdWithHttpInfo(namespaceId: string, _options?: Configuration): Observable<HttpInfo<Array<GetNamespaceResponse>>> {
        const requestContextPromise = this.requestFactory.getNamespaceById(namespaceId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getNamespaceByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get Authorization properties of a namespace
     * @param namespaceId Namespace ID
     */
    public getNamespaceById(namespaceId: string, _options?: Configuration): Observable<Array<GetNamespaceResponse>> {
        return this.getNamespaceByIdWithHttpInfo(namespaceId, _options).pipe(map((apiResponse: HttpInfo<Array<GetNamespaceResponse>>) => apiResponse.data));
    }

    /**
     * Get my access to the default project
     */
    public getProjectAccessWithHttpInfo(_options?: Configuration): Observable<HttpInfo<Array<GetProjectAccessResponse>>> {
        const requestContextPromise = this.requestFactory.getProjectAccess(_options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getProjectAccessWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get my access to the default project
     */
    public getProjectAccess(_options?: Configuration): Observable<Array<GetProjectAccessResponse>> {
        return this.getProjectAccessWithHttpInfo(_options).pipe(map((apiResponse: HttpInfo<Array<GetProjectAccessResponse>>) => apiResponse.data));
    }

    /**
     * Get my access to the default project
     * @param projectId Project ID
     */
    public getProjectAccessByIdWithHttpInfo(projectId: string, _options?: Configuration): Observable<HttpInfo<Array<GetProjectAccessResponse>>> {
        const requestContextPromise = this.requestFactory.getProjectAccessById(projectId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getProjectAccessByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get my access to the default project
     * @param projectId Project ID
     */
    public getProjectAccessById(projectId: string, _options?: Configuration): Observable<Array<GetProjectAccessResponse>> {
        return this.getProjectAccessByIdWithHttpInfo(projectId, _options).pipe(map((apiResponse: HttpInfo<Array<GetProjectAccessResponse>>) => apiResponse.data));
    }

    /**
     * Get user and role assignments to the current project
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getProjectAssignmentsWithHttpInfo(relations?: Array<ProjectRelation>, _options?: Configuration): Observable<HttpInfo<Array<GetProjectAssignmentsResponse>>> {
        const requestContextPromise = this.requestFactory.getProjectAssignments(relations, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getProjectAssignmentsWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get user and role assignments to the current project
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getProjectAssignments(relations?: Array<ProjectRelation>, _options?: Configuration): Observable<Array<GetProjectAssignmentsResponse>> {
        return this.getProjectAssignmentsWithHttpInfo(relations, _options).pipe(map((apiResponse: HttpInfo<Array<GetProjectAssignmentsResponse>>) => apiResponse.data));
    }

    /**
     * Get user and role assignments to a project
     * @param projectId Project ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getProjectAssignmentsByIdWithHttpInfo(projectId: string, relations?: Array<ProjectRelation>, _options?: Configuration): Observable<HttpInfo<Array<GetProjectAssignmentsResponse>>> {
        const requestContextPromise = this.requestFactory.getProjectAssignmentsById(projectId, relations, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getProjectAssignmentsByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get user and role assignments to a project
     * @param projectId Project ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getProjectAssignmentsById(projectId: string, relations?: Array<ProjectRelation>, _options?: Configuration): Observable<Array<GetProjectAssignmentsResponse>> {
        return this.getProjectAssignmentsByIdWithHttpInfo(projectId, relations, _options).pipe(map((apiResponse: HttpInfo<Array<GetProjectAssignmentsResponse>>) => apiResponse.data));
    }

    /**
     * Get my access to the default project
     * @param roleId Role ID
     */
    public getRoleAccessByIdWithHttpInfo(roleId: string, _options?: Configuration): Observable<HttpInfo<Array<GetRoleAccessResponse>>> {
        const requestContextPromise = this.requestFactory.getRoleAccessById(roleId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getRoleAccessByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get my access to the default project
     * @param roleId Role ID
     */
    public getRoleAccessById(roleId: string, _options?: Configuration): Observable<Array<GetRoleAccessResponse>> {
        return this.getRoleAccessByIdWithHttpInfo(roleId, _options).pipe(map((apiResponse: HttpInfo<Array<GetRoleAccessResponse>>) => apiResponse.data));
    }

    /**
     * Get user and role assignments to the current project
     * @param relations Relations to be loaded. If not specified, all relations are returned.
     * @param roleId Role ID
     */
    public getRoleAssignmentsByIdWithHttpInfo(relations: Array<ProjectRelation>, roleId: string, _options?: Configuration): Observable<HttpInfo<Array<GetRoleAssignmentsResponse>>> {
        const requestContextPromise = this.requestFactory.getRoleAssignmentsById(relations, roleId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getRoleAssignmentsByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get user and role assignments to the current project
     * @param relations Relations to be loaded. If not specified, all relations are returned.
     * @param roleId Role ID
     */
    public getRoleAssignmentsById(relations: Array<ProjectRelation>, roleId: string, _options?: Configuration): Observable<Array<GetRoleAssignmentsResponse>> {
        return this.getRoleAssignmentsByIdWithHttpInfo(relations, roleId, _options).pipe(map((apiResponse: HttpInfo<Array<GetRoleAssignmentsResponse>>) => apiResponse.data));
    }

    /**
     * Get my access to the server
     */
    public getServerAccessWithHttpInfo(_options?: Configuration): Observable<HttpInfo<Array<GetServerAccessResponse>>> {
        const requestContextPromise = this.requestFactory.getServerAccess(_options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getServerAccessWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get my access to the server
     */
    public getServerAccess(_options?: Configuration): Observable<Array<GetServerAccessResponse>> {
        return this.getServerAccessWithHttpInfo(_options).pipe(map((apiResponse: HttpInfo<Array<GetServerAccessResponse>>) => apiResponse.data));
    }

    /**
     * Get user and role assignments to the current project
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getServerAssignmentsWithHttpInfo(relations?: Array<ServerRelation>, _options?: Configuration): Observable<HttpInfo<Array<GetServerAssignmentsResponse>>> {
        const requestContextPromise = this.requestFactory.getServerAssignments(relations, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getServerAssignmentsWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get user and role assignments to the current project
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getServerAssignments(relations?: Array<ServerRelation>, _options?: Configuration): Observable<Array<GetServerAssignmentsResponse>> {
        return this.getServerAssignmentsWithHttpInfo(relations, _options).pipe(map((apiResponse: HttpInfo<Array<GetServerAssignmentsResponse>>) => apiResponse.data));
    }

    /**
     * Get my access to a table
     * @param tableId Table ID
     */
    public getTableAccessByIdWithHttpInfo(tableId: string, _options?: Configuration): Observable<HttpInfo<Array<GetTableAccessResponse>>> {
        const requestContextPromise = this.requestFactory.getTableAccessById(tableId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getTableAccessByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get my access to a table
     * @param tableId Table ID
     */
    public getTableAccessById(tableId: string, _options?: Configuration): Observable<Array<GetTableAccessResponse>> {
        return this.getTableAccessByIdWithHttpInfo(tableId, _options).pipe(map((apiResponse: HttpInfo<Array<GetTableAccessResponse>>) => apiResponse.data));
    }

    /**
     * Get my access to a view
     * @param viewId View ID
     */
    public getViewAccessByIdWithHttpInfo(viewId: string, _options?: Configuration): Observable<HttpInfo<Array<GetViewAccessResponse>>> {
        const requestContextPromise = this.requestFactory.getViewAccessById(viewId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getViewAccessByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get my access to a view
     * @param viewId View ID
     */
    public getViewAccessById(viewId: string, _options?: Configuration): Observable<Array<GetViewAccessResponse>> {
        return this.getViewAccessByIdWithHttpInfo(viewId, _options).pipe(map((apiResponse: HttpInfo<Array<GetViewAccessResponse>>) => apiResponse.data));
    }

    /**
     * Get user and role assignments for a view
     * @param namespaceId Namespace ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getViewAssignmentsByIdWithHttpInfo(namespaceId: string, relations?: Array<ViewRelation>, _options?: Configuration): Observable<HttpInfo<Array<GetViewAssignmentsResponse>>> {
        const requestContextPromise = this.requestFactory.getViewAssignmentsById(namespaceId, relations, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getViewAssignmentsByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get user and role assignments for a view
     * @param namespaceId Namespace ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getViewAssignmentsById(namespaceId: string, relations?: Array<ViewRelation>, _options?: Configuration): Observable<Array<GetViewAssignmentsResponse>> {
        return this.getViewAssignmentsByIdWithHttpInfo(namespaceId, relations, _options).pipe(map((apiResponse: HttpInfo<Array<GetViewAssignmentsResponse>>) => apiResponse.data));
    }

    /**
     * Get my access to a warehouse
     * @param warehouseId Warehouse ID
     */
    public getWarehouseAccessByIdWithHttpInfo(warehouseId: string, _options?: Configuration): Observable<HttpInfo<Array<GetWarehouseAccessResponse>>> {
        const requestContextPromise = this.requestFactory.getWarehouseAccessById(warehouseId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getWarehouseAccessByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get my access to a warehouse
     * @param warehouseId Warehouse ID
     */
    public getWarehouseAccessById(warehouseId: string, _options?: Configuration): Observable<Array<GetWarehouseAccessResponse>> {
        return this.getWarehouseAccessByIdWithHttpInfo(warehouseId, _options).pipe(map((apiResponse: HttpInfo<Array<GetWarehouseAccessResponse>>) => apiResponse.data));
    }

    /**
     * Get user and role assignments for a warehouse
     * @param warehouseId Warehouse ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getWarehouseAssignmentsByIdWithHttpInfo(warehouseId: string, relations?: Array<WarehouseRelation>, _options?: Configuration): Observable<HttpInfo<Array<GetWarehouseAssignmentsResponse>>> {
        const requestContextPromise = this.requestFactory.getWarehouseAssignmentsById(warehouseId, relations, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getWarehouseAssignmentsByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get user and role assignments for a warehouse
     * @param warehouseId Warehouse ID
     * @param [relations] Relations to be loaded. If not specified, all relations are returned.
     */
    public getWarehouseAssignmentsById(warehouseId: string, relations?: Array<WarehouseRelation>, _options?: Configuration): Observable<Array<GetWarehouseAssignmentsResponse>> {
        return this.getWarehouseAssignmentsByIdWithHttpInfo(warehouseId, relations, _options).pipe(map((apiResponse: HttpInfo<Array<GetWarehouseAssignmentsResponse>>) => apiResponse.data));
    }

    /**
     * Get Authorization properties of a warehouse
     * @param warehouseId Warehouse ID
     */
    public getWarehouseByIdWithHttpInfo(warehouseId: string, _options?: Configuration): Observable<HttpInfo<Array<GetWarehouseResponse>>> {
        const requestContextPromise = this.requestFactory.getWarehouseById(warehouseId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getWarehouseByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get Authorization properties of a warehouse
     * @param warehouseId Warehouse ID
     */
    public getWarehouseById(warehouseId: string, _options?: Configuration): Observable<Array<GetWarehouseResponse>> {
        return this.getWarehouseByIdWithHttpInfo(warehouseId, _options).pipe(map((apiResponse: HttpInfo<Array<GetWarehouseResponse>>) => apiResponse.data));
    }

    /**
     * Set managed access property of a namespace
     * @param namespaceId Namespace ID
     * @param setManagedAccessRequest 
     */
    public setNamespaceManagedAccessWithHttpInfo(namespaceId: string, setManagedAccessRequest: SetManagedAccessRequest, _options?: Configuration): Observable<HttpInfo<Array<any | null>>> {
        const requestContextPromise = this.requestFactory.setNamespaceManagedAccess(namespaceId, setManagedAccessRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.setNamespaceManagedAccessWithHttpInfo(rsp)));
            }));
    }

    /**
     * Set managed access property of a namespace
     * @param namespaceId Namespace ID
     * @param setManagedAccessRequest 
     */
    public setNamespaceManagedAccess(namespaceId: string, setManagedAccessRequest: SetManagedAccessRequest, _options?: Configuration): Observable<Array<any | null>> {
        return this.setNamespaceManagedAccessWithHttpInfo(namespaceId, setManagedAccessRequest, _options).pipe(map((apiResponse: HttpInfo<Array<any | null>>) => apiResponse.data));
    }

    /**
     * Set managed access property of a warehouse
     * @param warehouseId Warehouse ID
     * @param setManagedAccessRequest 
     */
    public setWarehouseManagedAccessWithHttpInfo(warehouseId: string, setManagedAccessRequest: SetManagedAccessRequest, _options?: Configuration): Observable<HttpInfo<Array<any | null>>> {
        const requestContextPromise = this.requestFactory.setWarehouseManagedAccess(warehouseId, setManagedAccessRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.setWarehouseManagedAccessWithHttpInfo(rsp)));
            }));
    }

    /**
     * Set managed access property of a warehouse
     * @param warehouseId Warehouse ID
     * @param setManagedAccessRequest 
     */
    public setWarehouseManagedAccess(warehouseId: string, setManagedAccessRequest: SetManagedAccessRequest, _options?: Configuration): Observable<Array<any | null>> {
        return this.setWarehouseManagedAccessWithHttpInfo(warehouseId, setManagedAccessRequest, _options).pipe(map((apiResponse: HttpInfo<Array<any | null>>) => apiResponse.data));
    }

    /**
     * Update permissions for a namespace
     * @param namespaceId Namespace ID
     * @param updateNamespaceAssignmentsRequest
     */
    public updateNamespaceAssignmentsByIdWithHttpInfo(namespaceId: string, updateNamespaceAssignmentsRequest: UpdateNamespaceAssignmentsRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.updateNamespaceAssignmentsById(namespaceId, updateNamespaceAssignmentsRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.updateNamespaceAssignmentsByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Update permissions for a namespace
     * @param namespaceId Namespace ID
     * @param updateNamespaceAssignmentsRequest
     */
    public updateNamespaceAssignmentsById(namespaceId: string, updateNamespaceAssignmentsRequest: UpdateNamespaceAssignmentsRequest, _options?: Configuration): Observable<void> {
        return this.updateNamespaceAssignmentsByIdWithHttpInfo(namespaceId, updateNamespaceAssignmentsRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Update permissions for the default project
     * @param updateProjectAssignmentsRequest
     */
    public updateProjectAssignmentsWithHttpInfo(updateProjectAssignmentsRequest: UpdateProjectAssignmentsRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.updateProjectAssignments(updateProjectAssignmentsRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.updateProjectAssignmentsWithHttpInfo(rsp)));
            }));
    }

    /**
     * Update permissions for the default project
     * @param updateProjectAssignmentsRequest
     */
    public updateProjectAssignments(updateProjectAssignmentsRequest: UpdateProjectAssignmentsRequest, _options?: Configuration): Observable<void> {
        return this.updateProjectAssignmentsWithHttpInfo(updateProjectAssignmentsRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Update permissions for a project
     * @param projectId Project ID
     * @param updateProjectAssignmentsRequest
     */
    public updateProjectAssignmentsByIdWithHttpInfo(projectId: string, updateProjectAssignmentsRequest: UpdateProjectAssignmentsRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.updateProjectAssignmentsById(projectId, updateProjectAssignmentsRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.updateProjectAssignmentsByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Update permissions for a project
     * @param projectId Project ID
     * @param updateProjectAssignmentsRequest
     */
    public updateProjectAssignmentsById(projectId: string, updateProjectAssignmentsRequest: UpdateProjectAssignmentsRequest, _options?: Configuration): Observable<void> {
        return this.updateProjectAssignmentsByIdWithHttpInfo(projectId, updateProjectAssignmentsRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Update permissions for a view
     * @param roleId Role ID
     * @param updateRoleAssignmentsRequest
     */
    public updateRoleAssignmentsByIdWithHttpInfo(roleId: string, updateRoleAssignmentsRequest: UpdateRoleAssignmentsRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.updateRoleAssignmentsById(roleId, updateRoleAssignmentsRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.updateRoleAssignmentsByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Update permissions for a view
     * @param roleId Role ID
     * @param updateRoleAssignmentsRequest
     */
    public updateRoleAssignmentsById(roleId: string, updateRoleAssignmentsRequest: UpdateRoleAssignmentsRequest, _options?: Configuration): Observable<void> {
        return this.updateRoleAssignmentsByIdWithHttpInfo(roleId, updateRoleAssignmentsRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Update permissions for this server
     * @param updateServerAssignmentsRequest
     */
    public updateServerAssignmentsWithHttpInfo(updateServerAssignmentsRequest: UpdateServerAssignmentsRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.updateServerAssignments(updateServerAssignmentsRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.updateServerAssignmentsWithHttpInfo(rsp)));
            }));
    }

    /**
     * Update permissions for this server
     * @param updateServerAssignmentsRequest
     */
    public updateServerAssignments(updateServerAssignmentsRequest: UpdateServerAssignmentsRequest, _options?: Configuration): Observable<void> {
        return this.updateServerAssignmentsWithHttpInfo(updateServerAssignmentsRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Update permissions for a table
     * @param tableId Table ID
     * @param updateTableAssignmentsRequest
     */
    public updateTableAssignmentsByIdWithHttpInfo(tableId: string, updateTableAssignmentsRequest: UpdateTableAssignmentsRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.updateTableAssignmentsById(tableId, updateTableAssignmentsRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.updateTableAssignmentsByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Update permissions for a table
     * @param tableId Table ID
     * @param updateTableAssignmentsRequest
     */
    public updateTableAssignmentsById(tableId: string, updateTableAssignmentsRequest: UpdateTableAssignmentsRequest, _options?: Configuration): Observable<void> {
        return this.updateTableAssignmentsByIdWithHttpInfo(tableId, updateTableAssignmentsRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Update permissions for a view
     * @param viewId View ID
     * @param updateViewAssignmentsRequest
     */
    public updateViewAssignmentsByIdWithHttpInfo(viewId: string, updateViewAssignmentsRequest: UpdateViewAssignmentsRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.updateViewAssignmentsById(viewId, updateViewAssignmentsRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.updateViewAssignmentsByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Update permissions for a view
     * @param viewId View ID
     * @param updateViewAssignmentsRequest
     */
    public updateViewAssignmentsById(viewId: string, updateViewAssignmentsRequest: UpdateViewAssignmentsRequest, _options?: Configuration): Observable<void> {
        return this.updateViewAssignmentsByIdWithHttpInfo(viewId, updateViewAssignmentsRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Update permissions for a project
     * @param warehouseId Warehouse ID
     * @param updateWarehouseAssignmentsRequest
     */
    public updateWarehouseAssignmentsByIdWithHttpInfo(warehouseId: string, updateWarehouseAssignmentsRequest: UpdateWarehouseAssignmentsRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.updateWarehouseAssignmentsById(warehouseId, updateWarehouseAssignmentsRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.updateWarehouseAssignmentsByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Update permissions for a project
     * @param warehouseId Warehouse ID
     * @param updateWarehouseAssignmentsRequest
     */
    public updateWarehouseAssignmentsById(warehouseId: string, updateWarehouseAssignmentsRequest: UpdateWarehouseAssignmentsRequest, _options?: Configuration): Observable<void> {
        return this.updateWarehouseAssignmentsByIdWithHttpInfo(warehouseId, updateWarehouseAssignmentsRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

}

import { ProjectApiRequestFactory, ProjectApiResponseProcessor} from "../apis/ProjectApi";
export class ObservableProjectApi {
    private requestFactory: ProjectApiRequestFactory;
    private responseProcessor: ProjectApiResponseProcessor;
    private configuration: Configuration;

    public constructor(
        configuration: Configuration,
        requestFactory?: ProjectApiRequestFactory,
        responseProcessor?: ProjectApiResponseProcessor
    ) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new ProjectApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new ProjectApiResponseProcessor();
    }

    /**
     * Create a new project
     * @param createProjectRequest 
     */
    public createProjectWithHttpInfo(createProjectRequest: CreateProjectRequest, _options?: Configuration): Observable<HttpInfo<Array<CreateProjectResponse>>> {
        const requestContextPromise = this.requestFactory.createProject(createProjectRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.createProjectWithHttpInfo(rsp)));
            }));
    }

    /**
     * Create a new project
     * @param createProjectRequest 
     */
    public createProject(createProjectRequest: CreateProjectRequest, _options?: Configuration): Observable<Array<CreateProjectResponse>> {
        return this.createProjectWithHttpInfo(createProjectRequest, _options).pipe(map((apiResponse: HttpInfo<Array<CreateProjectResponse>>) => apiResponse.data));
    }

    /**
     * Delete the default project
     */
    public deleteDefaultProjectWithHttpInfo(_options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.deleteDefaultProject(_options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.deleteDefaultProjectWithHttpInfo(rsp)));
            }));
    }

    /**
     * Delete the default project
     */
    public deleteDefaultProject(_options?: Configuration): Observable<void> {
        return this.deleteDefaultProjectWithHttpInfo(_options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Delete the default project
     * @param projectId
     */
    public deleteProjectByIdWithHttpInfo(projectId: string, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.deleteProjectById(projectId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.deleteProjectByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Delete the default project
     * @param projectId
     */
    public deleteProjectById(projectId: string, _options?: Configuration): Observable<void> {
        return this.deleteProjectByIdWithHttpInfo(projectId, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Get the default project
     */
    public getDefaultProjectWithHttpInfo(_options?: Configuration): Observable<HttpInfo<Array<GetProjectResponse>>> {
        const requestContextPromise = this.requestFactory.getDefaultProject(_options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getDefaultProjectWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get the default project
     */
    public getDefaultProject(_options?: Configuration): Observable<Array<GetProjectResponse>> {
        return this.getDefaultProjectWithHttpInfo(_options).pipe(map((apiResponse: HttpInfo<Array<GetProjectResponse>>) => apiResponse.data));
    }

    /**
     * Get a specific project by id
     * @param projectId
     */
    public getProjectByIdWithHttpInfo(projectId: string, _options?: Configuration): Observable<HttpInfo<Array<GetProjectResponse>>> {
        const requestContextPromise = this.requestFactory.getProjectById(projectId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getProjectByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get a specific project by id
     * @param projectId
     */
    public getProjectById(projectId: string, _options?: Configuration): Observable<Array<GetProjectResponse>> {
        return this.getProjectByIdWithHttpInfo(projectId, _options).pipe(map((apiResponse: HttpInfo<Array<GetProjectResponse>>) => apiResponse.data));
    }

    /**
     * List all projects the requesting user has access to
     */
    public listProjectsWithHttpInfo(_options?: Configuration): Observable<HttpInfo<Array<ListProjectsResponse>>> {
        const requestContextPromise = this.requestFactory.listProjects(_options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.listProjectsWithHttpInfo(rsp)));
            }));
    }

    /**
     * List all projects the requesting user has access to
     */
    public listProjects(_options?: Configuration): Observable<Array<ListProjectsResponse>> {
        return this.listProjectsWithHttpInfo(_options).pipe(map((apiResponse: HttpInfo<Array<ListProjectsResponse>>) => apiResponse.data));
    }

    /**
     * Rename the default project
     * @param renameProjectRequest 
     */
    public renameDefaultProjectWithHttpInfo(renameProjectRequest: RenameProjectRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.renameDefaultProject(renameProjectRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.renameDefaultProjectWithHttpInfo(rsp)));
            }));
    }

    /**
     * Rename the default project
     * @param renameProjectRequest 
     */
    public renameDefaultProject(renameProjectRequest: RenameProjectRequest, _options?: Configuration): Observable<void> {
        return this.renameDefaultProjectWithHttpInfo(renameProjectRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Rename project by id
     * @param projectId
     * @param renameProjectRequest 
     */
    public renameProjectByIdWithHttpInfo(projectId: string, renameProjectRequest: RenameProjectRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.renameProjectById(projectId, renameProjectRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.renameProjectByIdWithHttpInfo(rsp)));
            }));
    }

    /**
     * Rename project by id
     * @param projectId
     * @param renameProjectRequest 
     */
    public renameProjectById(projectId: string, renameProjectRequest: RenameProjectRequest, _options?: Configuration): Observable<void> {
        return this.renameProjectByIdWithHttpInfo(projectId, renameProjectRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

}

import { RoleApiRequestFactory, RoleApiResponseProcessor} from "../apis/RoleApi";
export class ObservableRoleApi {
    private requestFactory: RoleApiRequestFactory;
    private responseProcessor: RoleApiResponseProcessor;
    private configuration: Configuration;

    public constructor(
        configuration: Configuration,
        requestFactory?: RoleApiRequestFactory,
        responseProcessor?: RoleApiResponseProcessor
    ) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new RoleApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new RoleApiResponseProcessor();
    }

    /**
     * Create a new role
     * @param createRoleRequest
     */
    public createRoleWithHttpInfo(createRoleRequest: CreateRoleRequest, _options?: Configuration): Observable<HttpInfo<Array<Role>>> {
        const requestContextPromise = this.requestFactory.createRole(createRoleRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.createRoleWithHttpInfo(rsp)));
            }));
    }

    /**
     * Create a new role
     * @param createRoleRequest
     */
    public createRole(createRoleRequest: CreateRoleRequest, _options?: Configuration): Observable<Array<Role>> {
        return this.createRoleWithHttpInfo(createRoleRequest, _options).pipe(map((apiResponse: HttpInfo<Array<Role>>) => apiResponse.data));
    }

    /**
     * All permissions of the role are permanently removed.
     * Delete role
     * @param id
     */
    public deleteRoleWithHttpInfo(id: string, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.deleteRole(id, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.deleteRoleWithHttpInfo(rsp)));
            }));
    }

    /**
     * All permissions of the role are permanently removed.
     * Delete role
     * @param id
     */
    public deleteRole(id: string, _options?: Configuration): Observable<void> {
        return this.deleteRoleWithHttpInfo(id, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Get a role
     * @param id
     */
    public getRoleWithHttpInfo(id: string, _options?: Configuration): Observable<HttpInfo<Array<Role>>> {
        const requestContextPromise = this.requestFactory.getRole(id, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getRoleWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get a role
     * @param id
     */
    public getRole(id: string, _options?: Configuration): Observable<Array<Role>> {
        return this.getRoleWithHttpInfo(id, _options).pipe(map((apiResponse: HttpInfo<Array<Role>>) => apiResponse.data));
    }

    /**
     * List roles in a project
     * @param [name] Search for a specific role name
     * @param [pageToken] Next page token
     * @param [pageSize] Signals an upper bound of the number of results that a client will receive. Default: 100
     * @param [projectId] Project ID from which roles should be listed Only required if the project ID cannot be inferred from the users token and no default project is set.
     */
    public listRolesWithHttpInfo(name?: string, pageToken?: string, pageSize?: number, projectId?: string, _options?: Configuration): Observable<HttpInfo<Array<ListRolesResponse>>> {
        const requestContextPromise = this.requestFactory.listRoles(name, pageToken, pageSize, projectId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.listRolesWithHttpInfo(rsp)));
            }));
    }

    /**
     * List roles in a project
     * @param [name] Search for a specific role name
     * @param [pageToken] Next page token
     * @param [pageSize] Signals an upper bound of the number of results that a client will receive. Default: 100
     * @param [projectId] Project ID from which roles should be listed Only required if the project ID cannot be inferred from the users token and no default project is set.
     */
    public listRoles(name?: string, pageToken?: string, pageSize?: number, projectId?: string, _options?: Configuration): Observable<Array<ListRolesResponse>> {
        return this.listRolesWithHttpInfo(name, pageToken, pageSize, projectId, _options).pipe(map((apiResponse: HttpInfo<Array<ListRolesResponse>>) => apiResponse.data));
    }

    /**
     * Update a role
     * @param id
     * @param updateRoleRequest
     */
    public updateRoleWithHttpInfo(id: string, updateRoleRequest: UpdateRoleRequest, _options?: Configuration): Observable<HttpInfo<Array<Role>>> {
        const requestContextPromise = this.requestFactory.updateRole(id, updateRoleRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.updateRoleWithHttpInfo(rsp)));
            }));
    }

    /**
     * Update a role
     * @param id
     * @param updateRoleRequest
     */
    public updateRole(id: string, updateRoleRequest: UpdateRoleRequest, _options?: Configuration): Observable<Array<Role>> {
        return this.updateRoleWithHttpInfo(id, updateRoleRequest, _options).pipe(map((apiResponse: HttpInfo<Array<Role>>) => apiResponse.data));
    }

}

import { ServerApiRequestFactory, ServerApiResponseProcessor} from "../apis/ServerApi";
export class ObservableServerApi {
    private requestFactory: ServerApiRequestFactory;
    private responseProcessor: ServerApiResponseProcessor;
    private configuration: Configuration;

    public constructor(
        configuration: Configuration,
        requestFactory?: ServerApiRequestFactory,
        responseProcessor?: ServerApiResponseProcessor
    ) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new ServerApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new ServerApiResponseProcessor();
    }

    /**
     * If the user exists, it updates the users\' metadata from the token. The token sent to this endpoint should have \"profile\" and \"email\" scopes.
     * Creates the user in the catalog if it does not exist.
     * @param bootstrapRequest
     */
    public bootstrapWithHttpInfo(bootstrapRequest: BootstrapRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.bootstrap(bootstrapRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.bootstrapWithHttpInfo(rsp)));
            }));
    }

    /**
     * If the user exists, it updates the users\' metadata from the token. The token sent to this endpoint should have \"profile\" and \"email\" scopes.
     * Creates the user in the catalog if it does not exist.
     * @param bootstrapRequest
     */
    public bootstrap(bootstrapRequest: BootstrapRequest, _options?: Configuration): Observable<void> {
        return this.bootstrapWithHttpInfo(bootstrapRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Get information about the server
     */
    public getServerInfoWithHttpInfo(_options?: Configuration): Observable<HttpInfo<Array<ServerInfo>>> {
        const requestContextPromise = this.requestFactory.getServerInfo(_options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getServerInfoWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get information about the server
     */
    public getServerInfo(_options?: Configuration): Observable<Array<ServerInfo>> {
        return this.getServerInfoWithHttpInfo(_options).pipe(map((apiResponse: HttpInfo<Array<ServerInfo>>) => apiResponse.data));
    }

}

import { UserApiRequestFactory, UserApiResponseProcessor} from "../apis/UserApi";
export class ObservableUserApi {
    private requestFactory: UserApiRequestFactory;
    private responseProcessor: UserApiResponseProcessor;
    private configuration: Configuration;

    public constructor(
        configuration: Configuration,
        requestFactory?: UserApiRequestFactory,
        responseProcessor?: UserApiResponseProcessor
    ) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new UserApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new UserApiResponseProcessor();
    }

    /**
     * If the user exists, it updates the users\' metadata from the token. The token sent to this endpoint should have \"profile\" and \"email\" scopes.
     * Creates the user in the catalog if it does not exist.
     * @param createUserRequest
     */
    public createUserWithHttpInfo(createUserRequest: CreateUserRequest, _options?: Configuration): Observable<HttpInfo<Array<User>>> {
        const requestContextPromise = this.requestFactory.createUser(createUserRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.createUserWithHttpInfo(rsp)));
            }));
    }

    /**
     * If the user exists, it updates the users\' metadata from the token. The token sent to this endpoint should have \"profile\" and \"email\" scopes.
     * Creates the user in the catalog if it does not exist.
     * @param createUserRequest
     */
    public createUser(createUserRequest: CreateUserRequest, _options?: Configuration): Observable<Array<User>> {
        return this.createUserWithHttpInfo(createUserRequest, _options).pipe(map((apiResponse: HttpInfo<Array<User>>) => apiResponse.data));
    }

    /**
     * All permissions of the user are permanently removed and need to be re-added if the user is re-registered.
     * Delete user
     * @param id
     */
    public deleteUserWithHttpInfo(id: string, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.deleteUser(id, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.deleteUserWithHttpInfo(rsp)));
            }));
    }

    /**
     * All permissions of the user are permanently removed and need to be re-added if the user is re-registered.
     * Delete user
     * @param id
     */
    public deleteUser(id: string, _options?: Configuration): Observable<void> {
        return this.deleteUserWithHttpInfo(id, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Get a user by ID
     * @param id
     */
    public getUserWithHttpInfo(id: string, _options?: Configuration): Observable<HttpInfo<Array<User>>> {
        const requestContextPromise = this.requestFactory.getUser(id, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getUserWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get a user by ID
     * @param id
     */
    public getUser(id: string, _options?: Configuration): Observable<Array<User>> {
        return this.getUserWithHttpInfo(id, _options).pipe(map((apiResponse: HttpInfo<Array<User>>) => apiResponse.data));
    }

    /**
     * List users
     * @param [name] Search for a specific username
     * @param [pageToken] Next page token
     * @param [pageSize] Signals an upper bound of the number of results that a client will receive. Default: 100
     */
    public listUserWithHttpInfo(name?: string, pageToken?: string, pageSize?: number, _options?: Configuration): Observable<HttpInfo<Array<ListUsersResponse>>> {
        const requestContextPromise = this.requestFactory.listUser(name, pageToken, pageSize, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.listUserWithHttpInfo(rsp)));
            }));
    }

    /**
     * List users
     * @param [name] Search for a specific username
     * @param [pageToken] Next page token
     * @param [pageSize] Signals an upper bound of the number of results that a client will receive. Default: 100
     */
    public listUser(name?: string, pageToken?: string, pageSize?: number, _options?: Configuration): Observable<Array<ListUsersResponse>> {
        return this.listUserWithHttpInfo(name, pageToken, pageSize, _options).pipe(map((apiResponse: HttpInfo<Array<ListUsersResponse>>) => apiResponse.data));
    }

    /**
     * Search for users (Fuzzy)
     * @param searchUserRequest
     */
    public searchUserWithHttpInfo(searchUserRequest: SearchUserRequest, _options?: Configuration): Observable<HttpInfo<Array<SearchUserResponse>>> {
        const requestContextPromise = this.requestFactory.searchUser(searchUserRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.searchUserWithHttpInfo(rsp)));
            }));
    }

    /**
     * Search for users (Fuzzy)
     * @param searchUserRequest
     */
    public searchUser(searchUserRequest: SearchUserRequest, _options?: Configuration): Observable<Array<SearchUserResponse>> {
        return this.searchUserWithHttpInfo(searchUserRequest, _options).pipe(map((apiResponse: HttpInfo<Array<SearchUserResponse>>) => apiResponse.data));
    }

    /**
     * If a field is not provided, it is set to `None`.
     * Update details of a user. Replaces the current details with the new details.
     * @param id
     * @param updateUserRequest
     */
    public updateUserWithHttpInfo(id: string, updateUserRequest: UpdateUserRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.updateUser(id, updateUserRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.updateUserWithHttpInfo(rsp)));
            }));
    }

    /**
     * If a field is not provided, it is set to `None`.
     * Update details of a user. Replaces the current details with the new details.
     * @param id
     * @param updateUserRequest
     */
    public updateUser(id: string, updateUserRequest: UpdateUserRequest, _options?: Configuration): Observable<void> {
        return this.updateUserWithHttpInfo(id, updateUserRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Get the currently authenticated user
     */
    public whoamiWithHttpInfo(_options?: Configuration): Observable<HttpInfo<Array<User>>> {
        const requestContextPromise = this.requestFactory.whoami(_options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.whoamiWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get the currently authenticated user
     */
    public whoami(_options?: Configuration): Observable<Array<User>> {
        return this.whoamiWithHttpInfo(_options).pipe(map((apiResponse: HttpInfo<Array<User>>) => apiResponse.data));
    }

}

import { WarehouseApiRequestFactory, WarehouseApiResponseProcessor} from "../apis/WarehouseApi";
export class ObservableWarehouseApi {
    private requestFactory: WarehouseApiRequestFactory;
    private responseProcessor: WarehouseApiResponseProcessor;
    private configuration: Configuration;

    public constructor(
        configuration: Configuration,
        requestFactory?: WarehouseApiRequestFactory,
        responseProcessor?: WarehouseApiResponseProcessor
    ) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new WarehouseApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new WarehouseApiResponseProcessor();
    }

    /**
     * Activate a warehouse
     * @param warehouseId
     */
    public activateWarehouseWithHttpInfo(warehouseId: string, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.activateWarehouse(warehouseId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.activateWarehouseWithHttpInfo(rsp)));
            }));
    }

    /**
     * Activate a warehouse
     * @param warehouseId
     */
    public activateWarehouse(warehouseId: string, _options?: Configuration): Observable<void> {
        return this.activateWarehouseWithHttpInfo(warehouseId, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Create a new warehouse in the given project. The project of a warehouse cannot be changed after creation. The storage configuration is validated by this method.
     * Create a new warehouse.
     * @param createWarehouseRequest
     */
    public createWarehouseWithHttpInfo(createWarehouseRequest: CreateWarehouseRequest, _options?: Configuration): Observable<HttpInfo<Array<CreateWarehouseResponse>>> {
        const requestContextPromise = this.requestFactory.createWarehouse(createWarehouseRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.createWarehouseWithHttpInfo(rsp)));
            }));
    }

    /**
     * Create a new warehouse in the given project. The project of a warehouse cannot be changed after creation. The storage configuration is validated by this method.
     * Create a new warehouse.
     * @param createWarehouseRequest
     */
    public createWarehouse(createWarehouseRequest: CreateWarehouseRequest, _options?: Configuration): Observable<Array<CreateWarehouseResponse>> {
        return this.createWarehouseWithHttpInfo(createWarehouseRequest, _options).pipe(map((apiResponse: HttpInfo<Array<CreateWarehouseResponse>>) => apiResponse.data));
    }

    /**
     * Deactivate a warehouse
     * @param warehouseId
     */
    public deactivateWarehouseWithHttpInfo(warehouseId: string, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.deactivateWarehouse(warehouseId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.deactivateWarehouseWithHttpInfo(rsp)));
            }));
    }

    /**
     * Deactivate a warehouse
     * @param warehouseId
     */
    public deactivateWarehouse(warehouseId: string, _options?: Configuration): Observable<void> {
        return this.deactivateWarehouseWithHttpInfo(warehouseId, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Delete a warehouse by ID
     * @param warehouseId
     */
    public deleteWarehouseWithHttpInfo(warehouseId: string, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.deleteWarehouse(warehouseId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.deleteWarehouseWithHttpInfo(rsp)));
            }));
    }

    /**
     * Delete a warehouse by ID
     * @param warehouseId
     */
    public deleteWarehouse(warehouseId: string, _options?: Configuration): Observable<void> {
        return this.deleteWarehouseWithHttpInfo(warehouseId, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Get a warehouse by ID
     * @param warehouseId
     */
    public getWarehouseWithHttpInfo(warehouseId: string, _options?: Configuration): Observable<HttpInfo<Array<GetWarehouseResponse>>> {
        const requestContextPromise = this.requestFactory.getWarehouse(warehouseId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.getWarehouseWithHttpInfo(rsp)));
            }));
    }

    /**
     * Get a warehouse by ID
     * @param warehouseId
     */
    public getWarehouse(warehouseId: string, _options?: Configuration): Observable<Array<GetWarehouseResponse>> {
        return this.getWarehouseWithHttpInfo(warehouseId, _options).pipe(map((apiResponse: HttpInfo<Array<GetWarehouseResponse>>) => apiResponse.data));
    }

    /**
     * List all soft-deleted tabulars in the warehouse that are visible to you.
     * List soft-deleted tabulars
     * @param warehouseId
     * @param [pageToken] Next page token
     * @param [pageSize] Signals an upper bound of the number of results that a client will receive.
     */
    public listDeletedTabularsWithHttpInfo(warehouseId: string, pageToken?: string, pageSize?: number, _options?: Configuration): Observable<HttpInfo<Array<ListDeletedTabularsResponse>>> {
        const requestContextPromise = this.requestFactory.listDeletedTabulars(warehouseId, pageToken, pageSize, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.listDeletedTabularsWithHttpInfo(rsp)));
            }));
    }

    /**
     * List all soft-deleted tabulars in the warehouse that are visible to you.
     * List soft-deleted tabulars
     * @param warehouseId
     * @param [pageToken] Next page token
     * @param [pageSize] Signals an upper bound of the number of results that a client will receive.
     */
    public listDeletedTabulars(warehouseId: string, pageToken?: string, pageSize?: number, _options?: Configuration): Observable<Array<ListDeletedTabularsResponse>> {
        return this.listDeletedTabularsWithHttpInfo(warehouseId, pageToken, pageSize, _options).pipe(map((apiResponse: HttpInfo<Array<ListDeletedTabularsResponse>>) => apiResponse.data));
    }

    /**
     * By default, this endpoint does not return deactivated warehouses. To include deactivated warehouses, set the `include_deactivated` query parameter to `true`.
     * List all warehouses in a project
     * @param [warehouseStatus] Optional filter to return only warehouses with the specified status. If not provided, only active warehouses are returned.
     * @param [projectId] The project ID to list warehouses for. Setting a warehouse is required.
     */
    public listWarehousesWithHttpInfo(warehouseStatus?: Array<WarehouseStatus>, projectId?: string, _options?: Configuration): Observable<HttpInfo<Array<ListWarehousesResponse>>> {
        const requestContextPromise = this.requestFactory.listWarehouses(warehouseStatus, projectId, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.listWarehousesWithHttpInfo(rsp)));
            }));
    }

    /**
     * By default, this endpoint does not return deactivated warehouses. To include deactivated warehouses, set the `include_deactivated` query parameter to `true`.
     * List all warehouses in a project
     * @param [warehouseStatus] Optional filter to return only warehouses with the specified status. If not provided, only active warehouses are returned.
     * @param [projectId] The project ID to list warehouses for. Setting a warehouse is required.
     */
    public listWarehouses(warehouseStatus?: Array<WarehouseStatus>, projectId?: string, _options?: Configuration): Observable<Array<ListWarehousesResponse>> {
        return this.listWarehousesWithHttpInfo(warehouseStatus, projectId, _options).pipe(map((apiResponse: HttpInfo<Array<ListWarehousesResponse>>) => apiResponse.data));
    }

    /**
     * Rename a warehouse
     * @param warehouseId
     * @param renameWarehouseRequest
     */
    public renameWarehouseWithHttpInfo(warehouseId: string, renameWarehouseRequest: RenameWarehouseRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.renameWarehouse(warehouseId, renameWarehouseRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.renameWarehouseWithHttpInfo(rsp)));
            }));
    }

    /**
     * Rename a warehouse
     * @param warehouseId
     * @param renameWarehouseRequest
     */
    public renameWarehouse(warehouseId: string, renameWarehouseRequest: RenameWarehouseRequest, _options?: Configuration): Observable<void> {
        return this.renameWarehouseWithHttpInfo(warehouseId, renameWarehouseRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * This can be used to update credentials before expiration.
     * Update the storage credential of a warehouse. The storage profile is not modified.
     * @param warehouseId
     * @param updateWarehouseCredentialRequest
     */
    public updateStorageCredentialWithHttpInfo(warehouseId: string, updateWarehouseCredentialRequest: UpdateWarehouseCredentialRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.updateStorageCredential(warehouseId, updateWarehouseCredentialRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.updateStorageCredentialWithHttpInfo(rsp)));
            }));
    }

    /**
     * This can be used to update credentials before expiration.
     * Update the storage credential of a warehouse. The storage profile is not modified.
     * @param warehouseId
     * @param updateWarehouseCredentialRequest
     */
    public updateStorageCredential(warehouseId: string, updateWarehouseCredentialRequest: UpdateWarehouseCredentialRequest, _options?: Configuration): Observable<void> {
        return this.updateStorageCredentialWithHttpInfo(warehouseId, updateWarehouseCredentialRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Update the storage profile of a warehouse including its storage credential.
     * @param warehouseId
     * @param updateWarehouseStorageRequest
     */
    public updateStorageProfileWithHttpInfo(warehouseId: string, updateWarehouseStorageRequest: UpdateWarehouseStorageRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.updateStorageProfile(warehouseId, updateWarehouseStorageRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.updateStorageProfileWithHttpInfo(rsp)));
            }));
    }

    /**
     * Update the storage profile of a warehouse including its storage credential.
     * @param warehouseId
     * @param updateWarehouseStorageRequest
     */
    public updateStorageProfile(warehouseId: string, updateWarehouseStorageRequest: UpdateWarehouseStorageRequest, _options?: Configuration): Observable<void> {
        return this.updateStorageProfileWithHttpInfo(warehouseId, updateWarehouseStorageRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

    /**
     * Update the Deletion Profile (soft-delete) of a warehouse.
     * @param warehouseId
     * @param updateWarehouseDeleteProfileRequest
     */
    public updateWarehouseDeleteProfileWithHttpInfo(warehouseId: string, updateWarehouseDeleteProfileRequest: UpdateWarehouseDeleteProfileRequest, _options?: Configuration): Observable<HttpInfo<void>> {
        const requestContextPromise = this.requestFactory.updateWarehouseDeleteProfile(warehouseId, updateWarehouseDeleteProfileRequest, _options);

        // build promise chain
        let middlewarePreObservable = from<RequestContext>(requestContextPromise);
        for (const middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => middleware.pre(ctx)));
        }

        return middlewarePreObservable.pipe(mergeMap((ctx: RequestContext) => this.configuration.httpApi.send(ctx))).
            pipe(mergeMap((response: ResponseContext) => {
                let middlewarePostObservable = of(response);
                for (const middleware of this.configuration.middleware) {
                    middlewarePostObservable = middlewarePostObservable.pipe(mergeMap((rsp: ResponseContext) => middleware.post(rsp)));
                }
                return middlewarePostObservable.pipe(map((rsp: ResponseContext) => this.responseProcessor.updateWarehouseDeleteProfileWithHttpInfo(rsp)));
            }));
    }

    /**
     * Update the Deletion Profile (soft-delete) of a warehouse.
     * @param warehouseId
     * @param updateWarehouseDeleteProfileRequest
     */
    public updateWarehouseDeleteProfile(warehouseId: string, updateWarehouseDeleteProfileRequest: UpdateWarehouseDeleteProfileRequest, _options?: Configuration): Observable<void> {
        return this.updateWarehouseDeleteProfileWithHttpInfo(warehouseId, updateWarehouseDeleteProfileRequest, _options).pipe(map((apiResponse: HttpInfo<void>) => apiResponse.data));
    }

}
