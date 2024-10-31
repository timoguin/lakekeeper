// TODO: better import syntax?
import {BaseAPIRequestFactory, RequiredError, COLLECTION_FORMATS} from './baseapi';
import {Configuration} from '../configuration';
import {RequestContext, HttpMethod, ResponseContext, HttpFile, HttpInfo} from '../http/http';
import {ObjectSerializer} from '../models/ObjectSerializer';
import {ApiException} from './exception';
import {canConsumeForm, isCodeInRange} from '../util';
import {SecurityAuthentication} from '../auth/auth';


import { GetNamespaceAccessResponse } from '../models/GetNamespaceAccessResponse';
import { GetNamespaceAssignmentsResponse } from '../models/GetNamespaceAssignmentsResponse';
import { GetNamespaceResponse } from '../models/GetNamespaceResponse';
import { GetProjectAccessResponse } from '../models/GetProjectAccessResponse';
import { GetProjectAssignmentsResponse } from '../models/GetProjectAssignmentsResponse';
import { GetRoleAccessResponse } from '../models/GetRoleAccessResponse';
import { GetRoleAssignmentsResponse } from '../models/GetRoleAssignmentsResponse';
import { GetServerAccessResponse } from '../models/GetServerAccessResponse';
import { GetServerAssignmentsResponse } from '../models/GetServerAssignmentsResponse';
import { GetTableAccessResponse } from '../models/GetTableAccessResponse';
import { GetViewAccessResponse } from '../models/GetViewAccessResponse';
import { GetViewAssignmentsResponse } from '../models/GetViewAssignmentsResponse';
import { GetWarehouseAccessResponse } from '../models/GetWarehouseAccessResponse';
import { GetWarehouseAssignmentsResponse } from '../models/GetWarehouseAssignmentsResponse';
import { GetWarehouseResponse } from '../models/GetWarehouseResponse';
import { NamespaceRelation } from '../models/NamespaceRelation';
import { ProjectRelation } from '../models/ProjectRelation';
import { ServerRelation } from '../models/ServerRelation';
import { SetManagedAccessRequest } from '../models/SetManagedAccessRequest';
import { UpdateNamespaceAssignmentsRequest } from '../models/UpdateNamespaceAssignmentsRequest';
import { UpdateProjectAssignmentsRequest } from '../models/UpdateProjectAssignmentsRequest';
import { UpdateRoleAssignmentsRequest } from '../models/UpdateRoleAssignmentsRequest';
import { UpdateServerAssignmentsRequest } from '../models/UpdateServerAssignmentsRequest';
import { UpdateTableAssignmentsRequest } from '../models/UpdateTableAssignmentsRequest';
import { UpdateViewAssignmentsRequest } from '../models/UpdateViewAssignmentsRequest';
import { UpdateWarehouseAssignmentsRequest } from '../models/UpdateWarehouseAssignmentsRequest';
import { ViewRelation } from '../models/ViewRelation';
import { WarehouseRelation } from '../models/WarehouseRelation';

/**
 * no description
 */
export class PermissionsApiRequestFactory extends BaseAPIRequestFactory {

    /**
     * Get my access to a namespace
     * @param namespaceId Namespace ID
     */
    public async getNamespaceAccessById(namespaceId: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'namespaceId' is not null or undefined
        if (namespaceId === null || namespaceId === undefined) {
            throw new RequiredError("PermissionsApi", "getNamespaceAccessById", "namespaceId");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/namespace/{namespace_id}/access'
            .replace('{' + 'namespace_id' + '}', encodeURIComponent(String(namespaceId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get user and role assignments for a namespace
     * @param namespaceId Namespace ID
     * @param relations Relations to be loaded. If not specified, all relations are returned.
     */
    public async getNamespaceAssignmentsById(namespaceId: string, relations?: Array<NamespaceRelation>, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'namespaceId' is not null or undefined
        if (namespaceId === null || namespaceId === undefined) {
            throw new RequiredError("PermissionsApi", "getNamespaceAssignmentsById", "namespaceId");
        }



        // Path Params
        const localVarPath = '/management/v1/permissions/namespace/{namespace_id}/assignments'
            .replace('{' + 'namespace_id' + '}', encodeURIComponent(String(namespaceId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (relations !== undefined) {
            const serializedParams = ObjectSerializer.serialize(relations, "Array<NamespaceRelation>", "");
            for (const serializedParam of serializedParams) {
                requestContext.appendQueryParam("relations", serializedParam);
            }
        }


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get Authorization properties of a namespace
     * @param namespaceId Namespace ID
     */
    public async getNamespaceById(namespaceId: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'namespaceId' is not null or undefined
        if (namespaceId === null || namespaceId === undefined) {
            throw new RequiredError("PermissionsApi", "getNamespaceById", "namespaceId");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/namespace/{namespace_id}'
            .replace('{' + 'namespace_id' + '}', encodeURIComponent(String(namespaceId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get my access to the default project
     */
    public async getProjectAccess(_options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // Path Params
        const localVarPath = '/management/v1/permissions/project/access';

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get my access to the default project
     * @param projectId Project ID
     */
    public async getProjectAccessById(projectId: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'projectId' is not null or undefined
        if (projectId === null || projectId === undefined) {
            throw new RequiredError("PermissionsApi", "getProjectAccessById", "projectId");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/project/{project_id}/access'
            .replace('{' + 'project_id' + '}', encodeURIComponent(String(projectId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get user and role assignments to the current project
     * @param relations Relations to be loaded. If not specified, all relations are returned.
     */
    public async getProjectAssignments(relations?: Array<ProjectRelation>, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;


        // Path Params
        const localVarPath = '/management/v1/permissions/project/assignments';

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (relations !== undefined) {
            const serializedParams = ObjectSerializer.serialize(relations, "Array<ProjectRelation>", "");
            for (const serializedParam of serializedParams) {
                requestContext.appendQueryParam("relations", serializedParam);
            }
        }


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get user and role assignments to a project
     * @param projectId Project ID
     * @param relations Relations to be loaded. If not specified, all relations are returned.
     */
    public async getProjectAssignmentsById(projectId: string, relations?: Array<ProjectRelation>, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'projectId' is not null or undefined
        if (projectId === null || projectId === undefined) {
            throw new RequiredError("PermissionsApi", "getProjectAssignmentsById", "projectId");
        }



        // Path Params
        const localVarPath = '/management/v1/permissions/project/{project_id}/assignments'
            .replace('{' + 'project_id' + '}', encodeURIComponent(String(projectId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (relations !== undefined) {
            const serializedParams = ObjectSerializer.serialize(relations, "Array<ProjectRelation>", "");
            for (const serializedParam of serializedParams) {
                requestContext.appendQueryParam("relations", serializedParam);
            }
        }


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get my access to the default project
     * @param roleId Role ID
     */
    public async getRoleAccessById(roleId: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'roleId' is not null or undefined
        if (roleId === null || roleId === undefined) {
            throw new RequiredError("PermissionsApi", "getRoleAccessById", "roleId");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/role/{role_id}/access'
            .replace('{' + 'role_id' + '}', encodeURIComponent(String(roleId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get user and role assignments to the current project
     * @param relations Relations to be loaded. If not specified, all relations are returned.
     * @param roleId Role ID
     */
    public async getRoleAssignmentsById(relations: Array<ProjectRelation>, roleId: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'relations' is not null or undefined
        if (relations === null || relations === undefined) {
            throw new RequiredError("PermissionsApi", "getRoleAssignmentsById", "relations");
        }


        // verify required parameter 'roleId' is not null or undefined
        if (roleId === null || roleId === undefined) {
            throw new RequiredError("PermissionsApi", "getRoleAssignmentsById", "roleId");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/role/{role_id}/assignments'
            .replace('{' + 'relations' + '}', encodeURIComponent(String(relations)))
            .replace('{' + 'role_id' + '}', encodeURIComponent(String(roleId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get my access to the server
     */
    public async getServerAccess(_options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // Path Params
        const localVarPath = '/management/v1/permissions/server/access';

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get user and role assignments to the current project
     * @param relations Relations to be loaded. If not specified, all relations are returned.
     */
    public async getServerAssignments(relations?: Array<ServerRelation>, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;


        // Path Params
        const localVarPath = '/management/v1/permissions/server/assignments';

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (relations !== undefined) {
            const serializedParams = ObjectSerializer.serialize(relations, "Array<ServerRelation>", "");
            for (const serializedParam of serializedParams) {
                requestContext.appendQueryParam("relations", serializedParam);
            }
        }


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get my access to a table
     * @param tableId Table ID
     */
    public async getTableAccessById(tableId: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'tableId' is not null or undefined
        if (tableId === null || tableId === undefined) {
            throw new RequiredError("PermissionsApi", "getTableAccessById", "tableId");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/table/{table_id}/access'
            .replace('{' + 'table_id' + '}', encodeURIComponent(String(tableId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get my access to a view
     * @param viewId View ID
     */
    public async getViewAccessById(viewId: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'viewId' is not null or undefined
        if (viewId === null || viewId === undefined) {
            throw new RequiredError("PermissionsApi", "getViewAccessById", "viewId");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/view/{view_id}/access'
            .replace('{' + 'view_id' + '}', encodeURIComponent(String(viewId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get user and role assignments for a view
     * @param namespaceId Namespace ID
     * @param relations Relations to be loaded. If not specified, all relations are returned.
     */
    public async getViewAssignmentsById(namespaceId: string, relations?: Array<ViewRelation>, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'namespaceId' is not null or undefined
        if (namespaceId === null || namespaceId === undefined) {
            throw new RequiredError("PermissionsApi", "getViewAssignmentsById", "namespaceId");
        }



        // Path Params
        const localVarPath = '/management/v1/permissions/table/{namespace_id}/assignments'
            .replace('{' + 'namespace_id' + '}', encodeURIComponent(String(namespaceId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (relations !== undefined) {
            const serializedParams = ObjectSerializer.serialize(relations, "Array<ViewRelation>", "");
            for (const serializedParam of serializedParams) {
                requestContext.appendQueryParam("relations", serializedParam);
            }
        }


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get my access to a warehouse
     * @param warehouseId Warehouse ID
     */
    public async getWarehouseAccessById(warehouseId: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'warehouseId' is not null or undefined
        if (warehouseId === null || warehouseId === undefined) {
            throw new RequiredError("PermissionsApi", "getWarehouseAccessById", "warehouseId");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/warehouse/{warehouse_id}/access'
            .replace('{' + 'warehouse_id' + '}', encodeURIComponent(String(warehouseId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get user and role assignments for a warehouse
     * @param warehouseId Warehouse ID
     * @param relations Relations to be loaded. If not specified, all relations are returned.
     */
    public async getWarehouseAssignmentsById(warehouseId: string, relations?: Array<WarehouseRelation>, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'warehouseId' is not null or undefined
        if (warehouseId === null || warehouseId === undefined) {
            throw new RequiredError("PermissionsApi", "getWarehouseAssignmentsById", "warehouseId");
        }



        // Path Params
        const localVarPath = '/management/v1/permissions/warehouse/{warehouse_id}/assignments'
            .replace('{' + 'warehouse_id' + '}', encodeURIComponent(String(warehouseId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (relations !== undefined) {
            const serializedParams = ObjectSerializer.serialize(relations, "Array<WarehouseRelation>", "");
            for (const serializedParam of serializedParams) {
                requestContext.appendQueryParam("relations", serializedParam);
            }
        }


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Get Authorization properties of a warehouse
     * @param warehouseId Warehouse ID
     */
    public async getWarehouseById(warehouseId: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'warehouseId' is not null or undefined
        if (warehouseId === null || warehouseId === undefined) {
            throw new RequiredError("PermissionsApi", "getWarehouseById", "warehouseId");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/warehouse/{warehouse_id}'
            .replace('{' + 'warehouse_id' + '}', encodeURIComponent(String(warehouseId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Set managed access property of a namespace
     * @param namespaceId Namespace ID
     * @param setManagedAccessRequest 
     */
    public async setNamespaceManagedAccess(namespaceId: string, setManagedAccessRequest: SetManagedAccessRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'namespaceId' is not null or undefined
        if (namespaceId === null || namespaceId === undefined) {
            throw new RequiredError("PermissionsApi", "setNamespaceManagedAccess", "namespaceId");
        }


        // verify required parameter 'setManagedAccessRequest' is not null or undefined
        if (setManagedAccessRequest === null || setManagedAccessRequest === undefined) {
            throw new RequiredError("PermissionsApi", "setNamespaceManagedAccess", "setManagedAccessRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/namespace/{namespace_id}/managed-access'
            .replace('{' + 'namespace_id' + '}', encodeURIComponent(String(namespaceId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(setManagedAccessRequest, "SetManagedAccessRequest", ""),
            contentType
        );
        requestContext.setBody(serializedBody);

        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Set managed access property of a warehouse
     * @param warehouseId Warehouse ID
     * @param setManagedAccessRequest 
     */
    public async setWarehouseManagedAccess(warehouseId: string, setManagedAccessRequest: SetManagedAccessRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'warehouseId' is not null or undefined
        if (warehouseId === null || warehouseId === undefined) {
            throw new RequiredError("PermissionsApi", "setWarehouseManagedAccess", "warehouseId");
        }


        // verify required parameter 'setManagedAccessRequest' is not null or undefined
        if (setManagedAccessRequest === null || setManagedAccessRequest === undefined) {
            throw new RequiredError("PermissionsApi", "setWarehouseManagedAccess", "setManagedAccessRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/warehouse/{warehouse_id}/managed-access'
            .replace('{' + 'warehouse_id' + '}', encodeURIComponent(String(warehouseId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(setManagedAccessRequest, "SetManagedAccessRequest", ""),
            contentType
        );
        requestContext.setBody(serializedBody);

        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Update permissions for a namespace
     * @param namespaceId Namespace ID
     * @param updateNamespaceAssignmentsRequest 
     */
    public async updateNamespaceAssignmentsById(namespaceId: string, updateNamespaceAssignmentsRequest: UpdateNamespaceAssignmentsRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'namespaceId' is not null or undefined
        if (namespaceId === null || namespaceId === undefined) {
            throw new RequiredError("PermissionsApi", "updateNamespaceAssignmentsById", "namespaceId");
        }


        // verify required parameter 'updateNamespaceAssignmentsRequest' is not null or undefined
        if (updateNamespaceAssignmentsRequest === null || updateNamespaceAssignmentsRequest === undefined) {
            throw new RequiredError("PermissionsApi", "updateNamespaceAssignmentsById", "updateNamespaceAssignmentsRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/namespace/{namespace_id}/assignments'
            .replace('{' + 'namespace_id' + '}', encodeURIComponent(String(namespaceId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(updateNamespaceAssignmentsRequest, "UpdateNamespaceAssignmentsRequest", ""),
            contentType
        );
        requestContext.setBody(serializedBody);

        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Update permissions for the default project
     * @param updateProjectAssignmentsRequest 
     */
    public async updateProjectAssignments(updateProjectAssignmentsRequest: UpdateProjectAssignmentsRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'updateProjectAssignmentsRequest' is not null or undefined
        if (updateProjectAssignmentsRequest === null || updateProjectAssignmentsRequest === undefined) {
            throw new RequiredError("PermissionsApi", "updateProjectAssignments", "updateProjectAssignmentsRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/project/assignments';

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(updateProjectAssignmentsRequest, "UpdateProjectAssignmentsRequest", ""),
            contentType
        );
        requestContext.setBody(serializedBody);

        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Update permissions for a project
     * @param projectId Project ID
     * @param updateProjectAssignmentsRequest 
     */
    public async updateProjectAssignmentsById(projectId: string, updateProjectAssignmentsRequest: UpdateProjectAssignmentsRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'projectId' is not null or undefined
        if (projectId === null || projectId === undefined) {
            throw new RequiredError("PermissionsApi", "updateProjectAssignmentsById", "projectId");
        }


        // verify required parameter 'updateProjectAssignmentsRequest' is not null or undefined
        if (updateProjectAssignmentsRequest === null || updateProjectAssignmentsRequest === undefined) {
            throw new RequiredError("PermissionsApi", "updateProjectAssignmentsById", "updateProjectAssignmentsRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/project/{project_id}/assignments'
            .replace('{' + 'project_id' + '}', encodeURIComponent(String(projectId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(updateProjectAssignmentsRequest, "UpdateProjectAssignmentsRequest", ""),
            contentType
        );
        requestContext.setBody(serializedBody);

        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Update permissions for a view
     * @param roleId Role ID
     * @param updateRoleAssignmentsRequest 
     */
    public async updateRoleAssignmentsById(roleId: string, updateRoleAssignmentsRequest: UpdateRoleAssignmentsRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'roleId' is not null or undefined
        if (roleId === null || roleId === undefined) {
            throw new RequiredError("PermissionsApi", "updateRoleAssignmentsById", "roleId");
        }


        // verify required parameter 'updateRoleAssignmentsRequest' is not null or undefined
        if (updateRoleAssignmentsRequest === null || updateRoleAssignmentsRequest === undefined) {
            throw new RequiredError("PermissionsApi", "updateRoleAssignmentsById", "updateRoleAssignmentsRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/role/{role_id}/assignments'
            .replace('{' + 'role_id' + '}', encodeURIComponent(String(roleId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(updateRoleAssignmentsRequest, "UpdateRoleAssignmentsRequest", ""),
            contentType
        );
        requestContext.setBody(serializedBody);

        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Update permissions for this server
     * @param updateServerAssignmentsRequest 
     */
    public async updateServerAssignments(updateServerAssignmentsRequest: UpdateServerAssignmentsRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'updateServerAssignmentsRequest' is not null or undefined
        if (updateServerAssignmentsRequest === null || updateServerAssignmentsRequest === undefined) {
            throw new RequiredError("PermissionsApi", "updateServerAssignments", "updateServerAssignmentsRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/server/assignments';

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(updateServerAssignmentsRequest, "UpdateServerAssignmentsRequest", ""),
            contentType
        );
        requestContext.setBody(serializedBody);

        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Update permissions for a table
     * @param tableId Table ID
     * @param updateTableAssignmentsRequest 
     */
    public async updateTableAssignmentsById(tableId: string, updateTableAssignmentsRequest: UpdateTableAssignmentsRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'tableId' is not null or undefined
        if (tableId === null || tableId === undefined) {
            throw new RequiredError("PermissionsApi", "updateTableAssignmentsById", "tableId");
        }


        // verify required parameter 'updateTableAssignmentsRequest' is not null or undefined
        if (updateTableAssignmentsRequest === null || updateTableAssignmentsRequest === undefined) {
            throw new RequiredError("PermissionsApi", "updateTableAssignmentsById", "updateTableAssignmentsRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/table/{table_id}/assignments'
            .replace('{' + 'table_id' + '}', encodeURIComponent(String(tableId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(updateTableAssignmentsRequest, "UpdateTableAssignmentsRequest", ""),
            contentType
        );
        requestContext.setBody(serializedBody);

        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Update permissions for a view
     * @param viewId View ID
     * @param updateViewAssignmentsRequest 
     */
    public async updateViewAssignmentsById(viewId: string, updateViewAssignmentsRequest: UpdateViewAssignmentsRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'viewId' is not null or undefined
        if (viewId === null || viewId === undefined) {
            throw new RequiredError("PermissionsApi", "updateViewAssignmentsById", "viewId");
        }


        // verify required parameter 'updateViewAssignmentsRequest' is not null or undefined
        if (updateViewAssignmentsRequest === null || updateViewAssignmentsRequest === undefined) {
            throw new RequiredError("PermissionsApi", "updateViewAssignmentsById", "updateViewAssignmentsRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/view/{view_id}/assignments'
            .replace('{' + 'view_id' + '}', encodeURIComponent(String(viewId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(updateViewAssignmentsRequest, "UpdateViewAssignmentsRequest", ""),
            contentType
        );
        requestContext.setBody(serializedBody);

        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

    /**
     * Update permissions for a project
     * @param warehouseId Warehouse ID
     * @param updateWarehouseAssignmentsRequest 
     */
    public async updateWarehouseAssignmentsById(warehouseId: string, updateWarehouseAssignmentsRequest: UpdateWarehouseAssignmentsRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'warehouseId' is not null or undefined
        if (warehouseId === null || warehouseId === undefined) {
            throw new RequiredError("PermissionsApi", "updateWarehouseAssignmentsById", "warehouseId");
        }


        // verify required parameter 'updateWarehouseAssignmentsRequest' is not null or undefined
        if (updateWarehouseAssignmentsRequest === null || updateWarehouseAssignmentsRequest === undefined) {
            throw new RequiredError("PermissionsApi", "updateWarehouseAssignmentsById", "updateWarehouseAssignmentsRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/permissions/warehouse/{warehouse_id}/assignments'
            .replace('{' + 'warehouse_id' + '}', encodeURIComponent(String(warehouseId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(updateWarehouseAssignmentsRequest, "UpdateWarehouseAssignmentsRequest", ""),
            contentType
        );
        requestContext.setBody(serializedBody);

        let authMethod: SecurityAuthentication | undefined;
        // Apply auth methods
        authMethod = _config.authMethods["bearerAuth"]
        if (authMethod?.applySecurityAuthentication) {
            await authMethod?.applySecurityAuthentication(requestContext);
        }
        
        const defaultAuth: SecurityAuthentication | undefined = _options?.authMethods?.default || this.configuration?.authMethods?.default
        if (defaultAuth?.applySecurityAuthentication) {
            await defaultAuth?.applySecurityAuthentication(requestContext);
        }

        return requestContext;
    }

}

export class PermissionsApiResponseProcessor {

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getNamespaceAccessById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getNamespaceAccessByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetNamespaceAccessResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetNamespaceAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetNamespaceAccessResponse>", ""
            ) as Array<GetNamespaceAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetNamespaceAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetNamespaceAccessResponse>", ""
            ) as Array<GetNamespaceAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getNamespaceAssignmentsById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getNamespaceAssignmentsByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetNamespaceAssignmentsResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetNamespaceAssignmentsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetNamespaceAssignmentsResponse>", ""
            ) as Array<GetNamespaceAssignmentsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetNamespaceAssignmentsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetNamespaceAssignmentsResponse>", ""
            ) as Array<GetNamespaceAssignmentsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getNamespaceById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getNamespaceByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetNamespaceResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetNamespaceResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetNamespaceResponse>", ""
            ) as Array<GetNamespaceResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetNamespaceResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetNamespaceResponse>", ""
            ) as Array<GetNamespaceResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getProjectAccess
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getProjectAccessWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetProjectAccessResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetProjectAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetProjectAccessResponse>", ""
            ) as Array<GetProjectAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetProjectAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetProjectAccessResponse>", ""
            ) as Array<GetProjectAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getProjectAccessById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getProjectAccessByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetProjectAccessResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetProjectAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetProjectAccessResponse>", ""
            ) as Array<GetProjectAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetProjectAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetProjectAccessResponse>", ""
            ) as Array<GetProjectAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getProjectAssignments
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getProjectAssignmentsWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetProjectAssignmentsResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetProjectAssignmentsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetProjectAssignmentsResponse>", ""
            ) as Array<GetProjectAssignmentsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetProjectAssignmentsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetProjectAssignmentsResponse>", ""
            ) as Array<GetProjectAssignmentsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getProjectAssignmentsById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getProjectAssignmentsByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetProjectAssignmentsResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetProjectAssignmentsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetProjectAssignmentsResponse>", ""
            ) as Array<GetProjectAssignmentsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetProjectAssignmentsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetProjectAssignmentsResponse>", ""
            ) as Array<GetProjectAssignmentsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getRoleAccessById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getRoleAccessByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetRoleAccessResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetRoleAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetRoleAccessResponse>", ""
            ) as Array<GetRoleAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetRoleAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetRoleAccessResponse>", ""
            ) as Array<GetRoleAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getRoleAssignmentsById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getRoleAssignmentsByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetRoleAssignmentsResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetRoleAssignmentsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetRoleAssignmentsResponse>", ""
            ) as Array<GetRoleAssignmentsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetRoleAssignmentsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetRoleAssignmentsResponse>", ""
            ) as Array<GetRoleAssignmentsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getServerAccess
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getServerAccessWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetServerAccessResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetServerAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetServerAccessResponse>", ""
            ) as Array<GetServerAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetServerAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetServerAccessResponse>", ""
            ) as Array<GetServerAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getServerAssignments
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getServerAssignmentsWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetServerAssignmentsResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetServerAssignmentsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetServerAssignmentsResponse>", ""
            ) as Array<GetServerAssignmentsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetServerAssignmentsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetServerAssignmentsResponse>", ""
            ) as Array<GetServerAssignmentsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getTableAccessById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getTableAccessByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetTableAccessResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetTableAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetTableAccessResponse>", ""
            ) as Array<GetTableAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetTableAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetTableAccessResponse>", ""
            ) as Array<GetTableAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getViewAccessById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getViewAccessByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetViewAccessResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetViewAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetViewAccessResponse>", ""
            ) as Array<GetViewAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetViewAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetViewAccessResponse>", ""
            ) as Array<GetViewAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getViewAssignmentsById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getViewAssignmentsByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetViewAssignmentsResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetViewAssignmentsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetViewAssignmentsResponse>", ""
            ) as Array<GetViewAssignmentsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetViewAssignmentsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetViewAssignmentsResponse>", ""
            ) as Array<GetViewAssignmentsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getWarehouseAccessById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getWarehouseAccessByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetWarehouseAccessResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetWarehouseAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetWarehouseAccessResponse>", ""
            ) as Array<GetWarehouseAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetWarehouseAccessResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetWarehouseAccessResponse>", ""
            ) as Array<GetWarehouseAccessResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getWarehouseAssignmentsById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getWarehouseAssignmentsByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetWarehouseAssignmentsResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetWarehouseAssignmentsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetWarehouseAssignmentsResponse>", ""
            ) as Array<GetWarehouseAssignmentsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetWarehouseAssignmentsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetWarehouseAssignmentsResponse>", ""
            ) as Array<GetWarehouseAssignmentsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to getWarehouseById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getWarehouseByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetWarehouseResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<GetWarehouseResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetWarehouseResponse>", ""
            ) as Array<GetWarehouseResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<GetWarehouseResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<GetWarehouseResponse>", ""
            ) as Array<GetWarehouseResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to setNamespaceManagedAccess
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async setNamespaceManagedAccessWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<any | null> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<any | null> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<any | null>", ""
            ) as Array<any | null>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<any | null> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<any | null>", ""
            ) as Array<any | null>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to setWarehouseManagedAccess
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async setWarehouseManagedAccessWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<any | null> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<any | null> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<any | null>", ""
            ) as Array<any | null>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<any | null> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<any | null>", ""
            ) as Array<any | null>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to updateNamespaceAssignmentsById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async updateNamespaceAssignmentsByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, undefined);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: void = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "void", ""
            ) as void;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to updateProjectAssignments
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async updateProjectAssignmentsWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, undefined);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: void = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "void", ""
            ) as void;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to updateProjectAssignmentsById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async updateProjectAssignmentsByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, undefined);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: void = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "void", ""
            ) as void;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to updateRoleAssignmentsById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async updateRoleAssignmentsByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, undefined);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: void = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "void", ""
            ) as void;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to updateServerAssignments
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async updateServerAssignmentsWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, undefined);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: void = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "void", ""
            ) as void;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to updateTableAssignmentsById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async updateTableAssignmentsByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, undefined);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: void = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "void", ""
            ) as void;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to updateViewAssignmentsById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async updateViewAssignmentsByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, undefined);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: void = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "void", ""
            ) as void;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to updateWarehouseAssignmentsById
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async updateWarehouseAssignmentsByIdWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, undefined);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: void = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "void", ""
            ) as void;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

}
