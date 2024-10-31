// TODO: better import syntax?
import {BaseAPIRequestFactory, RequiredError, COLLECTION_FORMATS} from './baseapi';
import {Configuration} from '../configuration';
import {RequestContext, HttpMethod, ResponseContext, HttpFile, HttpInfo} from '../http/http';
import {ObjectSerializer} from '../models/ObjectSerializer';
import {ApiException} from './exception';
import {canConsumeForm, isCodeInRange} from '../util';
import {SecurityAuthentication} from '../auth/auth';


import { CreateWarehouseRequest } from '../models/CreateWarehouseRequest';
import { CreateWarehouseResponse } from '../models/CreateWarehouseResponse';
import { GetWarehouseResponse } from '../models/GetWarehouseResponse';
import { ListDeletedTabularsResponse } from '../models/ListDeletedTabularsResponse';
import { ListWarehousesResponse } from '../models/ListWarehousesResponse';
import { RenameWarehouseRequest } from '../models/RenameWarehouseRequest';
import { UpdateWarehouseCredentialRequest } from '../models/UpdateWarehouseCredentialRequest';
import { UpdateWarehouseDeleteProfileRequest } from '../models/UpdateWarehouseDeleteProfileRequest';
import { UpdateWarehouseStorageRequest } from '../models/UpdateWarehouseStorageRequest';
import { WarehouseStatus } from '../models/WarehouseStatus';

/**
 * no description
 */
export class WarehouseApiRequestFactory extends BaseAPIRequestFactory {

    /**
     * Activate a warehouse
     * @param warehouseId 
     */
    public async activateWarehouse(warehouseId: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'warehouseId' is not null or undefined
        if (warehouseId === null || warehouseId === undefined) {
            throw new RequiredError("WarehouseApi", "activateWarehouse", "warehouseId");
        }


        // Path Params
        const localVarPath = '/management/v1/warehouse/{warehouse_id}/activate'
            .replace('{' + 'warehouse_id' + '}', encodeURIComponent(String(warehouseId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
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
     * Create a new warehouse in the given project. The project of a warehouse cannot be changed after creation. The storage configuration is validated by this method.
     * Create a new warehouse.
     * @param createWarehouseRequest 
     */
    public async createWarehouse(createWarehouseRequest: CreateWarehouseRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'createWarehouseRequest' is not null or undefined
        if (createWarehouseRequest === null || createWarehouseRequest === undefined) {
            throw new RequiredError("WarehouseApi", "createWarehouse", "createWarehouseRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/warehouse';

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")


        // Body Params
        const contentType = ObjectSerializer.getPreferredMediaType([
            "application/json"
        ]);
        requestContext.setHeaderParam("Content-Type", contentType);
        const serializedBody = ObjectSerializer.stringify(
            ObjectSerializer.serialize(createWarehouseRequest, "CreateWarehouseRequest", ""),
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
     * Deactivate a warehouse
     * @param warehouseId 
     */
    public async deactivateWarehouse(warehouseId: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'warehouseId' is not null or undefined
        if (warehouseId === null || warehouseId === undefined) {
            throw new RequiredError("WarehouseApi", "deactivateWarehouse", "warehouseId");
        }


        // Path Params
        const localVarPath = '/management/v1/warehouse/{warehouse_id}/deactivate'
            .replace('{' + 'warehouse_id' + '}', encodeURIComponent(String(warehouseId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.POST);
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
     * Delete a warehouse by ID
     * @param warehouseId 
     */
    public async deleteWarehouse(warehouseId: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'warehouseId' is not null or undefined
        if (warehouseId === null || warehouseId === undefined) {
            throw new RequiredError("WarehouseApi", "deleteWarehouse", "warehouseId");
        }


        // Path Params
        const localVarPath = '/management/v1/warehouse/{warehouse_id}'
            .replace('{' + 'warehouse_id' + '}', encodeURIComponent(String(warehouseId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.DELETE);
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
     * Get a warehouse by ID
     * @param warehouseId 
     */
    public async getWarehouse(warehouseId: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'warehouseId' is not null or undefined
        if (warehouseId === null || warehouseId === undefined) {
            throw new RequiredError("WarehouseApi", "getWarehouse", "warehouseId");
        }


        // Path Params
        const localVarPath = '/management/v1/warehouse/{warehouse_id}'
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
     * List all soft-deleted tabulars in the warehouse that are visible to you.
     * List soft-deleted tabulars
     * @param warehouseId 
     * @param pageToken Next page token
     * @param pageSize Signals an upper bound of the number of results that a client will receive.
     */
    public async listDeletedTabulars(warehouseId: string, pageToken?: string, pageSize?: number, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'warehouseId' is not null or undefined
        if (warehouseId === null || warehouseId === undefined) {
            throw new RequiredError("WarehouseApi", "listDeletedTabulars", "warehouseId");
        }




        // Path Params
        const localVarPath = '/management/v1/warehouse/{warehouse_id}/deleted_tabulars'
            .replace('{' + 'warehouse_id' + '}', encodeURIComponent(String(warehouseId)));

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (pageToken !== undefined) {
            requestContext.setQueryParam("pageToken", ObjectSerializer.serialize(pageToken, "string", ""));
        }

        // Query Params
        if (pageSize !== undefined) {
            requestContext.setQueryParam("pageSize", ObjectSerializer.serialize(pageSize, "number", "int32"));
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
     * By default, this endpoint does not return deactivated warehouses. To include deactivated warehouses, set the `include_deactivated` query parameter to `true`.
     * List all warehouses in a project
     * @param warehouseStatus Optional filter to return only warehouses with the specified status. If not provided, only active warehouses are returned.
     * @param projectId The project ID to list warehouses for. Setting a warehouse is required.
     */
    public async listWarehouses(warehouseStatus?: Array<WarehouseStatus>, projectId?: string, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;



        // Path Params
        const localVarPath = '/management/v1/warehouse';

        // Make Request Context
        const requestContext = _config.baseServer.makeRequestContext(localVarPath, HttpMethod.GET);
        requestContext.setHeaderParam("Accept", "application/json, */*;q=0.8")

        // Query Params
        if (warehouseStatus !== undefined) {
            const serializedParams = ObjectSerializer.serialize(warehouseStatus, "Array<WarehouseStatus>", "");
            for (const serializedParam of serializedParams) {
                requestContext.appendQueryParam("warehouseStatus", serializedParam);
            }
        }

        // Query Params
        if (projectId !== undefined) {
            requestContext.setQueryParam("projectId", ObjectSerializer.serialize(projectId, "string", "uuid"));
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
     * Rename a warehouse
     * @param warehouseId 
     * @param renameWarehouseRequest 
     */
    public async renameWarehouse(warehouseId: string, renameWarehouseRequest: RenameWarehouseRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'warehouseId' is not null or undefined
        if (warehouseId === null || warehouseId === undefined) {
            throw new RequiredError("WarehouseApi", "renameWarehouse", "warehouseId");
        }


        // verify required parameter 'renameWarehouseRequest' is not null or undefined
        if (renameWarehouseRequest === null || renameWarehouseRequest === undefined) {
            throw new RequiredError("WarehouseApi", "renameWarehouse", "renameWarehouseRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/warehouse/{warehouse_id}/rename'
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
            ObjectSerializer.serialize(renameWarehouseRequest, "RenameWarehouseRequest", ""),
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
     * This can be used to update credentials before expiration.
     * Update the storage credential of a warehouse. The storage profile is not modified.
     * @param warehouseId 
     * @param updateWarehouseCredentialRequest 
     */
    public async updateStorageCredential(warehouseId: string, updateWarehouseCredentialRequest: UpdateWarehouseCredentialRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'warehouseId' is not null or undefined
        if (warehouseId === null || warehouseId === undefined) {
            throw new RequiredError("WarehouseApi", "updateStorageCredential", "warehouseId");
        }


        // verify required parameter 'updateWarehouseCredentialRequest' is not null or undefined
        if (updateWarehouseCredentialRequest === null || updateWarehouseCredentialRequest === undefined) {
            throw new RequiredError("WarehouseApi", "updateStorageCredential", "updateWarehouseCredentialRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/warehouse/{warehouse_id}/storage-credential'
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
            ObjectSerializer.serialize(updateWarehouseCredentialRequest, "UpdateWarehouseCredentialRequest", ""),
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
     * Update the storage profile of a warehouse including its storage credential.
     * @param warehouseId 
     * @param updateWarehouseStorageRequest 
     */
    public async updateStorageProfile(warehouseId: string, updateWarehouseStorageRequest: UpdateWarehouseStorageRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'warehouseId' is not null or undefined
        if (warehouseId === null || warehouseId === undefined) {
            throw new RequiredError("WarehouseApi", "updateStorageProfile", "warehouseId");
        }


        // verify required parameter 'updateWarehouseStorageRequest' is not null or undefined
        if (updateWarehouseStorageRequest === null || updateWarehouseStorageRequest === undefined) {
            throw new RequiredError("WarehouseApi", "updateStorageProfile", "updateWarehouseStorageRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/warehouse/{warehouse_id}/storage'
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
            ObjectSerializer.serialize(updateWarehouseStorageRequest, "UpdateWarehouseStorageRequest", ""),
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
     * Update the Deletion Profile (soft-delete) of a warehouse.
     * @param warehouseId 
     * @param updateWarehouseDeleteProfileRequest 
     */
    public async updateWarehouseDeleteProfile(warehouseId: string, updateWarehouseDeleteProfileRequest: UpdateWarehouseDeleteProfileRequest, _options?: Configuration): Promise<RequestContext> {
        let _config = _options || this.configuration;

        // verify required parameter 'warehouseId' is not null or undefined
        if (warehouseId === null || warehouseId === undefined) {
            throw new RequiredError("WarehouseApi", "updateWarehouseDeleteProfile", "warehouseId");
        }


        // verify required parameter 'updateWarehouseDeleteProfileRequest' is not null or undefined
        if (updateWarehouseDeleteProfileRequest === null || updateWarehouseDeleteProfileRequest === undefined) {
            throw new RequiredError("WarehouseApi", "updateWarehouseDeleteProfile", "updateWarehouseDeleteProfileRequest");
        }


        // Path Params
        const localVarPath = '/management/v1/warehouse/{warehouse_id}/delete-profile'
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
            ObjectSerializer.serialize(updateWarehouseDeleteProfileRequest, "UpdateWarehouseDeleteProfileRequest", ""),
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

export class WarehouseApiResponseProcessor {

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to activateWarehouse
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async activateWarehouseWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
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
     * @params response Response returned by the server for a request to createWarehouse
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async createWarehouseWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<CreateWarehouseResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("201", response.httpStatusCode)) {
            const body: Array<CreateWarehouseResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<CreateWarehouseResponse>", ""
            ) as Array<CreateWarehouseResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<CreateWarehouseResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<CreateWarehouseResponse>", ""
            ) as Array<CreateWarehouseResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to deactivateWarehouse
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async deactivateWarehouseWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
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
     * @params response Response returned by the server for a request to deleteWarehouse
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async deleteWarehouseWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
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
     * @params response Response returned by the server for a request to getWarehouse
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async getWarehouseWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<GetWarehouseResponse> >> {
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
     * @params response Response returned by the server for a request to listDeletedTabulars
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async listDeletedTabularsWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<ListDeletedTabularsResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<ListDeletedTabularsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<ListDeletedTabularsResponse>", ""
            ) as Array<ListDeletedTabularsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<ListDeletedTabularsResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<ListDeletedTabularsResponse>", ""
            ) as Array<ListDeletedTabularsResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to listWarehouses
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async listWarehousesWithHttpInfo(response: ResponseContext): Promise<HttpInfo<Array<ListWarehousesResponse> >> {
        const contentType = ObjectSerializer.normalizeMediaType(response.headers["content-type"]);
        if (isCodeInRange("200", response.httpStatusCode)) {
            const body: Array<ListWarehousesResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<ListWarehousesResponse>", ""
            ) as Array<ListWarehousesResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        // Work around for missing responses in specification, e.g. for petstore.yaml
        if (response.httpStatusCode >= 200 && response.httpStatusCode <= 299) {
            const body: Array<ListWarehousesResponse> = ObjectSerializer.deserialize(
                ObjectSerializer.parse(await response.body.text(), contentType),
                "Array<ListWarehousesResponse>", ""
            ) as Array<ListWarehousesResponse>;
            return new HttpInfo(response.httpStatusCode, response.headers, response.body, body);
        }

        throw new ApiException<string | Blob | undefined>(response.httpStatusCode, "Unknown API Status Code!", await response.getBodyAsAny(), response.headers);
    }

    /**
     * Unwraps the actual response sent by the server from the response context and deserializes the response content
     * to the expected objects
     *
     * @params response Response returned by the server for a request to renameWarehouse
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async renameWarehouseWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
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
     * @params response Response returned by the server for a request to updateStorageCredential
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async updateStorageCredentialWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
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
     * @params response Response returned by the server for a request to updateStorageProfile
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async updateStorageProfileWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
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
     * @params response Response returned by the server for a request to updateWarehouseDeleteProfile
     * @throws ApiException if the response code was not in [200, 299]
     */
     public async updateWarehouseDeleteProfileWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void >> {
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
