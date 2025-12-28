/**
 * Instance client for CDF Data Modeling instance operations.
 *
 * This module provides the InstanceClient class for performing CRUD operations
 * on CDF instances (nodes and edges) with support for batch operations,
 * parallel execution, and proper error handling.
 *
 * @packageDocumentation
 */

import type { PygenClientConfig } from "./auth/index.ts";
import { MultiRequestError } from "./exceptions.ts";
import { HTTPClient } from "./http_client/index.ts";
import type {
  FailedRequest,
  FailedResponse,
  HTTPResult,
  RequestMessage,
} from "./http_client/types.ts";
import type { Instance, InstanceWrite } from "./types/instance.ts";
import { dumpInstanceForAPI } from "./types/instance.ts";
import type { InstanceId, InstanceType, ViewReference } from "./types/references.ts";
import type { DeleteResponse, UpsertResult } from "./types/responses.ts";
import { extendUpsertResult } from "./types/responses.ts";
import { chunker } from "./types/utils.ts";

/** Upsert mode for instance operations */
export type UpsertMode = "replace" | "update" | "apply";

/**
 * Options for configuring worker counts in InstanceClient.
 */
export interface InstanceClientOptions {
  /**
   * Number of concurrent workers for write operations.
   * @default 5
   */
  writeWorkers?: number;

  /**
   * Number of concurrent workers for delete operations.
   * @default 3
   */
  deleteWorkers?: number;

  /**
   * Number of concurrent workers for retrieve operations.
   * @default 10
   */
  retrieveWorkers?: number;
}

/**
 * Serialized instance ID for API delete operations.
 */
interface SerializedInstanceId {
  instanceType: InstanceType;
  space: string;
  externalId: string;
}

/**
 * Internal representation of an instance ID with instance type.
 */
interface InternalInstanceId {
  instanceType: InstanceType;
  space: string;
  externalId: string;
}

/**
 * Client for performing CRUD operations on CDF instances.
 *
 * Provides methods for upserting and deleting instances with support for:
 * - Batch operations with automatic chunking (1000 items per request)
 * - Parallel execution with configurable worker counts
 * - Proper error handling with partial success support
 *
 * @example
 * ```typescript
 * const config: PygenClientConfig = {
 *   baseUrl: "https://api.cognitedata.com",
 *   project: "my-project",
 *   credentials: new TokenCredentials("my-token"),
 * };
 *
 * const client = new InstanceClient(config);
 *
 * // Upsert instances
 * const result = await client.upsert(instances, viewRef);
 * console.log(`Created: ${getCreated(result).length}`);
 *
 * // Delete instances
 * const deleted = await client.delete(instanceIds);
 * console.log(`Deleted: ${deleted.length}`);
 * ```
 */
export class InstanceClient {
  /** Maximum items per upsert request */
  static readonly UPSERT_LIMIT = 1000;

  /** Maximum items per delete request */
  static readonly DELETE_LIMIT = 1000;

  /** Maximum items per retrieve request */
  static readonly RETRIEVE_LIMIT = 1000;

  /** API endpoint for instance operations */
  protected static readonly ENDPOINT = "/models/instances";

  /** The HTTP client for making requests */
  protected readonly httpClient: HTTPClient;

  /** The client configuration */
  protected readonly config: PygenClientConfig;

  /** Number of concurrent workers for write operations */
  protected readonly writeWorkers: number;

  /** Number of concurrent workers for delete operations */
  protected readonly deleteWorkers: number;

  /** Number of concurrent workers for retrieve operations */
  protected readonly retrieveWorkers: number;

  /**
   * Creates a new InstanceClient.
   *
   * @param config - Configuration for the client including URL, project, and credentials
   * @param options - Optional configuration for worker counts
   */
  constructor(config: PygenClientConfig, options: InstanceClientOptions = {}) {
    this.config = config;
    this.httpClient = new HTTPClient(config);
    this.writeWorkers = options.writeWorkers ?? 5;
    this.deleteWorkers = options.deleteWorkers ?? 3;
    this.retrieveWorkers = options.retrieveWorkers ?? 10;
  }

  /**
   * Creates the full API URL for an endpoint.
   *
   * @param endpoint - The API endpoint path
   * @returns The full API URL
   */
  protected createApiUrl(endpoint: string): string {
    const baseUrl = this.config.baseUrl.replace(/\/$/, "");
    const project = this.config.project;
    return `${baseUrl}/api/v1/projects/${project}${endpoint}`;
  }

  /**
   * Upserts instances (creates or updates).
   *
   * @param items - Single instance or array of instances to upsert
   * @param viewRef - The view reference for the instances
   * @param mode - Upsert mode: "replace", "update", or "apply" (default: "apply")
   *   - "replace": Replaces the entire instance with the provided data
   *   - "update": First retrieves existing instances, then merges properties (not yet implemented)
   *   - "apply": Applies only the provided changes, leaving existing properties intact
   * @param skipOnVersionConflict - If true, skip items with version conflicts instead of failing
   * @returns UpsertResult containing details of the operation
   *
   * @throws MultiRequestError if any requests fail (contains successful results in error.result)
   *
   * @example
   * ```typescript
   * const result = await client.upsert(
   *   [person1, person2],
   *   { space: "mySpace", externalId: "Person", version: "1" },
   *   "apply"
   * );
   * console.log(`Created: ${getCreated(result).length}`);
   * console.log(`Updated: ${getUpdated(result).length}`);
   * ```
   */
  async upsert(
    items: InstanceWrite | readonly InstanceWrite[],
    viewRef: ViewReference,
    mode: UpsertMode = "apply",
    skipOnVersionConflict = false,
  ): Promise<UpsertResult> {
    const itemList = Array.isArray(items) ? items : [items];

    if (itemList.length === 0) {
      return { items: [], deleted: [] };
    }

    if (mode === "update") {
      throw new Error("Update mode is not yet implemented");
    }

    const upsertChunk = (chunk: readonly InstanceWrite[]): Promise<HTTPResult> => {
      return this.upsertChunk(chunk, viewRef, mode, skipOnVersionConflict);
    };

    const httpResults = await this.executeInParallel(
      itemList,
      InstanceClient.UPSERT_LIMIT,
      this.writeWorkers,
      upsertChunk,
    );

    return this.collectResults(httpResults, this.parseUpsertResponse);
  }

  /**
   * Upserts a single chunk of instances.
   */
  private async upsertChunk(
    items: readonly InstanceWrite[],
    viewRef: ViewReference,
    mode: UpsertMode,
    skipOnVersionConflict: boolean,
  ): Promise<HTTPResult> {
    const serializedItems = items.map((item) => dumpInstanceForAPI(item, viewRef));

    const body: Record<string, unknown> = {
      items: serializedItems,
      replace: mode === "replace",
      skipOnVersionConflict,
    };

    const request: RequestMessage = {
      endpointUrl: this.createApiUrl(InstanceClient.ENDPOINT),
      method: "POST",
      body,
    };

    return this.httpClient.request(request);
  }

  /**
   * Parses the response from the upsert API.
   */
  private parseUpsertResponse(body: string): UpsertResult {
    const parsed = JSON.parse(body) as UpsertResult;
    return {
      items: parsed.items ?? [],
      deleted: parsed.deleted ?? [],
    };
  }

  /**
   * Deletes instances.
   *
   * @param items - Instance identifiers to delete. Can be:
   *   - A single external ID string (requires space parameter)
   *   - A single InstanceId object
   *   - An InstanceWrite or Instance object
   *   - An array of any of the above
   * @param space - Optional space identifier if items are provided as strings
   * @param instanceType - Optional instance type (default: "node")
   * @returns Array of deleted InstanceId objects
   *
   * @throws Error if space is not provided when items are strings
   * @throws MultiRequestError if any requests fail
   *
   * @example
   * ```typescript
   * // Delete by InstanceId
   * const deleted = await client.delete([
   *   { space: "mySpace", externalId: "person-1" },
   *   { space: "mySpace", externalId: "person-2" }
   * ]);
   *
   * // Delete by external ID strings
   * const deleted = await client.delete(["person-1", "person-2"], "mySpace");
   *
   * // Delete instances directly
   * const deleted = await client.delete(persons);
   * ```
   */
  async delete(
    items:
      | string
      | InstanceId
      | Instance
      | InstanceWrite
      | readonly (string | InstanceId | Instance | InstanceWrite)[],
    space?: string,
    instanceType: InstanceType = "node",
  ): Promise<InstanceId[]> {
    // Normalize input to array
    const itemList = Array.isArray(items) ? items : [items];

    if (itemList.length === 0) {
      return [];
    }

    // Convert all items to InternalInstanceId
    const instanceIds = itemList.map((item) =>
      this.toInternalInstanceId(item, space, instanceType)
    );

    const httpResults = await this.executeInParallel(
      instanceIds,
      InstanceClient.DELETE_LIMIT,
      this.deleteWorkers,
      (chunk) => this.deleteChunk(chunk),
    );

    const result = this.collectResults(httpResults, this.parseDeleteResponse);
    return result.deleted;
  }

  /**
   * Converts various input types to InternalInstanceId.
   */
  private toInternalInstanceId(
    item: string | InstanceId | Instance | InstanceWrite,
    space: string | undefined,
    defaultInstanceType: InstanceType,
  ): InternalInstanceId {
    if (typeof item === "string") {
      if (!space) {
        throw new Error("space parameter is required when deleting by external_id string");
      }
      return {
        instanceType: defaultInstanceType,
        space,
        externalId: item,
      };
    }

    // Check if it's an Instance or InstanceWrite (has instanceType)
    if ("instanceType" in item) {
      return {
        instanceType: item.instanceType,
        space: item.space,
        externalId: item.externalId,
      };
    }

    // It's an InstanceId (space and externalId only)
    return {
      instanceType: defaultInstanceType,
      space: item.space,
      externalId: item.externalId,
    };
  }

  /**
   * Deletes a single chunk of instances.
   */
  private async deleteChunk(items: readonly InternalInstanceId[]): Promise<HTTPResult> {
    const serializedItems: SerializedInstanceId[] = items.map((item) => ({
      instanceType: item.instanceType,
      space: item.space,
      externalId: item.externalId,
    }));

    const body: Record<string, unknown> = {
      items: serializedItems,
    };

    const request: RequestMessage = {
      endpointUrl: this.createApiUrl("/models/instances/delete"),
      method: "POST",
      body,
    };

    return this.httpClient.request(request);
  }

  /**
   * Parses the response from the delete API.
   */
  private parseDeleteResponse(body: string): UpsertResult {
    const parsed = JSON.parse(body) as DeleteResponse;
    return {
      items: [],
      deleted: parsed.items ?? [],
    };
  }

  /**
   * Executes a task function in parallel on chunked items.
   *
   * Uses a pool-based approach to limit concurrent executions.
   *
   * @typeParam T - Type of items to process
   * @param items - List of items to process
   * @param chunkSize - Maximum size of each chunk
   * @param maxWorkers - Maximum number of concurrent workers
   * @param taskFn - Async function to execute on each chunk
   * @returns Array of HTTPResult objects from all tasks
   */
  protected async executeInParallel<T>(
    items: readonly T[],
    chunkSize: number,
    maxWorkers: number,
    taskFn: (chunk: readonly T[]) => Promise<HTTPResult>,
  ): Promise<HTTPResult[]> {
    const chunks = chunker(items, chunkSize);

    if (chunks.length === 0) {
      return [];
    }

    // For small numbers of chunks, just run all in parallel
    if (chunks.length <= maxWorkers) {
      return Promise.all(chunks.map((chunk) => taskFn(chunk)));
    }

    // Use a pool pattern for larger numbers
    // Create indexed tasks that we'll process
    const indexedTasks = chunks.map((chunk, index) => ({ chunk, index }));
    const results: HTTPResult[] = new Array(chunks.length);

    // Process tasks in batches of maxWorkers
    for (let i = 0; i < indexedTasks.length; i += maxWorkers) {
      const batch = indexedTasks.slice(i, i + maxWorkers);
      const batchResults = await Promise.all(
        batch.map(async ({ chunk, index }) => {
          const result = await taskFn(chunk);
          return { index, result };
        }),
      );
      for (const { index, result } of batchResults) {
        results[index] = result;
      }
    }

    return results;
  }

  /**
   * Collects results from HTTP responses, raising on failures.
   *
   * @param results - List of HTTPResult objects
   * @param parseSuccess - Function to parse a successful response body
   * @returns Combined UpsertResult from all successful operations
   * @throws MultiRequestError if any of the HTTPResults indicate a failure
   */
  protected collectResults(
    results: readonly HTTPResult[],
    parseSuccess: (body: string) => UpsertResult,
  ): UpsertResult {
    const combinedResult: UpsertResult = { items: [], deleted: [] };
    const failedResponses: FailedResponse[] = [];
    const failedRequests: FailedRequest[] = [];

    for (const result of results) {
      if (result.kind === "success") {
        const parsed = parseSuccess(result.body);
        extendUpsertResult(combinedResult, parsed);
      } else if (result.kind === "failed_response") {
        failedResponses.push(result);
      } else if (result.kind === "failed_request") {
        failedRequests.push(result);
      }
    }

    if (failedResponses.length > 0 || failedRequests.length > 0) {
      throw new MultiRequestError(failedResponses, failedRequests, combinedResult);
    }

    return combinedResult;
  }

  /**
   * Closes the client and releases any resources.
   *
   * In TypeScript, there's no need for explicit cleanup like thread pools,
   * but this method is provided for API consistency with Python.
   */
  close(): void {
    // No cleanup needed in TypeScript - fetch API handles connection pooling
  }
}

/**
 * Creates an InstanceClient with the using statement pattern.
 *
 * Note: TypeScript's using statement (Disposable) is still experimental.
 * This function is provided for future compatibility.
 *
 * @param config - Configuration for the client
 * @param options - Optional configuration for worker counts
 * @returns An InstanceClient
 *
 * @example
 * ```typescript
 * // Current usage
 * const client = createInstanceClient(config);
 * try {
 *   const result = await client.upsert(instances, viewRef);
 * } finally {
 *   client.close();
 * }
 * ```
 */
export function createInstanceClient(
  config: PygenClientConfig,
  options?: InstanceClientOptions,
): InstanceClient {
  return new InstanceClient(config, options);
}
