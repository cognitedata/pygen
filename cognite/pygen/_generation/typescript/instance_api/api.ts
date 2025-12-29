/**
 * Instance API for view-specific operations.
 *
 * This module contains the InstanceAPI base class that provides methods for
 * querying instances (nodes/edges) through a specific view.
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
import { getSuccessOrThrow } from "./http_client/types.ts";
import type { Filter } from "./types/filters.ts";
import type { Instance, InstanceRaw } from "./types/instance.ts";
import { InstanceList, parseInstances } from "./types/instance.ts";
import type { Aggregation, DebugParameters, PropertySort, UnitConversion } from "./types/query.ts";
import type { InstanceId, InstanceType, ViewReference } from "./types/references.ts";
import type { AggregateResponse, ListResponse, Page } from "./types/responses.ts";
import { chunker } from "./types/utils.ts";

/** Search operator for combining multiple search terms */
export type SearchOperator = "and" | "or";

/**
 * Generic resource API for CDF Data Modeling view-specific operations.
 *
 * This class provides methods for querying instances (nodes or edges) through
 * a specific view. It supports filtering, sorting, pagination, full-text search,
 * retrieve by ID, and aggregations.
 *
 * The InstanceAPI is designed to be subclassed to create view-specific APIs
 * with proper type hints.
 *
 * @typeParam TInstance - The specific instance type returned by this API
 *
 * @example
 * ```typescript
 * class PersonAPI extends InstanceAPI<Person> {
 *   constructor(config: PygenClientConfig) {
 *     super(
 *       config,
 *       { space: "mySpace", externalId: "Person", version: "v1" },
 *       "node",
 *     );
 *   }
 *
 *   async list(namePrefix?: string): Promise<InstanceList<Person>> {
 *     const filter = namePrefix
 *       ? { prefix: { property: ["mySpace", "Person/v1", "name"], value: namePrefix } }
 *       : undefined;
 *     return this._list({ filter });
 *   }
 * }
 * ```
 */
export class InstanceAPI<TInstance extends Instance> {
  /** API endpoint for list operations */
  protected static readonly LIST_ENDPOINT = "/models/instances/list";

  /** API endpoint for search operations */
  protected static readonly SEARCH_ENDPOINT = "/models/instances/search";

  /** API endpoint for retrieve operations */
  protected static readonly RETRIEVE_ENDPOINT = "/models/instances/byids";

  /** API endpoint for aggregate operations */
  protected static readonly AGGREGATE_ENDPOINT = "/models/instances/aggregate";

  /** Maximum items per list request */
  protected static readonly LIST_LIMIT = 1000;

  /** Maximum items per search request */
  protected static readonly SEARCH_LIMIT = 1000;

  /** Maximum items per retrieve request */
  protected static readonly RETRIEVE_LIMIT = 1000;

  /** Maximum items per aggregate request */
  protected static readonly AGGREGATE_LIMIT = 1000;

  /** Default limit for list operations */
  protected static readonly DEFAULT_LIST_LIMIT = 25;

  /** The HTTP client for making requests */
  protected readonly httpClient: HTTPClient;

  /** The view reference for this API */
  protected readonly viewRef: ViewReference;

  /** The instance type (node or edge) */
  protected readonly instanceType: InstanceType;

  /** Number of concurrent workers for retrieve operations */
  protected readonly retrieveWorkers: number;

  /** The client configuration */
  protected readonly config: PygenClientConfig;

  /**
   * Creates a new InstanceAPI.
   *
   * @param config - Configuration for the HTTP client
   * @param viewRef - Reference to the view for querying instances
   * @param instanceType - The type of instances to query ("node" or "edge")
   * @param retrieveWorkers - Number of concurrent workers for retrieve operations (default: 2)
   */
  constructor(
    config: PygenClientConfig,
    viewRef: ViewReference,
    instanceType: InstanceType,
    retrieveWorkers = 2,
  ) {
    this.config = config;
    this.httpClient = new HTTPClient(config);
    this.viewRef = viewRef;
    this.instanceType = instanceType;
    this.retrieveWorkers = retrieveWorkers;
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
   * Iterates over instances in the view with a single page.
   *
   * This method returns a single page of instances using the advancedListInstance
   * API endpoint. Use the cursor for pagination.
   *
   * @param options - Options for the iterate operation
   * @param options.includeTyping - If true, includes type information for direct relations
   * @param options.targetUnits - Unit conversion configuration for numeric properties with units
   * @param options.debug - Return query debug notices
   * @param options.cursor - Cursor for resuming pagination from a previous point
   * @param options.limit - Maximum number of instances to return (1-1000)
   * @param options.sort - Sort order for the results
   * @param options.filter - Filter to apply to the query
   * @returns Page containing instances and optional cursor for next page
   *
   * @throws Error if limit is out of range
   */
  protected async _iterate(options: {
    includeTyping?: boolean;
    targetUnits?: UnitConversion | readonly UnitConversion[];
    debug?: DebugParameters;
    cursor?: string;
    limit?: number;
    sort?: PropertySort | readonly PropertySort[];
    filter?: Filter;
  } = {}): Promise<Page<InstanceList<TInstance>>> {
    const limit = options.limit ?? InstanceAPI.DEFAULT_LIST_LIMIT;

    if (limit < 1 || limit > InstanceAPI.LIST_LIMIT) {
      throw new Error(`Limit must be between 1 and ${InstanceAPI.LIST_LIMIT}, got ${limit}.`);
    }

    const body = this.buildReadBody({
      viewKey: "sources",
      filter: options.filter,
      sort: options.sort,
      limit,
      cursor: options.cursor,
      includeTyping: options.includeTyping,
      debug: options.debug,
      targetUnits: options.targetUnits,
    });

    const request: RequestMessage = {
      endpointUrl: this.createApiUrl(InstanceAPI.LIST_ENDPOINT),
      method: "POST",
      body,
    };

    const result = await this.httpClient.request(request);
    const success = getSuccessOrThrow(result);
    return this.parsePage(success.body);
  }

  /**
   * Lists instances in the view with automatic pagination.
   *
   * This method collects all pages of results into a single list.
   * For large result sets, consider using _iterate() to process pages individually.
   *
   * @param options - Options for the list operation
   * @param options.includeTyping - If true, includes type information for direct relations
   * @param options.targetUnits - Unit conversion configuration for numeric properties with units
   * @param options.debug - Return query debug notices
   * @param options.limit - Maximum total number of instances to return. Set to undefined for no limit
   * @param options.sort - Sort order for the results
   * @param options.filter - Filter to apply to the query
   * @returns A list of instances matching the query
   *
   * @throws Error if limit is not positive
   */
  protected async _list(options: {
    includeTyping?: boolean;
    targetUnits?: UnitConversion | readonly UnitConversion[];
    debug?: DebugParameters;
    limit?: number;
    sort?: PropertySort | readonly PropertySort[];
    filter?: Filter;
  } = {}): Promise<InstanceList<TInstance>> {
    const limit = options.limit ?? InstanceAPI.DEFAULT_LIST_LIMIT;

    if (limit !== undefined && limit <= 0) {
      throw new Error("Limit must be a positive integer or undefined for no limit.");
    }

    const allItems = new InstanceList<TInstance>([], this.viewRef);
    let nextCursor: string | undefined;
    let total = 0;

    while (true) {
      const pageLimit = limit === undefined
        ? InstanceAPI.LIST_LIMIT
        : Math.min(limit - total, InstanceAPI.LIST_LIMIT);

      const page = await this._iterate({
        includeTyping: options.includeTyping,
        targetUnits: options.targetUnits,
        debug: options.debug,
        cursor: nextCursor,
        limit: pageLimit,
        sort: options.sort,
        filter: options.filter,
      });

      for (const item of page.items) {
        allItems.push(item);
      }
      total += page.items.length;

      if (page.nextCursor === undefined || (limit !== undefined && total >= limit)) {
        break;
      }
      nextCursor = page.nextCursor;
    }

    return allItems;
  }

  /**
   * Searches for instances using full-text search.
   *
   * This method uses the searchInstances API endpoint to perform full-text
   * search on text properties. It can be combined with filters for more precise results.
   *
   * @param options - Options for the search operation
   * @param options.query - The search query string
   * @param options.properties - Properties to search in. If undefined, searches all searchable properties
   * @param options.targetUnits - Unit conversion configuration for numeric properties with units
   * @param options.filter - Additional filter to apply after the search
   * @param options.includeTyping - If true, includes type information for direct relations
   * @param options.sort - Sort order for the results
   * @param options.operator - How to combine search terms ("and" or "or"). Defaults to "or"
   * @param options.limit - Maximum number of instances to return. Defaults to 25. Maximum is 1000
   * @returns A list of instances matching the search query
   *
   * @throws Error if limit is out of range
   */
  protected async _search(options: {
    query?: string;
    properties?: string | readonly string[];
    targetUnits?: UnitConversion | readonly UnitConversion[];
    filter?: Filter;
    includeTyping?: boolean;
    sort?: PropertySort | readonly PropertySort[];
    operator?: SearchOperator;
    limit?: number;
  } = {}): Promise<ListResponse<InstanceList<TInstance>>> {
    const limit = options.limit ?? InstanceAPI.DEFAULT_LIST_LIMIT;

    if (limit < 1 || limit > InstanceAPI.SEARCH_LIMIT) {
      throw new Error(`Limit must be between 1 and ${InstanceAPI.SEARCH_LIMIT}, got ${limit}.`);
    }

    const body = this.buildReadBody({
      viewKey: "view",
      query: options.query,
      properties: options.properties,
      filter: options.filter,
      sort: options.sort,
      limit,
      includeTyping: options.includeTyping,
      targetUnits: options.targetUnits,
      operator: options.operator,
    });

    const request: RequestMessage = {
      endpointUrl: this.createApiUrl(InstanceAPI.SEARCH_ENDPOINT),
      method: "POST",
      body,
    };

    const result = await this.httpClient.request(request);
    const success = getSuccessOrThrow(result);
    return this.parseListResponse(success.body);
  }

  /**
   * Retrieves instances by ID.
   *
   * This method retrieves instances by their external IDs using the byExternalIds
   * API endpoint. It supports both single and batch retrieval.
   *
   * @param id - Instance identifier(s). Can be:
   *   - A string external_id (requires space parameter)
   *   - An InstanceId object
   *   - A tuple of [space, externalId]
   *   - An array of any of the above
   * @param options - Additional options
   * @param options.space - Default space to use when id is a string
   * @param options.includeTyping - If true, includes type information for direct relations
   * @param options.targetUnits - Unit conversion configuration for numeric properties with units
   * @returns For single id: The instance if found, undefined otherwise. For array of ids: A list of found instances
   *
   * @throws Error if space is not provided when using string external_ids
   */
  protected async _retrieve(
    id: string | InstanceId | readonly [string, string],
    options?: {
      space?: string;
      includeTyping?: boolean;
      targetUnits?: UnitConversion | readonly UnitConversion[];
    },
  ): Promise<TInstance | undefined>;
  protected async _retrieve(
    id: readonly (string | InstanceId | readonly [string, string])[],
    options?: {
      space?: string;
      includeTyping?: boolean;
      targetUnits?: UnitConversion | readonly UnitConversion[];
    },
  ): Promise<InstanceList<TInstance>>;
  protected async _retrieve(
    id:
      | string
      | InstanceId
      | readonly [string, string]
      | readonly (string | InstanceId | readonly [string, string])[],
    options: {
      space?: string;
      includeTyping?: boolean;
      targetUnits?: UnitConversion | readonly UnitConversion[];
    } = {},
  ): Promise<TInstance | InstanceList<TInstance> | undefined> {
    // Determine if single or batch
    const isSingle = !Array.isArray(id) ||
      (id.length === 2 && typeof id[0] === "string" && typeof id[1] === "string");

    // Normalize input to array
    const idList: readonly (string | InstanceId | readonly [string, string])[] = isSingle
      ? [id as string | InstanceId | readonly [string, string]]
      : id as readonly (string | InstanceId | readonly [string, string])[];

    if (idList.length === 0) {
      if (isSingle) {
        return undefined;
      }
      return new InstanceList<TInstance>([], this.viewRef);
    }

    // Convert all ids to InstanceId objects
    const instanceIds = idList.map((item) => this.toInstanceId(item, options.space));

    // Retrieve instances in chunks (potentially in parallel)
    const chunks = chunker(instanceIds, InstanceAPI.RETRIEVE_LIMIT);
    let results: HTTPResult[];

    if (chunks.length <= this.retrieveWorkers) {
      // Run all in parallel
      results = await Promise.all(
        chunks.map((chunk) =>
          this.retrieveChunk(chunk, options.includeTyping ?? false, options.targetUnits)
        ),
      );
    } else {
      // Use a worker pool pattern for larger numbers
      results = new Array(chunks.length);
      const taskIterator = chunks.entries();

      const worker = async (): Promise<void> => {
        for (const [index, chunk] of taskIterator) {
          results[index] = await this.retrieveChunk(
            chunk,
            options.includeTyping ?? false,
            options.targetUnits,
          );
        }
      };

      const workers = Array.from(
        { length: Math.min(this.retrieveWorkers, chunks.length) },
        worker,
      );
      await Promise.all(workers);
    }

    // Collect results, gathering both successes and failures
    const allItems = this.collectRetrievedItems(results);

    if (isSingle) {
      return allItems.at(0);
    }
    return allItems;
  }

  /**
   * Collects retrieved items from HTTP results.
   *
   * This method processes HTTP results from retrieve operations, collecting
   * successful items and tracking failures. If any failures occurred, it throws
   * a MultiRequestError containing both the failures and the successfully
   * retrieved items.
   *
   * @param results - List of HTTPResult objects from retrieve operations
   * @returns An InstanceList of successfully retrieved items
   * @throws MultiRequestError if any requests failed (contains successful results in error.result)
   */
  private collectRetrievedItems(results: readonly HTTPResult[]): InstanceList<TInstance> {
    const allItems = new InstanceList<TInstance>([], this.viewRef);
    const failedResponses: FailedResponse[] = [];
    const failedRequests: FailedRequest[] = [];

    for (const result of results) {
      if (result.kind === "success") {
        const response = this.parseListResponse(result.body);
        for (const item of response.items) {
          allItems.push(item);
        }
      } else if (result.kind === "failed_response") {
        failedResponses.push(result);
      } else if (result.kind === "failed_request") {
        failedRequests.push(result);
      }
    }

    if (failedResponses.length > 0 || failedRequests.length > 0) {
      throw new MultiRequestError(failedResponses, failedRequests, allItems);
    }

    return allItems;
  }

  /**
   * Aggregates instances in the view.
   *
   * This method performs aggregations on instances using the CDF aggregate
   * API endpoint. It supports various aggregation types (count, sum, avg, min, max, histogram)
   * and optional grouping.
   *
   * @param aggregate - The aggregation(s) to perform
   * @param options - Additional options
   * @param options.query - Search query for full-text search filtering before aggregation
   * @param options.groupBy - Property or properties to group results by
   * @param options.properties - Properties to search in when using query
   * @param options.operator - How to combine search terms ("and" or "or")
   * @param options.targetUnits - Unit conversion configuration for numeric properties with units
   * @param options.includeTyping - If true, includes type information for direct relations
   * @param options.filter - Filter to apply before aggregation
   * @param options.limit - Maximum number of groups to return. Defaults to 10000
   * @returns AggregateResponse containing the aggregated values
   */
  protected async _aggregate(
    aggregate: Aggregation | readonly Aggregation[],
    options: {
      query?: string;
      groupBy?: string | readonly string[];
      properties?: string | readonly string[];
      operator?: SearchOperator;
      targetUnits?: UnitConversion | readonly UnitConversion[];
      includeTyping?: boolean;
      filter?: Filter;
      limit?: number;
    } = {},
  ): Promise<AggregateResponse> {
    const body = this.buildReadBody({
      viewKey: "view",
      query: options.query,
      targetUnits: options.targetUnits,
      includeTyping: options.includeTyping,
      properties: options.properties,
      filter: options.filter,
      limit: options.limit ?? InstanceAPI.AGGREGATE_LIMIT,
      operator: options.operator,
      aggregates: aggregate,
      groupBy: options.groupBy,
    });

    const request: RequestMessage = {
      endpointUrl: this.createApiUrl(InstanceAPI.AGGREGATE_ENDPOINT),
      method: "POST",
      body,
    };

    const result = await this.httpClient.request(request);
    const success = getSuccessOrThrow(result);
    return JSON.parse(success.body) as AggregateResponse;
  }

  /**
   * Retrieves a chunk of instances by their IDs.
   *
   * @param instanceIds - List of InstanceId objects to retrieve
   * @param includeTyping - Whether to include typing information
   * @param targetUnits - Unit conversion configuration
   * @returns HTTPResult from the API call
   */
  private async retrieveChunk(
    instanceIds: readonly InstanceId[],
    includeTyping: boolean,
    targetUnits?: UnitConversion | readonly UnitConversion[],
  ): Promise<HTTPResult> {
    const items = instanceIds.map((item) => ({
      instanceType: this.instanceType,
      space: item.space,
      externalId: item.externalId,
    }));

    const body = this.buildReadBody({
      viewKey: "sources",
      includeTyping,
      targetUnits,
    });
    (body as Record<string, unknown>).items = items;

    const request: RequestMessage = {
      endpointUrl: this.createApiUrl(InstanceAPI.RETRIEVE_ENDPOINT),
      method: "POST",
      body,
    };

    return this.httpClient.request(request);
  }

  /**
   * Converts various input types to InstanceId.
   *
   * @param item - The item to convert
   * @param defaultSpace - Default space to use if item is a string
   * @returns InstanceId object
   *
   * @throws Error if space cannot be determined
   */
  private toInstanceId(
    item: string | InstanceId | readonly [string, string],
    defaultSpace?: string,
  ): InstanceId {
    // Check if it's a tuple [space, externalId]
    if (Array.isArray(item) && item.length === 2) {
      return {
        instanceType: this.instanceType,
        space: item[0],
        externalId: item[1],
      };
    }

    // Check if it's an InstanceId object
    if (typeof item === "object" && item !== null && !Array.isArray(item) && "space" in item && "externalId" in item) {
      return {
        instanceType: this.instanceType,
        space: item.space,
        externalId: item.externalId,
      };
    }

    // It's a string external_id
    if (typeof item === "string") {
      if (defaultSpace === undefined) {
        throw new Error("space parameter is required when retrieving by external_id string");
      }
      return {
        instanceType: this.instanceType,
        space: defaultSpace,
        externalId: item,
      };
    }

    throw new TypeError(`Unsupported type for id: ${typeof item}`);
  }

  /**
   * Builds the request body for read operations.
   */
  private buildReadBody(options: {
    viewKey: "sources" | "view";
    limit?: number;
    query?: string;
    properties?: string | readonly string[];
    filter?: Filter;
    sort?: PropertySort | readonly PropertySort[];
    cursor?: string;
    includeTyping?: boolean;
    debug?: DebugParameters;
    targetUnits?: UnitConversion | readonly UnitConversion[];
    operator?: SearchOperator;
    aggregates?: Aggregation | readonly Aggregation[];
    groupBy?: string | readonly string[];
  }): Record<string, unknown> {
    const body: Record<string, unknown> = {
      instanceType: this.instanceType,
    };

    if (options.includeTyping !== undefined) {
      body.includeTyping = options.includeTyping;
    }

    if (options.limit !== undefined) {
      body.limit = options.limit;
    }

    if (options.viewKey === "view") {
      body.view = {
        type: "view",
        space: this.viewRef.space,
        externalId: this.viewRef.externalId,
        version: this.viewRef.version,
      };
    } else {
      const source: Record<string, unknown> = {
        source: {
          type: "view",
          space: this.viewRef.space,
          externalId: this.viewRef.externalId,
          version: this.viewRef.version,
        },
      };

      if (options.targetUnits !== undefined) {
        source.targetUnits = this.serializeArray(options.targetUnits);
      }

      body.sources = [source];
    }

    if (options.query !== undefined) {
      body.query = options.query;
    }

    if (options.properties !== undefined) {
      body.properties = typeof options.properties === "string"
        ? [options.properties]
        : [...options.properties];
    }

    if (options.filter !== undefined) {
      body.filter = options.filter;
    }

    if (options.sort !== undefined) {
      body.sort = this.serializeArray(options.sort);
    }

    if (options.cursor !== undefined) {
      body.cursor = options.cursor;
    }

    if (options.debug !== undefined) {
      body.debug = options.debug;
    }

    if (options.targetUnits !== undefined && options.viewKey === "view") {
      body.targetUnits = this.serializeArray(options.targetUnits);
    }

    if (options.operator !== undefined) {
      body.operator = options.operator.toUpperCase();
    }

    if (options.aggregates !== undefined) {
      const aggregatesList = Array.isArray(options.aggregates)
        ? options.aggregates
        : [options.aggregates];
      body.aggregates = aggregatesList.map((agg) => ({
        [agg.aggregate]: agg,
      }));
    }

    if (options.groupBy !== undefined) {
      body.groupBy = typeof options.groupBy === "string" ? [options.groupBy] : [...options.groupBy];
    }

    return body;
  }

  /**
   * Serializes a value or array to an array.
   */
  private serializeArray<T>(value: T | readonly T[]): T[] {
    if (Array.isArray(value)) {
      return [...value] as T[];
    }
    return [value] as T[];
  }

  /**
   * Parses a Page response from the API.
   */
  private parsePage(body: string): Page<InstanceList<TInstance>> {
    const parsed = JSON.parse(body) as {
      items: InstanceRaw[];
      nextCursor?: string;
      typing?: Record<string, unknown>;
      debug?: Record<string, unknown>;
    };

    const items = parseInstances<TInstance>(parsed.items, this.viewRef);
    const instanceList = new InstanceList([...items], this.viewRef);

    return {
      items: instanceList,
      nextCursor: parsed.nextCursor,
      typing: parsed.typing,
      debug: parsed.debug,
    };
  }

  /**
   * Parses a ListResponse from the API.
   */
  private parseListResponse(body: string): ListResponse<InstanceList<TInstance>> {
    const parsed = JSON.parse(body) as {
      items: InstanceRaw[];
      typing?: Record<string, unknown>;
    };

    const items = parseInstances<TInstance>(parsed.items, this.viewRef);
    const instanceList = new InstanceList([...items], this.viewRef);

    return {
      items: instanceList,
      typing: parsed.typing,
    };
  }
}
