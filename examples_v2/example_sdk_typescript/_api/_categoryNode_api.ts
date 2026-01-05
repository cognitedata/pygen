/**
 * API class for CategoryNode instances.
 *
 * @packageDocumentation
 */

import { InstanceAPI } from "../instance_api/api.ts";
import type { PygenClientConfig } from "../instance_api/auth/index.ts";
import type { ViewReference } from "../instance_api/types/references.ts";
import type { Aggregation, PropertySort, SortDirection } from "../instance_api/types/query.ts";
import type { AggregateResponse, Page } from "../instance_api/types/responses.ts";
import { InstanceList } from "../instance_api/types/instance.ts";

import {
  CATEGORY_NODE_VIEW,
  CategoryNode,
  CategoryNodeFilter,
  CategoryNodeList,
} from "../data_classes/index.ts";

/**
 * Creates a property reference for sorting.
 *
 * @param viewRef - The view reference
 * @param propertyName - The property name
 * @returns A property path array
 */
function createPropertyRef(
  viewRef: ViewReference,
  propertyName: string,
): [string, string, string] {
  return [viewRef.space, `${viewRef.externalId}/${viewRef.version}`, propertyName];
}

/**
 * API for CategoryNode instances with type-safe filter methods.
 */
export class CategoryNodeApi extends InstanceAPI<CategoryNode> {
  /**
   * Creates a new CategoryNodeApi.
   *
   * @param config - Client configuration for API access
   */
  constructor(config: PygenClientConfig) {
    super(config, CATEGORY_NODE_VIEW, "node");
  }

  /**
   * Builds a CategoryNodeFilter from the given options.
   *
   * @param options - Filter options
   * @returns A configured CategoryNodeFilter
   */
  private _buildFilter(options: {
    categoryName?: string | readonly string[];
    categoryNamePrefix?: string;
    externalIdPrefix?: string;
    space?: string | readonly string[];
  }): CategoryNodeFilter {
    const filter = new CategoryNodeFilter("and");
    filter.categoryName.equalsOrIn(options.categoryName ?? null);
    filter.categoryName.prefix(options.categoryNamePrefix ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    return filter;
  }

  /**
   * Retrieve CategoryNode instances by ID.
   *
   * @param id - Instance identifier. Can be a string, InstanceId, tuple, or array
   * @param options - Additional options
   * @returns For single id: The CategoryNode if found, undefined otherwise.
   *          For array of ids: A CategoryNodeList of found instances.
   */
  async retrieve(
    id: string | InstanceId | readonly [string, string],
    options?: { space?: string },
  ): Promise<CategoryNode | undefined>;
  async retrieve(
    id: readonly (string | InstanceId | readonly [string, string])[],
    options?: { space?: string },
  ): Promise<CategoryNodeList>;
  async retrieve(
    id:
      | string
      | InstanceId
      | readonly [string, string]
      | readonly (string | InstanceId | readonly [string, string])[],
    options: { space?: string } = {},
  ): Promise<CategoryNode | CategoryNodeList | undefined> {
    const isSingle = !Array.isArray(id) ||
      (id.length === 2 && typeof id[0] === "string" && typeof id[1] === "string");

    if (isSingle) {
      return await this._retrieve(
        id as string | InstanceId | readonly [string, string],
        options,
      ) as CategoryNode | undefined;
    }
    const result = await this._retrieve(
      id as readonly (string | InstanceId | readonly [string, string])[],
      options,
    );
    return new CategoryNodeList([...(result as InstanceList<CategoryNode>)]);
  }

  /**
   * Iterate over instances with pagination.
   *
   * @param options - Filter and pagination options
   * @returns A Page containing items and optional next cursor.
   */
  async iterate(options: {
    categoryName?: string | readonly string[];
    categoryNamePrefix?: string;
    externalIdPrefix?: string;
    space?: string | readonly string[];
    cursor?: string;
    limit?: number;
  } = {}): Promise<Page<CategoryNodeList>> {
    const filter = this._buildFilter(options);
    const page = await this._iterate({
      cursor: options.cursor,
      limit: options.limit,
      filter: filter.asFilter(),
    });

    return { ...page, items: new CategoryNodeList([...page.items]) };
  }

  /**
   * Search instances using full-text search.
   *
   * @param options - Search and filter options
   * @returns A CategoryNodeList of matching instances.
   */
  async search(options: {
    query?: string;
    properties?: string | readonly string[];
    categoryName?: string | readonly string[];
    categoryNamePrefix?: string;
    externalIdPrefix?: string;
    space?: string | readonly string[];
    limit?: number;
  } = {}): Promise<CategoryNodeList> {
    const filter = this._buildFilter(options);

    const result = await this._search({
      query: options.query,
      properties: options.properties,
      limit: options.limit,
      filter: filter.asFilter(),
    });

    return new CategoryNodeList([...result.items]);
  }

  /**
   * Aggregate instances.
   *
   * @param aggregate - Aggregation(s) to perform.
   * @param options - Filter and grouping options.
   * @returns AggregateResponse with aggregated values.
   */
  async aggregate(
    aggregate: Aggregation | readonly Aggregation[],
    options: {
      groupBy?: string | readonly string[];
    categoryName?: string | readonly string[];
    categoryNamePrefix?: string;
    externalIdPrefix?: string;
    space?: string | readonly string[];
    } = {},
  ): Promise<AggregateResponse> {
    const filter = this._buildFilter(options);

    return this._aggregate(aggregate, {
      groupBy: options.groupBy,
      filter: filter.asFilter(),
    });
  }

  /**
   * List instances with type-safe filtering.
   *
   * @param options - Filter, sort, and pagination options.
   * @returns A CategoryNodeList of matching instances.
   */
  async list(options: {
    categoryName?: string | readonly string[];
    categoryNamePrefix?: string;
    externalIdPrefix?: string;
    space?: string | readonly string[];
    sortBy?: string;
    sortDirection?: SortDirection;
    limit?: number;
  } = {}): Promise<CategoryNodeList> {
    const filter = this._buildFilter(options);
    const sort: PropertySort | undefined = options.sortBy !== undefined
      ? {
        property: createPropertyRef(CATEGORY_NODE_VIEW, options.sortBy),
        direction: options.sortDirection,
      }
      : undefined;

    const result = await this._list({
      limit: options.limit,
      filter: filter.asFilter(),
      sort,
    });

    return new CategoryNodeList([...result]);
  }
}