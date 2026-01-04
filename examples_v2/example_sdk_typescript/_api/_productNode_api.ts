/**
 * API class for ProductNode instances.
 *
 * @packageDocumentation
 */

import { InstanceAPI } from "../instance_api/api.ts";
import type { PygenClientConfig } from "../instance_api/auth/index.ts";
import type { ViewReference, InstanceId } from "../instance_api/types/references.ts";
import type { Aggregation, PropertySort, SortDirection } from "../instance_api/types/query.ts";
import type { AggregateResponse, Page } from "../instance_api/types/responses.ts";
import { InstanceList } from "../instance_api/types/instance.ts";

import {
  PRODUCT_NODE_VIEW,
  ProductNode,
  ProductNodeFilter,
  ProductNodeList,
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
 * API for ProductNode instances with type-safe filter methods.
 */
export class ProductNodeApi extends InstanceAPI<ProductNode> {
  /**
   * Creates a new ProductNodeApi.
   *
   * @param config - Client configuration for API access
   */
  constructor(config: PygenClientConfig) {
    super(config, PRODUCT_NODE_VIEW, "node");
  }

  /**
   * Builds a ProductNodeFilter from the given options.
   *
   * @param options - Filter options
   * @returns A configured ProductNodeFilter
   */
  private _buildFilter(options: {
    name?: string | readonly string[];
    namePrefix?: string;
    description?: string | readonly string[];
    descriptionPrefix?: string;
    minPrice?: number;
    maxPrice?: number;
    minQuantity?: number;
    maxQuantity?: number;
    active?: boolean;
    minCreatedDate?: Date | string;
    maxCreatedDate?: Date | string;
    minUpdatedTimestamp?: Date | string;
    maxUpdatedTimestamp?: Date | string;
    category?: string | InstanceId | readonly [string, string] | readonly (string | InstanceId | readonly [string, string])[];
    externalIdPrefix?: string;
    space?: string | readonly string[];
  }): ProductNodeFilter {
    const filter = new ProductNodeFilter("and");
    filter.name.equalsOrIn(options.name ?? null);
    filter.name.prefix(options.namePrefix ?? null);
    filter.description.equalsOrIn(options.description ?? null);
    filter.description.prefix(options.descriptionPrefix ?? null);
    filter.price.greaterThanOrEquals(options.minPrice ?? null);
    filter.price.lessThanOrEquals(options.maxPrice ?? null);
    filter.quantity.greaterThanOrEquals(options.minQuantity ?? null);
    filter.quantity.lessThanOrEquals(options.maxQuantity ?? null);
    filter.active.equals(options.active ?? null);
    filter.createdDate.greaterThanOrEquals(options.minCreatedDate ?? null);
    filter.createdDate.lessThanOrEquals(options.maxCreatedDate ?? null);
    filter.updatedTimestamp.greaterThanOrEquals(options.minUpdatedTimestamp ?? null);
    filter.updatedTimestamp.lessThanOrEquals(options.maxUpdatedTimestamp ?? null);
    filter.category.equalsOrIn(options.category ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    return filter;
  }

  /**
   * Retrieve ProductNode instances by ID.
   *
   * @param id - Instance identifier. Can be a string, InstanceId, tuple, or array
   * @param options - Additional options
   * @returns For single id: The ProductNode if found, undefined otherwise.
   *          For array of ids: A ProductNodeList of found instances.
   */
  async retrieve(
    id: string | InstanceId | readonly [string, string],
    options?: { space?: string },
  ): Promise<ProductNode | undefined>;
  async retrieve(
    id: readonly (string | InstanceId | readonly [string, string])[],
    options?: { space?: string },
  ): Promise<ProductNodeList>;
  async retrieve(
    id:
      | string
      | InstanceId
      | readonly [string, string]
      | readonly (string | InstanceId | readonly [string, string])[],
    options: { space?: string } = {},
  ): Promise<ProductNode | ProductNodeList | undefined> {
    const isSingle = !Array.isArray(id) ||
      (id.length === 2 && typeof id[0] === "string" && typeof id[1] === "string");

    if (isSingle) {
      return await this._retrieve(
        id as string | InstanceId | readonly [string, string],
        options,
      ) as ProductNode | undefined;
    }
    const result = await this._retrieve(
      id as readonly (string | InstanceId | readonly [string, string])[],
      options,
    );
    return new ProductNodeList([...(result as InstanceList<ProductNode>)]);
  }

  /**
   * Iterate over instances with pagination.
   *
   * @param options - Filter and pagination options
   * @returns A Page containing items and optional next cursor.
   */
  async iterate(options: {
    name?: string | readonly string[];
    namePrefix?: string;
    description?: string | readonly string[];
    descriptionPrefix?: string;
    minPrice?: number;
    maxPrice?: number;
    minQuantity?: number;
    maxQuantity?: number;
    active?: boolean;
    minCreatedDate?: Date | string;
    maxCreatedDate?: Date | string;
    minUpdatedTimestamp?: Date | string;
    maxUpdatedTimestamp?: Date | string;
    category?: string | InstanceId | readonly [string, string] | readonly (string | InstanceId | readonly [string, string])[];
    externalIdPrefix?: string;
    space?: string | readonly string[];
    cursor?: string;
    limit?: number;
  } = {}): Promise<Page<ProductNodeList>> {
    const filter = this._buildFilter(options);
    const page = await this._iterate({
      cursor: options.cursor,
      limit: options.limit,
      filter: filter.asFilter(),
    });

    return { ...page, items: new ProductNodeList([...page.items]) };
  }

  /**
   * Search instances using full-text search.
   *
   * @param options - Search and filter options
   * @returns A ProductNodeList of matching instances.
   */
  async search(options: {
    query?: string;
    properties?: string | readonly string[];
    name?: string | readonly string[];
    namePrefix?: string;
    description?: string | readonly string[];
    descriptionPrefix?: string;
    minPrice?: number;
    maxPrice?: number;
    minQuantity?: number;
    maxQuantity?: number;
    active?: boolean;
    minCreatedDate?: Date | string;
    maxCreatedDate?: Date | string;
    minUpdatedTimestamp?: Date | string;
    maxUpdatedTimestamp?: Date | string;
    category?: string | InstanceId | readonly [string, string] | readonly (string | InstanceId | readonly [string, string])[];
    externalIdPrefix?: string;
    space?: string | readonly string[];
    limit?: number;
  } = {}): Promise<ProductNodeList> {
    const filter = this._buildFilter(options);

    const result = await this._search({
      query: options.query,
      properties: options.properties,
      limit: options.limit,
      filter: filter.asFilter(),
    });

    return new ProductNodeList([...result.items]);
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
    name?: string | readonly string[];
    namePrefix?: string;
    description?: string | readonly string[];
    descriptionPrefix?: string;
    minPrice?: number;
    maxPrice?: number;
    minQuantity?: number;
    maxQuantity?: number;
    active?: boolean;
    minCreatedDate?: Date | string;
    maxCreatedDate?: Date | string;
    minUpdatedTimestamp?: Date | string;
    maxUpdatedTimestamp?: Date | string;
    category?: string | InstanceId | readonly [string, string] | readonly (string | InstanceId | readonly [string, string])[];
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
   * @returns A ProductNodeList of matching instances.
   */
  async list(options: {
    name?: string | readonly string[];
    namePrefix?: string;
    description?: string | readonly string[];
    descriptionPrefix?: string;
    minPrice?: number;
    maxPrice?: number;
    minQuantity?: number;
    maxQuantity?: number;
    active?: boolean;
    minCreatedDate?: Date | string;
    maxCreatedDate?: Date | string;
    minUpdatedTimestamp?: Date | string;
    maxUpdatedTimestamp?: Date | string;
    category?: string | InstanceId | readonly [string, string] | readonly (string | InstanceId | readonly [string, string])[];
    externalIdPrefix?: string;
    space?: string | readonly string[];
    sortBy?: string;
    sortDirection?: SortDirection;
    limit?: number;
  } = {}): Promise<ProductNodeList> {
    const filter = this._buildFilter(options);
    const sort: PropertySort | undefined = options.sortBy !== undefined
      ? {
        property: createPropertyRef(PRODUCT_NODE_VIEW, options.sortBy),
        direction: options.sortDirection,
      }
      : undefined;

    const result = await this._list({
      limit: options.limit,
      filter: filter.asFilter(),
      sort,
    });

    return new ProductNodeList([...result]);
  }
}