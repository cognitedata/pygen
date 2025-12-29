/**
 * API classes for the example SDK.
 *
 * This module contains view-specific API classes that extend InstanceAPI with
 * type-safe methods using unpacked parameters for common filter operations.
 *
 * @packageDocumentation
 */

import { InstanceAPI } from "../instance_api/api.ts";
import type { PygenClientConfig } from "../instance_api/auth/index.ts";
import type { InstanceId, ViewReference } from "../instance_api/types/references.ts";
import type { Aggregation, PropertySort, SortDirection } from "../instance_api/types/query.ts";
import type { AggregateResponse, Page } from "../instance_api/types/responses.ts";
import { InstanceList } from "../instance_api/types/instance.ts";

import {
  CategoryNode,
  CategoryNodeFilter,
  CategoryNodeList,
  CATEGORY_NODE_VIEW,
  ProductNode,
  ProductNodeFilter,
  ProductNodeList,
  PRODUCT_NODE_VIEW,
  RelatesTo,
  RelatesToFilter,
  RelatesToList,
  RELATES_TO_VIEW,
} from "./dataClasses.ts";

// ============================================================================
// Helper Functions
// ============================================================================

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

// ============================================================================
// ProductNodeAPI
// ============================================================================

/**
 * API for ProductNode instances with type-safe filter methods.
 *
 * Provides methods for querying ProductNode instances with unpacked filter parameters.
 *
 * @example
 * ```typescript
 * const api = new ProductNodeAPI(config);
 *
 * // List products with filters
 * const products = await api.list({
 *   minPrice: 10,
 *   maxPrice: 100,
 *   active: true,
 * });
 *
 * // Search products
 * const results = await api.search({
 *   query: "laptop",
 *   limit: 10,
 * });
 * ```
 */
export class ProductNodeAPI extends InstanceAPI<ProductNode> {
  /**
   * Creates a new ProductNodeAPI.
   *
   * @param config - Client configuration for API access
   */
  constructor(config: PygenClientConfig) {
    super(config, PRODUCT_NODE_VIEW, "node");
  }

  /**
   * Retrieve ProductNode instances by ID.
   *
   * @param id - Instance identifier. Can be a string, InstanceId, tuple, or array
   * @param options - Additional options
   * @param options.space - Default space to use when id is a string
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
    // Determine if single or batch
    const isSingle = !Array.isArray(id) ||
      (id.length === 2 && typeof id[0] === "string" && typeof id[1] === "string");

    if (isSingle) {
      const result = await this._retrieve(id as string | InstanceId | readonly [string, string], options);
      return result as ProductNode | undefined;
    } else {
      const result = await this._retrieve(id as readonly (string | InstanceId | readonly [string, string])[], options);
      return new ProductNodeList([...(result as InstanceList<ProductNode>)]);
    }
  }

  /**
   * Iterate over ProductNode instances with pagination.
   *
   * @param options - Filter and pagination options
   * @returns A Page containing items and optional next cursor.
   */
  async iterate(options: {
    name?: string | readonly string[];
    namePrefix?: string;
    minPrice?: number;
    maxPrice?: number;
    minQuantity?: number;
    maxQuantity?: number;
    active?: boolean;
    minCreatedDate?: Date | string;
    maxCreatedDate?: Date | string;
    category?: string | InstanceId | readonly [string, string] | readonly (string | InstanceId | readonly [string, string])[];
    externalIdPrefix?: string;
    space?: string | readonly string[];
    cursor?: string;
    limit?: number;
  } = {}): Promise<Page<ProductNodeList>> {
    const filter = new ProductNodeFilter("and");

    filter.name.equalsOrIn(options.name ?? null);
    filter.name.prefix(options.namePrefix ?? null);
    filter.price.greaterThanOrEquals(options.minPrice ?? null).lessThanOrEquals(options.maxPrice ?? null);
    filter.quantity.greaterThanOrEquals(options.minQuantity ?? null).lessThanOrEquals(options.maxQuantity ?? null);
    filter.active.equals(options.active ?? null);
    filter.createdDate.greaterThanOrEquals(options.minCreatedDate ?? null).lessThanOrEquals(options.maxCreatedDate ?? null);
    filter.category.equalsOrIn(options.category ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    const page = await this._iterate({
      cursor: options.cursor,
      limit: options.limit,
      filter: filter.asFilter(),
    });

    return {
      items: new ProductNodeList([...page.items]),
      nextCursor: page.nextCursor,
      typing: page.typing,
      debug: page.debug,
    };
  }

  /**
   * Search ProductNode instances using full-text search.
   *
   * @param options - Search and filter options
   * @returns A ProductNodeList of matching instances.
   */
  async search(options: {
    query?: string;
    properties?: string | readonly string[];
    name?: string | readonly string[];
    namePrefix?: string;
    minPrice?: number;
    maxPrice?: number;
    minQuantity?: number;
    maxQuantity?: number;
    active?: boolean;
    minCreatedDate?: Date | string;
    maxCreatedDate?: Date | string;
    category?: string | InstanceId | readonly [string, string] | readonly (string | InstanceId | readonly [string, string])[];
    externalIdPrefix?: string;
    space?: string | readonly string[];
    limit?: number;
  } = {}): Promise<ProductNodeList> {
    const filter = new ProductNodeFilter("and");

    filter.name.equalsOrIn(options.name ?? null);
    filter.name.prefix(options.namePrefix ?? null);
    filter.price.greaterThanOrEquals(options.minPrice ?? null).lessThanOrEquals(options.maxPrice ?? null);
    filter.quantity.greaterThanOrEquals(options.minQuantity ?? null).lessThanOrEquals(options.maxQuantity ?? null);
    filter.active.equals(options.active ?? null);
    filter.createdDate.greaterThanOrEquals(options.minCreatedDate ?? null).lessThanOrEquals(options.maxCreatedDate ?? null);
    filter.category.equalsOrIn(options.category ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    const result = await this._search({
      query: options.query,
      properties: options.properties,
      limit: options.limit,
      filter: filter.asFilter(),
    });

    return new ProductNodeList([...result.items]);
  }

  /**
   * Aggregate ProductNode instances.
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
      minPrice?: number;
      maxPrice?: number;
      minQuantity?: number;
      maxQuantity?: number;
      active?: boolean;
      minCreatedDate?: Date | string;
      maxCreatedDate?: Date | string;
      category?: string | InstanceId | readonly [string, string] | readonly (string | InstanceId | readonly [string, string])[];
      externalIdPrefix?: string;
      space?: string | readonly string[];
    } = {},
  ): Promise<AggregateResponse> {
    const filter = new ProductNodeFilter("and");

    filter.name.equalsOrIn(options.name ?? null);
    filter.name.prefix(options.namePrefix ?? null);
    filter.price.greaterThanOrEquals(options.minPrice ?? null).lessThanOrEquals(options.maxPrice ?? null);
    filter.quantity.greaterThanOrEquals(options.minQuantity ?? null).lessThanOrEquals(options.maxQuantity ?? null);
    filter.active.equals(options.active ?? null);
    filter.createdDate.greaterThanOrEquals(options.minCreatedDate ?? null).lessThanOrEquals(options.maxCreatedDate ?? null);
    filter.category.equalsOrIn(options.category ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    return this._aggregate(aggregate, {
      groupBy: options.groupBy,
      filter: filter.asFilter(),
    });
  }

  /**
   * List ProductNode instances with type-safe filtering.
   *
   * @param options - Filter, sort, and pagination options.
   * @returns A ProductNodeList of matching instances.
   */
  async list(options: {
    name?: string | readonly string[];
    namePrefix?: string;
    minPrice?: number;
    maxPrice?: number;
    minQuantity?: number;
    maxQuantity?: number;
    active?: boolean;
    minCreatedDate?: Date | string;
    maxCreatedDate?: Date | string;
    category?: string | InstanceId | readonly [string, string] | readonly (string | InstanceId | readonly [string, string])[];
    externalIdPrefix?: string;
    space?: string | readonly string[];
    sortBy?: string;
    sortDirection?: SortDirection;
    limit?: number;
  } = {}): Promise<ProductNodeList> {
    const filter = new ProductNodeFilter("and");

    filter.name.equalsOrIn(options.name ?? null);
    filter.name.prefix(options.namePrefix ?? null);
    filter.price.greaterThanOrEquals(options.minPrice ?? null).lessThanOrEquals(options.maxPrice ?? null);
    filter.quantity.greaterThanOrEquals(options.minQuantity ?? null).lessThanOrEquals(options.maxQuantity ?? null);
    filter.active.equals(options.active ?? null);
    filter.createdDate.greaterThanOrEquals(options.minCreatedDate ?? null).lessThanOrEquals(options.maxCreatedDate ?? null);
    filter.category.equalsOrIn(options.category ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    let sort: PropertySort | undefined;
    if (options.sortBy !== undefined) {
      sort = {
        property: createPropertyRef(PRODUCT_NODE_VIEW, options.sortBy),
        direction: options.sortDirection,
      };
    }

    const result = await this._list({
      limit: options.limit,
      filter: filter.asFilter(),
      sort,
    });

    return new ProductNodeList([...result]);
  }
}

// ============================================================================
// CategoryNodeAPI
// ============================================================================

/**
 * API for CategoryNode instances with type-safe filter methods.
 *
 * @example
 * ```typescript
 * const api = new CategoryNodeAPI(config);
 *
 * // List categories
 * const categories = await api.list({
 *   categoryNamePrefix: "Elec",
 * });
 * ```
 */
export class CategoryNodeAPI extends InstanceAPI<CategoryNode> {
  /**
   * Creates a new CategoryNodeAPI.
   *
   * @param config - Client configuration for API access
   */
  constructor(config: PygenClientConfig) {
    super(config, CATEGORY_NODE_VIEW, "node");
  }

  /**
   * Retrieve CategoryNode instances by ID.
   *
   * @param id - Instance identifier
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
    // Determine if single or batch
    const isSingle = !Array.isArray(id) ||
      (id.length === 2 && typeof id[0] === "string" && typeof id[1] === "string");

    if (isSingle) {
      const result = await this._retrieve(id as string | InstanceId | readonly [string, string], options);
      return result as CategoryNode | undefined;
    } else {
      const result = await this._retrieve(id as readonly (string | InstanceId | readonly [string, string])[], options);
      return new CategoryNodeList([...(result as InstanceList<CategoryNode>)]);
    }
  }

  /**
   * Iterate over CategoryNode instances with pagination.
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
    const filter = new CategoryNodeFilter("and");

    filter.categoryName.equalsOrIn(options.categoryName ?? null);
    filter.categoryName.prefix(options.categoryNamePrefix ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    const page = await this._iterate({
      cursor: options.cursor,
      limit: options.limit,
      filter: filter.asFilter(),
    });

    return {
      items: new CategoryNodeList([...page.items]),
      nextCursor: page.nextCursor,
      typing: page.typing,
      debug: page.debug,
    };
  }

  /**
   * Search CategoryNode instances using full-text search.
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
    const filter = new CategoryNodeFilter("and");

    filter.categoryName.equalsOrIn(options.categoryName ?? null);
    filter.categoryName.prefix(options.categoryNamePrefix ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    const result = await this._search({
      query: options.query,
      properties: options.properties,
      limit: options.limit,
      filter: filter.asFilter(),
    });

    return new CategoryNodeList([...result.items]);
  }

  /**
   * Aggregate CategoryNode instances.
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
    const filter = new CategoryNodeFilter("and");

    filter.categoryName.equalsOrIn(options.categoryName ?? null);
    filter.categoryName.prefix(options.categoryNamePrefix ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    return this._aggregate(aggregate, {
      groupBy: options.groupBy,
      filter: filter.asFilter(),
    });
  }

  /**
   * List CategoryNode instances with type-safe filtering.
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
    const filter = new CategoryNodeFilter("and");

    filter.categoryName.equalsOrIn(options.categoryName ?? null);
    filter.categoryName.prefix(options.categoryNamePrefix ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    let sort: PropertySort | undefined;
    if (options.sortBy !== undefined) {
      sort = {
        property: createPropertyRef(CATEGORY_NODE_VIEW, options.sortBy),
        direction: options.sortDirection,
      };
    }

    const result = await this._list({
      limit: options.limit,
      filter: filter.asFilter(),
      sort,
    });

    return new CategoryNodeList([...result]);
  }
}

// ============================================================================
// RelatesToAPI
// ============================================================================

/**
 * API for RelatesTo edge instances with type-safe filter methods.
 *
 * @example
 * ```typescript
 * const api = new RelatesToAPI(config);
 *
 * // List relations with filters
 * const relations = await api.list({
 *   relationType: "similar",
 *   minStrength: 0.5,
 * });
 * ```
 */
export class RelatesToAPI extends InstanceAPI<RelatesTo> {
  /**
   * Creates a new RelatesToAPI.
   *
   * @param config - Client configuration for API access
   */
  constructor(config: PygenClientConfig) {
    super(config, RELATES_TO_VIEW, "edge");
  }

  /**
   * Retrieve RelatesTo edge instances by ID.
   *
   * @param id - Instance identifier
   * @param options - Additional options
   * @returns For single id: The RelatesTo if found, undefined otherwise.
   *          For array of ids: A RelatesToList of found instances.
   */
  async retrieve(
    id: string | InstanceId | readonly [string, string],
    options?: { space?: string },
  ): Promise<RelatesTo | undefined>;
  async retrieve(
    id: readonly (string | InstanceId | readonly [string, string])[],
    options?: { space?: string },
  ): Promise<RelatesToList>;
  async retrieve(
    id:
      | string
      | InstanceId
      | readonly [string, string]
      | readonly (string | InstanceId | readonly [string, string])[],
    options: { space?: string } = {},
  ): Promise<RelatesTo | RelatesToList | undefined> {
    // Determine if single or batch
    const isSingle = !Array.isArray(id) ||
      (id.length === 2 && typeof id[0] === "string" && typeof id[1] === "string");

    if (isSingle) {
      const result = await this._retrieve(id as string | InstanceId | readonly [string, string], options);
      return result as RelatesTo | undefined;
    } else {
      const result = await this._retrieve(id as readonly (string | InstanceId | readonly [string, string])[], options);
      return new RelatesToList([...(result as InstanceList<RelatesTo>)]);
    }
  }

  /**
   * Iterate over RelatesTo edge instances with pagination.
   *
   * @param options - Filter and pagination options
   * @returns A Page containing items and optional next cursor.
   */
  async iterate(options: {
    relationType?: string | readonly string[];
    minStrength?: number;
    maxStrength?: number;
    minCreatedAt?: Date | string;
    maxCreatedAt?: Date | string;
    externalIdPrefix?: string;
    space?: string | readonly string[];
    cursor?: string;
    limit?: number;
  } = {}): Promise<Page<RelatesToList>> {
    const filter = new RelatesToFilter("and");

    filter.relationType.equalsOrIn(options.relationType ?? null);
    filter.strength.greaterThanOrEquals(options.minStrength ?? null).lessThanOrEquals(options.maxStrength ?? null);
    filter.createdAt.greaterThanOrEquals(options.minCreatedAt ?? null).lessThanOrEquals(options.maxCreatedAt ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    const page = await this._iterate({
      cursor: options.cursor,
      limit: options.limit,
      filter: filter.asFilter(),
    });

    return {
      items: new RelatesToList([...page.items]),
      nextCursor: page.nextCursor,
      typing: page.typing,
      debug: page.debug,
    };
  }

  /**
   * Search RelatesTo edge instances using full-text search.
   *
   * @param options - Search and filter options
   * @returns A RelatesToList of matching edges.
   */
  async search(options: {
    query?: string;
    properties?: string | readonly string[];
    relationType?: string | readonly string[];
    minStrength?: number;
    maxStrength?: number;
    minCreatedAt?: Date | string;
    maxCreatedAt?: Date | string;
    externalIdPrefix?: string;
    space?: string | readonly string[];
    limit?: number;
  } = {}): Promise<RelatesToList> {
    const filter = new RelatesToFilter("and");

    filter.relationType.equalsOrIn(options.relationType ?? null);
    filter.strength.greaterThanOrEquals(options.minStrength ?? null).lessThanOrEquals(options.maxStrength ?? null);
    filter.createdAt.greaterThanOrEquals(options.minCreatedAt ?? null).lessThanOrEquals(options.maxCreatedAt ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    const result = await this._search({
      query: options.query,
      properties: options.properties,
      limit: options.limit,
      filter: filter.asFilter(),
    });

    return new RelatesToList([...result.items]);
  }

  /**
   * Aggregate RelatesTo edge instances.
   *
   * @param aggregate - Aggregation(s) to perform.
   * @param options - Filter and grouping options.
   * @returns AggregateResponse with aggregated values.
   */
  async aggregate(
    aggregate: Aggregation | readonly Aggregation[],
    options: {
      groupBy?: string | readonly string[];
      relationType?: string | readonly string[];
      minStrength?: number;
      maxStrength?: number;
      minCreatedAt?: Date | string;
      maxCreatedAt?: Date | string;
      externalIdPrefix?: string;
      space?: string | readonly string[];
    } = {},
  ): Promise<AggregateResponse> {
    const filter = new RelatesToFilter("and");

    filter.relationType.equalsOrIn(options.relationType ?? null);
    filter.strength.greaterThanOrEquals(options.minStrength ?? null).lessThanOrEquals(options.maxStrength ?? null);
    filter.createdAt.greaterThanOrEquals(options.minCreatedAt ?? null).lessThanOrEquals(options.maxCreatedAt ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    return this._aggregate(aggregate, {
      groupBy: options.groupBy,
      filter: filter.asFilter(),
    });
  }

  /**
   * List RelatesTo edge instances with type-safe filtering.
   *
   * @param options - Filter, sort, and pagination options.
   * @returns A RelatesToList of matching edges.
   */
  async list(options: {
    relationType?: string | readonly string[];
    minStrength?: number;
    maxStrength?: number;
    minCreatedAt?: Date | string;
    maxCreatedAt?: Date | string;
    externalIdPrefix?: string;
    space?: string | readonly string[];
    sortBy?: string;
    sortDirection?: SortDirection;
    limit?: number;
  } = {}): Promise<RelatesToList> {
    const filter = new RelatesToFilter("and");

    filter.relationType.equalsOrIn(options.relationType ?? null);
    filter.strength.greaterThanOrEquals(options.minStrength ?? null).lessThanOrEquals(options.maxStrength ?? null);
    filter.createdAt.greaterThanOrEquals(options.minCreatedAt ?? null).lessThanOrEquals(options.maxCreatedAt ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    let sort: PropertySort | undefined;
    if (options.sortBy !== undefined) {
      sort = {
        property: createPropertyRef(RELATES_TO_VIEW, options.sortBy),
        direction: options.sortDirection,
      };
    }

    const result = await this._list({
      limit: options.limit,
      filter: filter.asFilter(),
      sort,
    });

    return new RelatesToList([...result]);
  }
}

