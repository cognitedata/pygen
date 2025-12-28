/**
 * Response classes for instance API operations.
 *
 * This module contains types and interfaces for API responses including:
 * - Paginated responses (Page, ListResponse)
 * - CRUD operation results (UpsertResult, DeleteResponse)
 * - Aggregation results (AggregateResponse, AggregateItem)
 */

import type { InstanceId, InstanceType } from "./references.ts";
import type { NodeReference } from "./references.ts";

/**
 * Response from a list operation.
 *
 * @template T - The type of the items list
 */
export interface ListResponse<T> {
  /**
   * The list of items returned by the operation.
   */
  items: T;

  /**
   * Optional typing information about the items.
   */
  typing?: Record<string, unknown>;
}

/**
 * A page of results from a paginated API response.
 *
 * @template T - The type of the items list
 *
 * @example
 * ```typescript
 * const page: Page<PersonList> = {
 *   items: personList,
 *   nextCursor: "cursor123",
 *   debug: { queryTimeMs: 42.5 }
 * };
 * ```
 */
export interface Page<T> {
  /**
   * The list of items in this page.
   */
  items: T;

  /**
   * The cursor for the next page, or undefined if this is the last page.
   */
  nextCursor?: string;

  /**
   * Optional typing information about the items.
   */
  typing?: Record<string, unknown>;

  /**
   * Optional debug information about the query execution.
   */
  debug?: Record<string, unknown>;
}

/**
 * Instance type - node or edge.
 * @deprecated Use InstanceType from references.ts instead
 */
export type { InstanceType };

/**
 * Result item from instance operations (create, update, delete).
 *
 * Contains metadata about the instance after the operation.
 *
 * @example
 * ```typescript
 * const resultItem: InstanceResultItem = {
 *   instanceType: "node",
 *   space: "mySpace",
 *   externalId: "person-1",
 *   version: 1,
 *   wasModified: true,
 *   createdTime: 1234567890000,
 *   lastUpdatedTime: 1234567890000
 * };
 * ```
 */
export interface InstanceResultItem {
  /**
   * The type of the instance (node or edge).
   */
  instanceType: InstanceType;

  /**
   * The space of the instance.
   */
  space: string;

  /**
   * The external ID of the instance.
   */
  externalId: string;

  /**
   * The version of the instance after the operation.
   */
  version: number;

  /**
   * Whether the instance was modified by the operation.
   */
  wasModified: boolean;

  /**
   * The time the instance was created (milliseconds since epoch).
   */
  createdTime: number;

  /**
   * The time the instance was last updated (milliseconds since epoch).
   */
  lastUpdatedTime: number;
}

/**
 * Result from instance CRUD operations (create, update, upsert).
 *
 * Provides convenient access to created, updated, unchanged, and deleted items.
 *
 * @example
 * ```typescript
 * const result: UpsertResult = {
 *   items: [resultItem1, resultItem2],
 *   deleted: []
 * };
 *
 * // Access categorized results
 * console.log(`Created: ${getCreated(result).length}`);
 * console.log(`Updated: ${getUpdated(result).length}`);
 * console.log(`Unchanged: ${getUnchanged(result).length}`);
 * ```
 */
export interface UpsertResult {
  /**
   * List of all instance result items from the operation.
   */
  items: InstanceResultItem[];

  /**
   * List of instance IDs that were deleted.
   */
  deleted: InstanceId[];
}

/**
 * Helper function to categorize items as created (modified with matching created/updated times).
 *
 * @param result - The upsert result
 * @returns Array of created items
 */
export function getCreated(result: UpsertResult): InstanceResultItem[] {
  return result.items.filter(
    (item) => item.wasModified && item.createdTime === item.lastUpdatedTime,
  );
}

/**
 * Helper function to categorize items as updated (modified with different created/updated times).
 *
 * @param result - The upsert result
 * @returns Array of updated items
 */
export function getUpdated(result: UpsertResult): InstanceResultItem[] {
  return result.items.filter(
    (item) => item.wasModified && item.createdTime !== item.lastUpdatedTime,
  );
}

/**
 * Helper function to categorize items as unchanged (not modified).
 *
 * @param result - The upsert result
 * @returns Array of unchanged items
 */
export function getUnchanged(result: UpsertResult): InstanceResultItem[] {
  return result.items.filter((item) => !item.wasModified);
}

/**
 * Extends one UpsertResult with another.
 *
 * @param target - The target result to extend
 * @param source - The source result to extend from
 */
export function extendUpsertResult(
  target: UpsertResult,
  source: UpsertResult,
): void {
  target.items.push(...source.items);
  target.deleted.push(...source.deleted);
}

/**
 * Response from the delete operation.
 *
 * This matches the CDF API response format from the
 * /models/instances endpoint (DELETE).
 *
 * @example
 * ```typescript
 * const response: DeleteResponse = {
 *   items: [
 *     { space: "mySpace", externalId: "person-1" },
 *     { space: "mySpace", externalId: "person-2" }
 *   ]
 * };
 * ```
 */
export interface DeleteResponse {
  /**
   * List of instance IDs that were deleted.
   */
  items: InstanceId[];
}

// ============================================================================
// Aggregation Response Types
// ============================================================================

/**
 * Aggregation type for numeric aggregations.
 */
export type NumericAggregateType = "avg" | "min" | "max" | "count" | "sum";

/**
 * An aggregated numeric value from an aggregation query.
 *
 * @example
 * ```typescript
 * const avgAge: AggregatedNumberValue = {
 *   aggregate: "avg",
 *   property: "age",
 *   value: 42.5
 * };
 * ```
 */
export interface AggregatedNumberValue {
  /**
   * The aggregation type.
   */
  aggregate: NumericAggregateType;

  /**
   * The property that was aggregated (undefined for count).
   */
  property?: string;

  /**
   * The aggregated numeric value.
   */
  value: number;
}

/**
 * A histogram bucket.
 *
 * @example
 * ```typescript
 * const bucket: Bucket = {
 *   start: 0,
 *   count: 15
 * };
 * ```
 */
export interface Bucket {
  /**
   * The start value of the bucket.
   */
  start: number;

  /**
   * The count of items in the bucket.
   */
  count: number;
}

/**
 * An aggregated histogram value from a histogram aggregation query.
 *
 * @example
 * ```typescript
 * const histogram: AggregatedHistogramValue = {
 *   aggregate: "histogram",
 *   property: "age",
 *   interval: 10,
 *   buckets: [
 *     { start: 0, count: 5 },
 *     { start: 10, count: 15 }
 *   ]
 * };
 * ```
 */
export interface AggregatedHistogramValue {
  /**
   * The aggregation type (always "histogram").
   */
  aggregate: "histogram";

  /**
   * The property that was aggregated.
   */
  property: string;

  /**
   * The interval used for histogram buckets.
   */
  interval: number;

  /**
   * The histogram buckets.
   */
  buckets: Bucket[];
}

/**
 * Union type for aggregated values.
 * Use the `aggregate` discriminator to determine which type is present.
 */
export type AggregatedValue =
  | AggregatedNumberValue
  | AggregatedHistogramValue;

/**
 * Possible group value types for aggregation grouping.
 */
export type GroupValue = string | number | boolean | NodeReference;

/**
 * A single item from an aggregation response.
 *
 * Contains the aggregated values and optional grouping information.
 *
 * @example
 * ```typescript
 * const item: AggregateItem = {
 *   instanceType: "node",
 *   group: { country: "Norway" },
 *   aggregates: [
 *     { aggregate: "count", value: 100 },
 *     { aggregate: "avg", property: "age", value: 42.5 }
 *   ]
 * };
 * ```
 */
export interface AggregateItem {
  /**
   * The instance type for this aggregation result.
   */
  instanceType: InstanceType;

  /**
   * Optional grouping values if the aggregation was grouped.
   * Maps property names to their values.
   */
  group?: Record<string, GroupValue>;

  /**
   * The list of aggregated values for this item.
   */
  aggregates: AggregatedValue[];
}

/**
 * Response from an aggregate operation.
 *
 * @example
 * ```typescript
 * const response: AggregateResponse = {
 *   items: [
 *     {
 *       instanceType: "node",
 *       aggregates: [
 *         { aggregate: "count", value: 100 }
 *       ]
 *     }
 *   ],
 *   typing: {}
 * };
 * ```
 */
export interface AggregateResponse {
  /**
   * The list of aggregation result items.
   */
  items: AggregateItem[];

  /**
   * Optional typing information.
   */
  typing?: Record<string, unknown>;
}
