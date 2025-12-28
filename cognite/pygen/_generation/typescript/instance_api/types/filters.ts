/**
 * Filter types for CDF Data Modeling API.
 *
 * This module provides filter data structures that match the CDF API filter format.
 * Filters are used to query and filter instances based on property values.
 *
 * @packageDocumentation
 */

import type { ViewReference, ContainerReference, NodeReference } from "./references.ts";

// ============================================================================
// Base Filter Types
// ============================================================================

/** Type of filter operation */
export type FilterType =
  | "equals"
  | "prefix"
  | "in"
  | "range"
  | "exists"
  | "containsAny"
  | "containsAll"
  | "matchAll"
  | "nested"
  | "overlaps"
  | "hasData"
  | "instanceReferences"
  | "and"
  | "or"
  | "not";

/** Property path in filter (space, viewId/version, propertyId) or (instanceType, propertyId) */
export type PropertyPath = [string, string] | [string, string, string];

/** JSON values that can be used in filters */
export type JsonValue = string | number | boolean | null;

// ============================================================================
// Property Reference Filters
// ============================================================================

/** Equals filter - property equals a value */
export interface EqualsFilter {
  readonly equals: {
    readonly property: PropertyPath;
    readonly value: JsonValue | PropertyReference | NodeReference;
  };
}

/** In filter - property is in a list of values */
export interface InFilter {
  readonly in: {
    readonly property: PropertyPath;
    readonly values: (JsonValue | NodeReference)[] | PropertyReference;
  };
}

/** Range filter - property is within a range */
export interface RangeFilter {
  readonly range: {
    readonly property: PropertyPath;
    readonly gt?: string | number;
    readonly gte?: string | number;
    readonly lt?: string | number;
    readonly lte?: string | number;
  };
}

/** Prefix filter - text property starts with prefix */
export interface PrefixFilter {
  readonly prefix: {
    readonly property: PropertyPath;
    readonly value: string;
  };
}

/** Exists filter - property exists on the instance */
export interface ExistsFilter {
  readonly exists: {
    readonly property: PropertyPath;
  };
}

/** ContainsAny filter - list property contains any of the values */
export interface ContainsAnyFilter {
  readonly containsAny: {
    readonly property: PropertyPath;
    readonly values: JsonValue[] | PropertyReference;
  };
}

/** ContainsAll filter - list property contains all of the values */
export interface ContainsAllFilter {
  readonly containsAll: {
    readonly property: PropertyPath;
    readonly values: JsonValue[] | PropertyReference;
  };
}

/** PropertyReference - reference to another property for comparison */
export interface PropertyReference {
  readonly property: PropertyPath;
}

// ============================================================================
// Special Filters
// ============================================================================

/** MatchAll filter - matches all instances */
export interface MatchAllFilter {
  readonly matchAll: Record<string, never>;
}

/** Nested filter - filter within a nested scope */
export interface NestedFilter {
  readonly nested: {
    readonly scope: PropertyPath;
    readonly filter: Filter;
  };
}

/** Overlaps filter - check if time ranges overlap */
export interface OverlapsFilter {
  readonly overlaps: {
    readonly property: PropertyPath;
    readonly startProperty: PropertyPath;
    readonly endProperty: PropertyPath;
    readonly gt?: string | number;
    readonly gte?: string | number;
    readonly lt?: string | number;
    readonly lte?: string | number;
  };
}

/** HasData filter - instance has data in specified views/containers */
export interface HasDataFilter {
  readonly hasData: (
    | ({ readonly type: "view" } & ViewReference)
    | ({ readonly type: "container" } & ContainerReference)
  )[];
}

/** InstanceReferences filter - matches specific instance IDs */
export interface InstanceReferencesFilter {
  readonly instanceReferences: NodeReference[];
}

// ============================================================================
// Logical Filters
// ============================================================================

/** And filter - all nested filters must match */
export interface AndFilter {
  readonly and: Filter[];
}

/** Or filter - at least one nested filter must match */
export interface OrFilter {
  readonly or: Filter[];
}

/** Not filter - negates the nested filter */
export interface NotFilter {
  readonly not: Filter;
}

// ============================================================================
// Union Type
// ============================================================================

/**
 * A filter that can be applied to instances.
 *
 * Filters follow the CDF Data Modeling API filter format where the filter type
 * is the key of the outer object.
 */
export type Filter =
  | EqualsFilter
  | InFilter
  | RangeFilter
  | PrefixFilter
  | ExistsFilter
  | ContainsAnyFilter
  | ContainsAllFilter
  | MatchAllFilter
  | NestedFilter
  | OverlapsFilter
  | HasDataFilter
  | InstanceReferencesFilter
  | AndFilter
  | OrFilter
  | NotFilter;

// ============================================================================
// Type Guards
// ============================================================================

/** Type guard for EqualsFilter */
export function isEqualsFilter(filter: Filter): filter is EqualsFilter {
  return "equals" in filter;
}

/** Type guard for InFilter */
export function isInFilter(filter: Filter): filter is InFilter {
  return "in" in filter;
}

/** Type guard for RangeFilter */
export function isRangeFilter(filter: Filter): filter is RangeFilter {
  return "range" in filter;
}

/** Type guard for PrefixFilter */
export function isPrefixFilter(filter: Filter): filter is PrefixFilter {
  return "prefix" in filter;
}

/** Type guard for AndFilter */
export function isAndFilter(filter: Filter): filter is AndFilter {
  return "and" in filter;
}

/** Type guard for OrFilter */
export function isOrFilter(filter: Filter): filter is OrFilter {
  return "or" in filter;
}

/** Type guard for NotFilter */
export function isNotFilter(filter: Filter): filter is NotFilter {
  return "not" in filter;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Creates an equals filter.
 *
 * @param property - Property path to filter on
 * @param value - Value to match
 * @returns An EqualsFilter
 */
export function equals(property: PropertyPath, value: JsonValue | PropertyReference): EqualsFilter {
  return { equals: { property, value } };
}

/**
 * Creates an in filter.
 *
 * @param property - Property path to filter on
 * @param values - Values to match
 * @returns An InFilter
 */
export function inFilter(
  property: PropertyPath,
  values: JsonValue[] | PropertyReference
): InFilter {
  return { in: { property, values } };
}

/**
 * Creates a range filter.
 *
 * @param property - Property path to filter on
 * @param options - Range bounds (gt, gte, lt, lte)
 * @returns A RangeFilter
 */
export function range(
  property: PropertyPath,
  options: {
    gt?: string | number;
    gte?: string | number;
    lt?: string | number;
    lte?: string | number;
  }
): RangeFilter {
  return { range: { property, ...options } };
}

/**
 * Creates a prefix filter.
 *
 * @param property - Property path to filter on
 * @param value - Prefix to match
 * @returns A PrefixFilter
 */
export function prefix(property: PropertyPath, value: string): PrefixFilter {
  return { prefix: { property, value } };
}

/**
 * Creates an exists filter.
 *
 * @param property - Property path to check for existence
 * @returns An ExistsFilter
 */
export function exists(property: PropertyPath): ExistsFilter {
  return { exists: { property } };
}

/**
 * Creates an and filter.
 *
 * @param filters - Filters to combine with AND logic
 * @returns An AndFilter
 */
export function and(...filters: Filter[]): AndFilter {
  return { and: filters };
}

/**
 * Creates an or filter.
 *
 * @param filters - Filters to combine with OR logic
 * @returns An OrFilter
 */
export function or(...filters: Filter[]): OrFilter {
  return { or: filters };
}

/**
 * Creates a not filter.
 *
 * @param filter - Filter to negate
 * @returns A NotFilter
 */
export function not(filter: Filter): NotFilter {
  return { not: filter };
}
