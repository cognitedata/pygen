/**
 * Query-related data structures for sorting, units, debug information, and aggregations.
 *
 * This module contains types and interfaces used for configuring instance queries including:
 * - Sorting: Order query results by property values
 * - Units: Convert property values to target units
 * - Debug: Include additional debug information in responses
 * - Aggregation: Aggregate data across instances
 */

/**
 * Sort direction for query results.
 */
export type SortDirection = "ascending" | "descending";

/**
 * Sort configuration for a property.
 *
 * This corresponds to the CDF API InstanceSort structure for sorting query results
 * by property values.
 *
 * @example
 * Sort by a view property in ascending order:
 * ```typescript
 * const sort: PropertySort = {
 *   property: ["my_space", "MyView/v1", "name"],
 *   direction: "ascending"
 * };
 * ```
 *
 * @example
 * Sort by externalId with nulls first:
 * ```typescript
 * const sort: PropertySort = {
 *   property: ["node", "externalId"],
 *   direction: "descending",
 *   nullsFirst: true
 * };
 * ```
 */
export interface PropertySort {
  /**
   * The property path to sort by. Can be:
   * - A list of strings for view properties: [<space>, <view/version>, <property>]
   * - A list of strings for node/edge properties: ["node", "externalId"] or ["edge", "type"]
   *
   * Minimum length: 2, Maximum length: 3
   */
  property: [string, string] | [string, string, string];

  /**
   * Sort direction - "ascending" or "descending". Defaults to "ascending".
   */
  direction?: SortDirection;

  /**
   * Whether null values should appear first. If not specified, null values
   * are sorted last for ascending and first for descending.
   */
  nullsFirst?: boolean;
}

/**
 * Reference to a specific unit by its external ID.
 */
export interface UnitReference {
  /**
   * The external ID of the unit.
   */
  externalId: string;
}

/**
 * Reference to a unit system by its name.
 */
export interface UnitSystemReference {
  /**
   * The name of the unit system.
   */
  unitSystemName: string;
}

/**
 * Unit conversion configuration for a property.
 *
 * Use this to specify a target unit for numeric properties that have units defined
 * in the container. The API will automatically convert values to the target unit.
 *
 * @example
 * ```typescript
 * const conversion: UnitConversion = {
 *   property: "temperature",
 *   unit: { externalId: "temperature:cel" }
 * };
 * ```
 */
export interface UnitConversion {
  /**
   * The property name to convert.
   */
  property: string;

  /**
   * The target unit to convert to. Must be a valid unit from the
   * same unit catalog as the source property's unit.
   */
  unit: UnitReference | UnitSystemReference;
}

/**
 * Debug parameters for a query request.
 *
 * When debug mode is enabled in list/iterate/search operations, the response includes
 * additional metadata about the query execution.
 *
 * @example
 * ```typescript
 * const debug: DebugParameters = {
 *   emitResults: false,
 *   profile: true
 * };
 * ```
 */
export interface DebugParameters {
  /**
   * Include the query result in the response. Set to false for
   * advanced query analysis features.
   *
   * @default true
   */
  emitResults?: boolean;

  /**
   * Query timeout in milliseconds. Can be used to override the default
   * timeout when analysing queries. Requires emitResults=false.
   */
  timeout?: number;

  /**
   * Most thorough level of query analysis. Requires emitResults=false.
   *
   * @default false
   */
  profile?: boolean;
}

// ============================================================================
// Aggregation Types
// ============================================================================

/**
 * Available aggregation types.
 */
export type AggregationType = "count" | "sum" | "avg" | "min" | "max" | "histogram";

/**
 * Base interface for aggregation data definitions.
 * @internal
 */
interface AggregationDataDefinition {
  aggregate: AggregationType;
}

/**
 * Count aggregation configuration.
 *
 * @example
 * ```typescript
 * const count: Count = { aggregate: "count" };
 * ```
 */
export interface Count extends AggregationDataDefinition {
  aggregate: "count";
  property?: string;
}

/**
 * Sum aggregation configuration.
 *
 * @example
 * ```typescript
 * const sum: Sum = {
 *   aggregate: "sum",
 *   property: "score"
 * };
 * ```
 */
export interface Sum extends AggregationDataDefinition {
  aggregate: "sum";
  property: string;
}

/**
 * Average aggregation configuration.
 *
 * @example
 * ```typescript
 * const avg: Avg = {
 *   aggregate: "avg",
 *   property: "age"
 * };
 * ```
 */
export interface Avg extends AggregationDataDefinition {
  aggregate: "avg";
  property: string;
}

/**
 * Minimum aggregation configuration.
 *
 * @example
 * ```typescript
 * const min: Min = {
 *   aggregate: "min",
 *   property: "age"
 * };
 * ```
 */
export interface Min extends AggregationDataDefinition {
  aggregate: "min";
  property: string;
}

/**
 * Maximum aggregation configuration.
 *
 * @example
 * ```typescript
 * const max: Max = {
 *   aggregate: "max",
 *   property: "age"
 * };
 * ```
 */
export interface Max extends AggregationDataDefinition {
  aggregate: "max";
  property: string;
}

/**
 * Histogram aggregation configuration.
 *
 * @example
 * ```typescript
 * const histogram: Histogram = {
 *   aggregate: "histogram",
 *   property: "age",
 *   interval: 10
 * };
 * ```
 */
export interface Histogram extends AggregationDataDefinition {
  aggregate: "histogram";
  property: string;
  interval: number;
}

/**
 * Union type for all aggregation configurations.
 * Use the `aggregate` discriminator to determine which type is present.
 */
export type Aggregation = Count | Sum | Avg | Min | Max | Histogram;

/**
 * Aggregation request format - a dictionary with a single aggregation type key.
 *
 * @example
 * ```typescript
 * const request: AggregationRequest = {
 *   count: { aggregate: "count" }
 * };
 * ```
 *
 * @example
 * ```typescript
 * const request: AggregationRequest = {
 *   avg: { aggregate: "avg", property: "age" }
 * };
 * ```
 */
export type AggregationRequest = {
  [K in AggregationType]?: Extract<Aggregation, { aggregate: K }>;
};

/**
 * Set of available aggregation type names.
 */
export const AVAILABLE_AGGREGATES = new Set<AggregationType>([
  "count",
  "sum",
  "avg",
  "min",
  "max",
  "histogram",
]);

/**
 * Validates and transforms aggregation request data.
 *
 * This function handles the transformation from the simple API format
 * (e.g., `{ "count": {} }`) to the internal format with discriminator
 * (e.g., `{ "count": { "aggregate": "count" } }`).
 *
 * @param value - The aggregation request value to validate
 * @returns The validated and transformed aggregation request
 * @throws {Error} If the aggregation format is invalid
 * @internal
 */
export function validateAggregationRequest(value: unknown): AggregationRequest {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Aggregate data must be an object.");
  }

  const entries = Object.entries(value);
  if (entries.length !== 1) {
    throw new Error("Aggregate data must have exactly one key.");
  }

  const [key, data] = entries[0] as [string, unknown];

  // Check if already in correct format
  if (
    typeof data === "object" &&
    data !== null &&
    "aggregate" in data &&
    data.aggregate === key
  ) {
    return value as AggregationRequest;
  }

  // Validate aggregation type
  if (!AVAILABLE_AGGREGATES.has(key as AggregationType)) {
    const available = Array.from(AVAILABLE_AGGREGATES).sort().join(", ");
    throw new Error(
      `Unknown aggregate: '${key}'. Available aggregates: ${available}.`,
    );
  }

  // Transform to internal format
  if (typeof data === "object" && data !== null) {
    return {
      [key]: { aggregate: key, ...data },
    } as AggregationRequest;
  }

  return value as AggregationRequest;
}
