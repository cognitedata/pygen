/**
 * Data type-specific filter builders for CDF Data Modeling.
 *
 * This module provides chainable filter builders for different property types,
 * allowing type-safe filter construction with a fluent API.
 *
 * @packageDocumentation
 */

import type { Filter, PropertyPath } from "./filters.js";
import type { ViewReference, InstanceId } from "./references.js";

// ============================================================================
// Base Filter Builder
// ============================================================================

/**
 * Base class for data type-specific filters.
 *
 * Provides common functionality for building filters with chainable methods.
 */
export class DataTypeFilter {
  protected readonly _propRef: ViewReference | "node" | "edge";
  protected readonly _propertyId: string;
  protected readonly _operator: "and" | "or";
  protected readonly _filters: Map<string, Filter>;

  /**
   * Creates a new DataTypeFilter.
   *
   * @param propRef - View reference or instance type ("node" or "edge")
   * @param propertyId - The property identifier
   * @param operator - How to combine multiple filter conditions ("and" or "or")
   */
  constructor(
    propRef: ViewReference | "node" | "edge",
    propertyId: string,
    operator: "and" | "or"
  ) {
    this._propRef = propRef;
    this._propertyId = propertyId;
    this._operator = operator;
    this._filters = new Map();
  }

  /**
   * Gets the property path for this filter.
   */
  protected get _propertyPath(): PropertyPath {
    if (typeof this._propRef === "string") {
      return [this._propRef, this._propertyId];
    }
    return [
      this._propRef.space,
      `${this._propRef.externalId}/${this._propRef.version}`,
      this._propertyId,
    ];
  }

  /**
   * Adds a filter condition.
   *
   * @param filterType - The type of filter to add
   * @param filter - The filter object to add
   * @returns This instance for chaining
   */
  protected _addFilter<T extends DataTypeFilter>(this: T, filterType: string, filter: Filter): T {
    this._filters.set(filterType, filter);
    return this;
  }

  /**
   * Converts the accumulated filter conditions to a Filter object.
   *
   * @returns A Filter object or undefined if no conditions have been set
   */
  asFilter(): Filter | undefined {
    const filters = Array.from(this._filters.values());

    if (filters.length === 0) {
      return undefined;
    }

    if (filters.length === 1) {
      return filters[0];
    }

    // Use type assertion since we know the operator is valid
    if (this._operator === "and") {
      return { and: filters };
    } else {
      return { or: filters };
    }
  }

  /**
   * Dumps the filter to a plain object.
   *
   * @returns Plain object representation of the filter
   */
  dump(): Record<string, unknown> | undefined {
    const filter = this.asFilter();
    return filter as Record<string, unknown> | undefined;
  }
}

// ============================================================================
// Filter Container
// ============================================================================

/**
 * Base class for view-specific filter containers.
 *
 * Contains filters for all properties on a view, including common instance metadata.
 */
export class FilterContainer {
  protected readonly _dataTypeFilters: DataTypeFilter[];
  protected readonly _operator: "and" | "or";

  /** Filter for the space property */
  readonly space: TextFilter;
  /** Filter for the externalId property */
  readonly externalId: TextFilter;
  /** Filter for the version property */
  readonly version: IntegerFilter;
  /** Filter for the type property */
  readonly type: DirectRelationFilter;
  /** Filter for the createdTime property */
  readonly createdTime: DateTimeFilter;
  /** Filter for the lastUpdatedTime property */
  readonly lastUpdatedTime: DateTimeFilter;
  /** Filter for the deletedTime property */
  readonly deletedTime: DateTimeFilter;
  /** Filter for the startNode property (edges only) */
  readonly startNode?: DirectRelationFilter;
  /** Filter for the endNode property (edges only) */
  readonly endNode?: DirectRelationFilter;

  /**
   * Creates a new FilterContainer.
   *
   * @param dataTypeFilters - Array to collect all data type filters
   * @param operator - How to combine filters ("and" or "or")
   * @param instanceType - Type of instance ("node" or "edge")
   */
  constructor(
    dataTypeFilters: DataTypeFilter[],
    operator: "and" | "or",
    instanceType: "node" | "edge"
  ) {
    this._dataTypeFilters = dataTypeFilters;
    this._operator = operator;

    this.space = new TextFilter(instanceType, "space", operator);
    this.externalId = new TextFilter(instanceType, "externalId", operator);
    this.version = new IntegerFilter(instanceType, "version", operator);
    this.type = new DirectRelationFilter(instanceType, "type", operator);
    this.createdTime = new DateTimeFilter(instanceType, "createdTime", operator);
    this.lastUpdatedTime = new DateTimeFilter(instanceType, "lastUpdatedTime", operator);
    this.deletedTime = new DateTimeFilter(instanceType, "deletedTime", operator);

    this._dataTypeFilters.push(
      this.space,
      this.externalId,
      this.version,
      this.type,
      this.createdTime,
      this.lastUpdatedTime,
      this.deletedTime
    );

    if (instanceType === "edge") {
      this.startNode = new DirectRelationFilter(instanceType, "startNode", operator);
      this.endNode = new DirectRelationFilter(instanceType, "endNode", operator);
      this._dataTypeFilters.push(this.startNode, this.endNode);
    }
  }

  /**
   * Converts all accumulated filter conditions to a single Filter object.
   *
   * @returns A Filter object or undefined if no conditions have been set
   */
  asFilter(): Filter | undefined {
    const leafFilters = this._dataTypeFilters
      .map((dtf) => dtf.asFilter())
      .filter((f): f is Filter => f !== undefined);

    if (leafFilters.length === 0) {
      return undefined;
    }

    if (leafFilters.length === 1) {
      return leafFilters[0];
    }

    // Use type assertion since we know the operator is valid
    if (this._operator === "and") {
      return { and: leafFilters };
    } else {
      return { or: leafFilters };
    }
  }
}

// ============================================================================
// Type-Specific Filters
// ============================================================================

/**
 * Filter builder for float/numeric properties.
 */
export class FloatFilter extends DataTypeFilter {
  /**
   * Filter for values equal to the given value.
   *
   * @param value - Value to match (or null to skip)
   * @returns This instance for chaining
   */
  equals(value: number | null): this {
    if (value === null) return this;
    return this._addFilter("equals", { equals: { property: this._propertyPath, value } });
  }

  /**
   * Filter for values less than the given value.
   *
   * @param value - Upper bound (exclusive)
   * @returns This instance for chaining
   */
  lessThan(value: number | null): this {
    if (value === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, lt: value },
    });
  }

  /**
   * Filter for values less than or equal to the given value.
   *
   * @param value - Upper bound (inclusive)
   * @returns This instance for chaining
   */
  lessThanOrEquals(value: number | null): this {
    if (value === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, lte: value },
    });
  }

  /**
   * Filter for values greater than the given value.
   *
   * @param value - Lower bound (exclusive)
   * @returns This instance for chaining
   */
  greaterThan(value: number | null): this {
    if (value === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, gt: value },
    });
  }

  /**
   * Filter for values greater than or equal to the given value.
   *
   * @param value - Lower bound (inclusive)
   * @returns This instance for chaining
   */
  greaterThanOrEquals(value: number | null): this {
    if (value === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, gte: value },
    });
  }
}

/**
 * Filter builder for integer properties.
 */
export class IntegerFilter extends DataTypeFilter {
  /**
   * Filter for values equal to the given value.
   *
   * @param value - Value to match (or null to skip)
   * @returns This instance for chaining
   */
  equals(value: number | null): this {
    if (value === null) return this;
    return this._addFilter("equals", {
      equals: { property: this._propertyPath, value: Math.floor(value) },
    });
  }

  /**
   * Filter for values less than the given value.
   *
   * @param value - Upper bound (exclusive)
   * @returns This instance for chaining
   */
  lessThan(value: number | null): this {
    if (value === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, lt: Math.floor(value) },
    });
  }

  /**
   * Filter for values less than or equal to the given value.
   *
   * @param value - Upper bound (inclusive)
   * @returns This instance for chaining
   */
  lessThanOrEquals(value: number | null): this {
    if (value === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, lte: Math.floor(value) },
    });
  }

  /**
   * Filter for values greater than the given value.
   *
   * @param value - Lower bound (exclusive)
   * @returns This instance for chaining
   */
  greaterThan(value: number | null): this {
    if (value === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, gt: Math.floor(value) },
    });
  }

  /**
   * Filter for values greater than or equal to the given value.
   *
   * @param value - Lower bound (inclusive)
   * @returns This instance for chaining
   */
  greaterThanOrEquals(value: number | null): this {
    if (value === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, gte: Math.floor(value) },
    });
  }
}

/**
 * Filter builder for datetime properties.
 */
export class DateTimeFilter extends DataTypeFilter {
  /**
   * Validates and converts a datetime value to ISO format.
   */
  private _validateValue(value: Date | string | null): string | null {
    if (value === null) return null;
    if (value instanceof Date) {
      return value.toISOString();
    }
    // Validate it's a valid ISO format by parsing it
    const parsed = new Date(value);
    if (isNaN(parsed.getTime())) {
      throw new Error(`String '${value}' is not a valid ISO format datetime.`);
    }
    return parsed.toISOString();
  }

  /**
   * Filter for values equal to the given value.
   *
   * @param value - Value to match (or null to skip)
   * @returns This instance for chaining
   */
  equals(value: Date | string | null): this {
    const validated = this._validateValue(value);
    if (validated === null) return this;
    return this._addFilter("equals", {
      equals: { property: this._propertyPath, value: validated },
    });
  }

  /**
   * Filter for values less than the given value.
   *
   * @param value - Upper bound (exclusive)
   * @returns This instance for chaining
   */
  lessThan(value: Date | string | null): this {
    const validated = this._validateValue(value);
    if (validated === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, lt: validated },
    });
  }

  /**
   * Filter for values less than or equal to the given value.
   *
   * @param value - Upper bound (inclusive)
   * @returns This instance for chaining
   */
  lessThanOrEquals(value: Date | string | null): this {
    const validated = this._validateValue(value);
    if (validated === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, lte: validated },
    });
  }

  /**
   * Filter for values greater than the given value.
   *
   * @param value - Lower bound (exclusive)
   * @returns This instance for chaining
   */
  greaterThan(value: Date | string | null): this {
    const validated = this._validateValue(value);
    if (validated === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, gt: validated },
    });
  }

  /**
   * Filter for values greater than or equal to the given value.
   *
   * @param value - Lower bound (inclusive)
   * @returns This instance for chaining
   */
  greaterThanOrEquals(value: Date | string | null): this {
    const validated = this._validateValue(value);
    if (validated === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, gte: validated },
    });
  }
}

/**
 * Filter builder for date properties.
 */
export class DateFilter extends DataTypeFilter {
  /**
   * Validates and converts a date value to ISO format.
   */
  private _validateValue(value: Date | string | null): string | null {
    if (value === null) return null;
    if (value instanceof Date) {
      const parts = value.toISOString().split("T");
      return parts[0]!; // Non-null assertion: ISO string always has date part
    }
    // Validate it's a valid ISO format by parsing it
    const parsed = new Date(value);
    if (isNaN(parsed.getTime())) {
      throw new Error(`String '${value}' is not a valid ISO format date.`);
    }
    const parts = parsed.toISOString().split("T");
    return parts[0]!; // Non-null assertion: ISO string always has date part
  }

  /**
   * Filter for values equal to the given value.
   *
   * @param value - Value to match (or null to skip)
   * @returns This instance for chaining
   */
  equals(value: Date | string | null): this {
    const validated = this._validateValue(value);
    if (validated === null) return this;
    return this._addFilter("equals", {
      equals: { property: this._propertyPath, value: validated },
    });
  }

  /**
   * Filter for values less than the given value.
   *
   * @param value - Upper bound (exclusive)
   * @returns This instance for chaining
   */
  lessThan(value: Date | string | null): this {
    const validated = this._validateValue(value);
    if (validated === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, lt: validated },
    });
  }

  /**
   * Filter for values less than or equal to the given value.
   *
   * @param value - Upper bound (inclusive)
   * @returns This instance for chaining
   */
  lessThanOrEquals(value: Date | string | null): this {
    const validated = this._validateValue(value);
    if (validated === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, lte: validated },
    });
  }

  /**
   * Filter for values greater than the given value.
   *
   * @param value - Lower bound (exclusive)
   * @returns This instance for chaining
   */
  greaterThan(value: Date | string | null): this {
    const validated = this._validateValue(value);
    if (validated === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, gt: validated },
    });
  }

  /**
   * Filter for values greater than or equal to the given value.
   *
   * @param value - Lower bound (inclusive)
   * @returns This instance for chaining
   */
  greaterThanOrEquals(value: Date | string | null): this {
    const validated = this._validateValue(value);
    if (validated === null) return this;
    const existing = this._filters.get("range") as
      | { range: { property: PropertyPath } }
      | undefined;
    return this._addFilter("range", {
      range: { ...existing?.range, property: this._propertyPath, gte: validated },
    });
  }
}

/**
 * Filter builder for text/string properties.
 */
export class TextFilter extends DataTypeFilter {
  /**
   * Validates and converts a string value or list.
   */
  private _validateValue(value: string | string[] | null): string | string[] | null {
    if (value === null) return null;
    if (Array.isArray(value)) {
      return value.map(String);
    }
    return String(value);
  }

  /**
   * Filter for values equal to the given string.
   *
   * @param value - Value to match (or null to skip)
   * @returns This instance for chaining
   */
  equals(value: string | null): this {
    const validated = this._validateValue(value);
    if (validated === null || Array.isArray(validated)) return this;
    return this._addFilter("equals", {
      equals: { property: this._propertyPath, value: validated },
    });
  }

  /**
   * Filter for values starting with the given prefix.
   *
   * @param value - Prefix to match (or null to skip)
   * @returns This instance for chaining
   */
  prefix(value: string | null): this {
    const validated = this._validateValue(value);
    if (validated === null || Array.isArray(validated)) return this;
    return this._addFilter("prefix", {
      prefix: { property: this._propertyPath, value: validated },
    });
  }

  /**
   * Filter for values that are in the given list.
   *
   * @param values - List of values to match (or null to skip)
   * @returns This instance for chaining
   */
  in(values: string[] | null): this {
    const validated = this._validateValue(values);
    if (validated === null || !Array.isArray(validated)) return this;
    return this._addFilter("in", { in: { property: this._propertyPath, values: validated } });
  }

  /**
   * Filter for values equal to the given string or in the given list.
   *
   * @param value - Single value or list of values (or null to skip)
   * @returns This instance for chaining
   */
  equalsOrIn(value: string | string[] | null): this {
    if (Array.isArray(value)) {
      return this.in(value);
    }
    return this.equals(value);
  }
}

/**
 * Filter builder for boolean properties.
 */
export class BooleanFilter extends DataTypeFilter {
  /**
   * Filter for values equal to the given boolean.
   *
   * @param value - Value to match (or null to skip)
   * @returns This instance for chaining
   */
  equals(value: boolean | null): this {
    if (value === null) return this;
    return this._addFilter("equals", { equals: { property: this._propertyPath, value } });
  }
}

/**
 * Filter builder for direct relation properties.
 */
export class DirectRelationFilter extends DataTypeFilter {
  /**
   * Validates and converts a direct relation value.
   */
  private _validateValue(
    value: string | InstanceId | [string, string] | null,
    space?: string
  ): { space: string; externalId: string } | null {
    if (value === null) return null;

    if (Array.isArray(value) && value.length === 2) {
      return { space: value[0], externalId: value[1] };
    }

    if (typeof value === "object" && "space" in value && "externalId" in value) {
      return { space: value.space, externalId: value.externalId };
    }

    if (typeof value === "string") {
      if (!space) {
        throw new Error("Space must be provided when value is a string.");
      }
      return { space, externalId: value };
    }

    throw new TypeError(`Expected string, InstanceId, or [string, string], got ${typeof value}`);
  }

  /**
   * Filter for values equal to the given relation ID.
   *
   * @param value - Relation ID to match (or null to skip)
   * @param space - Space for the relation (required if value is a string)
   * @returns This instance for chaining
   */
  equals(value: string | InstanceId | [string, string] | null, space?: string): this {
    const validated = this._validateValue(value, space);
    if (validated === null) return this;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return this._addFilter("equals", {
      equals: { property: this._propertyPath, value: validated as any },
    });
  }

  /**
   * Filter for values that are in the given list of relation IDs.
   *
   * @param values - List of relation IDs to match (or null to skip)
   * @param space - Space for the relations (required if values are strings)
   * @returns This instance for chaining
   */
  in(values: (string | InstanceId | [string, string])[] | null, space?: string): this {
    if (values === null) return this;
    const validated = values
      .map((v) => this._validateValue(v, space))
      .filter((v): v is NonNullable<typeof v> => v !== null);
    if (validated.length === 0) return this;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return this._addFilter("in", {
      in: { property: this._propertyPath, values: validated as any[] },
    });
  }

  /**
   * Filter for values equal to the given relation ID or in the given list.
   *
   * @param value - Single relation ID or list of relation IDs (or null to skip)
   * @param space - Space for the relation(s) (required if value(s) are strings)
   * @returns This instance for chaining
   */
  equalsOrIn(
    value:
      | string
      | InstanceId
      | [string, string]
      | (string | InstanceId | [string, string])[]
      | null,
    space?: string
  ): this {
    if (value === null) {
      return this;
    }
    // Check if it's a tuple [space, externalId] or an InstanceId object
    if (
      (Array.isArray(value) &&
        value.length === 2 &&
        typeof value[0] === "string" &&
        typeof value[1] === "string") ||
      (typeof value === "object" && "space" in value && "externalId" in value)
    ) {
      return this.equals(value as [string, string] | InstanceId, space);
    }
    // Otherwise, treat as an array of values
    if (Array.isArray(value)) {
      return this.in(value, space);
    }
    return this.equals(value, space);
  }
}
