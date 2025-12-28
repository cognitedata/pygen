/**
 * Utility functions for data type conversions.
 *
 * Provides utilities for converting between CDF API formats and TypeScript types,
 * including date/datetime handling with milliseconds since epoch.
 *
 * @packageDocumentation
 */

/**
 * Minimum valid CDF timestamp in milliseconds (1900-01-01 00:00:00.000).
 */
export const MIN_TIMESTAMP_MS = -2208988800000;

/**
 * Maximum valid CDF timestamp in milliseconds (2099-12-31 23:59:59.999).
 */
export const MAX_TIMESTAMP_MS = 4102444799999;

/**
 * Converts a CDF timestamp (milliseconds since epoch) to a JavaScript Date.
 *
 * @param ms - Milliseconds since epoch
 * @returns A JavaScript Date object
 * @throws Error if the timestamp is outside the valid CDF range
 *
 * @example
 * ```typescript
 * const date = msToDate(1703721600000);
 * console.log(date.toISOString()); // "2023-12-28T00:00:00.000Z"
 * ```
 */
export function msToDate(ms: number): Date {
  if (ms < MIN_TIMESTAMP_MS || ms > MAX_TIMESTAMP_MS) {
    throw new Error(
      `Timestamp ${String(ms)} is outside valid CDF range [${String(MIN_TIMESTAMP_MS)}, ${String(MAX_TIMESTAMP_MS)}]`
    );
  }
  return new Date(ms);
}

/**
 * Converts a JavaScript Date to a CDF timestamp (milliseconds since epoch).
 *
 * @param date - A JavaScript Date object
 * @returns Milliseconds since epoch
 *
 * @example
 * ```typescript
 * const ms = dateToMs(new Date("2023-12-28T00:00:00.000Z"));
 * console.log(ms); // 1703721600000
 * ```
 */
export function dateToMs(date: Date): number {
  return date.getTime();
}

/**
 * Parses a value as a Date if it's a number (milliseconds), or returns it if already a Date.
 *
 * @param value - Either a number (milliseconds) or a Date object
 * @returns A JavaScript Date object
 *
 * @example
 * ```typescript
 * const date1 = parseDate(1703721600000);
 * const date2 = parseDate(new Date("2023-12-28"));
 * ```
 */
export function parseDate(value: number | Date): Date {
  if (typeof value === "number") {
    return msToDate(value);
  }
  return value;
}

/**
 * Parses a value as a Date or undefined if null/undefined.
 *
 * @param value - Either a number (milliseconds), a Date object, null, or undefined
 * @returns A JavaScript Date object or undefined
 */
export function parseDateOptional(value: number | Date | null | undefined): Date | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }
  return parseDate(value);
}

/**
 * Converts snake_case to camelCase.
 *
 * @param str - A snake_case string
 * @returns A camelCase string
 *
 * @example
 * ```typescript
 * toCamelCase("external_id"); // "externalId"
 * toCamelCase("last_updated_time"); // "lastUpdatedTime"
 * ```
 */
export function toCamelCase(str: string): string {
  return str.replace(/_([a-z])/g, (_, letter: string) => letter.toUpperCase());
}

/**
 * Converts camelCase to snake_case.
 *
 * @param str - A camelCase string
 * @returns A snake_case string
 *
 * @example
 * ```typescript
 * toSnakeCase("externalId"); // "external_id"
 * toSnakeCase("lastUpdatedTime"); // "last_updated_time"
 * ```
 */
export function toSnakeCase(str: string): string {
  return str.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);
}

/**
 * Converts object keys from snake_case to camelCase.
 *
 * @param obj - An object with snake_case keys
 * @returns A new object with camelCase keys
 */
export function keysToCamelCase(obj: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      result[toCamelCase(key)] = obj[key];
    }
  }
  return result;
}

/**
 * Converts object keys from camelCase to snake_case.
 *
 * @param obj - An object with camelCase keys
 * @returns A new object with snake_case keys
 */
export function keysToSnakeCase(obj: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      result[toSnakeCase(key)] = obj[key];
    }
  }
  return result;
}

