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
 * ```_typescript
 * const date = msToDate(1703721600000);
 * console.log(date.toISOString()); // "2023-12-28T00:00:00.000Z"
 * ```
 */
export function msToDate(ms: number): Date {
  if (ms < MIN_TIMESTAMP_MS || ms > MAX_TIMESTAMP_MS) {
    throw new Error(
      `Timestamp ${String(ms)} is outside valid CDF range [${String(MIN_TIMESTAMP_MS)}, ${
        String(MAX_TIMESTAMP_MS)
      }]`,
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
 * ```_typescript
 * const ms = dateToMs(new Date("2023-12-28T00:00:00.000Z"));
 * console.log(ms); // 1703721600000
 * ```
 */
export function dateToMs(date: Date): number {
  const ms = date.getTime();
  if (ms < MIN_TIMESTAMP_MS || ms > MAX_TIMESTAMP_MS) {
    throw new Error(
      `Timestamp ${String(ms)} is outside valid CDF range [${String(MIN_TIMESTAMP_MS)}, ${
        String(MAX_TIMESTAMP_MS)
      }]`,
    );
  }
  return ms;
}

/**
 * Converts a camelCase key to snake_case.
 */
export function toSnakeCaseKey(key: string): string {
  const step1 = key
    // Break acronym runs followed by a capitalized word part: HTTPServer -> HTTP_Server
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    // Break lower/number followed by capital: serverID -> server_ID
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    // Break letter/number boundaries: id2X -> id2_X, 2Id -> 2_Id
    .replace(/([a-zA-Z])(\d)/g, "$1_$2")
    .replace(/(\d)([a-zA-Z])/g, "$1_$2");
  return step1.toLowerCase();
}

/**
 * Parses a value as a Date if it's a number (milliseconds), or returns it if already a Date.
 *
 * @param value - Either a number (milliseconds) or a Date object
 * @returns A JavaScript Date object
 *
 * @example
 * ```_typescript
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
 * Chunks an array into smaller arrays of a specified size.
 *
 * @typeParam T - The type of elements in the array
 * @param items - The array to chunk
 * @param chunkSize - Maximum size of each chunk
 * @returns An array of chunks
 *
 * @example
 * ```_typescript
 * const items = [1, 2, 3, 4, 5];
 * const chunks = chunker(items, 2);
 * console.log(chunks); // [[1, 2], [3, 4], [5]]
 * ```
 */
export function chunker<T>(items: readonly T[], chunkSize: number): T[][] {
  if (chunkSize <= 0) {
    throw new Error("Chunk size must be positive");
  }
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += chunkSize) {
    chunks.push(items.slice(i, i + chunkSize));
  }
  return chunks;
}
