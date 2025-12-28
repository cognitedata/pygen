/**
 * Exception hierarchy for Pygen TypeScript SDK.
 *
 * This module provides custom error classes for handling various error
 * conditions in the Pygen API client.
 *
 * @packageDocumentation
 */

import type { FailedRequest, FailedResponse } from "./http_client/types.ts";
import type { UpsertResult } from "./types/responses.ts";

/**
 * Base class for all exceptions raised by the Pygen API client.
 *
 * This serves as the root of the exception hierarchy, allowing users to
 * catch all Pygen-related errors with a single catch block.
 *
 * @example
 * ```typescript
 * try {
 *   await client.upsert(items);
 * } catch (error) {
 *   if (error instanceof PygenAPIError) {
 *     console.error("Pygen API error:", error.message);
 *   }
 * }
 * ```
 */
export class PygenAPIError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PygenAPIError";
    // Maintains proper stack trace for where our error was thrown (only available on V8)
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, PygenAPIError);
    }
  }
}

/**
 * Exception raised for OAuth2 authentication errors.
 *
 * This error is thrown when authentication fails, such as:
 * - Invalid credentials
 * - Token refresh failures
 * - Token endpoint errors
 *
 * @example
 * ```typescript
 * try {
 *   const token = await credentials.authorizationHeader();
 * } catch (error) {
 *   if (error instanceof OAuth2Error) {
 *     console.error("Authentication failed:", error.message);
 *   }
 * }
 * ```
 */
export class OAuth2Error extends PygenAPIError {
  constructor(message: string) {
    super(message);
    this.name = "OAuth2Error";
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, OAuth2Error);
    }
  }
}

/**
 * Exception raised when multiple requests are executed and at least one fails.
 *
 * This error is thrown during batch operations where some requests succeed
 * but others fail. It provides access to both the successful results and
 * the failed requests/responses.
 *
 * @example
 * ```typescript
 * try {
 *   await client.upsert(items);
 * } catch (error) {
 *   if (error instanceof MultiRequestError) {
 *     console.error("Partial failure:", error.message);
 *     console.log("Successful results:", error.result);
 *     console.log("Failed responses:", error.failedResponses.length);
 *     console.log("Failed requests:", error.failedRequests.length);
 *   }
 * }
 * ```
 */
export class MultiRequestError extends PygenAPIError {
  /**
   * List of failed HTTP responses (received response but with error status).
   */
  readonly failedResponses: readonly FailedResponse[];

  /**
   * List of failed HTTP requests (no response received).
   */
  readonly failedRequests: readonly FailedRequest[];

  /**
   * The successful part of the operation.
   */
  readonly result: UpsertResult;

  /**
   * Creates a new MultiRequestError.
   *
   * @param failedResponses - List of failed responses from the API
   * @param failedRequests - List of requests that failed to complete
   * @param result - The successful results from the operation
   */
  constructor(
    failedResponses: readonly FailedResponse[],
    failedRequests: readonly FailedRequest[],
    result: UpsertResult,
  ) {
    const message = MultiRequestError.createMessage(
      failedResponses,
      failedRequests,
    );
    super(message);
    this.name = "MultiRequestError";
    this.failedResponses = failedResponses;
    this.failedRequests = failedRequests;
    this.result = result;
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, MultiRequestError);
    }
  }

  /**
   * Creates a descriptive error message.
   */
  private static createMessage(
    failedResponses: readonly FailedResponse[],
    failedRequests: readonly FailedRequest[],
  ): string {
    const parts: string[] = [];
    if (failedResponses.length > 0) {
      parts.push(`${failedResponses.length} failed responses`);
    }
    if (failedRequests.length > 0) {
      parts.push(`${failedRequests.length} failed requests`);
    }
    return `MultiRequestError: ${parts.join("; ")}`;
  }

  /**
   * Gets all error messages from failed responses.
   *
   * @returns Array of error messages from failed API responses
   */
  getErrorMessages(): string[] {
    return this.failedResponses.map((r) => r.error.message);
  }

  /**
   * Gets all error codes from failed responses.
   *
   * @returns Array of error codes from failed API responses
   */
  getErrorCodes(): number[] {
    return this.failedResponses.map((r) => r.error.code);
  }

  /**
   * Checks if any failures were due to rate limiting (429).
   *
   * @returns True if any failures were rate limit errors
   */
  hasRateLimitErrors(): boolean {
    return this.failedResponses.some((r) => r.statusCode === 429);
  }

  /**
   * Checks if any failures were due to server errors (5xx).
   *
   * @returns True if any failures were server errors
   */
  hasServerErrors(): boolean {
    return this.failedResponses.some(
      (r) => r.statusCode >= 500 && r.statusCode < 600,
    );
  }

  /**
   * Checks if any failures were due to client errors (4xx).
   *
   * @returns True if any failures were client errors
   */
  hasClientErrors(): boolean {
    return this.failedResponses.some(
      (r) => r.statusCode >= 400 && r.statusCode < 500,
    );
  }
}

/**
 * Type guard to check if an error is a PygenAPIError.
 *
 * @param error - The error to check
 * @returns True if the error is a PygenAPIError
 */
export function isPygenAPIError(error: unknown): error is PygenAPIError {
  return error instanceof PygenAPIError;
}

/**
 * Type guard to check if an error is an OAuth2Error.
 *
 * @param error - The error to check
 * @returns True if the error is an OAuth2Error
 */
export function isOAuth2Error(error: unknown): error is OAuth2Error {
  return error instanceof OAuth2Error;
}

/**
 * Type guard to check if an error is a MultiRequestError.
 *
 * @param error - The error to check
 * @returns True if the error is a MultiRequestError
 */
export function isMultiRequestError(error: unknown): error is MultiRequestError {
  return error instanceof MultiRequestError;
}
