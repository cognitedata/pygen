/**
 * Client configuration types.
 *
 * @packageDocumentation
 */

import type { Credentials } from "./credentials.ts";

/**
 * Configuration for the Pygen client and HTTP client.
 *
 * @example
 * ```typescript
 * const config: PygenClientConfig = {
 *   baseUrl: "https://api.cognitedata.com",
 *   project: "my-project",
 *   credentials: new TokenCredentials("my-token"),
 * };
 * ```
 */
export interface PygenClientConfig {
  /**
   * The base URL for the CDF API.
   *
   * @example "https://api.cognitedata.com"
   */
  readonly baseUrl: string;

  /**
   * The CDF project name.
   */
  readonly project: string;

  /**
   * The credentials to use for authentication.
   */
  readonly credentials: Credentials;

  /**
   * Optional client application name for x-cdp-app header (default: "pygen-typescript").
   */
  readonly clientName?: string;

  /**
   * Optional request timeout in milliseconds (default: 30000).
   */
  readonly timeout?: number;

  /**
   * Optional CDF API version (default: "20230101").
   */
  readonly apiSubversion?: string;

  /**
   * Optional maximum number of retries for failed requests (default: 10).
   */
  readonly maxRetries?: number;

  /**
   * Optional HTTP status codes that should trigger a retry (default: 408, 429, 502, 503, 504).
   */
  readonly retryStatusCodes?: ReadonlySet<number>;

  /**
   * Optional maximum backoff time in seconds for retries (default: 60).
   */
  readonly maxRetryBackoff?: number;
}
