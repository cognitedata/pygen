/**
 * Client configuration types.
 *
 * @packageDocumentation
 */

import type { Credentials } from "./credentials.js";

/**
 * Configuration for the Pygen client.
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
  baseUrl: string;

  /**
   * The CDF project name.
   */
  project: string;

  /**
   * The credentials to use for authentication.
   */
  credentials: Credentials;

  /**
   * Optional request timeout in milliseconds (default: 30000).
   */
  timeout?: number;

  /**
   * Optional maximum number of retries for failed requests (default: 3).
   */
  maxRetries?: number;
}
