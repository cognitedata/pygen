/**
 * Base credentials interface for authentication.
 *
 * All credential types must implement this interface to provide
 * authentication headers for HTTP requests.
 *
 * @packageDocumentation
 */

/**
 * Abstract interface for authentication credentials.
 *
 * Credential implementations must provide a method to get
 * the authorization header for HTTP requests.
 */
export interface Credentials {
  /**
   * Get the authorization header for HTTP requests.
   *
   * @returns A promise resolving to a tuple of [header name, header value]
   *
   * @example
   * ```_typescript
   * const [name, value] = await credentials.authorizationHeader();
   * headers.set(name, value);
   * ```
   */
  authorizationHeader(): Promise<[string, string]>;

  /**
   * Close any resources held by the credentials.
   * This is optional - credentials that don't hold resources don't need to implement it.
   */
  close?(): void;
}
