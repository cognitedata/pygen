/**
 * Token-based authentication credentials.
 *
 * @packageDocumentation
 */

import type { Credentials } from "./credentials.ts";

/**
 * Simple token-based credentials for static authentication.
 *
 * Use this when you have a pre-obtained access token that doesn't need refreshing.
 *
 * @example
 * ```_typescript
 * const credentials = new TokenCredentials("my-access-token");
 * const [name, value] = await credentials.authorizationHeader();
 * // name: "Authorization", value: "Bearer my-access-token"
 * ```
 */
export class TokenCredentials implements Credentials {
  /**
   * Creates new TokenCredentials.
   *
   * @param token - The access token to use for authentication
   */
  constructor(private readonly token: string) {}

  /**
   * Get the authorization header with the static token.
   *
   * @returns A promise resolving to ["Authorization", "Bearer <token>"]
   */
  async authorizationHeader(): Promise<[string, string]> {
    return ["Authorization", `Bearer ${this.token}`];
  }
}
