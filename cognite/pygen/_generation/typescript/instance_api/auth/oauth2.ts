/**
 * OAuth2 Client Credentials authentication support.
 *
 * @packageDocumentation
 */

import type { Credentials } from "./credentials.ts";
import { OAuth2Error } from "../exceptions.ts";

/**
 * Configuration options for OAuth2 Client Credentials flow.
 */
export interface OAuth2Config {
  /** OAuth2 token endpoint URL */
  tokenUrl: string;
  /** OAuth2 client ID */
  clientId: string;
  /** OAuth2 client secret */
  clientSecret: string;
  /** Optional list of OAuth2 scopes to request */
  scopes?: string[];
  /** Optional audience parameter for the token request */
  audience?: string;
  /** Time in seconds before expiry to refresh the token (default: 300) */
  refreshMargin?: number;
}

interface TokenResponse {
  access_token: string;
  expires_in?: number;
}

/**
 * OAuth2 Client Credentials flow authentication.
 *
 * This implements the OAuth2 Client Credentials grant type (RFC 6749 Section 4.4).
 * It automatically handles token refresh when tokens expire.
 *
 * @example
 * ```typescript
 * const credentials = new OAuthClientCredentials({
 *   tokenUrl: "https://auth.example.com/oauth/token",
 *   clientId: "my-client-id",
 *   clientSecret: "my-client-secret",
 *   scopes: ["read", "write"],
 * });
 *
 * const [name, value] = await credentials.authorizationHeader();
 * ```
 */
export class OAuthClientCredentials implements Credentials {
  private readonly tokenUrl: string;
  private readonly clientId: string;
  private readonly clientSecret: string;
  private readonly scopes: string[];
  private readonly audience: string | undefined;
  private readonly refreshMargin: number;

  private accessToken: string | null = null;
  private tokenExpiry: Date | null = null;
  private refreshPromise: Promise<void> | null = null;

  constructor(config: OAuth2Config) {
    this.tokenUrl = config.tokenUrl;
    this.clientId = config.clientId;
    this.clientSecret = config.clientSecret;
    this.scopes = config.scopes ?? [];
    this.audience = config.audience;
    this.refreshMargin = config.refreshMargin ?? 300;
  }

  /**
   * Get the authorization header with a valid access token.
   *
   * Automatically refreshes the token if needed before returning headers.
   *
   * @returns A promise resolving to ["Authorization", "Bearer <token>"]
   * @throws {OAuth2Error} If token acquisition fails
   */
  async authorizationHeader(): Promise<[string, string]> {
    await this.refreshIfNeeded();
    if (!this.accessToken) {
      throw new OAuth2Error("Failed to acquire access token");
    }
    return ["Authorization", `Bearer ${this.accessToken}`];
  }

  /**
   * Check if the token needs to be refreshed.
   */
  private needsRefresh(): boolean {
    if (!this.accessToken || !this.tokenExpiry) {
      return true;
    }
    const timeUntilExpiry = (this.tokenExpiry.getTime() - Date.now()) / 1000;
    return timeUntilExpiry < this.refreshMargin;
  }

  /**
   * Refresh the token if needed, with concurrency protection.
   */
  private async refreshIfNeeded(): Promise<void> {
    if (!this.needsRefresh()) {
      return;
    }

    // If a refresh is already in progress, wait for it
    if (this.refreshPromise) {
      return this.refreshPromise;
    }

    // Start a new refresh
    this.refreshPromise = this.refreshToken();
    try {
      await this.refreshPromise;
    } finally {
      this.refreshPromise = null;
    }
  }

  /**
   * Perform the actual token refresh using OAuth2 client credentials flow.
   */
  private async refreshToken(): Promise<void> {
    const body = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: this.clientId,
      client_secret: this.clientSecret,
    });

    if (this.scopes.length > 0) {
      body.set("scope", this.scopes.join(" "));
    }
    if (this.audience) {
      body.set("audience", this.audience);
    }

    let response: Response;
    try {
      response = await fetch(this.tokenUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: body.toString(),
      });
    } catch (error) {
      throw new OAuth2Error(`Token request failed: ${error}`);
    }

    if (!response.ok) {
      const text = await response.text();
      throw new OAuth2Error(`Token request failed with status ${response.status}: ${text}`);
    }

    let tokenData: TokenResponse;
    try {
      tokenData = (await response.json()) as TokenResponse;
    } catch {
      throw new OAuth2Error("Invalid token response from server");
    }

    if (!tokenData.access_token) {
      throw new OAuth2Error("Token response missing access_token");
    }

    this.accessToken = tokenData.access_token;
    const expiresIn = tokenData.expires_in ?? 3600;
    this.tokenExpiry = new Date(Date.now() + expiresIn * 1000);
  }

  /**
   * Close resources held by the credentials.
   * For OAuthClientCredentials, this clears the cached token.
   */
  close(): void {
    this.accessToken = null;
    this.tokenExpiry = null;
    this.refreshPromise = null;
  }
}
