/**
 * Authentication support for Pygen TypeScript SDK.
 *
 * @packageDocumentation
 */

export type { Credentials } from "./credentials.ts";
export { TokenCredentials } from "./token.ts";
export { OAuthClientCredentials } from "./oauth2.ts";
export type { OAuth2Config } from "./oauth2.ts";
export type { PygenClientConfig } from "./config.ts";

// Re-export OAuth2Error from exceptions for backwards compatibility
export { OAuth2Error } from "../exceptions.ts";
