/**
 * HTTP client module for CDF API communication.
 *
 * @packageDocumentation
 */

export { HTTPClient } from "./client.js";
export type { HTTPClientConfig, HTTPClientOptions } from "./client.js";
export type {
  HTTPMethod,
  HTTPResult,
  SuccessResponse,
  FailedResponse,
  FailedRequest,
  ErrorDetails,
  RequestMessage,
  RequestState,
} from "./types.js";
export {
  isSuccess,
  getSuccessOrThrow,
  parseErrorDetails,
  createRequestState,
  getTotalAttempts,
} from "./types.js";
