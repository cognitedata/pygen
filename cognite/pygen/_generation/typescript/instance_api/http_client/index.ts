/**
 * HTTP client module for CDF API communication.
 *
 * @packageDocumentation
 */

export { HTTPClient } from "./client.ts";
export type {
  HTTPMethod,
  HTTPResult,
  SuccessResponse,
  FailedResponse,
  FailedRequest,
  ErrorDetails,
  RequestMessage,
  RequestState,
} from "./types.ts";
export {
  isSuccess,
  getSuccessOrThrow,
  parseErrorDetails,
  createRequestState,
  getTotalAttempts,
} from "./types.ts";
