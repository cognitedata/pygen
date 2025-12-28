/**
 * HTTP client module for CDF API communication.
 *
 * @packageDocumentation
 */

export { HTTPClient } from "./client.ts";
export type {
  ErrorDetails,
  FailedRequest,
  FailedResponse,
  HTTPMethod,
  HTTPResult,
  RequestMessage,
  RequestState,
  SuccessResponse,
} from "./types.ts";
export {
  createRequestState,
  getSuccessOrThrow,
  getTotalAttempts,
  isSuccess,
  parseErrorDetails,
} from "./types.ts";
