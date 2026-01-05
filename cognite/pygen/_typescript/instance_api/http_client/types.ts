/**
 * HTTP client types for CDF API communication.
 *
 * @packageDocumentation
 */

/** HTTP methods supported by the client */
export type HTTPMethod = "GET" | "POST" | "PATCH" | "DELETE" | "PUT";

/** Error details structure from CDF API responses */
export interface ErrorDetails {
  readonly code: number;
  readonly message: string;
  readonly missing?: readonly unknown[];
  readonly duplicated?: readonly unknown[];
}

/** Base interface for all HTTP results */
interface HTTPResultBase {
  readonly kind: "success" | "failed_response" | "failed_request";
}

/** Successful HTTP response */
export interface SuccessResponse extends HTTPResultBase {
  readonly kind: "success";
  readonly statusCode: number;
  readonly body: string;
}

/** Failed HTTP response (received response but with error status) */
export interface FailedResponse extends HTTPResultBase {
  readonly kind: "failed_response";
  readonly statusCode: number;
  readonly body: string;
  readonly error: ErrorDetails;
}

/** Failed request (no response received) */
export interface FailedRequest extends HTTPResultBase {
  readonly kind: "failed_request";
  readonly error: string;
}

/** Union type for all possible HTTP results */
export type HTTPResult = SuccessResponse | FailedResponse | FailedRequest;

/** Request message for HTTP client */
export interface RequestMessage {
  readonly endpointUrl: string;
  readonly method: HTTPMethod;
  readonly body?: Record<string, unknown>;
  readonly parameters?: Record<string, string | number | boolean>;
  readonly apiVersion?: string;
  readonly contentType?: string;
  readonly accept?: string;
}

/** Internal request state tracking retry attempts */
export interface RequestState {
  readonly message: RequestMessage;
  connectAttempt: number;
  readAttempt: number;
  statusAttempt: number;
}

/**
 * Creates a new RequestState from a RequestMessage.
 */
export function createRequestState(message: RequestMessage): RequestState {
  return {
    message,
    connectAttempt: 0,
    readAttempt: 0,
    statusAttempt: 0,
  };
}

/**
 * Gets total attempts across all retry types.
 */
export function getTotalAttempts(state: RequestState): number {
  return state.connectAttempt + state.readAttempt + state.statusAttempt;
}

/**
 * Parses error details from a response body.
 */
export function parseErrorDetails(statusCode: number, body: string): ErrorDetails {
  try {
    const parsed = JSON.parse(body) as { error?: ErrorDetails };
    if (parsed.error) {
      return parsed.error;
    }
  } catch {
    // Ignore parse and type errors
  }
  return { code: statusCode, message: body };
}

/**
 * Type guard to check if result is successful.
 */
export function isSuccess(result: HTTPResult): result is SuccessResponse {
  return result.kind === "success";
}

/**
 * Gets the success response or throws an error.
 */
export function getSuccessOrThrow(result: HTTPResult): SuccessResponse {
  if (result.kind === "success") {
    return result;
  } else if (result.kind === "failed_response") {
    throw new Error(`Request failed with status ${result.statusCode}: ${result.error.message}`);
  } else {
    throw new Error(`Request failed: ${result.error}`);
  }
}
