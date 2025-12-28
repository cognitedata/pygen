/**
 * HTTP client for CDF API communication.
 *
 * Handles retry logic, rate limiting, and error handling.
 *
 * @packageDocumentation
 */

import type {
  HTTPResult,
  RequestMessage,
  RequestState,
  SuccessResponse,
  FailedResponse,
  FailedRequest,
} from "./types.js";
import { createRequestState, getTotalAttempts, parseErrorDetails } from "./types.js";

/** Configuration for the HTTP client */
export interface HTTPClientConfig {
  /** Base URL for CDF API (e.g., "https://api.cognitedata.com") */
  readonly baseUrl: string;
  /** CDF project name */
  readonly project: string;
  /** Function to get authorization header value */
  readonly getAuthHeader: () => Promise<string> | string;
  /** Client application name for x-cdp-app header */
  readonly clientName?: string;
  /** Request timeout in milliseconds (default: 30000) */
  readonly timeout?: number;
  /** CDF API version (default: "v1") */
  readonly apiSubversion?: string;
}

/** Options for HTTP client behavior */
export interface HTTPClientOptions {
  /** Maximum retry attempts (default: 10) */
  readonly maxRetries?: number;
  /** HTTP status codes that should trigger a retry */
  readonly retryStatusCodes?: ReadonlySet<number>;
  /** Maximum backoff time in seconds (default: 60) */
  readonly maxRetryBackoff?: number;
}

const DEFAULT_RETRY_STATUS_CODES = new Set([408, 429, 502, 503, 504]);

/**
 * HTTP client for making requests to CDF API.
 *
 * Handles authentication, retries, rate limiting, and error handling.
 *
 * @example
 * ```typescript
 * const client = new HTTPClient({
 *   baseUrl: "https://api.cognitedata.com",
 *   project: "my-project",
 *   getAuthHeader: () => `Bearer ${token}`,
 * });
 *
 * const result = await client.request({
 *   endpointUrl: "/api/v1/projects/my-project/models/datamodels",
 *   method: "GET",
 * });
 * ```
 */
export class HTTPClient {
  private readonly config: HTTPClientConfig;
  private readonly maxRetries: number;
  private readonly retryStatusCodes: ReadonlySet<number>;
  private readonly maxRetryBackoff: number;
  private readonly timeout: number;

  constructor(config: HTTPClientConfig, options: HTTPClientOptions = {}) {
    this.config = config;
    this.maxRetries = options.maxRetries ?? 10;
    this.retryStatusCodes = options.retryStatusCodes ?? DEFAULT_RETRY_STATUS_CODES;
    this.maxRetryBackoff = options.maxRetryBackoff ?? 60;
    this.timeout = config.timeout ?? 30000;
  }

  /**
   * Sends an HTTP request with automatic retries.
   */
  async request(message: RequestMessage): Promise<HTTPResult> {
    const state = createRequestState(message);
    return this.requestWithRetries(state);
  }

  private async requestWithRetries(state: RequestState): Promise<HTTPResult> {
    while (true) {
      const result = await this.executeRequest(state);
      if (result.kind !== "retry") {
        return result.result;
      }
      // Continue loop for retry
    }
  }

  private async executeRequest(
    state: RequestState
  ): Promise<{ kind: "done"; result: HTTPResult } | { kind: "retry" }> {
    try {
      const response = await this.makeRequest(state.message);
      return this.handleResponse(response, state);
    } catch (error) {
      return this.handleError(error, state);
    }
  }

  private async makeRequest(message: RequestMessage): Promise<Response> {
    const headers = await this.createHeaders(message);
    const url = this.buildUrl(message);

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeout);

    try {
      const init: RequestInit = {
        method: message.method,
        headers,
        signal: controller.signal,
      };
      if (message.body) {
        init.body = JSON.stringify(message.body);
      }
      return await fetch(url, init);
    } finally {
      clearTimeout(timeoutId);
    }
  }

  private async createHeaders(message: RequestMessage): Promise<Record<string, string>> {
    const authHeader = await this.config.getAuthHeader();
    return {
      Authorization: authHeader,
      "Content-Type": message.contentType ?? "application/json",
      Accept: message.accept ?? "application/json",
      "x-cdp-app": this.config.clientName ?? "pygen-typescript",
      "cdf-version": message.apiVersion ?? this.config.apiSubversion ?? "v1",
    };
  }

  private buildUrl(message: RequestMessage): string {
    const url = new URL(message.endpointUrl, this.config.baseUrl);
    if (message.parameters) {
      for (const [key, value] of Object.entries(message.parameters)) {
        url.searchParams.set(key, String(value));
      }
    }
    return url.toString();
  }

  private async handleResponse(
    response: Response,
    state: RequestState
  ): Promise<{ kind: "done"; result: HTTPResult } | { kind: "retry" }> {
    const body = await response.text();

    if (response.ok) {
      const result: SuccessResponse = {
        kind: "success",
        statusCode: response.status,
        body,
      };
      return { kind: "done", result };
    }

    // Check for retry
    const retryAfter = this.getRetryAfter(response);
    if (
      retryAfter !== undefined &&
      response.status === 429 &&
      state.statusAttempt < this.maxRetries
    ) {
      state.statusAttempt++;
      await this.sleep(retryAfter * 1000);
      return { kind: "retry" };
    }

    if (state.statusAttempt < this.maxRetries && this.retryStatusCodes.has(response.status)) {
      state.statusAttempt++;
      await this.sleep(this.backoffTime(getTotalAttempts(state)));
      return { kind: "retry" };
    }

    // Permanent failure
    const result: FailedResponse = {
      kind: "failed_response",
      statusCode: response.status,
      body,
      error: parseErrorDetails(response.status, body),
    };
    return { kind: "done", result };
  }

  private async handleError(
    error: unknown,
    state: RequestState
  ): Promise<{ kind: "done"; result: HTTPResult } | { kind: "retry" }> {
    const isTimeout =
      error instanceof Error && (error.name === "AbortError" || error.name === "TimeoutError");
    const isConnectionError = error instanceof TypeError && error.message.includes("fetch");

    if (isTimeout) {
      state.readAttempt++;
      if (state.readAttempt <= this.maxRetries) {
        await this.sleep(this.backoffTime(getTotalAttempts(state)));
        return { kind: "retry" };
      }
    } else if (isConnectionError) {
      state.connectAttempt++;
      if (state.connectAttempt <= this.maxRetries) {
        await this.sleep(this.backoffTime(getTotalAttempts(state)));
        return { kind: "retry" };
      }
    }

    const result: FailedRequest = {
      kind: "failed_request",
      error: error instanceof Error ? error.message : String(error),
    };
    return { kind: "done", result };
  }

  private getRetryAfter(response: Response): number | undefined {
    const header = response.headers.get("Retry-After");
    if (!header) return undefined;
    const value = parseFloat(header);
    return isNaN(value) ? undefined : value;
  }

  private backoffTime(attempts: number): number {
    const baseBackoff = 0.5 * Math.pow(2, attempts);
    const capped = Math.min(baseBackoff, this.maxRetryBackoff);
    return capped * Math.random() * 1000; // Convert to milliseconds
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
