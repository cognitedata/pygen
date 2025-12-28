/**
 * HTTP client for CDF API communication.
 *
 * Handles retry logic, rate limiting, and error handling.
 *
 * @packageDocumentation
 */

import type { Credentials, PygenClientConfig } from "../auth/index.js";
import type {
  HTTPResult,
  RequestMessage,
  RequestState,
  SuccessResponse,
  FailedResponse,
  FailedRequest,
} from "./types.js";
import { createRequestState, getTotalAttempts, parseErrorDetails } from "./types.js";

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
 *   credentials: new TokenCredentials("my-token"),
 * });
 *
 * const result = await client.request({
 *   endpointUrl: "/api/v1/projects/my-project/models/datamodels",
 *   method: "GET",
 * });
 * ```
 */
export class HTTPClient {
  private readonly credentials: Credentials;
  private readonly baseUrl: string;
  private readonly project: string;
  private readonly clientName: string;
  private readonly apiSubversion: string;
  private readonly timeout: number;
  private readonly maxRetries: number;
  private readonly retryStatusCodes: ReadonlySet<number>;
  private readonly maxRetryBackoff: number;

  constructor(config: PygenClientConfig) {
    this.credentials = config.credentials;
    this.baseUrl = config.baseUrl;
    this.project = config.project;
    this.clientName = config.clientName ?? "pygen-typescript";
    this.apiSubversion = config.apiSubversion ?? "20230101";
    this.timeout = config.timeout ?? 30000;
    this.maxRetries = config.maxRetries ?? 10;
    this.retryStatusCodes = config.retryStatusCodes ?? DEFAULT_RETRY_STATUS_CODES;
    this.maxRetryBackoff = config.maxRetryBackoff ?? 60;
  }

  /**
   * Get the project name this client is configured for.
   */
  get projectName(): string {
    return this.project;
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
      return await this.handleError(error, state);
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
    const [, authValue] = await this.credentials.authorizationHeader();
    return {
      Authorization: authValue,
      "Content-Type": message.contentType ?? "application/json",
      Accept: message.accept ?? "application/json",
      "x-cdp-app": this.clientName,
      "cdf-version": message.apiVersion ?? this.apiSubversion,
    };
  }

  private buildUrl(message: RequestMessage): string {
    const url = new URL(message.endpointUrl, this.baseUrl);
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
    const isConnectionError = error instanceof TypeError;

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
