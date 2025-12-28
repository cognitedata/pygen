import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  HTTPClient,
  TokenCredentials,
  isSuccess,
  type Credentials,
} from "@cognite/pygen-typescript";

describe("HTTPClient", () => {
  let originalFetch: typeof global.fetch;

  beforeEach((): void => {
    originalFetch = global.fetch;
    vi.useFakeTimers();
  });

  afterEach((): void => {
    global.fetch = originalFetch;
    vi.useRealTimers();
  });

  const createClient = (): HTTPClient => {
    return new HTTPClient({
      baseUrl: "https://api.cognitedata.com",
      project: "test-project",
      credentials: new TokenCredentials("test-token"),
      timeout: 5000,
    });
  };

  describe("constructor", () => {
    it("should create client with default options", (): void => {
      const client = new HTTPClient({
        baseUrl: "https://api.cognitedata.com",
        project: "my-project",
        credentials: new TokenCredentials("token"),
      });

      expect(client).toBeInstanceOf(HTTPClient);
    });

    it("should accept custom options", (): void => {
      const client = new HTTPClient(
        {
          baseUrl: "https://api.cognitedata.com",
          project: "my-project",
          credentials: new TokenCredentials("token"),
          timeout: 60000,
          clientName: "my-app",
          maxRetries: 5,
        },
        {
          maxRetryBackoff: 30,
        }
      );

      expect(client).toBeInstanceOf(HTTPClient);
    });

    it("should expose project name", (): void => {
      const client = new HTTPClient({
        baseUrl: "https://api.cognitedata.com",
        project: "my-project",
        credentials: new TokenCredentials("token"),
      });

      expect(client.projectName).toBe("my-project");
    });
  });

  describe("request", () => {
    it("should make successful GET request", async () => {
      const mockResponse = { items: [{ id: 1 }] };
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        text: () => Promise.resolve(JSON.stringify(mockResponse)),
        headers: new Headers(),
      });

      const client = createClient();

      const resultPromise = client.request({
        endpointUrl: "/api/v1/projects/test/models",
        method: "GET",
      });

      await vi.runAllTimersAsync();
      const result = await resultPromise;

      expect(isSuccess(result)).toBe(true);
      if (isSuccess(result)) {
        expect(result.statusCode).toBe(200);
        expect(JSON.parse(result.body)).toEqual(mockResponse);
      }
    });

    it("should make successful POST request with body", async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 201,
        text: () => Promise.resolve('{"created": true}'),
        headers: new Headers(),
      });

      const client = createClient();

      const resultPromise = client.request({
        endpointUrl: "/api/v1/projects/test/models",
        method: "POST",
        body: { name: "test-model" },
      });

      await vi.runAllTimersAsync();
      const result = await resultPromise;

      expect(isSuccess(result)).toBe(true);
      expect(global.fetch).toHaveBeenCalledWith(
        expect.stringContaining("/api/v1/projects/test/models"),
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({ name: "test-model" }),
        })
      );
    });

    it("should include correct headers", async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        text: (): Promise<string> => Promise.resolve("{}"),
        headers: new Headers(),
      });

      const client = new HTTPClient({
        baseUrl: "https://api.cognitedata.com",
        project: "test",
        credentials: new TokenCredentials("my-token"),
        clientName: "my-app",
        apiSubversion: "beta",
      });

      const resultPromise = client.request({
        endpointUrl: "/test",
        method: "GET",
      });

      await vi.runAllTimersAsync();
      await resultPromise;

      expect(global.fetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: {
            Authorization: "Bearer my-token",
            "Content-Type": "application/json",
            Accept: "application/json",
            "x-cdp-app": "my-app",
            "cdf-version": "beta",
          },
        })
      );
    });

    it("should return failed response for 4xx errors", async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 404,
        text: () =>
          Promise.resolve(
            JSON.stringify({
              error: { code: 404, message: "Not found" },
            })
          ),
        headers: new Headers(),
      });

      const client = createClient();

      const resultPromise = client.request({
        endpointUrl: "/api/v1/missing",
        method: "GET",
      });

      await vi.runAllTimersAsync();
      const result = await resultPromise;

      expect(result.kind).toBe("failed_response");
      if (result.kind === "failed_response") {
        expect(result.statusCode).toBe(404);
        expect(result.error.message).toBe("Not found");
      }
    });

    it("should add query parameters to URL", async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        text: () => Promise.resolve("{}"),
        headers: new Headers(),
      });

      const client = createClient();

      const resultPromise = client.request({
        endpointUrl: "/api/v1/test",
        method: "GET",
        parameters: {
          limit: 100,
          includeDeleted: true,
        },
      });

      await vi.runAllTimersAsync();
      await resultPromise;

      const calledUrl = (global.fetch as ReturnType<typeof vi.fn>).mock.calls[0]?.[0] as string;
      expect(calledUrl).toContain("limit=100");
      expect(calledUrl).toContain("includeDeleted=true");
    });
  });

  describe("retry behavior", () => {
    it("should retry on 429 with Retry-After header", async () => {
      let callCount = 0;
      global.fetch = vi.fn().mockImplementation(() => {
        callCount++;
        if (callCount === 1) {
          return Promise.resolve({
            ok: false,
            status: 429,
            text: () => Promise.resolve('{"error":{"code":429,"message":"Rate limited"}}'),
            headers: new Headers({ "Retry-After": "1" }),
          });
        }
        return Promise.resolve({
          ok: true,
          status: 200,
          text: () => Promise.resolve('{"success":true}'),
          headers: new Headers(),
        });
      });

      const client = createClient();

      const resultPromise = client.request({
        endpointUrl: "/api/v1/test",
        method: "GET",
      });

      // Run timers to process retries
      await vi.runAllTimersAsync();
      const result = await resultPromise;

      expect(callCount).toBe(2);
      expect(result.kind).toBe("success");
    });

    it("should retry on 503 with exponential backoff", async () => {
      let callCount = 0;
      global.fetch = vi.fn().mockImplementation(() => {
        callCount++;
        if (callCount <= 2) {
          return Promise.resolve({
            ok: false,
            status: 503,
            text: () => Promise.resolve('{"error":{"code":503,"message":"Service unavailable"}}'),
            headers: new Headers(),
          });
        }
        return Promise.resolve({
          ok: true,
          status: 200,
          text: () => Promise.resolve('{"success":true}'),
          headers: new Headers(),
        });
      });

      const client = createClient();

      const resultPromise = client.request({
        endpointUrl: "/api/v1/test",
        method: "GET",
      });

      await vi.runAllTimersAsync();
      const result = await resultPromise;

      expect(callCount).toBe(3);
      expect(result.kind).toBe("success");
    });

    it("should not retry on non-retryable status codes", async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 400,
        text: () => Promise.resolve('{"error":{"code":400,"message":"Bad request"}}'),
        headers: new Headers(),
      });

      const client = createClient();

      const resultPromise = client.request({
        endpointUrl: "/api/v1/test",
        method: "GET",
      });

      await vi.runAllTimersAsync();
      const result = await resultPromise;

      expect(global.fetch).toHaveBeenCalledTimes(1);
      expect(result.kind).toBe("failed_response");
    });
  });

  describe("credentials integration", () => {
    it("should call credentials for each request", async () => {
      let requestCount = 0;
      global.fetch = vi.fn().mockImplementation((): Promise<Response> => {
        requestCount++;
        return Promise.resolve({
          ok: true,
          status: 200,
          text: (): Promise<string> => Promise.resolve("{}"),
          headers: new Headers(),
        } as Response);
      });

      const authorizationHeaderMock = vi
        .fn()
        .mockResolvedValue(["Authorization", "Bearer dynamic-token"]);

      const mockCredentials: Credentials = {
        authorizationHeader: authorizationHeaderMock,
      };

      const client = new HTTPClient({
        baseUrl: "https://api.cognitedata.com",
        project: "test",
        credentials: mockCredentials,
      });

      // Make first request
      const result1Promise = client.request({
        endpointUrl: "/test1",
        method: "GET",
      });
      await vi.runAllTimersAsync();
      await result1Promise;

      // Make second request
      const result2Promise = client.request({
        endpointUrl: "/test2",
        method: "GET",
      });
      await vi.runAllTimersAsync();
      await result2Promise;

      // Credentials should be called for each request
      expect(authorizationHeaderMock).toHaveBeenCalledTimes(2);
      expect(requestCount).toBe(2);
    });

    it("should use token from credentials in Authorization header", async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        text: (): Promise<string> => Promise.resolve("{}"),
        headers: new Headers(),
      });

      const client = new HTTPClient({
        baseUrl: "https://api.cognitedata.com",
        project: "test",
        credentials: new TokenCredentials("my-secret-token"),
      });

      const resultPromise = client.request({
        endpointUrl: "/test",
        method: "GET",
      });

      await vi.runAllTimersAsync();
      await resultPromise;

      expect(global.fetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
          headers: expect.objectContaining({
            Authorization: "Bearer my-secret-token",
          }),
        })
      );
    });
  });

  describe("error handling", () => {
    it("should retry on timeout (AbortError) and eventually succeed", async () => {
      let callCount = 0;
      global.fetch = vi.fn().mockImplementation((): Promise<Response> => {
        callCount++;
        if (callCount === 1) {
          const error = new Error("The operation was aborted");
          error.name = "AbortError";
          return Promise.reject(error);
        }
        return Promise.resolve({
          ok: true,
          status: 200,
          text: (): Promise<string> => Promise.resolve('{"success":true}'),
          headers: new Headers(),
        } as Response);
      });

      const client = createClient();

      const resultPromise = client.request({
        endpointUrl: "/api/v1/test",
        method: "GET",
      });

      await vi.runAllTimersAsync();
      const result = await resultPromise;

      expect(callCount).toBe(2);
      expect(result.kind).toBe("success");
    });

    it("should retry on connection error (TypeError) and eventually succeed", async () => {
      let callCount = 0;
      global.fetch = vi.fn().mockImplementation((): Promise<Response> => {
        callCount++;
        if (callCount === 1) {
          return Promise.reject(new TypeError("fetch failed"));
        }
        return Promise.resolve({
          ok: true,
          status: 200,
          text: (): Promise<string> => Promise.resolve('{"success":true}'),
          headers: new Headers(),
        } as Response);
      });

      const client = createClient();

      const resultPromise = client.request({
        endpointUrl: "/api/v1/test",
        method: "GET",
      });

      await vi.runAllTimersAsync();
      const result = await resultPromise;

      expect(callCount).toBe(2);
      expect(result.kind).toBe("success");
    });

    it("should return failed request after max timeout retries", async () => {
      const abortError = new Error("The operation was aborted");
      abortError.name = "AbortError";
      global.fetch = vi.fn().mockRejectedValue(abortError);

      const client = new HTTPClient({
        baseUrl: "https://api.cognitedata.com",
        project: "test",
        credentials: new TokenCredentials("token"),
        maxRetries: 2,
      });

      const resultPromise = client.request({
        endpointUrl: "/api/v1/test",
        method: "GET",
      });

      await vi.runAllTimersAsync();
      const result = await resultPromise;

      expect(result.kind).toBe("failed_request");
      if (result.kind === "failed_request") {
        expect(result.error).toContain("aborted");
      }
    });

    it("should return failed request after max connection retries", async () => {
      global.fetch = vi.fn().mockRejectedValue(new TypeError("fetch failed"));

      const client = new HTTPClient({
        baseUrl: "https://api.cognitedata.com",
        project: "test",
        credentials: new TokenCredentials("token"),
        maxRetries: 2,
      });

      const resultPromise = client.request({
        endpointUrl: "/api/v1/test",
        method: "GET",
      });

      await vi.runAllTimersAsync();
      const result = await resultPromise;

      expect(result.kind).toBe("failed_request");
      if (result.kind === "failed_request") {
        expect(result.error).toContain("fetch failed");
      }
    });

    it("should return failed request for unknown errors", async () => {
      global.fetch = vi.fn().mockRejectedValue(new Error("Unknown error"));

      const client = createClient();

      const resultPromise = client.request({
        endpointUrl: "/api/v1/test",
        method: "GET",
      });

      await vi.runAllTimersAsync();
      const result = await resultPromise;

      expect(result.kind).toBe("failed_request");
      if (result.kind === "failed_request") {
        expect(result.error).toBe("Unknown error");
      }
    });

    it("should handle non-Error thrown values", async () => {
      global.fetch = vi.fn().mockRejectedValue("string error");

      const client = createClient();

      const resultPromise = client.request({
        endpointUrl: "/api/v1/test",
        method: "GET",
      });

      await vi.runAllTimersAsync();
      const result = await resultPromise;

      expect(result.kind).toBe("failed_request");
      if (result.kind === "failed_request") {
        expect(result.error).toBe("string error");
      }
    });
  });
});
