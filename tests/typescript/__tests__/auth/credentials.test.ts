import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  type Credentials,
  type OAuth2Config,
  OAuth2Error,
  OAuthClientCredentials,
  type PygenClientConfig,
  TokenCredentials,
} from "@cognite/pygen-typescript";

describe("TokenCredentials", () => {
  it("should return authorization header with bearer token", async () => {
    const credentials = new TokenCredentials("my-test-token");

    const [name, value] = await credentials.authorizationHeader();

    expect(name).toBe("Authorization");
    expect(value).toBe("Bearer my-test-token");
  });

  it("should implement Credentials interface", async () => {
    const credentials: Credentials = new TokenCredentials("token");

    const header = await credentials.authorizationHeader();

    expect(header).toEqual(["Authorization", "Bearer token"]);
  });

  it("should handle tokens with special characters", async () => {
    const token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test-payload.signature";
    const credentials = new TokenCredentials(token);

    const [, value] = await credentials.authorizationHeader();

    expect(value).toBe(`Bearer ${token}`);
  });
});

describe("OAuthClientCredentials", () => {
  const mockConfig: OAuth2Config = {
    tokenUrl: "https://auth.example.com/oauth/token",
    clientId: "test-client-id",
    clientSecret: "test-client-secret",
    scopes: ["read", "write"],
    audience: "https://api.example.com",
  };

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("should fetch token and return authorization header", async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          access_token: "fetched-token",
          expires_in: 3600,
        }),
    });
    vi.stubGlobal("fetch", mockFetch);

    const credentials = new OAuthClientCredentials(mockConfig);
    const [name, value] = await credentials.authorizationHeader();

    expect(name).toBe("Authorization");
    expect(value).toBe("Bearer fetched-token");
    expect(mockFetch).toHaveBeenCalledWith(
      mockConfig.tokenUrl,
      expect.objectContaining({
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
      }),
    );
  });

  it("should include scopes and audience in token request", async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ access_token: "token", expires_in: 3600 }),
    });
    vi.stubGlobal("fetch", mockFetch);

    const credentials = new OAuthClientCredentials(mockConfig);
    await credentials.authorizationHeader();

    const callArgs = mockFetch.mock.calls[0] as [string, RequestInit];
    const body = new URLSearchParams(callArgs[1].body as string);
    expect(body.get("scope")).toBe("read write");
    expect(body.get("audience")).toBe("https://api.example.com");
  });

  it("should reuse cached token before expiry", async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ access_token: "token", expires_in: 3600 }),
    });
    vi.stubGlobal("fetch", mockFetch);

    const credentials = new OAuthClientCredentials(mockConfig);

    await credentials.authorizationHeader();
    await credentials.authorizationHeader();

    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("should refresh token when near expiry", async () => {
    let callCount = 0;
    const mockFetch = vi.fn().mockImplementation(() => {
      callCount++;
      const tokenName = "token-" + String(callCount);
      return Promise.resolve({
        ok: true,
        json: () =>
          Promise.resolve({
            access_token: tokenName,
            expires_in: 600,
          }),
      });
    });
    vi.stubGlobal("fetch", mockFetch);

    const credentials = new OAuthClientCredentials({
      ...mockConfig,
      refreshMargin: 300,
    });

    const [, firstValue] = await credentials.authorizationHeader();
    expect(firstValue).toBe("Bearer token-1");

    // Advance time to 301 seconds before expiry (within refresh margin)
    vi.advanceTimersByTime(301 * 1000);

    const [, secondValue] = await credentials.authorizationHeader();
    expect(secondValue).toBe("Bearer token-2");
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  it("should throw OAuth2Error on fetch failure", async () => {
    const mockFetch = vi.fn().mockRejectedValue(new Error("Network error"));
    vi.stubGlobal("fetch", mockFetch);

    const credentials = new OAuthClientCredentials(mockConfig);

    await expect(credentials.authorizationHeader()).rejects.toThrow(OAuth2Error);
    await expect(credentials.authorizationHeader()).rejects.toThrow(
      "Token request failed: Error: Network error",
    );
  });

  it("should throw OAuth2Error on non-ok response", async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 401,
      text: () => Promise.resolve("Unauthorized"),
    });
    vi.stubGlobal("fetch", mockFetch);

    const credentials = new OAuthClientCredentials(mockConfig);

    await expect(credentials.authorizationHeader()).rejects.toThrow(OAuth2Error);
    await expect(credentials.authorizationHeader()).rejects.toThrow(
      "Token request failed with status 401: Unauthorized",
    );
  });

  it("should throw OAuth2Error on invalid JSON response", async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.reject(new Error("Invalid JSON")),
    });
    vi.stubGlobal("fetch", mockFetch);

    const credentials = new OAuthClientCredentials(mockConfig);

    await expect(credentials.authorizationHeader()).rejects.toThrow(OAuth2Error);
    await expect(credentials.authorizationHeader()).rejects.toThrow(
      "Invalid token response from server",
    );
  });

  it("should throw OAuth2Error when response missing access_token", async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ expires_in: 3600 }),
    });
    vi.stubGlobal("fetch", mockFetch);

    const credentials = new OAuthClientCredentials(mockConfig);

    await expect(credentials.authorizationHeader()).rejects.toThrow(OAuth2Error);
    await expect(credentials.authorizationHeader()).rejects.toThrow(
      "Token response missing access_token",
    );
  });

  it("should use default expires_in of 3600 if not provided", async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ access_token: "token" }),
    });
    vi.stubGlobal("fetch", mockFetch);

    const credentials = new OAuthClientCredentials(mockConfig);

    await credentials.authorizationHeader();

    // Token should still be valid after 3299 seconds (with 300s margin)
    vi.advanceTimersByTime(3299 * 1000);
    await credentials.authorizationHeader();
    expect(mockFetch).toHaveBeenCalledTimes(1);

    // But should refresh after 3301 seconds
    vi.advanceTimersByTime(2 * 1000);
    await credentials.authorizationHeader();
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  it("should handle concurrent refresh requests", async () => {
    let resolveFirst: (() => void) | undefined;
    const mockFetch = vi.fn().mockImplementation(() => {
      return new Promise((resolve) => {
        resolveFirst = (): void => {
          resolve({
            ok: true,
            json: () => Promise.resolve({ access_token: "token", expires_in: 3600 }),
          });
        };
      });
    });
    vi.stubGlobal("fetch", mockFetch);

    const credentials = new OAuthClientCredentials(mockConfig);

    const promise1 = credentials.authorizationHeader();
    const promise2 = credentials.authorizationHeader();

    // Both should be waiting for the same refresh
    expect(mockFetch).toHaveBeenCalledTimes(1);

    // Resolve the fetch
    resolveFirst?.();

    const [result1, result2] = await Promise.all([promise1, promise2]);
    expect(result1).toEqual(result2);
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("should work without optional config fields", async () => {
    const minimalConfig: OAuth2Config = {
      tokenUrl: "https://auth.example.com/token",
      clientId: "client",
      clientSecret: "secret",
    };

    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ access_token: "token" }),
    });
    vi.stubGlobal("fetch", mockFetch);

    const credentials = new OAuthClientCredentials(minimalConfig);
    const [name, value] = await credentials.authorizationHeader();

    expect(name).toBe("Authorization");
    expect(value).toBe("Bearer token");

    const callArgs = mockFetch.mock.calls[0] as [string, RequestInit];
    const body = new URLSearchParams(callArgs[1].body as string);
    expect(body.has("scope")).toBe(false);
    expect(body.has("audience")).toBe(false);
  });

  it("should clear token state on close", async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ access_token: "token", expires_in: 3600 }),
    });
    vi.stubGlobal("fetch", mockFetch);

    const credentials = new OAuthClientCredentials(mockConfig);
    await credentials.authorizationHeader();
    expect(mockFetch).toHaveBeenCalledTimes(1);

    credentials.close();

    // After close, should fetch new token
    await credentials.authorizationHeader();
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });
});

describe("PygenClientConfig", () => {
  it("should allow configuration with TokenCredentials", () => {
    const config: PygenClientConfig = {
      baseUrl: "https://api.cognitedata.com",
      project: "my-project",
      credentials: new TokenCredentials("my-token"),
    };

    expect(config.baseUrl).toBe("https://api.cognitedata.com");
    expect(config.project).toBe("my-project");
    expect(config.credentials).toBeInstanceOf(TokenCredentials);
  });

  it("should allow optional timeout and maxRetries", () => {
    const config: PygenClientConfig = {
      baseUrl: "https://api.cognitedata.com",
      project: "my-project",
      credentials: new TokenCredentials("token"),
      timeout: 60000,
      maxRetries: 5,
    };

    expect(config.timeout).toBe(60000);
    expect(config.maxRetries).toBe(5);
  });
});
