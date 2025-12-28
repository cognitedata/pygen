import { describe, expect, it } from "vitest";
import {
  MultiRequestError,
  OAuth2Error,
  PygenAPIError,
  isMultiRequestError,
  isOAuth2Error,
  isPygenAPIError,
  type FailedRequest,
  type FailedResponse,
  type UpsertResult,
} from "@cognite/pygen-typescript";

describe("PygenAPIError", () => {
  it("should create error with message", () => {
    const error = new PygenAPIError("Test error message");

    expect(error.message).toBe("Test error message");
    expect(error.name).toBe("PygenAPIError");
  });

  it("should be instance of Error", () => {
    const error = new PygenAPIError("Test");

    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(PygenAPIError);
  });

  it("should have a stack trace", () => {
    const error = new PygenAPIError("Test");

    expect(error.stack).toBeDefined();
    expect(error.stack).toContain("PygenAPIError");
  });

  it("should be catchable as Error", () => {
    let caught: Error | undefined;
    try {
      throw new PygenAPIError("Test");
    } catch (e) {
      caught = e as Error;
    }

    expect(caught).toBeDefined();
    expect(caught?.message).toBe("Test");
  });
});

describe("OAuth2Error", () => {
  it("should create error with message", () => {
    const error = new OAuth2Error("Authentication failed");

    expect(error.message).toBe("Authentication failed");
    expect(error.name).toBe("OAuth2Error");
  });

  it("should inherit from PygenAPIError", () => {
    const error = new OAuth2Error("Test");

    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(PygenAPIError);
    expect(error).toBeInstanceOf(OAuth2Error);
  });

  it("should be catchable as PygenAPIError", () => {
    let caught: PygenAPIError | undefined;
    try {
      throw new OAuth2Error("Test");
    } catch (e) {
      if (e instanceof PygenAPIError) {
        caught = e;
      }
    }

    expect(caught).toBeDefined();
    expect(caught).toBeInstanceOf(OAuth2Error);
  });

  it("should have proper error name in stack trace", () => {
    const error = new OAuth2Error("Token expired");

    expect(error.stack).toBeDefined();
    expect(error.stack).toContain("OAuth2Error");
  });
});

describe("MultiRequestError", () => {
  const createFailedResponse = (
    statusCode: number,
    message: string,
  ): FailedResponse => ({
    kind: "failed_response",
    statusCode,
    body: JSON.stringify({ error: { code: statusCode, message } }),
    error: { code: statusCode, message },
  });

  const createFailedRequest = (errorMessage: string): FailedRequest => ({
    kind: "failed_request",
    error: errorMessage,
  });

  const createUpsertResult = (): UpsertResult => ({
    items: [],
    deleted: [],
  });

  it("should create error with failed responses only", () => {
    const failedResponses = [
      createFailedResponse(400, "Bad request"),
      createFailedResponse(500, "Server error"),
    ];
    const error = new MultiRequestError(failedResponses, [], createUpsertResult());

    expect(error.message).toBe("MultiRequestError: 2 failed responses");
    expect(error.name).toBe("MultiRequestError");
    expect(error.failedResponses).toHaveLength(2);
    expect(error.failedRequests).toHaveLength(0);
  });

  it("should create error with failed requests only", () => {
    const failedRequests = [
      createFailedRequest("Network timeout"),
      createFailedRequest("Connection refused"),
    ];
    const error = new MultiRequestError([], failedRequests, createUpsertResult());

    expect(error.message).toBe("MultiRequestError: 2 failed requests");
    expect(error.failedResponses).toHaveLength(0);
    expect(error.failedRequests).toHaveLength(2);
  });

  it("should create error with both failed responses and requests", () => {
    const failedResponses = [createFailedResponse(400, "Bad request")];
    const failedRequests = [createFailedRequest("Network timeout")];
    const error = new MultiRequestError(
      failedResponses,
      failedRequests,
      createUpsertResult(),
    );

    expect(error.message).toBe(
      "MultiRequestError: 1 failed responses; 1 failed requests",
    );
    expect(error.failedResponses).toHaveLength(1);
    expect(error.failedRequests).toHaveLength(1);
  });

  it("should inherit from PygenAPIError", () => {
    const error = new MultiRequestError([], [], createUpsertResult());

    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(PygenAPIError);
    expect(error).toBeInstanceOf(MultiRequestError);
  });

  it("should store the result", () => {
    const result: UpsertResult = {
      items: [
        {
          instanceType: "node",
          space: "mySpace",
          externalId: "item-1",
          version: 1,
          wasModified: true,
          createdTime: 1000,
          lastUpdatedTime: 1000,
        },
      ],
      deleted: [],
    };
    const error = new MultiRequestError([], [], result);

    expect(error.result).toBe(result);
    expect(error.result.items).toHaveLength(1);
  });

  it("should return error messages from failed responses", () => {
    const failedResponses = [
      createFailedResponse(400, "Bad request"),
      createFailedResponse(404, "Not found"),
    ];
    const error = new MultiRequestError(failedResponses, [], createUpsertResult());

    expect(error.getErrorMessages()).toEqual(["Bad request", "Not found"]);
  });

  it("should return error codes from failed responses", () => {
    const failedResponses = [
      createFailedResponse(400, "Bad request"),
      createFailedResponse(500, "Server error"),
    ];
    const error = new MultiRequestError(failedResponses, [], createUpsertResult());

    expect(error.getErrorCodes()).toEqual([400, 500]);
  });

  it("should detect rate limit errors", () => {
    const failedResponses = [
      createFailedResponse(429, "Rate limit exceeded"),
    ];
    const error = new MultiRequestError(failedResponses, [], createUpsertResult());

    expect(error.hasRateLimitErrors()).toBe(true);
  });

  it("should detect server errors", () => {
    const failedResponses = [
      createFailedResponse(500, "Internal server error"),
      createFailedResponse(502, "Bad gateway"),
    ];
    const error = new MultiRequestError(failedResponses, [], createUpsertResult());

    expect(error.hasServerErrors()).toBe(true);
    expect(error.hasClientErrors()).toBe(false);
  });

  it("should detect client errors", () => {
    const failedResponses = [
      createFailedResponse(400, "Bad request"),
      createFailedResponse(404, "Not found"),
    ];
    const error = new MultiRequestError(failedResponses, [], createUpsertResult());

    expect(error.hasClientErrors()).toBe(true);
    expect(error.hasServerErrors()).toBe(false);
  });

  it("should handle mixed status codes", () => {
    const failedResponses = [
      createFailedResponse(400, "Bad request"),
      createFailedResponse(429, "Rate limit"),
      createFailedResponse(500, "Server error"),
    ];
    const error = new MultiRequestError(failedResponses, [], createUpsertResult());

    expect(error.hasRateLimitErrors()).toBe(true);
    expect(error.hasServerErrors()).toBe(true);
    expect(error.hasClientErrors()).toBe(true);
  });

  it("should be catchable as PygenAPIError", () => {
    let caught: PygenAPIError | undefined;
    try {
      throw new MultiRequestError([], [], createUpsertResult());
    } catch (e) {
      if (e instanceof PygenAPIError) {
        caught = e;
      }
    }

    expect(caught).toBeDefined();
    expect(caught).toBeInstanceOf(MultiRequestError);
  });

  it("should have readonly failedResponses and failedRequests", () => {
    const failedResponses = [createFailedResponse(400, "Bad request")];
    const failedRequests = [createFailedRequest("Network error")];
    const error = new MultiRequestError(
      failedResponses,
      failedRequests,
      createUpsertResult(),
    );

    // TypeScript will prevent mutation at compile time
    // At runtime, we verify the arrays are preserved
    expect(error.failedResponses[0]).toEqual(failedResponses[0]);
    expect(error.failedRequests[0]).toEqual(failedRequests[0]);
  });
});

describe("Type guard functions", () => {
  it("isPygenAPIError should return true for PygenAPIError", () => {
    const error = new PygenAPIError("Test");
    expect(isPygenAPIError(error)).toBe(true);
  });

  it("isPygenAPIError should return true for OAuth2Error", () => {
    const error = new OAuth2Error("Test");
    expect(isPygenAPIError(error)).toBe(true);
  });

  it("isPygenAPIError should return true for MultiRequestError", () => {
    const error = new MultiRequestError([], [], { items: [], deleted: [] });
    expect(isPygenAPIError(error)).toBe(true);
  });

  it("isPygenAPIError should return false for regular Error", () => {
    const error = new Error("Test");
    expect(isPygenAPIError(error)).toBe(false);
  });

  it("isPygenAPIError should return false for non-errors", () => {
    expect(isPygenAPIError(null)).toBe(false);
    expect(isPygenAPIError(undefined)).toBe(false);
    expect(isPygenAPIError("error")).toBe(false);
    expect(isPygenAPIError({ message: "error" })).toBe(false);
  });

  it("isOAuth2Error should return true for OAuth2Error", () => {
    const error = new OAuth2Error("Test");
    expect(isOAuth2Error(error)).toBe(true);
  });

  it("isOAuth2Error should return false for PygenAPIError", () => {
    const error = new PygenAPIError("Test");
    expect(isOAuth2Error(error)).toBe(false);
  });

  it("isOAuth2Error should return false for MultiRequestError", () => {
    const error = new MultiRequestError([], [], { items: [], deleted: [] });
    expect(isOAuth2Error(error)).toBe(false);
  });

  it("isMultiRequestError should return true for MultiRequestError", () => {
    const error = new MultiRequestError([], [], { items: [], deleted: [] });
    expect(isMultiRequestError(error)).toBe(true);
  });

  it("isMultiRequestError should return false for PygenAPIError", () => {
    const error = new PygenAPIError("Test");
    expect(isMultiRequestError(error)).toBe(false);
  });

  it("isMultiRequestError should return false for OAuth2Error", () => {
    const error = new OAuth2Error("Test");
    expect(isMultiRequestError(error)).toBe(false);
  });
});

describe("Error hierarchy integration", () => {
  it("should allow catching all Pygen errors with PygenAPIError", () => {
    const errors: unknown[] = [
      new PygenAPIError("API error"),
      new OAuth2Error("Auth error"),
      new MultiRequestError([], [], { items: [], deleted: [] }),
    ];

    const caughtErrors: PygenAPIError[] = [];
    for (const error of errors) {
      try {
        throw error;
      } catch (e) {
        if (e instanceof PygenAPIError) {
          caughtErrors.push(e);
        }
      }
    }

    expect(caughtErrors).toHaveLength(3);
  });

  it("should allow distinguishing error types", () => {
    const errors: unknown[] = [
      new PygenAPIError("API error"),
      new OAuth2Error("Auth error"),
      new MultiRequestError([], [], { items: [], deleted: [] }),
    ];

    let apiErrors = 0;
    let authErrors = 0;
    let multiErrors = 0;

    for (const error of errors) {
      if (error instanceof MultiRequestError) {
        multiErrors++;
      } else if (error instanceof OAuth2Error) {
        authErrors++;
      } else if (error instanceof PygenAPIError) {
        apiErrors++;
      }
    }

    expect(apiErrors).toBe(1);
    expect(authErrors).toBe(1);
    expect(multiErrors).toBe(1);
  });
});

