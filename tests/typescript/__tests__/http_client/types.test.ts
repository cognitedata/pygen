import { describe, expect, it } from "vitest";
import {
  createRequestState,
  type FailedRequest,
  type FailedResponse,
  getSuccessOrThrow,
  getTotalAttempts,
  type HTTPResult,
  isSuccess,
  parseErrorDetails,
  type RequestMessage,
  type SuccessResponse,
} from "@cognite/pygen-_typescript";

describe("HTTP Client Types", () => {
  describe("createRequestState", () => {
    it("should create a request state with zero attempts", () => {
      const message: RequestMessage = {
        endpointUrl: "/api/v1/test",
        method: "GET",
      };

      const state = createRequestState(message);

      expect(state).toEqual({
        message,
        connectAttempt: 0,
        readAttempt: 0,
        statusAttempt: 0,
      });
    });
  });

  describe("getTotalAttempts", () => {
    it("should return sum of all attempts", () => {
      const state = {
        message: { endpointUrl: "/test", method: "GET" as const },
        connectAttempt: 1,
        readAttempt: 2,
        statusAttempt: 3,
      };

      expect(getTotalAttempts(state)).toBe(6);
    });
  });

  describe("parseErrorDetails", () => {
    it("should parse CDF error response format", () => {
      const body = JSON.stringify({
        error: {
          code: 404,
          message: "Not found",
        },
      });

      const error = parseErrorDetails(404, body);

      expect(error).toEqual({
        code: 404,
        message: "Not found",
      });
    });

    it("should parse error with missing/duplicated fields", () => {
      const body = JSON.stringify({
        error: {
          code: 400,
          message: "Invalid request",
          missing: [{ externalId: "item1" }],
          duplicated: [{ externalId: "item2" }],
        },
      });

      const error = parseErrorDetails(400, body);

      expect(error).toEqual({
        code: 400,
        message: "Invalid request",
        missing: [{ externalId: "item1" }],
        duplicated: [{ externalId: "item2" }],
      });
    });

    it("should fallback to raw body when parsing fails", () => {
      const body = "Internal Server Error";

      const error = parseErrorDetails(500, body);

      expect(error).toEqual({
        code: 500,
        message: "Internal Server Error",
      });
    });
  });

  describe("isSuccess", () => {
    it("should return true for success response", () => {
      const result: SuccessResponse = {
        kind: "success",
        statusCode: 200,
        body: '{"items":[]}',
      };

      expect(isSuccess(result)).toBe(true);
    });

    it("should return false for failed response", () => {
      const result: FailedResponse = {
        kind: "failed_response",
        statusCode: 404,
        body: "Not found",
        error: { code: 404, message: "Not found" },
      };

      expect(isSuccess(result)).toBe(false);
    });

    it("should return false for failed request", () => {
      const result: FailedRequest = {
        kind: "failed_request",
        error: "Network error",
      };

      expect(isSuccess(result)).toBe(false);
    });
  });

  describe("getSuccessOrThrow", () => {
    it("should return success response when successful", () => {
      const result: SuccessResponse = {
        kind: "success",
        statusCode: 200,
        body: '{"items":[]}',
      };

      expect(getSuccessOrThrow(result)).toBe(result);
    });

    it("should throw for failed response", () => {
      const result: FailedResponse = {
        kind: "failed_response",
        statusCode: 404,
        body: "Not found",
        error: { code: 404, message: "Resource not found" },
      };

      expect(() => getSuccessOrThrow(result)).toThrow(
        "Request failed with status 404: Resource not found",
      );
    });

    it("should throw for failed request", () => {
      const result: FailedRequest = {
        kind: "failed_request",
        error: "Connection refused",
      };

      expect(() => getSuccessOrThrow(result)).toThrow("Request failed: Connection refused");
    });
  });

  describe("HTTPResult type guards", () => {
    it("should work with discriminated union", () => {
      const results: HTTPResult[] = [
        { kind: "success", statusCode: 200, body: "{}" },
        {
          kind: "failed_response",
          statusCode: 500,
          body: "error",
          error: { code: 500, message: "error" },
        },
        { kind: "failed_request", error: "timeout" },
      ];

      const successCount = results.filter(isSuccess).length;
      expect(successCount).toBe(1);
    });
  });
});
