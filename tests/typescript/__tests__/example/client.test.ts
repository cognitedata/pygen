/**
 * Tests for ExampleClient.
 */
import { describe, expect, it } from "vitest";

import { type PygenClientConfig, TokenCredentials } from "@cognite/pygen-typescript";

import { ExampleClient } from "../../../../cognite/pygen/_generation/typescript/example/client.ts";
import {
  CategoryNodeAPI,
  ProductNodeAPI,
  RelatesToAPI,
} from "../../../../cognite/pygen/_generation/typescript/example/api.ts";

// Create test config
function createTestConfig(): PygenClientConfig {
  const credentials = new TokenCredentials({ tokenProvider: async () => "test-token" });
  return {
    baseUrl: "https://test.cognitedata.com",
    project: "test-project",
    credentials,
  };
}

// ============================================================================
// ExampleClient Tests
// ============================================================================

describe("ExampleClient", () => {
  describe("constructor", () => {
    it("should create ExampleClient with default worker counts", () => {
      const config = createTestConfig();
      const client = new ExampleClient(config);

      expect(client).toBeDefined();
      expect(client.productNode).toBeDefined();
      expect(client.categoryNode).toBeDefined();
      expect(client.relatesTo).toBeDefined();
    });

    it("should create ExampleClient with custom worker counts", () => {
      const config = createTestConfig();
      const client = new ExampleClient(config, 10, 5, 20);

      expect(client).toBeDefined();
    });

    it("should have productNode API of correct type", () => {
      const config = createTestConfig();
      const client = new ExampleClient(config);

      expect(client.productNode).toBeInstanceOf(ProductNodeAPI);
    });

    it("should have categoryNode API of correct type", () => {
      const config = createTestConfig();
      const client = new ExampleClient(config);

      expect(client.categoryNode).toBeInstanceOf(CategoryNodeAPI);
    });

    it("should have relatesTo API of correct type", () => {
      const config = createTestConfig();
      const client = new ExampleClient(config);

      expect(client.relatesTo).toBeInstanceOf(RelatesToAPI);
    });
  });

  describe("API access", () => {
    it("should have productNode with iterate method", () => {
      const config = createTestConfig();
      const client = new ExampleClient(config);

      expect(typeof client.productNode.iterate).toBe("function");
    });

    it("should have productNode with list method", () => {
      const config = createTestConfig();
      const client = new ExampleClient(config);

      expect(typeof client.productNode.list).toBe("function");
    });

    it("should have productNode with search method", () => {
      const config = createTestConfig();
      const client = new ExampleClient(config);

      expect(typeof client.productNode.search).toBe("function");
    });

    it("should have productNode with aggregate method", () => {
      const config = createTestConfig();
      const client = new ExampleClient(config);

      expect(typeof client.productNode.aggregate).toBe("function");
    });

    it("should have productNode with retrieve method", () => {
      const config = createTestConfig();
      const client = new ExampleClient(config);

      expect(typeof client.productNode.retrieve).toBe("function");
    });

    it("should have categoryNode with list method", () => {
      const config = createTestConfig();
      const client = new ExampleClient(config);

      expect(typeof client.categoryNode.list).toBe("function");
    });

    it("should have relatesTo with list method", () => {
      const config = createTestConfig();
      const client = new ExampleClient(config);

      expect(typeof client.relatesTo.list).toBe("function");
    });
  });
});
