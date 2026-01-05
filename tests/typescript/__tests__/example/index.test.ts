/**
 * Tests for example SDK index exports.
 */
import { describe, expect, it } from "vitest";

import {
  CATEGORY_NODE_VIEW,
  CategoryNodeAPI,
  categoryNodeAsWrite,
  CategoryNodeFilter,
  CategoryNodeList,
  EXAMPLE_SPACE,
  EXAMPLE_VERSION,
  // Client
  ExampleClient,
  PRODUCT_NODE_VIEW,
  // API classes
  ProductNodeAPI,
  productNodeAsWrite,
  ProductNodeFilter,
  // Data classes
  ProductNodeList,
  RELATES_TO_VIEW,
  RelatesToAPI,
  relatesToAsWrite,
  RelatesToFilter,
  RelatesToList,
} from "../../../../cognite/pygen/_typescript/example/index.ts";

describe("Example SDK Index Exports", () => {
  describe("Data class exports", () => {
    it("should export ProductNodeList", () => {
      expect(ProductNodeList).toBeDefined();
    });

    it("should export ProductNodeFilter", () => {
      expect(ProductNodeFilter).toBeDefined();
    });

    it("should export productNodeAsWrite", () => {
      expect(productNodeAsWrite).toBeDefined();
      expect(typeof productNodeAsWrite).toBe("function");
    });

    it("should export PRODUCT_NODE_VIEW", () => {
      expect(PRODUCT_NODE_VIEW).toBeDefined();
    });

    it("should export CategoryNodeList", () => {
      expect(CategoryNodeList).toBeDefined();
    });

    it("should export CategoryNodeFilter", () => {
      expect(CategoryNodeFilter).toBeDefined();
    });

    it("should export categoryNodeAsWrite", () => {
      expect(categoryNodeAsWrite).toBeDefined();
      expect(typeof categoryNodeAsWrite).toBe("function");
    });

    it("should export CATEGORY_NODE_VIEW", () => {
      expect(CATEGORY_NODE_VIEW).toBeDefined();
    });

    it("should export RelatesToList", () => {
      expect(RelatesToList).toBeDefined();
    });

    it("should export RelatesToFilter", () => {
      expect(RelatesToFilter).toBeDefined();
    });

    it("should export relatesToAsWrite", () => {
      expect(relatesToAsWrite).toBeDefined();
      expect(typeof relatesToAsWrite).toBe("function");
    });

    it("should export RELATES_TO_VIEW", () => {
      expect(RELATES_TO_VIEW).toBeDefined();
    });

    it("should export EXAMPLE_SPACE", () => {
      expect(EXAMPLE_SPACE).toBe("pygen_example");
    });

    it("should export EXAMPLE_VERSION", () => {
      expect(EXAMPLE_VERSION).toBe("v1");
    });
  });

  describe("API class exports", () => {
    it("should export ProductNodeAPI", () => {
      expect(ProductNodeAPI).toBeDefined();
    });

    it("should export CategoryNodeAPI", () => {
      expect(CategoryNodeAPI).toBeDefined();
    });

    it("should export RelatesToAPI", () => {
      expect(RelatesToAPI).toBeDefined();
    });
  });

  describe("Client exports", () => {
    it("should export ExampleClient", () => {
      expect(ExampleClient).toBeDefined();
    });
  });
});
