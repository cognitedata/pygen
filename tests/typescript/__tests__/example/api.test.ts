/**
 * Tests for example API classes.
 */
import { describe, expect, it } from "vitest";

import {
  HTTPClient,
  type HTTPResult,
  type RequestMessage,
  TokenCredentials,
  type PygenClientConfig,
} from "@cognite/pygen-typescript";

import {
  CategoryNodeAPI,
  ProductNodeAPI,
  RelatesToAPI,
} from "../../../../cognite/pygen/_generation/typescript/example/api.ts";
import {
  CategoryNode,
  ProductNode,
  RelatesTo,
  CATEGORY_NODE_VIEW,
  PRODUCT_NODE_VIEW,
  RELATES_TO_VIEW,
} from "../../../../cognite/pygen/_generation/typescript/example/dataClasses.ts";

// Mockable HTTPClient subclass
class MockHTTPClient extends HTTPClient {
  public mockRequestFn:
    | ((message: RequestMessage) => Promise<HTTPResult>)
    | null = null;
  public capturedRequests: RequestMessage[] = [];

  async request(message: RequestMessage): Promise<HTTPResult> {
    this.capturedRequests.push(message);
    if (this.mockRequestFn) {
      return this.mockRequestFn(message);
    }
    return super.request(message);
  }
}

// Helper function to create mock product node response
function createMockProductListResponse(items: ProductNode[], nextCursor?: string): HTTPResult {
  const rawItems = items.map((item) => ({
    instanceType: item.instanceType,
    space: item.space,
    externalId: item.externalId,
    version: item.dataRecord.version,
    createdTime: item.dataRecord.createdTime.getTime(),
    lastUpdatedTime: item.dataRecord.lastUpdatedTime.getTime(),
    properties: {
      [PRODUCT_NODE_VIEW.space]: {
        [`${PRODUCT_NODE_VIEW.externalId}/${PRODUCT_NODE_VIEW.version}`]: {
          name: item.name,
          price: item.price,
          quantity: item.quantity,
          createdDate: item.createdDate.toISOString().split("T")[0],
          ...(item.description !== undefined ? { description: item.description } : {}),
          ...(item.active !== undefined ? { active: item.active } : {}),
        },
      },
    },
  }));

  return {
    kind: "success",
    statusCode: 200,
    body: JSON.stringify({
      items: rawItems,
      ...(nextCursor ? { nextCursor } : {}),
    }),
  };
}

// Helper function to create mock category node response
function createMockCategoryListResponse(items: CategoryNode[], nextCursor?: string): HTTPResult {
  const rawItems = items.map((item) => ({
    instanceType: item.instanceType,
    space: item.space,
    externalId: item.externalId,
    version: item.dataRecord.version,
    createdTime: item.dataRecord.createdTime.getTime(),
    lastUpdatedTime: item.dataRecord.lastUpdatedTime.getTime(),
    properties: {
      [CATEGORY_NODE_VIEW.space]: {
        [`${CATEGORY_NODE_VIEW.externalId}/${CATEGORY_NODE_VIEW.version}`]: {
          categoryName: item.categoryName,
        },
      },
    },
  }));

  return {
    kind: "success",
    statusCode: 200,
    body: JSON.stringify({
      items: rawItems,
      ...(nextCursor ? { nextCursor } : {}),
    }),
  };
}

// Helper function to create mock relates to response
function createMockRelatesToListResponse(items: RelatesTo[], nextCursor?: string): HTTPResult {
  const rawItems = items.map((item) => ({
    instanceType: item.instanceType,
    space: item.space,
    externalId: item.externalId,
    version: item.dataRecord.version,
    createdTime: item.dataRecord.createdTime.getTime(),
    lastUpdatedTime: item.dataRecord.lastUpdatedTime.getTime(),
    startNode: item.startNode,
    endNode: item.endNode,
    properties: {
      [RELATES_TO_VIEW.space]: {
        [`${RELATES_TO_VIEW.externalId}/${RELATES_TO_VIEW.version}`]: {
          relationType: item.relationType,
          createdAt: item.createdAt.toISOString(),
          ...(item.strength !== undefined ? { strength: item.strength } : {}),
        },
      },
    },
  }));

  return {
    kind: "success",
    statusCode: 200,
    body: JSON.stringify({
      items: rawItems,
      ...(nextCursor ? { nextCursor } : {}),
    }),
  };
}

// Create test product instance
function createTestProduct(id: string, name: string, price = 10.0): ProductNode {
  return {
    instanceType: "node",
    space: "test-space",
    externalId: id,
    dataRecord: {
      version: 1,
      createdTime: new Date("2024-01-01"),
      lastUpdatedTime: new Date("2024-01-02"),
    },
    name,
    price,
    quantity: 5,
    createdDate: new Date("2024-01-01"),
  };
}

// Create test category instance
function createTestCategory(id: string, categoryName: string): CategoryNode {
  return {
    instanceType: "node",
    space: "test-space",
    externalId: id,
    dataRecord: {
      version: 1,
      createdTime: new Date("2024-01-01"),
      lastUpdatedTime: new Date("2024-01-02"),
    },
    categoryName,
  };
}

// Create test relates to instance
function createTestRelatesTo(id: string, relationType: string): RelatesTo {
  return {
    instanceType: "edge",
    space: "test-space",
    externalId: id,
    dataRecord: {
      version: 1,
      createdTime: new Date("2024-01-01"),
      lastUpdatedTime: new Date("2024-01-02"),
    },
    startNode: { space: "test-space", externalId: "start-node" },
    endNode: { space: "test-space", externalId: "end-node" },
    relationType,
    createdAt: new Date("2024-01-01T00:00:00Z"),
  };
}

// Helper to create mock aggregate response
function createMockAggregateResponse(): HTTPResult {
  return {
    kind: "success",
    statusCode: 200,
    body: JSON.stringify({
      items: [{ aggregates: [{ aggregate: "count", value: 10 }] }],
    }),
  };
}

// Helper to create mock search response (same format as list)
function createMockSearchResponse<T>(items: T[], createFn: (items: T[], cursor?: string) => HTTPResult): HTTPResult {
  return createFn(items);
}

// Create test config and API helper
function createTestableAPI<T>(APIClass: new (config: PygenClientConfig) => T): { api: T; mockClient: MockHTTPClient; config: PygenClientConfig } {
  const credentials = new TokenCredentials({ tokenProvider: async () => "test-token" });
  const config: PygenClientConfig = {
    baseUrl: "https://test.cognitedata.com",
    project: "test-project",
    credentials,
  };

  const api = new APIClass(config);
  const mockClient = new MockHTTPClient(config);

  // Replace httpClient with our mock
  // @ts-expect-error - we're overriding readonly for testing
  api.httpClient = mockClient;

  return { api, mockClient, config };
}

// ============================================================================
// ProductNodeAPI Tests
// ============================================================================

describe("ProductNodeAPI", () => {
  describe("constructor", () => {
    it("should create ProductNodeAPI with correct view reference", () => {
      const { api } = createTestableAPI(ProductNodeAPI);
      // @ts-expect-error - accessing protected property for testing
      expect(api.viewRef).toEqual(PRODUCT_NODE_VIEW);
    });
  });

  describe("iterate", () => {
    it("should iterate with no filters", async () => {
      const { api, mockClient } = createTestableAPI(ProductNodeAPI);
      const testProduct = createTestProduct("product-1", "Test Product");

      mockClient.mockRequestFn = async () => createMockProductListResponse([testProduct]);

      const result = await api.iterate();

      expect(result.items.length).toBe(1);
      expect(result.items.at(0)?.name).toBe("Test Product");
    });

    it("should iterate with name filter as string", async () => {
      const { api, mockClient } = createTestableAPI(ProductNodeAPI);
      const testProduct = createTestProduct("product-1", "Test Product");

      mockClient.mockRequestFn = async () => createMockProductListResponse([testProduct]);

      const result = await api.iterate({ name: "Test Product" });

      expect(result.items.length).toBe(1);
      expect(mockClient.capturedRequests.length).toBe(1);
    });

    it("should iterate with name filter as array", async () => {
      const { api, mockClient } = createTestableAPI(ProductNodeAPI);
      const testProducts = [
        createTestProduct("product-1", "Product A"),
        createTestProduct("product-2", "Product B"),
      ];

      mockClient.mockRequestFn = async () => createMockProductListResponse(testProducts);

      const result = await api.iterate({ name: ["Product A", "Product B"] });

      expect(result.items.length).toBe(2);
    });

    it("should iterate with price range filters", async () => {
      const { api, mockClient } = createTestableAPI(ProductNodeAPI);
      const testProduct = createTestProduct("product-1", "Test Product", 50.0);

      mockClient.mockRequestFn = async () => createMockProductListResponse([testProduct]);

      const result = await api.iterate({ minPrice: 10, maxPrice: 100 });

      expect(result.items.length).toBe(1);
    });

    it("should iterate with cursor for pagination", async () => {
      const { api, mockClient } = createTestableAPI(ProductNodeAPI);
      const testProduct = createTestProduct("product-1", "Test Product");

      mockClient.mockRequestFn = async () => createMockProductListResponse([testProduct], "next-cursor");

      const result = await api.iterate({ cursor: "start-cursor", limit: 10 });

      expect(result.nextCursor).toBe("next-cursor");
    });

    it("should iterate with category filter", async () => {
      const { api, mockClient } = createTestableAPI(ProductNodeAPI);
      const testProduct = createTestProduct("product-1", "Test Product");

      mockClient.mockRequestFn = async () => createMockProductListResponse([testProduct]);

      const result = await api.iterate({
        category: { space: "test-space", externalId: "category-1" },
      });

      expect(result.items.length).toBe(1);
    });

    it("should iterate with space filter as array", async () => {
      const { api, mockClient } = createTestableAPI(ProductNodeAPI);
      const testProduct = createTestProduct("product-1", "Test Product");

      mockClient.mockRequestFn = async () => createMockProductListResponse([testProduct]);

      const result = await api.iterate({ space: ["space-1", "space-2"] });

      expect(result.items.length).toBe(1);
    });
  });

  describe("search", () => {
    it("should search with query", async () => {
      const { api, mockClient } = createTestableAPI(ProductNodeAPI);
      const testProduct = createTestProduct("product-1", "Test Product");

      mockClient.mockRequestFn = async () => createMockProductListResponse([testProduct]);

      const result = await api.search({ query: "Test" });

      expect(result.length).toBe(1);
      expect(result.at(0)?.name).toBe("Test Product");
    });

    it("should search with filters", async () => {
      const { api, mockClient } = createTestableAPI(ProductNodeAPI);
      const testProduct = createTestProduct("product-1", "Test Product");

      mockClient.mockRequestFn = async () => createMockProductListResponse([testProduct]);

      const result = await api.search({
        query: "Test",
        minPrice: 5,
        active: true,
      });

      expect(result.length).toBe(1);
    });
  });

  describe("aggregate", () => {
    it("should aggregate with count", async () => {
      const { api, mockClient } = createTestableAPI(ProductNodeAPI);

      mockClient.mockRequestFn = async () => createMockAggregateResponse();

      const result = await api.aggregate({ type: "count" });

      expect(result.items.length).toBe(1);
    });

    it("should aggregate with filters", async () => {
      const { api, mockClient } = createTestableAPI(ProductNodeAPI);

      mockClient.mockRequestFn = async () => createMockAggregateResponse();

      const result = await api.aggregate(
        { type: "count" },
        { name: "Test", minPrice: 10 },
      );

      expect(result.items.length).toBe(1);
    });
  });

  describe("list", () => {
    it("should list with filters and sorting", async () => {
      const { api, mockClient } = createTestableAPI(ProductNodeAPI);
      const testProducts = [
        createTestProduct("product-1", "Product A", 10),
        createTestProduct("product-2", "Product B", 20),
      ];

      mockClient.mockRequestFn = async () => createMockProductListResponse(testProducts);

      const result = await api.list({
        minPrice: 5,
        sortBy: "price",
        sortDirection: "ascending",
      });

      expect(result.length).toBe(2);
    });
  });

  describe("retrieve", () => {
    it("should retrieve single item by id tuple", async () => {
      const { api, mockClient } = createTestableAPI(ProductNodeAPI);
      const testProduct = createTestProduct("product-1", "Test Product");

      mockClient.mockRequestFn = async () => createMockProductListResponse([testProduct]);

      const result = await api.retrieve(["test-space", "product-1"]);

      expect(result).toBeDefined();
      expect(result?.name).toBe("Test Product");
    });

    it("should retrieve multiple items by id array", async () => {
      const { api, mockClient } = createTestableAPI(ProductNodeAPI);
      const testProducts = [
        createTestProduct("product-1", "Product A"),
        createTestProduct("product-2", "Product B"),
      ];

      mockClient.mockRequestFn = async () => createMockProductListResponse(testProducts);

      const result = await api.retrieve([
        { space: "test-space", externalId: "product-1" },
        { space: "test-space", externalId: "product-2" },
      ]);

      expect(result.length).toBe(2);
    });
  });
});

// ============================================================================
// CategoryNodeAPI Tests
// ============================================================================

describe("CategoryNodeAPI", () => {
  describe("constructor", () => {
    it("should create CategoryNodeAPI with correct view reference", () => {
      const { api } = createTestableAPI(CategoryNodeAPI);
      // @ts-expect-error - accessing protected property for testing
      expect(api.viewRef).toEqual(CATEGORY_NODE_VIEW);
    });
  });

  describe("iterate", () => {
    it("should iterate with categoryName filter", async () => {
      const { api, mockClient } = createTestableAPI(CategoryNodeAPI);
      const testCategory = createTestCategory("category-1", "Electronics");

      mockClient.mockRequestFn = async () => createMockCategoryListResponse([testCategory]);

      const result = await api.iterate({ categoryName: "Electronics" });

      expect(result.items.length).toBe(1);
      expect(result.items.at(0)?.categoryName).toBe("Electronics");
    });

    it("should iterate with categoryName prefix filter", async () => {
      const { api, mockClient } = createTestableAPI(CategoryNodeAPI);
      const testCategory = createTestCategory("category-1", "Electronics");

      mockClient.mockRequestFn = async () => createMockCategoryListResponse([testCategory]);

      const result = await api.iterate({ categoryNamePrefix: "Elec" });

      expect(result.items.length).toBe(1);
    });
  });

  describe("search", () => {
    it("should search categories", async () => {
      const { api, mockClient } = createTestableAPI(CategoryNodeAPI);
      const testCategory = createTestCategory("category-1", "Electronics");

      mockClient.mockRequestFn = async () => createMockCategoryListResponse([testCategory]);

      const result = await api.search({ query: "Electronics" });

      expect(result.length).toBe(1);
    });
  });

  describe("aggregate", () => {
    it("should aggregate categories", async () => {
      const { api, mockClient } = createTestableAPI(CategoryNodeAPI);

      mockClient.mockRequestFn = async () => createMockAggregateResponse();

      const result = await api.aggregate({ type: "count" });

      expect(result.items.length).toBe(1);
    });
  });

  describe("list", () => {
    it("should list categories with sorting", async () => {
      const { api, mockClient } = createTestableAPI(CategoryNodeAPI);
      const testCategories = [
        createTestCategory("category-1", "Electronics"),
        createTestCategory("category-2", "Books"),
      ];

      mockClient.mockRequestFn = async () => createMockCategoryListResponse(testCategories);

      const result = await api.list({
        sortBy: "categoryName",
        sortDirection: "ascending",
      });

      expect(result.length).toBe(2);
    });
  });

  describe("retrieve", () => {
    it("should retrieve single category", async () => {
      const { api, mockClient } = createTestableAPI(CategoryNodeAPI);
      const testCategory = createTestCategory("category-1", "Electronics");

      mockClient.mockRequestFn = async () => createMockCategoryListResponse([testCategory]);

      const result = await api.retrieve(["test-space", "category-1"]);

      expect(result?.categoryName).toBe("Electronics");
    });
  });
});

// ============================================================================
// RelatesToAPI Tests
// ============================================================================

describe("RelatesToAPI", () => {
  describe("constructor", () => {
    it("should create RelatesToAPI with correct view reference", () => {
      const { api } = createTestableAPI(RelatesToAPI);
      // @ts-expect-error - accessing protected property for testing
      expect(api.viewRef).toEqual(RELATES_TO_VIEW);
    });
  });

  describe("iterate", () => {
    it("should iterate with relationType filter", async () => {
      const { api, mockClient } = createTestableAPI(RelatesToAPI);
      const testRelation = createTestRelatesTo("relation-1", "similar");

      mockClient.mockRequestFn = async () => createMockRelatesToListResponse([testRelation]);

      const result = await api.iterate({ relationType: "similar" });

      expect(result.items.length).toBe(1);
      expect(result.items.at(0)?.relationType).toBe("similar");
    });

    it("should iterate with strength range", async () => {
      const { api, mockClient } = createTestableAPI(RelatesToAPI);
      const testRelation = createTestRelatesTo("relation-1", "similar");

      mockClient.mockRequestFn = async () => createMockRelatesToListResponse([testRelation]);

      const result = await api.iterate({ minStrength: 0.5, maxStrength: 1.0 });

      expect(result.items.length).toBe(1);
    });
  });

  describe("search", () => {
    it("should search edges", async () => {
      const { api, mockClient } = createTestableAPI(RelatesToAPI);
      const testRelation = createTestRelatesTo("relation-1", "similar");

      mockClient.mockRequestFn = async () => createMockRelatesToListResponse([testRelation]);

      const result = await api.search({ query: "similar" });

      expect(result.length).toBe(1);
    });
  });

  describe("aggregate", () => {
    it("should aggregate edges", async () => {
      const { api, mockClient } = createTestableAPI(RelatesToAPI);

      mockClient.mockRequestFn = async () => createMockAggregateResponse();

      const result = await api.aggregate({ type: "count" });

      expect(result.items.length).toBe(1);
    });
  });

  describe("list", () => {
    it("should list edges with sorting", async () => {
      const { api, mockClient } = createTestableAPI(RelatesToAPI);
      const testRelations = [
        createTestRelatesTo("relation-1", "similar"),
        createTestRelatesTo("relation-2", "related"),
      ];

      mockClient.mockRequestFn = async () => createMockRelatesToListResponse(testRelations);

      const result = await api.list({
        sortBy: "relationType",
        sortDirection: "ascending",
      });

      expect(result.length).toBe(2);
    });
  });

  describe("retrieve", () => {
    it("should retrieve single edge", async () => {
      const { api, mockClient } = createTestableAPI(RelatesToAPI);
      const testRelation = createTestRelatesTo("relation-1", "similar");

      mockClient.mockRequestFn = async () => createMockRelatesToListResponse([testRelation]);

      const result = await api.retrieve(["test-space", "relation-1"]);

      expect(result?.relationType).toBe("similar");
    });
  });
});

