/**
 * Tests for example data classes.
 */
import { describe, expect, it } from "vitest";

import {
  CATEGORY_NODE_VIEW,
  CategoryNode,
  categoryNodeAsWrite,
  CategoryNodeFilter,
  CategoryNodeList,
  EXAMPLE_SPACE,
  EXAMPLE_VERSION,
  PRODUCT_NODE_VIEW,
  ProductNode,
  productNodeAsWrite,
  ProductNodeFilter,
  ProductNodeList,
  RELATES_TO_VIEW,
  RelatesTo,
  relatesToAsWrite,
  RelatesToFilter,
  RelatesToList,
} from "../../../../cognite/pygen/_generation/typescript/example/dataClasses.ts";

// ============================================================================
// Constants Tests
// ============================================================================

describe("Example Constants", () => {
  it("should have correct space constant", () => {
    expect(EXAMPLE_SPACE).toBe("pygen_example");
  });

  it("should have correct version constant", () => {
    expect(EXAMPLE_VERSION).toBe("v1");
  });

  it("should have correct ProductNode view reference", () => {
    expect(PRODUCT_NODE_VIEW).toEqual({
      space: "pygen_example",
      externalId: "ProductNode",
      version: "v1",
    });
  });

  it("should have correct CategoryNode view reference", () => {
    expect(CATEGORY_NODE_VIEW).toEqual({
      space: "pygen_example",
      externalId: "CategoryNode",
      version: "v1",
    });
  });

  it("should have correct RelatesTo view reference", () => {
    expect(RELATES_TO_VIEW).toEqual({
      space: "pygen_example",
      externalId: "RelatesTo",
      version: "v1",
    });
  });
});

// ============================================================================
// ProductNode Tests
// ============================================================================

describe("ProductNode", () => {
  const sampleProductNode: ProductNode = {
    instanceType: "node",
    space: "test-space",
    externalId: "product-1",
    dataRecord: {
      version: 1,
      lastUpdatedTime: new Date("2025-01-01T00:00:00Z"),
      createdTime: new Date("2025-01-01T00:00:00Z"),
    },
    name: "Test Product",
    price: 99.99,
    quantity: 10,
    createdDate: new Date("2025-01-01"),
    description: "A test product",
    tags: ["tag1", "tag2"],
    active: true,
    category: { space: "test-space", externalId: "category-1" },
  };

  describe("productNodeAsWrite", () => {
    it("should convert ProductNode to ProductNodeWrite", () => {
      const write = productNodeAsWrite(sampleProductNode);

      expect(write.instanceType).toBe("node");
      expect(write.space).toBe("test-space");
      expect(write.externalId).toBe("product-1");
      expect(write.name).toBe("Test Product");
      expect(write.price).toBe(99.99);
      expect(write.quantity).toBe(10);
      expect(write.description).toBe("A test product");
      expect(write.tags).toEqual(["tag1", "tag2"]);
      expect(write.active).toBe(true);
      expect(write.category).toEqual({ space: "test-space", externalId: "category-1" });
    });

    it("should include existingVersion from dataRecord", () => {
      const write = productNodeAsWrite(sampleProductNode);
      expect(write.dataRecord?.existingVersion).toBe(1);
    });
  });
});

describe("ProductNodeList", () => {
  const sampleProducts: ProductNode[] = [
    {
      instanceType: "node",
      space: "test-space",
      externalId: "product-1",
      dataRecord: {
        version: 1,
        lastUpdatedTime: new Date("2025-01-01T00:00:00Z"),
        createdTime: new Date("2025-01-01T00:00:00Z"),
      },
      name: "Product 1",
      price: 10.0,
      quantity: 5,
      createdDate: new Date("2025-01-01"),
    },
    {
      instanceType: "node",
      space: "test-space",
      externalId: "product-2",
      dataRecord: {
        version: 1,
        lastUpdatedTime: new Date("2025-01-01T00:00:00Z"),
        createdTime: new Date("2025-01-01T00:00:00Z"),
      },
      name: "Product 2",
      price: 20.0,
      quantity: 10,
      createdDate: new Date("2025-01-02"),
    },
  ];

  it("should create ProductNodeList from array", () => {
    const list = new ProductNodeList(sampleProducts);
    expect(list.length).toBe(2);
    expect(list.viewId).toEqual(PRODUCT_NODE_VIEW);
  });

  it("should have asWrite method that converts all items", () => {
    const list = new ProductNodeList(sampleProducts);
    const writes = list.asWrite();

    expect(writes.length).toBe(2);
    expect(writes[0]!.name).toBe("Product 1");
    expect(writes[1]!.name).toBe("Product 2");
  });

  it("should create from array using static fromArray method", () => {
    const list = ProductNodeList.fromArray(sampleProducts);
    expect(list.length).toBe(2);
  });

  it("should be iterable", () => {
    const list = new ProductNodeList(sampleProducts);
    const names = [];
    for (const product of list) {
      names.push(product.name);
    }
    expect(names).toEqual(["Product 1", "Product 2"]);
  });
});

describe("ProductNodeFilter", () => {
  it("should create filter container with all property filters", () => {
    const filter = new ProductNodeFilter();

    expect(filter.name).toBeDefined();
    expect(filter.description).toBeDefined();
    expect(filter.price).toBeDefined();
    expect(filter.quantity).toBeDefined();
    expect(filter.active).toBeDefined();
    expect(filter.createdDate).toBeDefined();
    expect(filter.category).toBeDefined();
    // Also has base filters
    expect(filter.space).toBeDefined();
    expect(filter.externalId).toBeDefined();
  });

  it("should build filter with name equals", () => {
    const filter = new ProductNodeFilter();
    filter.name.equals("Test");
    const result = filter.asFilter();

    expect(result).toBeDefined();
    expect(result).toHaveProperty("equals");
  });

  it("should build filter with price range", () => {
    const filter = new ProductNodeFilter();
    filter.price.greaterThanOrEquals(10).lessThanOrEquals(100);
    const result = filter.asFilter();

    expect(result).toBeDefined();
    expect(result).toHaveProperty("range");
  });

  it("should build filter combining multiple conditions with AND", () => {
    const filter = new ProductNodeFilter("and");
    filter.name.equals("Test");
    filter.active.equals(true);
    const result = filter.asFilter();

    expect(result).toBeDefined();
    expect(result).toHaveProperty("and");
    expect((result as { and: unknown[] }).and.length).toBe(2);
  });

  it("should return undefined when no filters applied", () => {
    const filter = new ProductNodeFilter();
    const result = filter.asFilter();

    expect(result).toBeUndefined();
  });
});

// ============================================================================
// CategoryNode Tests
// ============================================================================

describe("CategoryNode", () => {
  const sampleCategoryNode: CategoryNode = {
    instanceType: "node",
    space: "test-space",
    externalId: "category-1",
    dataRecord: {
      version: 1,
      lastUpdatedTime: new Date("2025-01-01T00:00:00Z"),
      createdTime: new Date("2025-01-01T00:00:00Z"),
    },
    categoryName: "Electronics",
  };

  describe("categoryNodeAsWrite", () => {
    it("should convert CategoryNode to CategoryNodeWrite", () => {
      const write = categoryNodeAsWrite(sampleCategoryNode);

      expect(write.instanceType).toBe("node");
      expect(write.space).toBe("test-space");
      expect(write.externalId).toBe("category-1");
      expect(write.categoryName).toBe("Electronics");
      expect(write.dataRecord?.existingVersion).toBe(1);
    });
  });
});

describe("CategoryNodeList", () => {
  const sampleCategories: CategoryNode[] = [
    {
      instanceType: "node",
      space: "test-space",
      externalId: "category-1",
      dataRecord: {
        version: 1,
        lastUpdatedTime: new Date("2025-01-01T00:00:00Z"),
        createdTime: new Date("2025-01-01T00:00:00Z"),
      },
      categoryName: "Electronics",
    },
    {
      instanceType: "node",
      space: "test-space",
      externalId: "category-2",
      dataRecord: {
        version: 1,
        lastUpdatedTime: new Date("2025-01-01T00:00:00Z"),
        createdTime: new Date("2025-01-01T00:00:00Z"),
      },
      categoryName: "Books",
    },
  ];

  it("should create CategoryNodeList from array", () => {
    const list = new CategoryNodeList(sampleCategories);
    expect(list.length).toBe(2);
    expect(list.viewId).toEqual(CATEGORY_NODE_VIEW);
  });

  it("should have asWrite method that converts all items", () => {
    const list = new CategoryNodeList(sampleCategories);
    const writes = list.asWrite();

    expect(writes.length).toBe(2);
    expect(writes[0]!.categoryName).toBe("Electronics");
    expect(writes[1]!.categoryName).toBe("Books");
  });
});

describe("CategoryNodeFilter", () => {
  it("should create filter container with categoryName filter", () => {
    const filter = new CategoryNodeFilter();

    expect(filter.categoryName).toBeDefined();
    expect(filter.space).toBeDefined();
    expect(filter.externalId).toBeDefined();
  });

  it("should build filter with categoryName prefix", () => {
    const filter = new CategoryNodeFilter();
    filter.categoryName.prefix("Elec");
    const result = filter.asFilter();

    expect(result).toBeDefined();
    expect(result).toHaveProperty("prefix");
  });
});

// ============================================================================
// RelatesTo Tests
// ============================================================================

describe("RelatesTo", () => {
  const sampleRelatesTo: RelatesTo = {
    instanceType: "edge",
    space: "test-space",
    externalId: "relates-1",
    dataRecord: {
      version: 1,
      lastUpdatedTime: new Date("2025-01-01T00:00:00Z"),
      createdTime: new Date("2025-01-01T00:00:00Z"),
    },
    startNode: { space: "test-space", externalId: "product-1" },
    endNode: { space: "test-space", externalId: "product-2" },
    relationType: "similar",
    strength: 0.8,
    createdAt: new Date("2025-01-01T00:00:00Z"),
  };

  describe("relatesToAsWrite", () => {
    it("should convert RelatesTo to RelatesToWrite", () => {
      const write = relatesToAsWrite(sampleRelatesTo);

      expect(write.instanceType).toBe("edge");
      expect(write.space).toBe("test-space");
      expect(write.externalId).toBe("relates-1");
      expect(write.startNode).toEqual({ space: "test-space", externalId: "product-1" });
      expect(write.endNode).toEqual({ space: "test-space", externalId: "product-2" });
      expect(write.relationType).toBe("similar");
      expect(write.strength).toBe(0.8);
      expect(write.dataRecord?.existingVersion).toBe(1);
    });
  });
});

describe("RelatesToList", () => {
  const sampleRelations: RelatesTo[] = [
    {
      instanceType: "edge",
      space: "test-space",
      externalId: "relates-1",
      dataRecord: {
        version: 1,
        lastUpdatedTime: new Date("2025-01-01T00:00:00Z"),
        createdTime: new Date("2025-01-01T00:00:00Z"),
      },
      startNode: { space: "test-space", externalId: "product-1" },
      endNode: { space: "test-space", externalId: "product-2" },
      relationType: "similar",
      strength: 0.8,
      createdAt: new Date("2025-01-01T00:00:00Z"),
    },
  ];

  it("should create RelatesToList from array", () => {
    const list = new RelatesToList(sampleRelations);
    expect(list.length).toBe(1);
    expect(list.viewId).toEqual(RELATES_TO_VIEW);
  });

  it("should have asWrite method that converts all items", () => {
    const list = new RelatesToList(sampleRelations);
    const writes = list.asWrite();

    expect(writes.length).toBe(1);
    expect(writes[0]!.relationType).toBe("similar");
  });
});

describe("RelatesToFilter", () => {
  it("should create filter container with edge filters", () => {
    const filter = new RelatesToFilter();

    expect(filter.relationType).toBeDefined();
    expect(filter.strength).toBeDefined();
    expect(filter.createdAt).toBeDefined();
    // Edge-specific filters
    expect(filter.startNode).toBeDefined();
    expect(filter.endNode).toBeDefined();
  });

  it("should build filter with relationType equals", () => {
    const filter = new RelatesToFilter();
    filter.relationType.equals("similar");
    const result = filter.asFilter();

    expect(result).toBeDefined();
    expect(result).toHaveProperty("equals");
  });

  it("should build filter with strength range", () => {
    const filter = new RelatesToFilter();
    filter.strength.greaterThanOrEquals(0.5).lessThanOrEquals(1.0);
    const result = filter.asFilter();

    expect(result).toBeDefined();
    expect(result).toHaveProperty("range");
  });
});
