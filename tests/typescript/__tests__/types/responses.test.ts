/**
 * Tests for response types (paginated responses, CRUD results, aggregations).
 */

import { describe, expect, it } from "vitest";
import type {
  AggregatedHistogramValue,
  AggregatedNumberValue,
  AggregateItem,
  AggregateResponse,
  Bucket,
  DeleteResponse,
  InstanceResultItem,
  ListResponse,
  Page,
  UpsertResult,
} from "../../../../cognite/pygen/_typescript/instance_api/types/responses.ts";
import {
  extendUpsertResult,
  getCreated,
  getUnchanged,
  getUpdated,
} from "../../../../cognite/pygen/_typescript/instance_api/types/responses.ts";
import type { InstanceId } from "../../../../cognite/pygen/_typescript/instance_api/types/instance.ts";

describe("ListResponse", () => {
  it("should create a valid ListResponse", () => {
    const response: ListResponse<string[]> = {
      items: ["item1", "item2"],
    };

    expect(response.items).toEqual(["item1", "item2"]);
    expect(response.typing).toBeUndefined();
  });

  it("should create a ListResponse with typing information", () => {
    const response: ListResponse<string[]> = {
      items: ["item1", "item2"],
      typing: { test: "info" },
    };

    expect(response.items).toEqual(["item1", "item2"]);
    expect(response.typing).toEqual({ test: "info" });
  });
});

describe("Page", () => {
  it("should create a valid Page without cursor", () => {
    const page: Page<string[]> = {
      items: ["item1", "item2"],
    };

    expect(page.items).toEqual(["item1", "item2"]);
    expect(page.nextCursor).toBeUndefined();
  });

  it("should create a Page with nextCursor", () => {
    const page: Page<string[]> = {
      items: ["item1", "item2"],
      nextCursor: "cursor123",
    };

    expect(page.items).toEqual(["item1", "item2"]);
    expect(page.nextCursor).toBe("cursor123");
  });

  it("should create a Page with debug information", () => {
    const page: Page<string[]> = {
      items: ["item1", "item2"],
      debug: {
        queryTimeMs: 42.5,
        parseTimeMs: 2.3,
        serializeTimeMs: 1.1,
      },
    };

    expect(page.items).toEqual(["item1", "item2"]);
    expect(page.debug).toEqual({
      queryTimeMs: 42.5,
      parseTimeMs: 2.3,
      serializeTimeMs: 1.1,
    });
  });

  it("should create a Page with typing information", () => {
    const page: Page<string[]> = {
      items: ["item1"],
      typing: { Person: { name: "Text", age: "Int32" } },
    };

    expect(page.typing).toEqual({ Person: { name: "Text", age: "Int32" } });
  });
});

describe("InstanceResultItem", () => {
  it("should create a valid node InstanceResultItem", () => {
    const item: InstanceResultItem = {
      instanceType: "node",
      space: "mySpace",
      externalId: "person-1",
      version: 1,
      wasModified: true,
      createdTime: 1234567890000,
      lastUpdatedTime: 1234567890000,
    };

    expect(item.instanceType).toBe("node");
    expect(item.space).toBe("mySpace");
    expect(item.externalId).toBe("person-1");
    expect(item.version).toBe(1);
    expect(item.wasModified).toBe(true);
    expect(item.createdTime).toBe(1234567890000);
    expect(item.lastUpdatedTime).toBe(1234567890000);
  });

  it("should create a valid edge InstanceResultItem", () => {
    const item: InstanceResultItem = {
      instanceType: "edge",
      space: "mySpace",
      externalId: "edge-1",
      version: 2,
      wasModified: false,
      createdTime: 1234567890000,
      lastUpdatedTime: 2345678900000,
    };

    expect(item.instanceType).toBe("edge");
    expect(item.wasModified).toBe(false);
  });
});

describe("UpsertResult", () => {
  const createdItem: InstanceResultItem = {
    instanceType: "node",
    space: "test",
    externalId: "person-1",
    version: 1,
    wasModified: true,
    createdTime: 1234567890000,
    lastUpdatedTime: 1234567890000,
  };

  const updatedItem: InstanceResultItem = {
    instanceType: "node",
    space: "test",
    externalId: "person-2",
    version: 2,
    wasModified: true,
    createdTime: 1234567890000,
    lastUpdatedTime: 2345678900000,
  };

  const unchangedItem: InstanceResultItem = {
    instanceType: "node",
    space: "test",
    externalId: "person-3",
    version: 1,
    wasModified: false,
    createdTime: 1234567890000,
    lastUpdatedTime: 1234567890000,
  };

  it("should create an empty UpsertResult", () => {
    const result: UpsertResult = {
      items: [],
      deleted: [],
    };

    expect(result.items).toEqual([]);
    expect(result.deleted).toEqual([]);
  });

  it("should categorize created items correctly", () => {
    const result: UpsertResult = {
      items: [createdItem],
      deleted: [],
    };

    const created = getCreated(result);
    expect(created).toHaveLength(1);
    expect(created[0].externalId).toBe("person-1");
  });

  it("should categorize updated items correctly", () => {
    const result: UpsertResult = {
      items: [updatedItem],
      deleted: [],
    };

    const updated = getUpdated(result);
    expect(updated).toHaveLength(1);
    expect(updated[0].externalId).toBe("person-2");
  });

  it("should categorize unchanged items correctly", () => {
    const result: UpsertResult = {
      items: [unchangedItem],
      deleted: [],
    };

    const unchanged = getUnchanged(result);
    expect(unchanged).toHaveLength(1);
    expect(unchanged[0].externalId).toBe("person-3");
  });

  it("should categorize mixed items correctly", () => {
    const result: UpsertResult = {
      items: [createdItem, updatedItem, unchangedItem],
      deleted: [],
    };

    const created = getCreated(result);
    const updated = getUpdated(result);
    const unchanged = getUnchanged(result);

    expect(created).toHaveLength(1);
    expect(updated).toHaveLength(1);
    expect(unchanged).toHaveLength(1);

    expect(created[0].externalId).toBe("person-1");
    expect(updated[0].externalId).toBe("person-2");
    expect(unchanged[0].externalId).toBe("person-3");
  });

  it("should track deleted items", () => {
    const deleted: InstanceId[] = [
      { space: "test", externalId: "deleted-1" },
      { space: "test", externalId: "deleted-2" },
    ];

    const result: UpsertResult = {
      items: [],
      deleted,
    };

    expect(result.deleted).toHaveLength(2);
    expect(result.deleted[0].externalId).toBe("deleted-1");
  });

  it("should extend UpsertResult with another result", () => {
    const result1: UpsertResult = {
      items: [createdItem],
      deleted: [{ space: "test", externalId: "deleted-1" }],
    };

    const result2: UpsertResult = {
      items: [updatedItem],
      deleted: [{ space: "test", externalId: "deleted-2" }],
    };

    extendUpsertResult(result1, result2);

    expect(result1.items).toHaveLength(2);
    expect(result1.deleted).toHaveLength(2);
    expect(result1.items[0].externalId).toBe("person-1");
    expect(result1.items[1].externalId).toBe("person-2");
  });
});

describe("DeleteResponse", () => {
  it("should create a valid DeleteResponse", () => {
    const response: DeleteResponse = {
      items: [
        { space: "mySpace", externalId: "person-1" },
        { space: "mySpace", externalId: "person-2" },
      ],
    };

    expect(response.items).toHaveLength(2);
    expect(response.items[0].externalId).toBe("person-1");
    expect(response.items[1].externalId).toBe("person-2");
  });

  it("should create an empty DeleteResponse", () => {
    const response: DeleteResponse = {
      items: [],
    };

    expect(response.items).toEqual([]);
  });
});

describe("AggregatedNumberValue", () => {
  it("should create a count aggregation result", () => {
    const result: AggregatedNumberValue = {
      aggregate: "count",
      value: 100,
    };

    expect(result.aggregate).toBe("count");
    expect(result.value).toBe(100);
    expect(result.property).toBeUndefined();
  });

  it("should create an avg aggregation result", () => {
    const result: AggregatedNumberValue = {
      aggregate: "avg",
      property: "age",
      value: 42.5,
    };

    expect(result.aggregate).toBe("avg");
    expect(result.property).toBe("age");
    expect(result.value).toBe(42.5);
  });

  it("should create a min aggregation result", () => {
    const result: AggregatedNumberValue = {
      aggregate: "min",
      property: "age",
      value: 18,
    };

    expect(result.aggregate).toBe("min");
    expect(result.property).toBe("age");
    expect(result.value).toBe(18);
  });

  it("should create a max aggregation result", () => {
    const result: AggregatedNumberValue = {
      aggregate: "max",
      property: "age",
      value: 99,
    };

    expect(result.aggregate).toBe("max");
    expect(result.property).toBe("age");
    expect(result.value).toBe(99);
  });

  it("should create a sum aggregation result", () => {
    const result: AggregatedNumberValue = {
      aggregate: "sum",
      property: "score",
      value: 1500,
    };

    expect(result.aggregate).toBe("sum");
    expect(result.property).toBe("score");
    expect(result.value).toBe(1500);
  });
});

describe("AggregatedHistogramValue", () => {
  it("should create a histogram aggregation result", () => {
    const buckets: Bucket[] = [
      { start: 0, count: 5 },
      { start: 10, count: 15 },
      { start: 20, count: 10 },
    ];

    const result: AggregatedHistogramValue = {
      aggregate: "histogram",
      property: "age",
      interval: 10,
      buckets,
    };

    expect(result.aggregate).toBe("histogram");
    expect(result.property).toBe("age");
    expect(result.interval).toBe(10);
    expect(result.buckets).toHaveLength(3);
    expect(result.buckets[1].start).toBe(10);
    expect(result.buckets[1].count).toBe(15);
  });

  it("should create an empty histogram", () => {
    const result: AggregatedHistogramValue = {
      aggregate: "histogram",
      property: "age",
      interval: 5,
      buckets: [],
    };

    expect(result.buckets).toEqual([]);
  });
});

describe("AggregateItem", () => {
  it("should create an AggregateItem without grouping", () => {
    const item: AggregateItem = {
      instanceType: "node",
      aggregates: [
        { aggregate: "count", value: 100 },
        { aggregate: "avg", property: "age", value: 42.5 },
      ],
    };

    expect(item.instanceType).toBe("node");
    expect(item.group).toBeUndefined();
    expect(item.aggregates).toHaveLength(2);
  });

  it("should create an AggregateItem with grouping", () => {
    const item: AggregateItem = {
      instanceType: "node",
      group: {
        country: "Norway",
        city: "Oslo",
      },
      aggregates: [{ aggregate: "count", value: 50 }],
    };

    expect(item.group).toEqual({
      country: "Norway",
      city: "Oslo",
    });
    expect(item.aggregates[0].value).toBe(50);
  });

  it("should allow NodeReference in group values", () => {
    const item: AggregateItem = {
      instanceType: "node",
      group: {
        category: {
          space: "mySpace",
          externalId: "category-1",
        },
      },
      aggregates: [{ aggregate: "count", value: 25 }],
    };

    expect(item.group?.category).toEqual({
      space: "mySpace",
      externalId: "category-1",
    });
  });

  it("should create an AggregateItem with histogram", () => {
    const item: AggregateItem = {
      instanceType: "node",
      aggregates: [
        {
          aggregate: "histogram",
          property: "age",
          interval: 10,
          buckets: [
            { start: 0, count: 5 },
            { start: 10, count: 15 },
          ],
        },
      ],
    };

    expect(item.aggregates[0].aggregate).toBe("histogram");
    const histogramAgg = item.aggregates[0] as AggregatedHistogramValue;
    expect(histogramAgg.buckets).toHaveLength(2);
  });
});

describe("AggregateResponse", () => {
  it("should create a valid AggregateResponse", () => {
    const response: AggregateResponse = {
      items: [
        {
          instanceType: "node",
          aggregates: [{ aggregate: "count", value: 100 }],
        },
      ],
    };

    expect(response.items).toHaveLength(1);
    expect(response.items[0].aggregates[0].value).toBe(100);
  });

  it("should create an AggregateResponse with typing", () => {
    const response: AggregateResponse = {
      items: [
        {
          instanceType: "node",
          aggregates: [{ aggregate: "count", value: 100 }],
        },
      ],
      typing: { test: "info" },
    };

    expect(response.typing).toEqual({ test: "info" });
  });

  it("should create an AggregateResponse with multiple items", () => {
    const response: AggregateResponse = {
      items: [
        {
          instanceType: "node",
          group: { country: "Norway" },
          aggregates: [{ aggregate: "count", value: 50 }],
        },
        {
          instanceType: "node",
          group: { country: "Sweden" },
          aggregates: [{ aggregate: "count", value: 30 }],
        },
      ],
    };

    expect(response.items).toHaveLength(2);
    expect(response.items[0].aggregates[0].value).toBe(50);
    expect(response.items[1].aggregates[0].value).toBe(30);
  });

  it("should create an AggregateResponse with multiple aggregations per item", () => {
    const response: AggregateResponse = {
      items: [
        {
          instanceType: "node",
          aggregates: [
            { aggregate: "count", value: 100 },
            { aggregate: "avg", property: "age", value: 42.5 },
            { aggregate: "min", property: "age", value: 18 },
            { aggregate: "max", property: "age", value: 99 },
          ],
        },
      ],
    };

    expect(response.items[0].aggregates).toHaveLength(4);
    expect(response.items[0].aggregates[1].aggregate).toBe("avg");
  });
});
