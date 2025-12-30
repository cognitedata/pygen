/**
 * Tests for data type filter builders.
 */

import { beforeEach, describe, expect, it } from "vitest";
import {
  BooleanFilter,
  type DataTypeFilter,
  DateFilter,
  DateTimeFilter,
  DirectRelationFilter,
  FilterContainer,
  FloatFilter,
  IntegerFilter,
  TextFilter,
  type ViewReference,
} from "@cognite/pygen-_typescript";

describe("FloatFilter", () => {
  let filter: FloatFilter;
  const viewRef: ViewReference = { space: "mySpace", externalId: "myView", version: "v1" };

  beforeEach(() => {
    filter = new FloatFilter(viewRef, "temperature", "and");
  });

  it("should create an equals filter", () => {
    filter.equals(25.5);
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["mySpace", "myView/v1", "temperature"],
        value: 25.5,
      },
    });
  });

  it("should skip null values", () => {
    filter.equals(null);
    const result = filter.asFilter();

    expect(result).toBeUndefined();
  });

  it("should create a range filter with less than", () => {
    filter.lessThan(100);
    const result = filter.asFilter();

    expect(result).toEqual({
      range: {
        property: ["mySpace", "myView/v1", "temperature"],
        lt: 100,
      },
    });
  });

  it("should create a range filter with greater than or equals", () => {
    filter.greaterThanOrEquals(0);
    const result = filter.asFilter();

    expect(result).toEqual({
      range: {
        property: ["mySpace", "myView/v1", "temperature"],
        gte: 0,
      },
    });
  });

  it("should combine multiple range conditions", () => {
    filter.greaterThanOrEquals(0).lessThan(100);
    const result = filter.asFilter();

    expect(result).toEqual({
      range: {
        property: ["mySpace", "myView/v1", "temperature"],
        gte: 0,
        lt: 100,
      },
    });
  });

  it("should support method chaining", () => {
    const result = filter.greaterThan(10).lessThanOrEquals(90).asFilter();

    expect(result).toEqual({
      range: {
        property: ["mySpace", "myView/v1", "temperature"],
        gt: 10,
        lte: 90,
      },
    });
  });

  it("should handle lessThanOrEquals", () => {
    filter.lessThanOrEquals(100);
    const result = filter.asFilter();

    expect(result).toEqual({
      range: {
        property: ["mySpace", "myView/v1", "temperature"],
        lte: 100,
      },
    });
  });

  it("should handle greaterThanOrEquals", () => {
    filter.greaterThanOrEquals(0);
    const result = filter.asFilter();

    expect(result).toEqual({
      range: {
        property: ["mySpace", "myView/v1", "temperature"],
        gte: 0,
      },
    });
  });
});

describe("IntegerFilter", () => {
  let filter: IntegerFilter;

  beforeEach(() => {
    filter = new IntegerFilter("node", "count", "and");
  });

  it("should create an equals filter", () => {
    filter.equals(42);
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["node", "count"],
        value: 42,
      },
    });
  });

  it("should floor float values", () => {
    filter.equals(42.7);
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["node", "count"],
        value: 42,
      },
    });
  });

  it("should create a range filter", () => {
    filter.greaterThanOrEquals(1).lessThanOrEquals(10);
    const result = filter.asFilter();

    expect(result).toEqual({
      range: {
        property: ["node", "count"],
        gte: 1,
        lte: 10,
      },
    });
  });

  it("should handle all range methods", () => {
    const filter1 = new IntegerFilter("node", "value1", "and");
    filter1.lessThan(100);
    expect(filter1.asFilter()).toEqual({
      range: {
        property: ["node", "value1"],
        lt: 100,
      },
    });

    const filter2 = new IntegerFilter("node", "value2", "and");
    filter2.greaterThan(0);
    expect(filter2.asFilter()).toEqual({
      range: {
        property: ["node", "value2"],
        gt: 0,
      },
    });
  });
});

describe("DateTimeFilter", () => {
  let filter: DateTimeFilter;
  const viewRef: ViewReference = { space: "mySpace", externalId: "myView", version: "v1" };

  beforeEach(() => {
    filter = new DateTimeFilter(viewRef, "timestamp", "and");
  });

  it("should create an equals filter with Date object", () => {
    const date = new Date("2023-06-15T10:30:00Z");
    filter.equals(date);
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["mySpace", "myView/v1", "timestamp"],
        value: "2023-06-15T10:30:00.000Z",
      },
    });
  });

  it("should create an equals filter with ISO string", () => {
    filter.equals("2023-06-15T10:30:00Z");
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["mySpace", "myView/v1", "timestamp"],
        value: "2023-06-15T10:30:00.000Z",
      },
    });
  });

  it("should throw error for invalid datetime string", () => {
    expect(() => {
      filter.equals("not a date");
    }).toThrow("String 'not a date' is not a valid ISO 8601 datetime format.");
  });

  it("should throw error for invalid date that matches ISO format but is not a real date", () => {
    expect(() => {
      filter.equals("9999-99-99T99:99:99.999Z");
    }).toThrow("String '9999-99-99T99:99:99.999Z' is not a valid ISO 8601 datetime format.");
  });

  it("should create a range filter", () => {
    filter
      .greaterThanOrEquals(new Date("2023-01-01T00:00:00Z"))
      .lessThan(new Date("2024-01-01T00:00:00Z"));
    const result = filter.asFilter();

    expect(result).toEqual({
      range: {
        property: ["mySpace", "myView/v1", "timestamp"],
        gte: "2023-01-01T00:00:00.000Z",
        lt: "2024-01-01T00:00:00.000Z",
      },
    });
  });

  it("should skip null values", () => {
    filter.equals(null);
    const result = filter.asFilter();

    expect(result).toBeUndefined();
  });

  it("should handle all range methods individually", () => {
    const filter1 = new DateTimeFilter(viewRef, "time1", "and");
    filter1.lessThan(new Date("2024-01-01T00:00:00Z"));
    expect(filter1.asFilter()).toHaveProperty("range");

    const filter2 = new DateTimeFilter(viewRef, "time2", "and");
    filter2.greaterThan(new Date("2023-01-01T00:00:00Z"));
    expect(filter2.asFilter()).toHaveProperty("range");

    const filter3 = new DateTimeFilter(viewRef, "time3", "and");
    filter3.lessThanOrEquals(new Date("2024-01-01T00:00:00Z"));
    expect(filter3.asFilter()).toHaveProperty("range");
  });
});

describe("DateFilter", () => {
  let filter: DateFilter;

  beforeEach(() => {
    filter = new DateFilter("node", "birthdate", "and");
  });

  it("should create an equals filter with Date object", () => {
    const date = new Date("2023-06-15T10:30:00Z");
    filter.equals(date);
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["node", "birthdate"],
        value: "2023-06-15",
      },
    });
  });

  it("should create an equals filter with date string", () => {
    filter.equals("2023-06-15");
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["node", "birthdate"],
        value: "2023-06-15",
      },
    });
  });

  it("should create a range filter", () => {
    filter.greaterThanOrEquals("2023-01-01").lessThan("2024-01-01");
    const result = filter.asFilter();

    expect(result).toEqual({
      range: {
        property: ["node", "birthdate"],
        gte: "2023-01-01",
        lt: "2024-01-01",
      },
    });
  });

  it("should handle all range methods individually", () => {
    const filter1 = new DateFilter("node", "date1", "and");
    filter1.lessThanOrEquals("2024-01-01");
    expect(filter1.asFilter()).toHaveProperty("range");

    const filter2 = new DateFilter("node", "date2", "and");
    filter2.greaterThan("2023-01-01");
    expect(filter2.asFilter()).toHaveProperty("range");

    const filter3 = new DateFilter("node", "date3", "and");
    filter3.equals(null);
    expect(filter3.asFilter()).toBeUndefined();

    const filter4 = new DateFilter("node", "date4", "and");
    expect(() => filter4.equals("invalid-date")).toThrow("not a valid ISO format date");
  });
});

describe("TextFilter", () => {
  let filter: TextFilter;
  const viewRef: ViewReference = { space: "mySpace", externalId: "myView", version: "v1" };

  beforeEach(() => {
    filter = new TextFilter(viewRef, "name", "and");
  });

  it("should create an equals filter", () => {
    filter.equals("John Doe");
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["mySpace", "myView/v1", "name"],
        value: "John Doe",
      },
    });
  });

  it("should create a prefix filter", () => {
    filter.prefix("John");
    const result = filter.asFilter();

    expect(result).toEqual({
      prefix: {
        property: ["mySpace", "myView/v1", "name"],
        value: "John",
      },
    });
  });

  it("should create an in filter", () => {
    filter.in(["Alice", "Bob", "Charlie"]);
    const result = filter.asFilter();

    expect(result).toEqual({
      in: {
        property: ["mySpace", "myView/v1", "name"],
        values: ["Alice", "Bob", "Charlie"],
      },
    });
  });

  it("should support equalsOrIn with single value", () => {
    filter.equalsOrIn("John");
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["mySpace", "myView/v1", "name"],
        value: "John",
      },
    });
  });

  it("should support equalsOrIn with array", () => {
    filter.equalsOrIn(["Alice", "Bob"]);
    const result = filter.asFilter();

    expect(result).toEqual({
      in: {
        property: ["mySpace", "myView/v1", "name"],
        values: ["Alice", "Bob"],
      },
    });
  });

  it("should skip null values", () => {
    filter.equals(null);
    const result = filter.asFilter();

    expect(result).toBeUndefined();
  });
});

describe("BooleanFilter", () => {
  let filter: BooleanFilter;

  beforeEach(() => {
    filter = new BooleanFilter("node", "isActive", "and");
  });

  it("should create an equals filter for true", () => {
    filter.equals(true);
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["node", "isActive"],
        value: true,
      },
    });
  });

  it("should create an equals filter for false", () => {
    filter.equals(false);
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["node", "isActive"],
        value: false,
      },
    });
  });

  it("should skip null values", () => {
    filter.equals(null);
    const result = filter.asFilter();

    expect(result).toBeUndefined();
  });
});

describe("DirectRelationFilter", () => {
  let filter: DirectRelationFilter;
  const viewRef: ViewReference = { space: "mySpace", externalId: "myView", version: "v1" };

  beforeEach(() => {
    filter = new DirectRelationFilter(viewRef, "relatedTo", "and");
  });

  it("should create an equals filter with tuple", () => {
    filter.equals(["space1", "id1"]);
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["mySpace", "myView/v1", "relatedTo"],
        value: { space: "space1", externalId: "id1" },
      },
    });
  });

  it("should create an equals filter with InstanceId object", () => {
    filter.equals({ space: "space1", externalId: "id1" });
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["mySpace", "myView/v1", "relatedTo"],
        value: { space: "space1", externalId: "id1" },
      },
    });
  });

  it("should create an equals filter with string and space", () => {
    filter.equals("id1", "space1");
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["mySpace", "myView/v1", "relatedTo"],
        value: { space: "space1", externalId: "id1" },
      },
    });
  });

  it("should throw error when string without space", () => {
    expect(() => {
      filter.equals("id1");
    }).toThrow("Space must be provided when value is a string");
  });

  it("should create an in filter", () => {
    filter.in([
      ["space1", "id1"],
      ["space2", "id2"],
    ]);
    const result = filter.asFilter();

    expect(result).toEqual({
      in: {
        property: ["mySpace", "myView/v1", "relatedTo"],
        values: [
          { space: "space1", externalId: "id1" },
          { space: "space2", externalId: "id2" },
        ],
      },
    });
  });

  it("should create an in filter with strings and space", () => {
    filter.in(["id1", "id2"], "space1");
    const result = filter.asFilter();

    expect(result).toEqual({
      in: {
        property: ["mySpace", "myView/v1", "relatedTo"],
        values: [
          { space: "space1", externalId: "id1" },
          { space: "space1", externalId: "id2" },
        ],
      },
    });
  });

  it("should support equalsOrIn with single value", () => {
    filter.equalsOrIn(["space1", "id1"]);
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["mySpace", "myView/v1", "relatedTo"],
        value: { space: "space1", externalId: "id1" },
      },
    });
  });

  it("should support equalsOrIn with array", () => {
    filter.equalsOrIn([
      ["space1", "id1"],
      ["space2", "id2"],
    ]);
    const result = filter.asFilter();

    expect(result).toEqual({
      in: {
        property: ["mySpace", "myView/v1", "relatedTo"],
        values: [
          { space: "space1", externalId: "id1" },
          { space: "space2", externalId: "id2" },
        ],
      },
    });
  });

  it("should skip null values", () => {
    filter.equals(null);
    const result = filter.asFilter();

    expect(result).toBeUndefined();
  });

  it("should throw error for invalid value type", () => {
    expect(() => {
      // @ts-expect-error - Testing invalid type
      filter.equals(123);
    }).toThrow("Expected string, InstanceId, or [string, string]");
  });

  it("should skip null values in equalsOrIn", () => {
    filter.equalsOrIn(null);
    const result = filter.asFilter();

    expect(result).toBeUndefined();
  });

  it("should handle string value in equalsOrIn with space", () => {
    filter.equalsOrIn("id1", "space1");
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["mySpace", "myView/v1", "relatedTo"],
        value: { space: "space1", externalId: "id1" },
      },
    });
  });

  it("should skip null values in in filter", () => {
    filter.in(null);
    const result = filter.asFilter();

    expect(result).toBeUndefined();
  });

  it("should filter out null values from in array", () => {
    // Create a filter that will have null after validation
    const result = filter.in([]);
    expect(result.asFilter()).toBeUndefined();
  });
});

describe("FilterContainer", () => {
  let container: FilterContainer;
  let filters: DataTypeFilter[];

  beforeEach(() => {
    filters = [];
    container = new FilterContainer(filters, "and", "node");
  });

  it("should have standard instance metadata filters", () => {
    expect(container.space).toBeInstanceOf(TextFilter);
    expect(container.externalId).toBeInstanceOf(TextFilter);
    expect(container.version).toBeInstanceOf(IntegerFilter);
    expect(container.type).toBeInstanceOf(DirectRelationFilter);
    expect(container.createdTime).toBeInstanceOf(DateTimeFilter);
    expect(container.lastUpdatedTime).toBeInstanceOf(DateTimeFilter);
    expect(container.deletedTime).toBeInstanceOf(DateTimeFilter);
  });

  it("should not have edge-specific filters for nodes", () => {
    expect(container.startNode).toBeUndefined();
    expect(container.endNode).toBeUndefined();
  });

  it("should have edge-specific filters for edges", () => {
    const edgeFilters: DataTypeFilter[] = [];
    const edgeContainer = new FilterContainer(edgeFilters, "and", "edge");

    expect(edgeContainer.startNode).toBeInstanceOf(DirectRelationFilter);
    expect(edgeContainer.endNode).toBeInstanceOf(DirectRelationFilter);
  });

  it("should return undefined when no filters are set", () => {
    const result = container.asFilter();
    expect(result).toBeUndefined();
  });

  it("should return single filter when only one is set", () => {
    container.space.equals("mySpace");
    const result = container.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["node", "space"],
        value: "mySpace",
      },
    });
  });

  it("should combine multiple filters with AND", () => {
    container.space.equals("mySpace");
    container.version.greaterThanOrEquals(1);
    const result = container.asFilter();

    expect(result).toEqual({
      and: [
        {
          equals: {
            property: ["node", "space"],
            value: "mySpace",
          },
        },
        {
          range: {
            property: ["node", "version"],
            gte: 1,
          },
        },
      ],
    });
  });

  it("should combine multiple filters with OR", () => {
    const orFilters: DataTypeFilter[] = [];
    const orContainer = new FilterContainer(orFilters, "or", "node");

    orContainer.space.equals("space1");
    orContainer.version.equals(1);
    orContainer.externalId.equals("id1");
    const result = orContainer.asFilter();

    expect(result).toHaveProperty("or");
    const orFilter = result as { or: unknown[] };
    expect(orFilter.or.length).toBeGreaterThan(1);
  });

  it("should skip filters that have no conditions", () => {
    container.space.equals("mySpace");
    // Create a filter but don't add any conditions to version
    const result = container.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["node", "space"],
        value: "mySpace",
      },
    });
  });
});

describe("DataTypeFilter property paths", () => {
  it("should use instance type path for node", () => {
    const filter = new TextFilter("node", "space", "and");
    filter.equals("mySpace");
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["node", "space"],
        value: "mySpace",
      },
    });
  });

  it("should use instance type path for edge", () => {
    const filter = new TextFilter("edge", "space", "and");
    filter.equals("mySpace");
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["edge", "space"],
        value: "mySpace",
      },
    });
  });

  it("should use view reference path", () => {
    const viewRef: ViewReference = { space: "mySpace", externalId: "myView", version: "v1" };
    const filter = new TextFilter(viewRef, "myProp", "and");
    filter.equals("value");
    const result = filter.asFilter();

    expect(result).toEqual({
      equals: {
        property: ["mySpace", "myView/v1", "myProp"],
        value: "value",
      },
    });
  });
});

describe("Filter combination with operator", () => {
  it("should combine multiple conditions with AND operator (range merge)", () => {
    const viewRef: ViewReference = { space: "mySpace", externalId: "myView", version: "v1" };
    const filter = new FloatFilter(viewRef, "temperature", "and");

    filter.greaterThan(0).lessThan(100);
    const result = filter.asFilter();

    // Should combine into a single range filter
    expect(result).toEqual({
      range: {
        property: ["mySpace", "myView/v1", "temperature"],
        gt: 0,
        lt: 100,
      },
    });
  });

  it("should combine multiple conditions with AND operator (multiple filters)", () => {
    const viewRef: ViewReference = { space: "mySpace", externalId: "myView", version: "v1" };
    const filter = new FloatFilter(viewRef, "value", "and");

    // Create two separate filter types to force multiple filter objects
    filter.equals(10);
    filter.greaterThan(100);
    const result = filter.asFilter();

    // Should combine with AND
    expect(result).toHaveProperty("and");
    const andFilter = result as { and: unknown[] };
    expect(andFilter.and).toHaveLength(2);
  });

  it("should combine multiple conditions with OR operator", () => {
    const viewRef: ViewReference = { space: "mySpace", externalId: "myView", version: "v1" };
    const filter = new FloatFilter(viewRef, "value", "or");

    // Create two separate filter types to force multiple filter objects
    filter.equals(10);
    filter.greaterThan(100);
    const result = filter.asFilter();

    // Should combine with OR
    expect(result).toHaveProperty("or");
    const orFilter = result as { or: unknown[] };
    expect(orFilter.or).toHaveLength(2);
  });

  it("should handle dump method", () => {
    const filter = new TextFilter("node", "name", "and");
    filter.equals("test");
    const dumped = filter.dump();

    expect(dumped).toEqual({
      equals: {
        property: ["node", "name"],
        value: "test",
      },
    });
  });

  it("should return undefined from dump when no conditions", () => {
    const filter = new TextFilter("node", "name", "and");
    const dumped = filter.dump();

    expect(dumped).toBeUndefined();
  });
});
