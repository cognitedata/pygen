/**
 * Tests for filter types and helper functions.
 */

import { describe, it, expect } from "vitest";
import {
  equals,
  inFilter,
  range,
  prefix,
  exists,
  and,
  or,
  not,
  isEqualsFilter,
  isInFilter,
  isRangeFilter,
  isPrefixFilter,
  isAndFilter,
  isOrFilter,
  isNotFilter,
  type Filter,
} from "@cognite/pygen-typescript";

describe("Filter helper functions", () => {
  describe("equals", () => {
    it("should create an equals filter", () => {
      const filter = equals(["mySpace", "myView/v1", "myProp"], "value");
      expect(filter).toEqual({
        equals: {
          property: ["mySpace", "myView/v1", "myProp"],
          value: "value",
        },
      });
    });

    it("should work with numeric values", () => {
      const filter = equals(["node", "id"], 42);
      expect(filter).toEqual({
        equals: {
          property: ["node", "id"],
          value: 42,
        },
      });
    });

    it("should work with boolean values", () => {
      const filter = equals(["node", "active"], true);
      expect(filter).toEqual({
        equals: {
          property: ["node", "active"],
          value: true,
        },
      });
    });
  });

  describe("inFilter", () => {
    it("should create an in filter", () => {
      const filter = inFilter(["mySpace", "myView/v1", "status"], ["active", "pending"]);
      expect(filter).toEqual({
        in: {
          property: ["mySpace", "myView/v1", "status"],
          values: ["active", "pending"],
        },
      });
    });

    it("should work with numeric values", () => {
      const filter = inFilter(["node", "priority"], [1, 2, 3]);
      expect(filter).toEqual({
        in: {
          property: ["node", "priority"],
          values: [1, 2, 3],
        },
      });
    });
  });

  describe("range", () => {
    it("should create a range filter with all bounds", () => {
      const filter = range(["mySpace", "myView/v1", "value"], {
        gt: 0,
        gte: 1,
        lt: 100,
        lte: 99,
      });
      expect(filter).toEqual({
        range: {
          property: ["mySpace", "myView/v1", "value"],
          gt: 0,
          gte: 1,
          lt: 100,
          lte: 99,
        },
      });
    });

    it("should create a range filter with only lower bound", () => {
      const filter = range(["node", "value"], { gte: 10 });
      expect(filter).toEqual({
        range: {
          property: ["node", "value"],
          gte: 10,
        },
      });
    });

    it("should create a range filter with only upper bound", () => {
      const filter = range(["node", "value"], { lte: 100 });
      expect(filter).toEqual({
        range: {
          property: ["node", "value"],
          lte: 100,
        },
      });
    });

    it("should work with string values for datetime ranges", () => {
      const filter = range(["node", "createdTime"], {
        gte: "2023-01-01T00:00:00Z",
        lt: "2024-01-01T00:00:00Z",
      });
      expect(filter).toEqual({
        range: {
          property: ["node", "createdTime"],
          gte: "2023-01-01T00:00:00Z",
          lt: "2024-01-01T00:00:00Z",
        },
      });
    });
  });

  describe("prefix", () => {
    it("should create a prefix filter", () => {
      const filter = prefix(["mySpace", "myView/v1", "name"], "test");
      expect(filter).toEqual({
        prefix: {
          property: ["mySpace", "myView/v1", "name"],
          value: "test",
        },
      });
    });
  });

  describe("exists", () => {
    it("should create an exists filter", () => {
      const filter = exists(["mySpace", "myView/v1", "optionalField"]);
      expect(filter).toEqual({
        exists: {
          property: ["mySpace", "myView/v1", "optionalField"],
        },
      });
    });
  });

  describe("and", () => {
    it("should create an and filter with multiple filters", () => {
      const filter1 = equals(["node", "type"], "typeA");
      const filter2 = range(["node", "value"], { gte: 10 });
      const combined = and(filter1, filter2);

      expect(combined).toEqual({
        and: [
          { equals: { property: ["node", "type"], value: "typeA" } },
          { range: { property: ["node", "value"], gte: 10 } },
        ],
      });
    });

    it("should work with single filter", () => {
      const filter = equals(["node", "type"], "typeA");
      const combined = and(filter);

      expect(combined).toEqual({
        and: [{ equals: { property: ["node", "type"], value: "typeA" } }],
      });
    });
  });

  describe("or", () => {
    it("should create an or filter with multiple filters", () => {
      const filter1 = equals(["node", "status"], "active");
      const filter2 = equals(["node", "status"], "pending");
      const combined = or(filter1, filter2);

      expect(combined).toEqual({
        or: [
          { equals: { property: ["node", "status"], value: "active" } },
          { equals: { property: ["node", "status"], value: "pending" } },
        ],
      });
    });
  });

  describe("not", () => {
    it("should create a not filter", () => {
      const filter = equals(["node", "archived"], true);
      const negated = not(filter);

      expect(negated).toEqual({
        not: { equals: { property: ["node", "archived"], value: true } },
      });
    });

    it("should work with complex filters", () => {
      const filter = and(equals(["node", "type"], "typeA"), range(["node", "value"], { gte: 10 }));
      const negated = not(filter);

      expect(negated).toEqual({
        not: {
          and: [
            { equals: { property: ["node", "type"], value: "typeA" } },
            { range: { property: ["node", "value"], gte: 10 } },
          ],
        },
      });
    });
  });

  describe("nested logical filters", () => {
    it("should support nested and/or combinations", () => {
      const typeFilter = or(equals(["node", "type"], "typeA"), equals(["node", "type"], "typeB"));
      const valueFilter = range(["node", "value"], { gte: 10, lte: 100 });
      const combined = and(typeFilter, valueFilter);

      expect(combined).toEqual({
        and: [
          {
            or: [
              { equals: { property: ["node", "type"], value: "typeA" } },
              { equals: { property: ["node", "type"], value: "typeB" } },
            ],
          },
          { range: { property: ["node", "value"], gte: 10, lte: 100 } },
        ],
      });
    });
  });
});

describe("Filter type guards", () => {
  describe("isEqualsFilter", () => {
    it("should return true for equals filter", () => {
      const filter: Filter = equals(["node", "id"], "123");
      expect(isEqualsFilter(filter)).toBe(true);
    });

    it("should return false for other filter types", () => {
      const filter: Filter = inFilter(["node", "id"], ["123", "456"]);
      expect(isEqualsFilter(filter)).toBe(false);
    });
  });

  describe("isInFilter", () => {
    it("should return true for in filter", () => {
      const filter: Filter = inFilter(["node", "id"], ["123", "456"]);
      expect(isInFilter(filter)).toBe(true);
    });

    it("should return false for other filter types", () => {
      const filter: Filter = equals(["node", "id"], "123");
      expect(isInFilter(filter)).toBe(false);
    });
  });

  describe("isRangeFilter", () => {
    it("should return true for range filter", () => {
      const filter: Filter = range(["node", "value"], { gte: 10 });
      expect(isRangeFilter(filter)).toBe(true);
    });

    it("should return false for other filter types", () => {
      const filter: Filter = equals(["node", "value"], 10);
      expect(isRangeFilter(filter)).toBe(false);
    });
  });

  describe("isPrefixFilter", () => {
    it("should return true for prefix filter", () => {
      const filter: Filter = prefix(["node", "name"], "test");
      expect(isPrefixFilter(filter)).toBe(true);
    });

    it("should return false for other filter types", () => {
      const filter: Filter = equals(["node", "name"], "test");
      expect(isPrefixFilter(filter)).toBe(false);
    });
  });

  describe("isAndFilter", () => {
    it("should return true for and filter", () => {
      const filter: Filter = and(equals(["node", "id"], "123"));
      expect(isAndFilter(filter)).toBe(true);
    });

    it("should return false for other filter types", () => {
      const filter: Filter = equals(["node", "id"], "123");
      expect(isAndFilter(filter)).toBe(false);
    });
  });

  describe("isOrFilter", () => {
    it("should return true for or filter", () => {
      const filter: Filter = or(equals(["node", "id"], "123"));
      expect(isOrFilter(filter)).toBe(true);
    });

    it("should return false for other filter types", () => {
      const filter: Filter = equals(["node", "id"], "123");
      expect(isOrFilter(filter)).toBe(false);
    });
  });

  describe("isNotFilter", () => {
    it("should return true for not filter", () => {
      const filter: Filter = not(equals(["node", "id"], "123"));
      expect(isNotFilter(filter)).toBe(true);
    });

    it("should return false for other filter types", () => {
      const filter: Filter = equals(["node", "id"], "123");
      expect(isNotFilter(filter)).toBe(false);
    });
  });
});
