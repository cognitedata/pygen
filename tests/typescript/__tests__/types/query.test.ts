/**
 * Tests for query-related types (sorting, units, debug, aggregations).
 */

import { describe, expect, it } from "vitest";
import {
  type Aggregation,
  type AggregationRequest,
  AVAILABLE_AGGREGATES,
  type Avg,
  type Count,
  type DebugParameters,
  type Histogram,
  type Max,
  type Min,
  type PropertySort,
  type Sum,
  type UnitConversion,
  type UnitReference,
  type UnitSystemReference,
  validateAggregationRequest,
} from "../../../../cognite/pygen/_generation/typescript/instance_api/types/query.ts";

describe("PropertySort", () => {
  it("should create a valid PropertySort with view property", () => {
    const sort: PropertySort = {
      property: ["my_space", "MyView/v1", "name"],
      direction: "ascending",
    };

    expect(sort.property).toEqual(["my_space", "MyView/v1", "name"]);
    expect(sort.direction).toBe("ascending");
  });

  it("should create a valid PropertySort with node property", () => {
    const sort: PropertySort = {
      property: ["node", "externalId"],
      direction: "descending",
      nullsFirst: true,
    };

    expect(sort.property).toEqual(["node", "externalId"]);
    expect(sort.direction).toBe("descending");
    expect(sort.nullsFirst).toBe(true);
  });

  it("should use default direction when not specified", () => {
    const sort: PropertySort = {
      property: ["node", "externalId"],
    };

    expect(sort.direction).toBeUndefined();
  });

  it("should allow nullsFirst to be undefined", () => {
    const sort: PropertySort = {
      property: ["node", "externalId"],
    };

    expect(sort.nullsFirst).toBeUndefined();
  });
});

describe("UnitConversion", () => {
  it("should create a UnitConversion with UnitReference", () => {
    const unitRef: UnitReference = { externalId: "temperature:cel" };
    const conversion: UnitConversion = {
      property: "temperature",
      unit: unitRef,
    };

    expect(conversion.property).toBe("temperature");
    expect(conversion.unit).toEqual({ externalId: "temperature:cel" });
  });

  it("should create a UnitConversion with UnitSystemReference", () => {
    const unitSysRef: UnitSystemReference = { unitSystemName: "SI" };
    const conversion: UnitConversion = {
      property: "distance",
      unit: unitSysRef,
    };

    expect(conversion.property).toBe("distance");
    expect(conversion.unit).toEqual({ unitSystemName: "SI" });
  });
});

describe("DebugParameters", () => {
  it("should create DebugParameters with all fields", () => {
    const debug: DebugParameters = {
      emitResults: false,
      timeout: 5000,
      profile: true,
    };

    expect(debug.emitResults).toBe(false);
    expect(debug.timeout).toBe(5000);
    expect(debug.profile).toBe(true);
  });

  it("should allow undefined fields", () => {
    const debug: DebugParameters = {};

    expect(debug.emitResults).toBeUndefined();
    expect(debug.timeout).toBeUndefined();
    expect(debug.profile).toBeUndefined();
  });

  it("should create DebugParameters with partial fields", () => {
    const debug: DebugParameters = {
      emitResults: false,
      profile: true,
    };

    expect(debug.emitResults).toBe(false);
    expect(debug.timeout).toBeUndefined();
    expect(debug.profile).toBe(true);
  });
});

describe("Aggregation Types", () => {
  it("should create a Count aggregation", () => {
    const count: Count = {
      aggregate: "count",
    };

    expect(count.aggregate).toBe("count");
    expect(count.property).toBeUndefined();
  });

  it("should create a Count aggregation with property", () => {
    const count: Count = {
      aggregate: "count",
      property: "externalId",
    };

    expect(count.aggregate).toBe("count");
    expect(count.property).toBe("externalId");
  });

  it("should create a Sum aggregation", () => {
    const sum: Sum = {
      aggregate: "sum",
      property: "score",
    };

    expect(sum.aggregate).toBe("sum");
    expect(sum.property).toBe("score");
  });

  it("should create an Avg aggregation", () => {
    const avg: Avg = {
      aggregate: "avg",
      property: "age",
    };

    expect(avg.aggregate).toBe("avg");
    expect(avg.property).toBe("age");
  });

  it("should create a Min aggregation", () => {
    const min: Min = {
      aggregate: "min",
      property: "age",
    };

    expect(min.aggregate).toBe("min");
    expect(min.property).toBe("age");
  });

  it("should create a Max aggregation", () => {
    const max: Max = {
      aggregate: "max",
      property: "age",
    };

    expect(max.aggregate).toBe("max");
    expect(max.property).toBe("age");
  });

  it("should create a Histogram aggregation", () => {
    const histogram: Histogram = {
      aggregate: "histogram",
      property: "age",
      interval: 10,
    };

    expect(histogram.aggregate).toBe("histogram");
    expect(histogram.property).toBe("age");
    expect(histogram.interval).toBe(10);
  });

  it("should allow Aggregation union type", () => {
    const aggregations: Aggregation[] = [
      { aggregate: "count" },
      { aggregate: "sum", property: "score" },
      { aggregate: "avg", property: "age" },
      { aggregate: "min", property: "age" },
      { aggregate: "max", property: "age" },
      { aggregate: "histogram", property: "age", interval: 5 },
    ];

    expect(aggregations).toHaveLength(6);
    expect(aggregations[0].aggregate).toBe("count");
    expect(aggregations[5].aggregate).toBe("histogram");
  });
});

describe("AVAILABLE_AGGREGATES", () => {
  it("should contain all aggregation types", () => {
    expect(AVAILABLE_AGGREGATES.size).toBe(6);
    expect(AVAILABLE_AGGREGATES.has("count")).toBe(true);
    expect(AVAILABLE_AGGREGATES.has("sum")).toBe(true);
    expect(AVAILABLE_AGGREGATES.has("avg")).toBe(true);
    expect(AVAILABLE_AGGREGATES.has("min")).toBe(true);
    expect(AVAILABLE_AGGREGATES.has("max")).toBe(true);
    expect(AVAILABLE_AGGREGATES.has("histogram")).toBe(true);
  });
});

describe("validateAggregationRequest", () => {
  it("should validate count aggregation", () => {
    const input = { count: {} };
    const result = validateAggregationRequest(input);

    expect(result).toEqual({
      count: { aggregate: "count" },
    });
  });

  it("should validate avg aggregation", () => {
    const input = { avg: { property: "age" } };
    const result = validateAggregationRequest(input);

    expect(result).toEqual({
      avg: { aggregate: "avg", property: "age" },
    });
  });

  it("should validate min aggregation", () => {
    const input = { min: { property: "age" } };
    const result = validateAggregationRequest(input);

    expect(result).toEqual({
      min: { aggregate: "min", property: "age" },
    });
  });

  it("should validate max aggregation", () => {
    const input = { max: { property: "age" } };
    const result = validateAggregationRequest(input);

    expect(result).toEqual({
      max: { aggregate: "max", property: "age" },
    });
  });

  it("should validate sum aggregation", () => {
    const input = { sum: { property: "age" } };
    const result = validateAggregationRequest(input);

    expect(result).toEqual({
      sum: { aggregate: "sum", property: "age" },
    });
  });

  it("should validate histogram aggregation", () => {
    const input = { histogram: { property: "age", interval: 5 } };
    const result = validateAggregationRequest(input);

    expect(result).toEqual({
      histogram: { aggregate: "histogram", property: "age", interval: 5 },
    });
  });

  it("should accept already formatted aggregation", () => {
    const input = { count: { aggregate: "count" } };
    const result = validateAggregationRequest(input);

    expect(result).toEqual(input);
  });

  it("should throw error for unknown aggregation type", () => {
    const input = { unknown_agg: {} };

    expect(() => validateAggregationRequest(input)).toThrow(
      "Unknown aggregate: 'unknown_agg'",
    );
  });

  it("should throw error for invalid type", () => {
    expect(() => validateAggregationRequest(["not", "a", "dict"])).toThrow(
      "Aggregate data must be an object",
    );
  });

  it("should throw error for null input", () => {
    expect(() => validateAggregationRequest(null)).toThrow(
      "Aggregate data must be an object",
    );
  });

  it("should throw error for empty object", () => {
    expect(() => validateAggregationRequest({})).toThrow(
      "Aggregate data must have exactly one key",
    );
  });

  it("should throw error for multiple aggregations in one dictionary", () => {
    const input = { count: {}, avg: { property: "age" } };

    expect(() => validateAggregationRequest(input)).toThrow(
      "Aggregate data must have exactly one key",
    );
  });
});

describe("AggregationRequest type", () => {
  it("should create valid AggregationRequest", () => {
    const request: AggregationRequest = {
      count: { aggregate: "count" },
    };

    expect(request.count?.aggregate).toBe("count");
  });

  it("should allow different aggregation types", () => {
    const requests: AggregationRequest[] = [
      { count: { aggregate: "count" } },
      { sum: { aggregate: "sum", property: "score" } },
      { avg: { aggregate: "avg", property: "age" } },
      { min: { aggregate: "min", property: "age" } },
      { max: { aggregate: "max", property: "age" } },
      { histogram: { aggregate: "histogram", property: "age", interval: 5 } },
    ];

    expect(requests).toHaveLength(6);
  });
});
