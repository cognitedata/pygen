import { describe, it, expect } from "vitest";
import {
  msToDate,
  dateToMs,
  parseDate,
  parseDateOptional,
  toCamelCase,
  toSnakeCase,
  keysToCamelCase,
  keysToSnakeCase,
  MIN_TIMESTAMP_MS,
  MAX_TIMESTAMP_MS,
} from "@cognite/pygen-typescript";

describe("Date Utilities", () => {
  describe("msToDate", () => {
    it("should convert milliseconds to Date", () => {
      const ms = 1703721600000; // 2023-12-28T00:00:00.000Z
      const date = msToDate(ms);

      expect(date.getTime()).toBe(ms);
      expect(date.toISOString()).toBe("2023-12-28T00:00:00.000Z");
    });

    it("should handle epoch time (0)", () => {
      const date = msToDate(0);

      expect(date.toISOString()).toBe("1970-01-01T00:00:00.000Z");
    });

    it("should throw for timestamp before minimum", () => {
      expect(() => msToDate(MIN_TIMESTAMP_MS - 1)).toThrow();
    });

    it("should throw for timestamp after maximum", () => {
      expect(() => msToDate(MAX_TIMESTAMP_MS + 1)).toThrow();
    });

    it("should accept minimum valid timestamp", () => {
      const date = msToDate(MIN_TIMESTAMP_MS);

      expect(date.getTime()).toBe(MIN_TIMESTAMP_MS);
    });

    it("should accept maximum valid timestamp", () => {
      const date = msToDate(MAX_TIMESTAMP_MS);

      expect(date.getTime()).toBe(MAX_TIMESTAMP_MS);
    });
  });

  describe("dateToMs", () => {
    it("should convert Date to milliseconds", () => {
      const date = new Date("2023-12-28T00:00:00.000Z");
      const ms = dateToMs(date);

      expect(ms).toBe(1703721600000);
    });

    it("should handle epoch date", () => {
      const date = new Date("1970-01-01T00:00:00.000Z");

      expect(dateToMs(date)).toBe(0);
    });
  });

  describe("parseDate", () => {
    it("should parse number as milliseconds", () => {
      const date = parseDate(1703721600000);

      expect(date.toISOString()).toBe("2023-12-28T00:00:00.000Z");
    });

    it("should pass through Date objects", () => {
      const original = new Date("2023-12-28T00:00:00.000Z");
      const result = parseDate(original);

      expect(result).toBe(original);
    });
  });

  describe("parseDateOptional", () => {
    it("should parse number as Date", () => {
      const date = parseDateOptional(1703721600000);

      expect(date?.toISOString()).toBe("2023-12-28T00:00:00.000Z");
    });

    it("should return undefined for null", () => {
      expect(parseDateOptional(null)).toBeUndefined();
    });

    it("should return undefined for undefined", () => {
      expect(parseDateOptional(undefined)).toBeUndefined();
    });

    it("should pass through Date objects", () => {
      const original = new Date("2023-12-28T00:00:00.000Z");
      const result = parseDateOptional(original);

      expect(result).toBe(original);
    });
  });
});

describe("String Case Utilities", () => {
  describe("toCamelCase", () => {
    it("should convert snake_case to camelCase", () => {
      expect(toCamelCase("external_id")).toBe("externalId");
    });

    it("should handle multiple underscores", () => {
      expect(toCamelCase("last_updated_time")).toBe("lastUpdatedTime");
    });

    it("should handle already camelCase", () => {
      expect(toCamelCase("externalId")).toBe("externalId");
    });

    it("should handle single word", () => {
      expect(toCamelCase("space")).toBe("space");
    });
  });

  describe("toSnakeCase", () => {
    it("should convert camelCase to snake_case", () => {
      expect(toSnakeCase("externalId")).toBe("external_id");
    });

    it("should handle multiple capitals", () => {
      expect(toSnakeCase("lastUpdatedTime")).toBe("last_updated_time");
    });

    it("should handle already snake_case", () => {
      expect(toSnakeCase("external_id")).toBe("external_id");
    });

    it("should handle single word", () => {
      expect(toSnakeCase("space")).toBe("space");
    });
  });

  describe("keysToCamelCase", () => {
    it("should convert all keys to camelCase", () => {
      const input = {
        external_id: "abc",
        last_updated_time: 123,
        space: "my-space",
      };
      const result = keysToCamelCase(input);

      expect(result).toEqual({
        externalId: "abc",
        lastUpdatedTime: 123,
        space: "my-space",
      });
    });

    it("should handle empty object", () => {
      expect(keysToCamelCase({})).toEqual({});
    });
  });

  describe("keysToSnakeCase", () => {
    it("should convert all keys to snake_case", () => {
      const input = {
        externalId: "abc",
        lastUpdatedTime: 123,
        space: "my-space",
      };
      const result = keysToSnakeCase(input);

      expect(result).toEqual({
        external_id: "abc",
        last_updated_time: 123,
        space: "my-space",
      });
    });

    it("should handle empty object", () => {
      expect(keysToSnakeCase({})).toEqual({});
    });
  });
});

