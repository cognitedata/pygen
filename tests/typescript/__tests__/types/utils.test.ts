import { describe, it, expect } from "vitest";
import {
  msToDate,
  dateToMs,
  parseDate,
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

    it("should throw for date before minimum", () => {
      const date = new Date(MIN_TIMESTAMP_MS - 1);

      expect(() => dateToMs(date)).toThrow("outside valid CDF range");
    });

    it("should throw for date after maximum", () => {
      const date = new Date(MAX_TIMESTAMP_MS + 1);

      expect(() => dateToMs(date)).toThrow("outside valid CDF range");
    });

    it("should accept minimum valid date", () => {
      const date = new Date(MIN_TIMESTAMP_MS);

      expect(dateToMs(date)).toBe(MIN_TIMESTAMP_MS);
    });

    it("should accept maximum valid date", () => {
      const date = new Date(MAX_TIMESTAMP_MS);

      expect(dateToMs(date)).toBe(MAX_TIMESTAMP_MS);
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
});
