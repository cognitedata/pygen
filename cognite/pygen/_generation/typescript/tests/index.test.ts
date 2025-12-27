import { describe, it, expect } from "vitest";
import { VERSION } from "../src/index.js";

describe("Package exports", () => {
  it("should export VERSION", () => {
    expect(VERSION).toBe("0.0.1");
  });

  it("should export types from types module", async () => {
    const exports = await import("../src/index.js");

    expect(exports.createInstanceId).toBeDefined();
    expect(exports.createViewReference).toBeDefined();
    expect(exports.createContainerReference).toBeDefined();
  });
});
