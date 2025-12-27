import { describe, it, expect } from "vitest";
import {
  createInstanceId,
  createViewReference,
  createContainerReference,
  type InstanceId,
  type ViewReference,
  type ContainerReference,
} from "../../src/types/index.js";

describe("InstanceId", () => {
  it("should create an InstanceId with correct properties", () => {
    const instanceId = createInstanceId("my-external-id", "my-space");

    expect(instanceId).toEqual({
      externalId: "my-external-id",
      space: "my-space",
    });
  });

  it("should be readonly", () => {
    const instanceId: InstanceId = createInstanceId("ext-id", "space");

    // TypeScript should prevent modification, but we verify structure
    expect(instanceId.externalId).toBe("ext-id");
    expect(instanceId.space).toBe("space");
  });
});

describe("ViewReference", () => {
  it("should create a ViewReference with correct properties", () => {
    const viewRef = createViewReference("my-view", "my-space", "v1");

    expect(viewRef).toEqual({
      externalId: "my-view",
      space: "my-space",
      version: "v1",
    });
  });

  it("should include version property", () => {
    const viewRef: ViewReference = createViewReference("view", "space", "v2");

    expect(viewRef.version).toBe("v2");
  });
});

describe("ContainerReference", () => {
  it("should create a ContainerReference with correct properties", () => {
    const containerRef = createContainerReference("my-container", "my-space");

    expect(containerRef).toEqual({
      externalId: "my-container",
      space: "my-space",
    });
  });

  it("should not have version property", () => {
    const containerRef: ContainerReference = createContainerReference("container", "space");

    expect(containerRef).not.toHaveProperty("version");
  });
});
