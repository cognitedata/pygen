import { describe, it, expect } from "vitest";
import {
  createInstanceId,
  createViewReference,
  createContainerReference,
  createDataModelReference,
  type InstanceId,
  type ViewReference,
  type ContainerReference,
  type DataModelReference,
} from "@cognite/pygen-typescript";

describe("Reference Types", () => {
  describe("createInstanceId", () => {
    it("should create an InstanceId with correct properties", () => {
      const id: InstanceId = createInstanceId("my-space", "my-instance");

      expect(id).toEqual({
        space: "my-space",
        externalId: "my-instance",
      });
    });

    it("should create readonly properties", () => {
      const id = createInstanceId("space", "ext-id");

      expect(id.space).toBe("space");
      expect(id.externalId).toBe("ext-id");
    });
  });

  describe("createViewReference", () => {
    it("should create a ViewReference with correct properties", () => {
      const ref: ViewReference = createViewReference("my-space", "my-view", "v1");

      expect(ref).toEqual({
        space: "my-space",
        externalId: "my-view",
        version: "v1",
      });
    });
  });

  describe("createContainerReference", () => {
    it("should create a ContainerReference with correct properties", () => {
      const ref: ContainerReference = createContainerReference(
        "my-space",
        "my-container"
      );

      expect(ref).toEqual({
        space: "my-space",
        externalId: "my-container",
      });
    });
  });

  describe("createDataModelReference", () => {
    it("should create a DataModelReference with correct properties", () => {
      const ref: DataModelReference = createDataModelReference(
        "my-space",
        "my-model",
        "v2"
      );

      expect(ref).toEqual({
        space: "my-space",
        externalId: "my-model",
        version: "v2",
      });
    });
  });
});

