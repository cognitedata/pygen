import { describe, expect, it } from "vitest";
import {
  type ContainerReference,
  createContainerReference,
  createDataModelReference,
  createEdgeReference,
  createInstanceId,
  createNodeReference,
  createViewReference,
  type DataModelReference,
  type EdgeReference,
  type InstanceId,
  instanceIdEquals,
  instanceIdToString,
  type NodeReference,
  type ViewReference,
  viewReferenceToString,
} from "@cognite/pygen-_typescript";

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
      const ref: ContainerReference = createContainerReference("my-space", "my-container");

      expect(ref).toEqual({
        space: "my-space",
        externalId: "my-container",
      });
    });
  });

  describe("createDataModelReference", () => {
    it("should create a DataModelReference with correct properties", () => {
      const ref: DataModelReference = createDataModelReference("my-space", "my-model", "v2");

      expect(ref).toEqual({
        space: "my-space",
        externalId: "my-model",
        version: "v2",
      });
    });
  });

  describe("createNodeReference", () => {
    it("should create a NodeReference with correct properties", () => {
      const ref: NodeReference = createNodeReference("my-space", "my-node");

      expect(ref).toEqual({
        space: "my-space",
        externalId: "my-node",
      });
    });
  });

  describe("createEdgeReference", () => {
    it("should create an EdgeReference with correct properties", () => {
      const ref: EdgeReference = createEdgeReference("my-space", "my-edge");

      expect(ref).toEqual({
        space: "my-space",
        externalId: "my-edge",
      });
    });
  });

  describe("instanceIdEquals", () => {
    it("should return true for equal InstanceIds", () => {
      const a = createInstanceId("my-space", "my-instance");
      const b = createInstanceId("my-space", "my-instance");

      expect(instanceIdEquals(a, b)).toBe(true);
    });

    it("should return false for different spaces", () => {
      const a = createInstanceId("space-1", "my-instance");
      const b = createInstanceId("space-2", "my-instance");

      expect(instanceIdEquals(a, b)).toBe(false);
    });

    it("should return false for different externalIds", () => {
      const a = createInstanceId("my-space", "instance-1");
      const b = createInstanceId("my-space", "instance-2");

      expect(instanceIdEquals(a, b)).toBe(false);
    });
  });

  describe("instanceIdToString", () => {
    it("should format InstanceId as space:externalId", () => {
      const id = createInstanceId("my-space", "my-instance");

      expect(instanceIdToString(id)).toBe("my-space:my-instance");
    });
  });

  describe("viewReferenceToString", () => {
    it("should format ViewReference with version", () => {
      const ref = createViewReference("my-space", "my-view", "v1");

      expect(viewReferenceToString(ref)).toBe("my-space:my-view(version=v1)");
    });
  });
});
