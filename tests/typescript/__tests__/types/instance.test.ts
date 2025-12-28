import { describe, it, expect } from "vitest";
import {
  parseDataRecord,
  serializeDataRecordWrite,
  parseInstance,
  parseInstances,
  dumpInstance,
  dumpInstanceForAPI,
  InstanceList,
  isNode,
  isEdge,
  isNodeWrite,
  isEdgeWrite,
  type DataRecord,
  type DataRecordWrite,
  type DataRecordRaw,
  type NodeInstance,
  type EdgeInstance,
  type NodeInstanceWrite,
  type EdgeInstanceWrite,
  type InstanceRaw,
  type ViewReference,
} from "@cognite/pygen-typescript";

// Test fixtures
const TEST_VIEW_ID: ViewReference = {
  space: "test-space",
  externalId: "test-view",
  version: "v1",
};

const TEST_DATA_RECORD_RAW: DataRecordRaw = {
  version: 1,
  lastUpdatedTime: 1703721600000, // 2023-12-28T00:00:00.000Z
  createdTime: 1703635200000, // 2023-12-27T00:00:00.000Z
};

const TEST_NODE_RAW: InstanceRaw = {
  instanceType: "node",
  space: "test-space",
  externalId: "node-1",
  version: 1,
  lastUpdatedTime: 1703721600000,
  createdTime: 1703635200000,
  properties: {
    "test-space": {
      "test-view/v1": {
        name: "Test Node",
        value: 42,
      },
    },
  },
};

const TEST_EDGE_RAW: InstanceRaw = {
  instanceType: "edge",
  space: "test-space",
  externalId: "edge-1",
  version: 1,
  lastUpdatedTime: 1703721600000,
  createdTime: 1703635200000,
  startNode: { space: "test-space", externalId: "node-1" },
  endNode: { space: "test-space", externalId: "node-2" },
  properties: {
    "test-space": {
      "test-view/v1": {
        weight: 0.5,
      },
    },
  },
};

describe("DataRecord", () => {
  describe("parseDataRecord", () => {
    it("should parse raw data record with required fields", () => {
      const result: DataRecord = parseDataRecord(TEST_DATA_RECORD_RAW);

      expect(result.version).toBe(1);
      expect(result.lastUpdatedTime.toISOString()).toBe("2023-12-28T00:00:00.000Z");
      expect(result.createdTime.toISOString()).toBe("2023-12-27T00:00:00.000Z");
      expect(result.deletedTime).toBeUndefined();
    });

    it("should parse raw data record with deletedTime", () => {
      const rawWithDeleted: DataRecordRaw = {
        ...TEST_DATA_RECORD_RAW,
        deletedTime: 1703808000000, // 2023-12-29T00:00:00.000Z
      };
      const result = parseDataRecord(rawWithDeleted);

      expect(result.deletedTime?.toISOString()).toBe("2023-12-29T00:00:00.000Z");
    });
  });

  describe("serializeDataRecordWrite", () => {
    it("should serialize empty data record write", () => {
      const record: DataRecordWrite = {};
      const result = serializeDataRecordWrite(record);

      expect(result).toEqual({});
    });

    it("should serialize data record with existingVersion", () => {
      const record: DataRecordWrite = { existingVersion: 5 };
      const result = serializeDataRecordWrite(record);

      expect(result).toEqual({ existingVersion: 5 });
    });
  });
});

describe("Instance Parsing", () => {
  describe("parseInstance", () => {
    it("should parse a node instance", () => {
      const result = parseInstance<NodeInstance & { name: string; value: number }>(
        TEST_NODE_RAW,
        TEST_VIEW_ID
      );

      expect(result.instanceType).toBe("node");
      expect(result.space).toBe("test-space");
      expect(result.externalId).toBe("node-1");
      expect(result.dataRecord.version).toBe(1);
      expect(result.name).toBe("Test Node");
      expect(result.value).toBe(42);
    });

    it("should parse an edge instance", () => {
      const result = parseInstance<EdgeInstance & { weight: number }>(TEST_EDGE_RAW, TEST_VIEW_ID);

      expect(result.instanceType).toBe("edge");
      expect(result.space).toBe("test-space");
      expect(result.externalId).toBe("edge-1");
      expect(result.startNode).toEqual({ space: "test-space", externalId: "node-1" });
      expect(result.endNode).toEqual({ space: "test-space", externalId: "node-2" });
      expect(result.weight).toBe(0.5);
    });

    it("should parse instance without view (no properties)", () => {
      const result = parseInstance<NodeInstance>(TEST_NODE_RAW);

      expect(result.instanceType).toBe("node");
      expect(result.space).toBe("test-space");
      expect(result.externalId).toBe("node-1");
      // Properties should not be present
      expect((result as unknown as Record<string, unknown>).name).toBeUndefined();
    });

    it("should handle instance with no properties in response", () => {
      const rawNoProps: InstanceRaw = {
        instanceType: "node",
        space: "test-space",
        externalId: "node-2",
        version: 1,
        lastUpdatedTime: 1703721600000,
        createdTime: 1703635200000,
      };
      const result = parseInstance<NodeInstance>(rawNoProps, TEST_VIEW_ID);

      expect(result.instanceType).toBe("node");
      expect(result.externalId).toBe("node-2");
    });
  });

  describe("parseInstances", () => {
    it("should parse multiple instances into InstanceList", () => {
      const items = [TEST_NODE_RAW, { ...TEST_NODE_RAW, externalId: "node-2" }];
      const result = parseInstances<NodeInstance>(items, TEST_VIEW_ID);

      expect(result).toBeInstanceOf(InstanceList);
      expect(result.length).toBe(2);
      expect(result.at(0)?.externalId).toBe("node-1");
      expect(result.at(1)?.externalId).toBe("node-2");
      expect(result.viewId).toEqual(TEST_VIEW_ID);
    });

    it("should return empty list for empty input", () => {
      const result = parseInstances<NodeInstance>([], TEST_VIEW_ID);

      expect(result).toBeInstanceOf(InstanceList);
      expect(result.length).toBe(0);
    });
  });
});

describe("Instance Serialization", () => {
  const testNode: NodeInstance & { name: string } = {
    instanceType: "node",
    space: "test-space",
    externalId: "node-1",
    dataRecord: {
      version: 1,
      lastUpdatedTime: new Date("2023-12-28T00:00:00.000Z"),
      createdTime: new Date("2023-12-27T00:00:00.000Z"),
    },
    name: "Test Node",
  };

  describe("dumpInstance", () => {
    it("should dump instance with camelCase keys", () => {
      const result = dumpInstance(testNode, true);

      expect(result.instanceType).toBe("node");
      expect(result.space).toBe("test-space");
      expect(result.externalId).toBe("node-1");
      expect(result.version).toBe(1);
      expect(result.lastUpdatedTime).toBe(1703721600000);
      expect(result.createdTime).toBe(1703635200000);
      expect(result.name).toBe("Test Node");
    });

    it("should dump instance with snake_case keys", () => {
      const result = dumpInstance(testNode, false);

      expect(result.instance_type).toBe("node");
      expect(result.space).toBe("test-space");
      expect(result.external_id).toBe("node-1");
      expect(result.name).toBe("Test Node");
    });

    it("should flatten data record fields", () => {
      const result = dumpInstance(testNode, true);

      // Data record fields should be at top level
      expect(result.version).toBe(1);
      expect(result.lastUpdatedTime).toBe(1703721600000);
      // dataRecord should not be present as a nested object
      expect(result.dataRecord).toBeUndefined();
    });
  });

  describe("dumpInstanceForAPI", () => {
    it("should dump node for API upsert format", () => {
      const nodeWrite: NodeInstanceWrite & { name: string; value: number } = {
        instanceType: "node",
        space: "test-space",
        externalId: "node-1",
        name: "Test Node",
        value: 42,
      };
      const result = dumpInstanceForAPI(nodeWrite, TEST_VIEW_ID);

      expect(result.instanceType).toBe("node");
      expect(result.space).toBe("test-space");
      expect(result.externalId).toBe("node-1");
      expect(result.sources).toEqual([
        {
          source: {
            type: "view",
            space: "test-space",
            externalId: "test-view",
            version: "v1",
          },
          properties: {
            name: "Test Node",
            value: 42,
          },
        },
      ]);
    });

    it("should dump edge for API upsert format", () => {
      const edgeWrite: EdgeInstanceWrite & { weight: number } = {
        instanceType: "edge",
        space: "test-space",
        externalId: "edge-1",
        startNode: { space: "test-space", externalId: "node-1" },
        endNode: { space: "test-space", externalId: "node-2" },
        weight: 0.5,
      };
      const result = dumpInstanceForAPI(edgeWrite, TEST_VIEW_ID);

      expect(result.instanceType).toBe("edge");
      expect(result.startNode).toEqual({ space: "test-space", externalId: "node-1" });
      expect(result.endNode).toEqual({ space: "test-space", externalId: "node-2" });
      expect(result.sources).toEqual([
        {
          source: {
            type: "view",
            space: "test-space",
            externalId: "test-view",
            version: "v1",
          },
          properties: {
            weight: 0.5,
          },
        },
      ]);
    });

    it("should include existingVersion when specified", () => {
      const nodeWrite: NodeInstanceWrite & { name: string } = {
        instanceType: "node",
        space: "test-space",
        externalId: "node-1",
        dataRecord: { existingVersion: 5 },
        name: "Test Node",
      };
      const result = dumpInstanceForAPI(nodeWrite, TEST_VIEW_ID);

      expect(result.existingVersion).toBe(5);
    });

    it("should convert Date properties to milliseconds", () => {
      const nodeWrite: NodeInstanceWrite & { createdAt: Date } = {
        instanceType: "node",
        space: "test-space",
        externalId: "node-1",
        createdAt: new Date("2023-12-28T00:00:00.000Z"),
      };
      const result = dumpInstanceForAPI(nodeWrite, TEST_VIEW_ID);

      expect(
        (result.sources as { properties: Record<string, unknown> }[])[0].properties.createdAt
      ).toBe(1703721600000);
    });
  });
});

describe("InstanceList", () => {
  const testInstances: NodeInstance[] = [
    {
      instanceType: "node",
      space: "space-1",
      externalId: "node-1",
      dataRecord: {
        version: 1,
        lastUpdatedTime: new Date("2023-12-28T00:00:00.000Z"),
        createdTime: new Date("2023-12-27T00:00:00.000Z"),
      },
    },
    {
      instanceType: "node",
      space: "space-1",
      externalId: "node-2",
      dataRecord: {
        version: 1,
        lastUpdatedTime: new Date("2023-12-28T00:00:00.000Z"),
        createdTime: new Date("2023-12-27T00:00:00.000Z"),
      },
    },
    {
      instanceType: "node",
      space: "space-2",
      externalId: "node-3",
      dataRecord: {
        version: 1,
        lastUpdatedTime: new Date("2023-12-28T00:00:00.000Z"),
        createdTime: new Date("2023-12-27T00:00:00.000Z"),
      },
    },
  ];

  describe("constructor and static from", () => {
    it("should create empty list", () => {
      const list = new InstanceList<NodeInstance>();

      expect(list.length).toBe(0);
    });

    it("should create list with items", () => {
      const list = new InstanceList(testInstances);

      expect(list.length).toBe(3);
    });

    it("should create list with viewId", () => {
      const list = new InstanceList(testInstances, TEST_VIEW_ID);

      expect(list.viewId).toEqual(TEST_VIEW_ID);
    });

    it("should create from array using static method", () => {
      const list = InstanceList.from(testInstances, TEST_VIEW_ID);

      expect(list.length).toBe(3);
      expect(list.viewId).toEqual(TEST_VIEW_ID);
    });
  });

  describe("getIds", () => {
    it("should return all instance IDs", () => {
      const list = new InstanceList(testInstances);
      const ids = list.getIds();

      expect(ids).toEqual([
        { space: "space-1", externalId: "node-1" },
        { space: "space-1", externalId: "node-2" },
        { space: "space-2", externalId: "node-3" },
      ]);
    });
  });

  describe("toRecord", () => {
    it("should convert to record by externalId", () => {
      const list = new InstanceList(testInstances);
      const record = list.toRecord();

      expect(Object.keys(record)).toEqual(["node-1", "node-2", "node-3"]);
      expect(record["node-1"].space).toBe("space-1");
    });
  });

  describe("toRecordByFullId", () => {
    it("should convert to record by space:externalId", () => {
      const list = new InstanceList(testInstances);
      const record = list.toRecordByFullId();

      expect(Object.keys(record)).toEqual(["space-1:node-1", "space-1:node-2", "space-2:node-3"]);
      expect(record["space-2:node-3"].externalId).toBe("node-3");
    });
  });

  describe("filterBySpace", () => {
    it("should filter instances by space", () => {
      const list = new InstanceList(testInstances, TEST_VIEW_ID);
      const filtered = list.filterBySpace("space-1");

      expect(filtered.length).toBe(2);
      expect(filtered.at(0)?.externalId).toBe("node-1");
      expect(filtered.at(1)?.externalId).toBe("node-2");
      expect(filtered.viewId).toEqual(TEST_VIEW_ID);
    });

    it("should return empty list for non-existent space", () => {
      const list = new InstanceList(testInstances);
      const filtered = list.filterBySpace("space-3");

      expect(filtered.length).toBe(0);
    });
  });

  describe("first and last", () => {
    it("should return first item", () => {
      const list = new InstanceList(testInstances);

      expect(list.first()?.externalId).toBe("node-1");
    });

    it("should return last item", () => {
      const list = new InstanceList(testInstances);

      expect(list.last()?.externalId).toBe("node-3");
    });

    it("should return undefined for empty list", () => {
      const list = new InstanceList<NodeInstance>();

      expect(list.first()).toBeUndefined();
      expect(list.last()).toBeUndefined();
    });
  });

  describe("dump", () => {
    it("should dump all instances", () => {
      const list = new InstanceList(testInstances);
      const dumped = list.dump();

      expect(dumped.length).toBe(3);
      expect(dumped[0].externalId).toBe("node-1");
      expect(dumped[1].externalId).toBe("node-2");
      expect(dumped[2].externalId).toBe("node-3");
    });
  });

  describe("toArray", () => {
    it("should convert to plain array", () => {
      const list = new InstanceList(testInstances);
      const arr = list.toArray();

      expect(Array.isArray(arr)).toBe(true);
      expect(arr.length).toBe(3);
      expect(arr[0].externalId).toBe("node-1");
    });
  });

  describe("array behavior", () => {
    it("should support map", () => {
      const list = new InstanceList(testInstances);
      const ids = list.map((item) => item.externalId);

      expect(ids).toEqual(["node-1", "node-2", "node-3"]);
    });

    it("should support filter", () => {
      const list = new InstanceList(testInstances);
      const filtered = list.filter((item) => item.space === "space-1");

      expect(filtered.length).toBe(2);
    });

    it("should support iteration", () => {
      const list = new InstanceList(testInstances);
      const ids: string[] = [];

      for (const item of list) {
        ids.push(item.externalId);
      }

      expect(ids).toEqual(["node-1", "node-2", "node-3"]);
    });
  });
});

describe("Type Guards", () => {
  const testNode: NodeInstance = {
    instanceType: "node",
    space: "test-space",
    externalId: "node-1",
    dataRecord: {
      version: 1,
      lastUpdatedTime: new Date(),
      createdTime: new Date(),
    },
  };

  const testEdge: EdgeInstance = {
    instanceType: "edge",
    space: "test-space",
    externalId: "edge-1",
    dataRecord: {
      version: 1,
      lastUpdatedTime: new Date(),
      createdTime: new Date(),
    },
    startNode: { space: "test-space", externalId: "node-1" },
    endNode: { space: "test-space", externalId: "node-2" },
  };

  const testNodeWrite: NodeInstanceWrite = {
    instanceType: "node",
    space: "test-space",
    externalId: "node-1",
  };

  const testEdgeWrite: EdgeInstanceWrite = {
    instanceType: "edge",
    space: "test-space",
    externalId: "edge-1",
    startNode: { space: "test-space", externalId: "node-1" },
    endNode: { space: "test-space", externalId: "node-2" },
  };

  describe("isNode", () => {
    it("should return true for node instances", () => {
      expect(isNode(testNode)).toBe(true);
    });

    it("should return false for edge instances", () => {
      expect(isNode(testEdge)).toBe(false);
    });
  });

  describe("isEdge", () => {
    it("should return true for edge instances", () => {
      expect(isEdge(testEdge)).toBe(true);
    });

    it("should return false for node instances", () => {
      expect(isEdge(testNode)).toBe(false);
    });
  });

  describe("isNodeWrite", () => {
    it("should return true for node write instances", () => {
      expect(isNodeWrite(testNodeWrite)).toBe(true);
    });

    it("should return false for edge write instances", () => {
      expect(isNodeWrite(testEdgeWrite)).toBe(false);
    });
  });

  describe("isEdgeWrite", () => {
    it("should return true for edge write instances", () => {
      expect(isEdgeWrite(testEdgeWrite)).toBe(true);
    });

    it("should return false for node write instances", () => {
      expect(isEdgeWrite(testNodeWrite)).toBe(false);
    });
  });
});
