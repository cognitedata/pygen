import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  type AggregateResponse,
  type Filter,
  type HTTPResult,
  type Instance,
  InstanceAPI,
  type InstanceId,
  InstanceList,
  type Page,
  type PropertySort,
  type PygenClientConfig,
  TokenCredentials,
  type UnitConversion,
  type ViewReference,
} from "@cognite/pygen-typescript";

// Test instance type
interface TestInstance extends Instance {
  name: string;
  age?: number;
}

// Test instance list class
class TestInstanceList extends InstanceList<TestInstance> {}

// Get the InstanceAPI class as any type to access protected members
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const InstanceAPIAny = InstanceAPI as any;

// Testable subclass to expose protected methods
class TestableInstanceAPI extends InstanceAPI<TestInstance, TestInstanceList> {
  // Expose protected methods for testing
  async iterate(options?: Parameters<typeof this._iterate>[0]): Promise<Page<TestInstanceList>> {
    return this._iterate(options);
  }

  async list(options?: Parameters<typeof this._list>[0]): Promise<TestInstanceList> {
    return this._list(options);
  }

  async search(
    options?: Parameters<typeof this._search>[0],
  ): ReturnType<typeof this._search> {
    return this._search(options);
  }

  async retrieveSingle(
    id: string | InstanceId | readonly [string, string],
    options?: {
      space?: string;
      includeTyping?: boolean;
      targetUnits?: UnitConversion | readonly UnitConversion[];
    },
  ): Promise<TestInstance | undefined> {
    return this._retrieve(id, options);
  }

  async retrieveBatch(
    ids: readonly (string | InstanceId | readonly [string, string])[],
    options?: {
      space?: string;
      includeTyping?: boolean;
      targetUnits?: UnitConversion | readonly UnitConversion[];
    },
  ): Promise<TestInstanceList> {
    return this._retrieve(ids, options);
  }

  async aggregate(
    agg: Parameters<typeof this._aggregate>[0],
    options?: Parameters<typeof this._aggregate>[1],
  ): Promise<AggregateResponse> {
    return this._aggregate(agg, options);
  }

  // Expose createApiUrl for testing
  getApiUrl(endpoint: string): string {
    return this.createApiUrl(endpoint);
  }
}

describe("InstanceAPI", () => {
  const viewRef: ViewReference = {
    space: "mySpace",
    externalId: "TestView",
    version: "v1",
  };

  const createConfig = (): PygenClientConfig => ({
    baseUrl: "https://api.cognitedata.com",
    project: "test-project",
    credentials: new TokenCredentials("test-token"),
  });

  describe("constructor", () => {
    it("should create instance with default worker count", () => {
      const api = new TestableInstanceAPI(
        createConfig(),
        viewRef,
        "node",
        TestInstanceList,
      );
      expect(api).toBeInstanceOf(InstanceAPI);
    });

    it("should create instance with custom worker count", () => {
      const api = new TestableInstanceAPI(
        createConfig(),
        viewRef,
        "node",
        TestInstanceList,
        20,
      );
      expect(api).toBeInstanceOf(InstanceAPI);
    });
  });

  describe("static properties", () => {
    it("should have correct limit constants", () => {
      expect(InstanceAPIAny.LIST_LIMIT).toBe(1000);
      expect(InstanceAPIAny.SEARCH_LIMIT).toBe(1000);
      expect(InstanceAPIAny.RETRIEVE_LIMIT).toBe(1000);
      expect(InstanceAPIAny.AGGREGATE_LIMIT).toBe(10000);
      expect(InstanceAPIAny.DEFAULT_LIST_LIMIT).toBe(25);
    });

    it("should have correct endpoints", () => {
      expect(InstanceAPIAny.LIST_ENDPOINT).toBe("/models/instances/list");
      expect(InstanceAPIAny.SEARCH_ENDPOINT).toBe("/models/instances/search");
      expect(InstanceAPIAny.RETRIEVE_ENDPOINT).toBe("/models/instances/byids");
      expect(InstanceAPIAny.AGGREGATE_ENDPOINT).toBe("/models/instances/aggregate");
    });
  });

  describe("createApiUrl", () => {
    it("should create correct API URL", () => {
      const api = new TestableInstanceAPI(
        createConfig(),
        viewRef,
        "node",
        TestInstanceList,
      );
      expect(api.getApiUrl("/models/instances")).toBe(
        "https://api.cognitedata.com/api/v1/projects/test-project/models/instances",
      );
    });

    it("should handle base URL with trailing slash", () => {
      const config: PygenClientConfig = {
        baseUrl: "https://api.cognitedata.com/",
        project: "test-project",
        credentials: new TokenCredentials("test-token"),
      };
      const api = new TestableInstanceAPI(config, viewRef, "node", TestInstanceList);
      expect(api.getApiUrl("/models/instances/list")).toBe(
        "https://api.cognitedata.com/api/v1/projects/test-project/models/instances/list",
      );
    });
  });
});

describe("InstanceAPI._iterate (unit tests)", () => {
  const viewRef: ViewReference = {
    space: "mySpace",
    externalId: "TestView",
    version: "v1",
  };

  const createConfig = (): PygenClientConfig => ({
    baseUrl: "https://api.cognitedata.com",
    project: "test-project",
    credentials: new TokenCredentials("test-token"),
  });

  it("should throw error for limit less than 1", async () => {
    const api = new TestableInstanceAPI(
      createConfig(),
      viewRef,
      "node",
      TestInstanceList,
    );

    await expect(api.iterate({ limit: 0 })).rejects.toThrow(
      "Limit must be between 1 and 1000, got 0.",
    );
  });

  it("should throw error for limit greater than 1000", async () => {
    const api = new TestableInstanceAPI(
      createConfig(),
      viewRef,
      "node",
      TestInstanceList,
    );

    await expect(api.iterate({ limit: 1001 })).rejects.toThrow(
      "Limit must be between 1 and 1000, got 1001.",
    );
  });
});

describe("InstanceAPI._list (unit tests)", () => {
  const viewRef: ViewReference = {
    space: "mySpace",
    externalId: "TestView",
    version: "v1",
  };

  const createConfig = (): PygenClientConfig => ({
    baseUrl: "https://api.cognitedata.com",
    project: "test-project",
    credentials: new TokenCredentials("test-token"),
  });

  it("should throw error for negative limit", async () => {
    const api = new TestableInstanceAPI(
      createConfig(),
      viewRef,
      "node",
      TestInstanceList,
    );

    await expect(api.list({ limit: -5 })).rejects.toThrow(
      "Limit must be a positive integer or undefined for no limit.",
    );
  });

  it("should throw error for zero limit", async () => {
    const api = new TestableInstanceAPI(
      createConfig(),
      viewRef,
      "node",
      TestInstanceList,
    );

    await expect(api.list({ limit: 0 })).rejects.toThrow(
      "Limit must be a positive integer or undefined for no limit.",
    );
  });
});

describe("InstanceAPI._search (unit tests)", () => {
  const viewRef: ViewReference = {
    space: "mySpace",
    externalId: "TestView",
    version: "v1",
  };

  const createConfig = (): PygenClientConfig => ({
    baseUrl: "https://api.cognitedata.com",
    project: "test-project",
    credentials: new TokenCredentials("test-token"),
  });

  it("should throw error for limit less than 1", async () => {
    const api = new TestableInstanceAPI(
      createConfig(),
      viewRef,
      "node",
      TestInstanceList,
    );

    await expect(api.search({ limit: 0 })).rejects.toThrow(
      "Limit must be between 1 and 1000, got 0.",
    );
  });

  it("should throw error for limit greater than 1000", async () => {
    const api = new TestableInstanceAPI(
      createConfig(),
      viewRef,
      "node",
      TestInstanceList,
    );

    await expect(api.search({ limit: 1001 })).rejects.toThrow(
      "Limit must be between 1 and 1000, got 1001.",
    );
  });
});

describe("InstanceAPI._retrieve (unit tests)", () => {
  const viewRef: ViewReference = {
    space: "mySpace",
    externalId: "TestView",
    version: "v1",
  };

  const createConfig = (): PygenClientConfig => ({
    baseUrl: "https://api.cognitedata.com",
    project: "test-project",
    credentials: new TokenCredentials("test-token"),
  });

  it("should return undefined for empty single retrieve", async () => {
    const api = new TestableInstanceAPI(
      createConfig(),
      viewRef,
      "node",
      TestInstanceList,
    );

    // Empty batch returns an empty list
    const result = await api.retrieveBatch([]);
    expect(result.length).toBe(0);
  });

  it("should throw error when space is not provided for string id", async () => {
    const api = new TestableInstanceAPI(
      createConfig(),
      viewRef,
      "node",
      TestInstanceList,
    );

    await expect(api.retrieveSingle("my-id")).rejects.toThrow(
      "space parameter is required when retrieving by external_id string",
    );
  });
});

describe("InstanceAPI filter types", () => {
  it("should accept various filter types", () => {
    const equalsFilter: Filter = {
      equals: {
        property: ["mySpace", "TestView/v1", "name"],
        value: "test",
      },
    };
    expect(equalsFilter.equals.value).toBe("test");

    const andFilter: Filter = {
      and: [
        { equals: { property: ["mySpace", "TestView/v1", "name"], value: "test" } },
        { range: { property: ["mySpace", "TestView/v1", "age"], gte: 18 } },
      ],
    };
    expect(andFilter.and).toHaveLength(2);
  });
});

describe("InstanceAPI sort types", () => {
  it("should accept PropertySort", () => {
    const sort: PropertySort = {
      property: ["mySpace", "TestView/v1", "name"],
      direction: "ascending",
    };
    expect(sort.property).toHaveLength(3);
    expect(sort.direction).toBe("ascending");
  });

  it("should accept PropertySort with nullsFirst", () => {
    const sort: PropertySort = {
      property: ["node", "externalId"],
      direction: "descending",
      nullsFirst: true,
    };
    expect(sort.nullsFirst).toBe(true);
  });
});

describe("InstanceAPI unit conversion types", () => {
  it("should accept UnitConversion with externalId", () => {
    const conversion: UnitConversion = {
      property: "temperature",
      unit: { externalId: "temperature:cel" },
    };
    expect(conversion.property).toBe("temperature");
  });

  it("should accept UnitConversion with unitSystemName", () => {
    const conversion: UnitConversion = {
      property: "temperature",
      unit: { unitSystemName: "SI" },
    };
    expect(conversion.property).toBe("temperature");
  });
});
