import { describe, expect, it, vi } from "vitest";
import {
  type AggregateResponse,
  type Aggregation,
  type DebugParameters,
  type Filter,
  HTTPClient,
  type HTTPResult,
  type Instance,
  InstanceAPI,
  type InstanceId,
  InstanceList,
  type ListResponse,
  type Page,
  type PropertySort,
  type PygenClientConfig,
  type RequestMessage,
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

// Helper function to create a mock response
function createMockListResponse(items: TestInstance[], nextCursor?: string): HTTPResult {
  const viewRef: ViewReference = {
    space: "mySpace",
    externalId: "TestView",
    version: "v1",
  };

  const rawItems = items.map((item) => ({
    instanceType: item.instanceType,
    space: item.space,
    externalId: item.externalId,
    version: item.dataRecord.version,
    createdTime: item.dataRecord.createdTime.getTime(),
    lastUpdatedTime: item.dataRecord.lastUpdatedTime.getTime(),
    properties: {
      [viewRef.space]: {
        [`${viewRef.externalId}/${viewRef.version}`]: {
          name: item.name,
          ...(item.age !== undefined ? { age: item.age } : {}),
        },
      },
    },
  }));

  const body = JSON.stringify({
    items: rawItems,
    ...(nextCursor ? { nextCursor } : {}),
  });

  return {
    kind: "success",
    statusCode: 200,
    body,
  };
}

// Helper function to create a mock aggregate response
function createMockAggregateResponse(items: AggregateResponse["items"]): HTTPResult {
  return {
    kind: "success",
    statusCode: 200,
    body: JSON.stringify({ items }),
  };
}

// Create test instance
function createTestInstance(id: string, name: string, age?: number): TestInstance {
  return {
    instanceType: "node",
    space: "mySpace",
    externalId: id,
    dataRecord: {
      version: 1,
      createdTime: new Date("2024-01-01"),
      lastUpdatedTime: new Date("2024-01-02"),
    },
    name,
    age,
  };
}

// Mockable HTTPClient subclass
class MockHTTPClient extends HTTPClient {
  public mockRequestFn:
    | ((message: RequestMessage) => Promise<HTTPResult>)
    | null = null;
  public capturedRequests: RequestMessage[] = [];

  async request(message: RequestMessage): Promise<HTTPResult> {
    this.capturedRequests.push(message);
    if (this.mockRequestFn) {
      return this.mockRequestFn(message);
    }
    return super.request(message);
  }
}

// Testable subclass with mockable HTTP client
class TestableInstanceAPI extends InstanceAPI<TestInstance> {
  public readonly testHttpClient: MockHTTPClient;

  constructor(
    config: PygenClientConfig,
    viewRef: ViewReference,
    instanceType: "node" | "edge",
    retrieveWorkers = 10,
  ) {
    super(config, viewRef, instanceType, retrieveWorkers);
    // Replace httpClient with our mock
    this.testHttpClient = new MockHTTPClient(config);
    // @ts-expect-error - we're overriding readonly for testing
    this.httpClient = this.testHttpClient;
  }

  // Expose protected methods for testing
  async iterate(options?: {
    includeTyping?: boolean;
    targetUnits?: UnitConversion | readonly UnitConversion[];
    debug?: DebugParameters;
    cursor?: string;
    limit?: number;
    sort?: PropertySort | readonly PropertySort[];
    filter?: Filter;
  }): Promise<Page<InstanceList<TestInstance>>> {
    return this._iterate(options);
  }

  async list(options?: {
    includeTyping?: boolean;
    targetUnits?: UnitConversion | readonly UnitConversion[];
    debug?: DebugParameters;
    limit?: number;
    sort?: PropertySort | readonly PropertySort[];
    filter?: Filter;
  }): Promise<InstanceList<TestInstance>> {
    return this._list(options);
  }

  async search(options?: {
    query?: string;
    properties?: string | readonly string[];
    targetUnits?: UnitConversion | readonly UnitConversion[];
    filter?: Filter;
    includeTyping?: boolean;
    sort?: PropertySort | readonly PropertySort[];
    operator?: "and" | "or";
    limit?: number;
  }): Promise<ListResponse<InstanceList<TestInstance>>> {
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
  ): Promise<InstanceList<TestInstance>> {
    return this._retrieve(ids, options);
  }

  async aggregate(
    agg: Aggregation | readonly Aggregation[],
    options?: {
      query?: string;
      groupBy?: string | readonly string[];
      properties?: string | readonly string[];
      operator?: "and" | "or";
      targetUnits?: UnitConversion | readonly UnitConversion[];
      includeTyping?: boolean;
      filter?: Filter;
      limit?: number;
    },
  ): Promise<AggregateResponse> {
    return this._aggregate(agg, options);
  }

  // Expose createApiUrl for testing
  getApiUrl(endpoint: string): string {
    return this.createApiUrl(endpoint);
  }

  // Set mock response
  setMockResponse(fn: (message: RequestMessage) => Promise<HTTPResult>): void {
    this.testHttpClient.mockRequestFn = fn;
  }

  // Get captured requests
  getCapturedRequests(): RequestMessage[] {
    return this.testHttpClient.capturedRequests;
  }

  // Clear captured requests
  clearCapturedRequests(): void {
    this.testHttpClient.capturedRequests = [];
  }
}

// Factory to create API with config
function createTestAPI(instanceType: "node" | "edge" = "node"): TestableInstanceAPI {
  const viewRef: ViewReference = {
    space: "mySpace",
    externalId: "TestView",
    version: "v1",
  };
  const config: PygenClientConfig = {
    baseUrl: "https://api.cognitedata.com",
    project: "test-project",
    credentials: new TokenCredentials("test-token"),
  };
  return new TestableInstanceAPI(config, viewRef, instanceType);
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
      );
      expect(api).toBeInstanceOf(InstanceAPI);
    });

    it("should create instance with custom worker count", () => {
      const api = new TestableInstanceAPI(
        createConfig(),
        viewRef,
        "node",
        20,
      );
      expect(api).toBeInstanceOf(InstanceAPI);
    });

    it("should create instance for edge type", () => {
      const api = new TestableInstanceAPI(
        createConfig(),
        viewRef,
        "edge",
      );
      expect(api).toBeInstanceOf(InstanceAPI);
    });
  });

  describe("static properties", () => {
    it("should have correct limit constants", () => {
      expect(InstanceAPIAny.LIST_LIMIT).toBe(1000);
      expect(InstanceAPIAny.SEARCH_LIMIT).toBe(1000);
      expect(InstanceAPIAny.RETRIEVE_LIMIT).toBe(1000);
      expect(InstanceAPIAny.AGGREGATE_LIMIT).toBe(1000);
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
      const api = new TestableInstanceAPI(config, viewRef, "node");
      expect(api.getApiUrl("/models/instances/list")).toBe(
        "https://api.cognitedata.com/api/v1/projects/test-project/models/instances/list",
      );
    });
  });
});

describe("InstanceAPI._iterate", () => {
  it("should throw error for limit less than 1", async () => {
    const api = createTestAPI();
    await expect(api.iterate({ limit: 0 })).rejects.toThrow(
      "Limit must be between 1 and 1000, got 0.",
    );
  });

  it("should throw error for limit greater than 1000", async () => {
    const api = createTestAPI();
    await expect(api.iterate({ limit: 1001 })).rejects.toThrow(
      "Limit must be between 1 and 1000, got 1001.",
    );
  });

  it("should return page with items", async () => {
    const api = createTestAPI();
    const mockItems = [
      createTestInstance("id1", "Alice", 30),
      createTestInstance("id2", "Bob", 25),
    ];
    api.setMockResponse(async () => createMockListResponse(mockItems));

    const result = await api.iterate({ limit: 10 });
    expect(result.items.length).toBe(2);
    expect(result.nextCursor).toBeUndefined();
  });

  it("should return page with cursor for pagination", async () => {
    const api = createTestAPI();
    const mockItems = [createTestInstance("id1", "Alice", 30)];
    api.setMockResponse(async () => createMockListResponse(mockItems, "next-cursor-123"));

    const result = await api.iterate({ limit: 1 });
    expect(result.items.length).toBe(1);
    expect(result.nextCursor).toBe("next-cursor-123");
  });

  it("should pass filter to request", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    const filter: Filter = {
      equals: { property: ["mySpace", "TestView/v1", "name"], value: "Alice" },
    };
    await api.iterate({ filter, limit: 10 });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.filter).toEqual(filter);
  });

  it("should pass sort to request", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    const sort: PropertySort = {
      property: ["mySpace", "TestView/v1", "name"],
      direction: "ascending",
    };
    await api.iterate({ sort, limit: 10 });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.sort).toEqual([sort]);
  });

  it("should pass multiple sorts to request", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    const sorts: PropertySort[] = [
      { property: ["mySpace", "TestView/v1", "name"], direction: "ascending" },
      { property: ["mySpace", "TestView/v1", "age"], direction: "descending" },
    ];
    await api.iterate({ sort: sorts, limit: 10 });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.sort).toEqual(sorts);
  });

  it("should pass targetUnits to request", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    const targetUnits: UnitConversion = {
      property: "temperature",
      unit: { externalId: "temperature:cel" },
    };
    await api.iterate({ targetUnits, limit: 10 });

    const body = api.getCapturedRequests()[0]?.body;
    const sources = body?.sources as unknown[];
    expect(sources).toBeDefined();
    expect((sources[0] as Record<string, unknown>).targetUnits).toEqual([targetUnits]);
  });

  it("should pass debug to request", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    await api.iterate({ debug: { emitResults: true }, limit: 10 });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.debug).toEqual({ emitResults: true });
  });

  it("should pass includeTyping to request", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    await api.iterate({ includeTyping: true, limit: 10 });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.includeTyping).toBe(true);
  });

  it("should pass cursor to request", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    await api.iterate({ cursor: "my-cursor", limit: 10 });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.cursor).toBe("my-cursor");
  });
});

describe("InstanceAPI._list", () => {
        it("should throw error for negative limit", async () => {
            const api = createTestAPI();
            await expect(api.list({limit: -5})).rejects.toThrow(
                "Limit must be a positive integer or undefined for no limit.",
            );
        });

        it("should throw error for zero limit", async () => {
            const api = createTestAPI();
            await expect(api.list({limit: 0})).rejects.toThrow(
                "Limit must be a positive integer or undefined for no limit.",
            );
        });

        it("should collect all items from single page", async () => {
            const api = createTestAPI();
            const mockItems = [
                createTestInstance("id1", "Alice", 30),
                createTestInstance("id2", "Bob", 25),
            ];
            api.setMockResponse(async () => createMockListResponse(mockItems));

            const result = await api.list({limit: 10});
            expect(result.length).toBe(2);
        });

        it("should paginate through multiple pages", async () => {
            const api = createTestAPI();
            const page1Items = [createTestInstance("id1", "Alice", 30)];
            const page2Items = [createTestInstance("id2", "Bob", 25)];

            let callCount = 0;
            api.setMockResponse(async () => {
                callCount++;
                if (callCount === 1) {
                    return createMockListResponse(page1Items, "next-cursor");
                }
                return createMockListResponse(page2Items);
            });

            const result = await api.list({limit: 100});
            expect(result.length).toBe(2);
            expect(callCount).toBe(2);
        });

        it("should respect limit across pages", async () => {
            const api = createTestAPI();
            const page1Items = [
                createTestInstance("id1", "Alice", 30),
                createTestInstance("id2", "Bob", 25),
            ];

            api.setMockResponse(async () => createMockListResponse(page1Items, "next-cursor"));

            const result = await api.list({limit: 2});
            expect(result.length).toBe(2);
            // Should not have paginated since limit was reached
            expect(api.getCapturedRequests().length).toBe(1);
        });

        it("Should return all items when no limit is set", async () => {
            const api = createTestAPI();
            const page1Items = [
                createTestInstance("id1", "Alice", 30),
                createTestInstance("id2", "Bob", 25),
            ];
            const page2Items = [
                createTestInstance("id3", "Charlie", 35),
            ];
            let callCount = 0;
            api.setMockResponse(async () => {
                    callCount++;
                    if (callCount === 1) {
                        return createMockListResponse(page1Items, "next-cursor");
                    }
                    return createMockListResponse(page2Items)
                }
            );
            const result = await api.list();
            expect(result.length).toBe(3);
            expect(callCount).toBe(2);
        });
    }
)

describe("InstanceAPI._search", () => {
  it("should throw error for limit less than 1", async () => {
    const api = createTestAPI();
    await expect(api.search({ limit: 0 })).rejects.toThrow(
      "Limit must be between 1 and 1000, got 0.",
    );
  });

  it("should throw error for limit greater than 1000", async () => {
    const api = createTestAPI();
    await expect(api.search({ limit: 1001 })).rejects.toThrow(
      "Limit must be between 1 and 1000, got 1001.",
    );
  });

  it("should return search results", async () => {
    const api = createTestAPI();
    const mockItems = [createTestInstance("id1", "Alice", 30)];
    api.setMockResponse(async () => createMockListResponse(mockItems));

    const result = await api.search({ query: "Alice" });
    expect(result.items.length).toBe(1);
  });

  it("should pass query to request", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    await api.search({ query: "test query" });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.query).toBe("test query");
  });

  it("should pass properties as array", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    await api.search({ properties: "name" });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.properties).toEqual(["name"]);
  });

  it("should pass multiple properties", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    await api.search({ properties: ["name", "description"] });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.properties).toEqual(["name", "description"]);
  });

  it("should pass operator in uppercase", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    await api.search({ operator: "and" });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.operator).toBe("AND");
  });

  it("should use view key for search", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    await api.search({});

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.view).toBeDefined();
    expect(body?.sources).toBeUndefined();
  });

  it("should pass targetUnits with view key", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    const targetUnits: UnitConversion = {
      property: "temperature",
      unit: { externalId: "temperature:cel" },
    };
    await api.search({ targetUnits });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.targetUnits).toEqual([targetUnits]);
  });
});

describe("InstanceAPI._retrieve", () => {
  it("should return empty list for empty batch", async () => {
    const api = createTestAPI();
    const result = await api.retrieveBatch([]);
    expect(result.length).toBe(0);
  });

  it("should throw error when space is not provided for string id", async () => {
    const api = createTestAPI();
    await expect(api.retrieveSingle("my-id")).rejects.toThrow(
      "space parameter is required when retrieving by external_id string",
    );
  });

  it("should retrieve single instance by InstanceId", async () => {
    const api = createTestAPI();
    const mockItems = [createTestInstance("id1", "Alice", 30)];
    api.setMockResponse(async () => createMockListResponse(mockItems));

    const result = await api.retrieveSingle({ space: "mySpace", externalId: "id1" });
    expect(result).toBeDefined();
    expect(result?.externalId).toBe("id1");
  });

  it("should retrieve single instance by tuple", async () => {
    const api = createTestAPI();
    const mockItems = [createTestInstance("id1", "Alice", 30)];
    api.setMockResponse(async () => createMockListResponse(mockItems));

    const result = await api.retrieveSingle(["mySpace", "id1"]);
    expect(result).toBeDefined();
  });

  it("should retrieve single instance by string with space", async () => {
    const api = createTestAPI();
    const mockItems = [createTestInstance("id1", "Alice", 30)];
    api.setMockResponse(async () => createMockListResponse(mockItems));

    const result = await api.retrieveSingle("id1", { space: "mySpace" });
    expect(result).toBeDefined();
  });

  it("should return undefined for non-existing instance", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    const result = await api.retrieveSingle({ space: "mySpace", externalId: "non-existing" });
    expect(result).toBeUndefined();
  });

  it("should retrieve batch of instances", async () => {
    const api = createTestAPI();
    const mockItems = [
      createTestInstance("id1", "Alice", 30),
      createTestInstance("id2", "Bob", 25),
    ];
    api.setMockResponse(async () => createMockListResponse(mockItems));

    const result = await api.retrieveBatch([
      { space: "mySpace", externalId: "id1" },
      { space: "mySpace", externalId: "id2" },
    ]);
    expect(result.length).toBe(2);
  });

  it("should handle mixed id types in batch", async () => {
    const api = createTestAPI();
    const mockItems = [
      createTestInstance("id1", "Alice", 30),
      createTestInstance("id2", "Bob", 25),
      createTestInstance("id3", "Charlie", 35),
    ];
    api.setMockResponse(async () => createMockListResponse(mockItems));

    const result = await api.retrieveBatch(
      [{ space: "mySpace", externalId: "id1" }, ["mySpace", "id2"], "id3"],
      { space: "mySpace" },
    );
    expect(result.length).toBe(3);
  });

  it("should pass includeTyping to retrieve request", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    await api.retrieveSingle({ space: "mySpace", externalId: "id1" }, { includeTyping: true });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.includeTyping).toBe(true);
  });

  it("should pass targetUnits to retrieve request", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockListResponse([]));

    const targetUnits: UnitConversion = {
      property: "temperature",
      unit: { externalId: "temperature:cel" },
    };
    await api.retrieveSingle(
      { space: "mySpace", externalId: "id1" },
      { targetUnits },
    );

    const body = api.getCapturedRequests()[0]?.body;
    const sources = body?.sources as unknown[];
    expect(sources).toBeDefined();
    expect((sources[0] as Record<string, unknown>).targetUnits).toEqual([targetUnits]);
  });
});

describe("InstanceAPI._aggregate", () => {
  it("should return aggregate response with count", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () =>
      createMockAggregateResponse([
        { instanceType: "node", aggregates: [{ aggregate: "count", value: 42 }] },
      ])
    );

    const result = await api.aggregate({ aggregate: "count" });
    expect(result.items).toHaveLength(1);
    expect(result.items[0]?.aggregates[0]?.value).toBe(42);
  });

  it("should pass aggregation to request", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockAggregateResponse([]));

    const agg: Aggregation = { aggregate: "avg", property: "age" };
    await api.aggregate(agg);

    const body = api.getCapturedRequests()[0]?.body;
    const aggregates = body?.aggregates as unknown[];
    expect(aggregates).toHaveLength(1);
    expect((aggregates[0] as Record<string, unknown>).avg).toEqual(agg);
  });

  it("should pass multiple aggregations", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockAggregateResponse([]));

    const aggs: Aggregation[] = [{ aggregate: "count" }, { aggregate: "avg", property: "age" }];
    await api.aggregate(aggs);

    const body = api.getCapturedRequests()[0]?.body;
    const aggregates = body?.aggregates as unknown[];
    expect(aggregates).toHaveLength(2);
  });

  it("should pass groupBy as array", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockAggregateResponse([]));

    await api.aggregate({ aggregate: "count" }, { groupBy: "category" });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.groupBy).toEqual(["category"]);
  });

  it("should pass multiple groupBy properties", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockAggregateResponse([]));

    await api.aggregate({ aggregate: "count" }, { groupBy: ["category", "status"] });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.groupBy).toEqual(["category", "status"]);
  });

  it("should pass filter to aggregate request", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockAggregateResponse([]));

    const filter: Filter = {
      equals: { property: ["mySpace", "TestView/v1", "status"], value: "active" },
    };
    await api.aggregate({ aggregate: "count" }, { filter });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.filter).toEqual(filter);
  });

  it("should use default limit for aggregation", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockAggregateResponse([]));

    await api.aggregate({ aggregate: "count" });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.limit).toBe(1000);
  });

  it("should use custom limit for aggregation", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockAggregateResponse([]));

    await api.aggregate({ aggregate: "count" }, { limit: 100 });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.limit).toBe(100);
  });

  it("should pass query to aggregate request", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockAggregateResponse([]));

    await api.aggregate({ aggregate: "count" }, { query: "search term" });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.query).toBe("search term");
  });

  it("should pass properties for aggregate search", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockAggregateResponse([]));

    await api.aggregate({ aggregate: "count" }, { properties: ["name", "description"] });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.properties).toEqual(["name", "description"]);
  });

  it("should pass operator for aggregate", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockAggregateResponse([]));

    await api.aggregate({ aggregate: "count" }, { operator: "and" });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.operator).toBe("AND");
  });

  it("should pass includeTyping to aggregate request", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockAggregateResponse([]));

    await api.aggregate({ aggregate: "count" }, { includeTyping: true });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.includeTyping).toBe(true);
  });

  it("should pass targetUnits with view key for aggregate", async () => {
    const api = createTestAPI();
    api.setMockResponse(async () => createMockAggregateResponse([]));

    const targetUnits: UnitConversion = {
      property: "temperature",
      unit: { externalId: "temperature:cel" },
    };
    await api.aggregate({ aggregate: "count" }, { targetUnits });

    const body = api.getCapturedRequests()[0]?.body;
    expect(body?.targetUnits).toEqual([targetUnits]);
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
