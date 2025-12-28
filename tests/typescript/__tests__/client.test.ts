import { describe, expect, it } from "vitest";
import {
  chunker,
  type HTTPResult,
  InstanceClient,
  type InstanceWrite,
  MultiRequestError,
  type PygenClientConfig,
  TokenCredentials,
  type UpsertMode,
  type UpsertResult,
  type ViewReference,
} from "@cognite/pygen-typescript";

describe("InstanceClient", () => {
  const createConfig = (): PygenClientConfig => ({
    baseUrl: "https://api.cognitedata.com",
    project: "test-project",
    credentials: new TokenCredentials("test-token"),
  });

  describe("constructor", () => {
    it("should create instance with default worker counts", () => {
      const client = new InstanceClient(createConfig());
      expect(client).toBeInstanceOf(InstanceClient);
    });

    it("should create instance with custom worker counts", () => {
      const client = new InstanceClient(createConfig(), 10, 5, 20);
      expect(client).toBeInstanceOf(InstanceClient);
    });
  });

  describe("static properties", () => {
    it("should have correct limit constants", () => {
      expect(InstanceClient.UPSERT_LIMIT).toBe(1000);
      expect(InstanceClient.DELETE_LIMIT).toBe(1000);
      expect(InstanceClient.RETRIEVE_LIMIT).toBe(1000);
    });
  });
});

describe("InstanceClient.upsert (unit tests)", () => {
  const createConfig = (): PygenClientConfig => ({
    baseUrl: "https://api.cognitedata.com",
    project: "test-project",
    credentials: new TokenCredentials("test-token"),
  });

  const viewRef: ViewReference = {
    space: "mySpace",
    externalId: "Person",
    version: "1",
  };

  it("should return empty result for empty input", async () => {
    const client = new InstanceClient(createConfig());
    const result = await client.upsert([], viewRef);
    expect(result).toEqual({ items: [], deleted: [] });
  });

  it("should throw NotImplementedError for update mode", async () => {
    const client = new InstanceClient(createConfig());
    const instance: InstanceWrite = {
      instanceType: "node",
      space: "mySpace",
      externalId: "person-1",
    };

    await expect(client.upsert(instance, viewRef, "update")).rejects.toThrow(
      "Update mode is not yet implemented",
    );
  });
});

describe("InstanceClient.delete (unit tests)", () => {
  const createConfig = (): PygenClientConfig => ({
    baseUrl: "https://api.cognitedata.com",
    project: "test-project",
    credentials: new TokenCredentials("test-token"),
  });

  it("should return empty array for empty input", async () => {
    const client = new InstanceClient(createConfig());
    const result = await client.delete([]);
    expect(result).toEqual([]);
  });

  it("should throw error when space is not provided for string items", async () => {
    const client = new InstanceClient(createConfig());

    await expect(client.delete("person-1")).rejects.toThrow(
      "space parameter is required when deleting by external_id string",
    );
  });
});

describe("chunker utility", () => {
  it("should chunk arrays correctly", () => {
    expect(chunker([1, 2, 3, 4, 5], 2)).toEqual([[1, 2], [3, 4], [5]]);
    expect(chunker([1, 2, 3], 3)).toEqual([[1, 2, 3]]);
    expect(chunker([1, 2, 3], 5)).toEqual([[1, 2, 3]]);
    expect(chunker([], 2)).toEqual([]);
  });

  it("should throw for non-positive chunk size", () => {
    expect(() => chunker([1, 2, 3], 0)).toThrow("Chunk size must be positive");
    expect(() => chunker([1, 2, 3], -1)).toThrow("Chunk size must be positive");
  });
});

// Test helpers
const createSuccessUpsertResponse = (
  items: Array<{
    instanceType: "node" | "edge";
    space: string;
    externalId: string;
    wasModified: boolean;
  }>,
): HTTPResult => ({
  kind: "success",
  statusCode: 200,
  body: JSON.stringify({
    items: items.map((item) => ({
      ...item,
      version: 1,
      createdTime: 1000,
      lastUpdatedTime: item.wasModified ? 2000 : 1000,
    })),
  }),
});

const createFailedResponse = (
  statusCode: number,
  message: string,
): HTTPResult => ({
  kind: "failed_response",
  statusCode,
  body: JSON.stringify({ error: { code: statusCode, message } }),
  error: { code: statusCode, message },
});

// Testable subclass to expose protected methods
class TestableInstanceClient extends InstanceClient {
  testCreateApiUrl(endpoint: string): string {
    return this.createApiUrl(endpoint);
  }

  testCollectResults(
    results: readonly HTTPResult[],
    parseSuccess: (body: string) => UpsertResult,
  ): UpsertResult {
    return this.collectResults(results, parseSuccess);
  }

  async testExecuteInParallel<T>(
    items: readonly T[],
    chunkSize: number,
    maxWorkers: number,
    taskFn: (chunk: readonly T[]) => Promise<HTTPResult>,
  ): Promise<HTTPResult[]> {
    return this.executeInParallel(items, chunkSize, maxWorkers, taskFn);
  }
}

describe("InstanceClient API URL generation", () => {
  const createConfig = (): PygenClientConfig => ({
    baseUrl: "https://api.cognitedata.com",
    project: "test-project",
    credentials: new TokenCredentials("test-token"),
  });

  it("should create correct API URL", () => {
    const client = new TestableInstanceClient(createConfig());
    expect(client.testCreateApiUrl("/models/instances")).toBe(
      "https://api.cognitedata.com/api/v1/projects/test-project/models/instances",
    );
  });

  it("should handle base URL with trailing slash", () => {
    const config: PygenClientConfig = {
      baseUrl: "https://api.cognitedata.com/",
      project: "test-project",
      credentials: new TokenCredentials("test-token"),
    };
    const client = new TestableInstanceClient(config);
    expect(client.testCreateApiUrl("/models/instances")).toBe(
      "https://api.cognitedata.com/api/v1/projects/test-project/models/instances",
    );
  });
});

describe("InstanceClient.collectResults", () => {
  const createConfig = (): PygenClientConfig => ({
    baseUrl: "https://api.cognitedata.com",
    project: "test-project",
    credentials: new TokenCredentials("test-token"),
  });

  const parseSuccess = (body: string): UpsertResult => {
    const parsed = JSON.parse(body) as { items: unknown[] };
    return {
      items: parsed.items as UpsertResult["items"],
      deleted: [],
    };
  };

  it("should combine successful results", () => {
    const client = new TestableInstanceClient(createConfig());

    const results: HTTPResult[] = [
      createSuccessUpsertResponse([
        { instanceType: "node", space: "s1", externalId: "e1", wasModified: true },
      ]),
      createSuccessUpsertResponse([
        { instanceType: "node", space: "s1", externalId: "e2", wasModified: false },
      ]),
    ];

    const combined = client.testCollectResults(results, parseSuccess);

    expect(combined.items).toHaveLength(2);
    expect(combined.deleted).toHaveLength(0);
  });

  it("should throw MultiRequestError on failures", () => {
    const client = new TestableInstanceClient(createConfig());

    const results: HTTPResult[] = [
      createSuccessUpsertResponse([
        { instanceType: "node", space: "s1", externalId: "e1", wasModified: true },
      ]),
      createFailedResponse(400, "Bad request"),
    ];

    expect(() => client.testCollectResults(results, parseSuccess)).toThrow(
      MultiRequestError,
    );

    try {
      client.testCollectResults(results, parseSuccess);
    } catch (error) {
      if (error instanceof MultiRequestError) {
        // Successful results should be in error.result
        expect(error.result.items).toHaveLength(1);
        expect(error.failedResponses).toHaveLength(1);
      }
    }
  });

  it("should handle failed requests (no response)", () => {
    const client = new TestableInstanceClient(createConfig());

    const results: HTTPResult[] = [
      createSuccessUpsertResponse([
        { instanceType: "node", space: "s1", externalId: "e1", wasModified: true },
      ]),
      {
        kind: "failed_request",
        error: "Network timeout",
      },
    ];

    try {
      client.testCollectResults(results, parseSuccess);
      expect.fail("Should have thrown");
    } catch (error) {
      expect(error).toBeInstanceOf(MultiRequestError);
      if (error instanceof MultiRequestError) {
        expect(error.result.items).toHaveLength(1);
        expect(error.failedRequests).toHaveLength(1);
      }
    }
  });
});

describe("InstanceClient.executeInParallel", () => {
  const createConfig = (): PygenClientConfig => ({
    baseUrl: "https://api.cognitedata.com",
    project: "test-project",
    credentials: new TokenCredentials("test-token"),
  });

  it("should execute tasks in parallel", async () => {
    const client = new TestableInstanceClient(createConfig());

    const items = [1, 2, 3, 4, 5, 6, 7];
    const processedChunks: number[][] = [];

    const taskFn = async (chunk: readonly number[]): Promise<HTTPResult> => {
      processedChunks.push([...chunk]);
      return {
        kind: "success",
        statusCode: 200,
        body: JSON.stringify({ items: chunk }),
      };
    };

    const results = await client.testExecuteInParallel(items, 2, 3, taskFn);

    // Should have 4 chunks: [1,2], [3,4], [5,6], [7]
    expect(processedChunks).toHaveLength(4);
    expect(results).toHaveLength(4);

    // Verify all items were processed
    const allProcessed = processedChunks.flat().sort();
    expect(allProcessed).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  it("should return empty array for empty input", async () => {
    const client = new TestableInstanceClient(createConfig());

    const taskFn = async (): Promise<HTTPResult> => ({
      kind: "success",
      statusCode: 200,
      body: "{}",
    });

    const results = await client.testExecuteInParallel([], 10, 5, taskFn);
    expect(results).toEqual([]);
  });

  it("should handle small number of chunks without batching", async () => {
    const client = new TestableInstanceClient(createConfig());

    const items = [1, 2, 3]; // 2 chunks with size 2
    const processedChunks: number[][] = [];

    const taskFn = async (chunk: readonly number[]): Promise<HTTPResult> => {
      processedChunks.push([...chunk]);
      return {
        kind: "success",
        statusCode: 200,
        body: JSON.stringify({ items: chunk }),
      };
    };

    // With maxWorkers=5 and only 2 chunks, all should run in parallel
    const results = await client.testExecuteInParallel(items, 2, 5, taskFn);

    expect(processedChunks).toHaveLength(2);
    expect(results).toHaveLength(2);
  });

  it("should preserve order of results", async () => {
    const client = new TestableInstanceClient(createConfig());

    const items = [1, 2, 3, 4, 5, 6];

    const taskFn = async (chunk: readonly number[]): Promise<HTTPResult> => {
      // Add random delay to simulate varying response times
      await new Promise((resolve) => setTimeout(resolve, Math.random() * 10));
      return {
        kind: "success",
        statusCode: 200,
        body: JSON.stringify({ first: chunk[0] }),
      };
    };

    const results = await client.testExecuteInParallel(items, 2, 2, taskFn);

    // Results should be in order regardless of execution time
    const firstValues = results.map((r) => {
      const parsed = JSON.parse((r as { body: string }).body) as { first: number };
      return parsed.first;
    });

    expect(firstValues).toEqual([1, 3, 5]);
  });
});

describe("UpsertMode type", () => {
  it("should accept valid upsert modes", () => {
    const modes: UpsertMode[] = ["replace", "update", "apply"];
    expect(modes).toHaveLength(3);
  });
});
