/**
 * Example client for the pygen example SDK.
 *
 * This module contains the ExampleClient that composes the view-specific APIs
 * for ProductNode, CategoryNode, and RelatesTo views.
 *
 * @packageDocumentation
 */

import type { PygenClientConfig } from "../instance_api/auth/index.ts";
import { InstanceClient } from "../instance_api/client.ts";

import { CategoryNodeAPI, ProductNodeAPI, RelatesToAPI } from "./api.ts";

/**
 * Example client for the pygen example data model.
 *
 * This client provides access to the three views in the example data model:
 * - productNode: ProductNode view (nodes with various property types)
 * - categoryNode: CategoryNode view (nodes with reverse direct relation)
 * - relatesTo: RelatesTo view (edges between nodes)
 *
 * @example
 * ```typescript
 * const client = new ExampleClient({
 *   baseUrl: "https://api.cognitedata.com",
 *   project: "my-project",
 *   credentials: { tokenProvider: async () => "my-token" },
 * });
 *
 * // List products with filters
 * const products = await client.productNode.list({
 *   minPrice: 10,
 *   active: true,
 * });
 *
 * // Get a specific category
 * const category = await client.categoryNode.retrieve("my-category-id", { space: "my-space" });
 *
 * // Search for relations
 * const relations = await client.relatesTo.search({
 *   query: "similar",
 *   limit: 10,
 * });
 * ```
 */
export class ExampleClient extends InstanceClient {
  /** API for ProductNode instances */
  readonly productNode: ProductNodeAPI;
  /** API for CategoryNode instances */
  readonly categoryNode: CategoryNodeAPI;
  /** API for RelatesTo edge instances */
  readonly relatesTo: RelatesToAPI;

  /**
   * Creates a new ExampleClient.
   *
   * @param config - Configuration for the client including URL, project, and credentials.
   * @param writeWorkers - Number of concurrent workers for write operations. Default is 5.
   * @param deleteWorkers - Number of concurrent workers for delete operations. Default is 3.
   * @param retrieveWorkers - Number of concurrent workers for retrieve operations. Default is 10.
   */
  constructor(
    config: PygenClientConfig,
    writeWorkers = 5,
    deleteWorkers = 3,
    retrieveWorkers = 10,
  ) {
    super(config, writeWorkers, deleteWorkers, retrieveWorkers);

    // Initialize view-specific APIs
    this.productNode = new ProductNodeAPI(config);
    this.categoryNode = new CategoryNodeAPI(config);
    this.relatesTo = new RelatesToAPI(config);
  }
}
