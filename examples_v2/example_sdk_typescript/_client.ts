/**
 * Client for the generated SDK.
 *
 * This module contains the ExampleClient that composes view-specific APIs.
 *
 * @packageDocumentation
 */

import type { PygenClientConfig } from "./instance_api/auth/index.ts";
import { InstanceClient } from "./instance_api/client.ts";

import { CategoryNodeApi, ProductNodeApi, RelatesToApi } from "./_api/index.ts";

/**
 * Generated client for interacting with the data model.
 *
 * This client provides access to the following views:
 * - categoryNode: CategoryNodeApi
 * - productNode: ProductNodeApi
 * - relatesTo: RelatesToApi
 */
export class ExampleClient extends InstanceClient {
  /** API for CategoryNode instances */
  readonly categoryNode: CategoryNodeApi;
  /** API for ProductNode instances */
  readonly productNode: ProductNodeApi;
  /** API for RelatesTo instances */
  readonly relatesTo: RelatesToApi;

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
    this.categoryNode = new CategoryNodeApi(config);
    this.productNode = new ProductNodeApi(config);
    this.relatesTo = new RelatesToApi(config);
  }
}