/**
 * Example SDK for the pygen example data model.
 *
 * This module provides an example implementation of a generated SDK that demonstrates
 * how to use the generic InstanceClient and InstanceAPI classes to create type-safe
 * clients for specific CDF Data Models.
 *
 * The example data model includes:
 * - ProductNode: Node view with various property types (text, float, int, bool, date, datetime)
 *   and a direct relation to CategoryNode
 * - CategoryNode: Node view with a reverse direct relation to ProductNode
 * - RelatesTo: Edge view for relating nodes with properties
 *
 * @packageDocumentation
 */

// Data classes
export {
  // ProductNode
  type ProductNode,
  type ProductNodeWrite,
  ProductNodeList,
  ProductNodeFilter,
  productNodeAsWrite,
  PRODUCT_NODE_VIEW,

  // CategoryNode
  type CategoryNode,
  type CategoryNodeWrite,
  CategoryNodeList,
  CategoryNodeFilter,
  categoryNodeAsWrite,
  CATEGORY_NODE_VIEW,

  // RelatesTo
  type RelatesTo,
  type RelatesToWrite,
  RelatesToList,
  RelatesToFilter,
  relatesToAsWrite,
  RELATES_TO_VIEW,

  // Constants
  EXAMPLE_SPACE,
  EXAMPLE_VERSION,
} from "./dataClasses.ts";

// API classes
export {
  ProductNodeAPI,
  CategoryNodeAPI,
  RelatesToAPI,
} from "./api.ts";

// Client
export { ExampleClient } from "./client.ts";

