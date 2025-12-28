/**
 * Reference types for CDF Data Modeling.
 *
 * These types represent references to various CDF resources like
 * spaces, views, containers, and instances.
 *
 * @packageDocumentation
 */

/**
 * Type of instance: node or edge.
 */
export type InstanceType = "node" | "edge";

/**
 * Represents an instance identifier in CDF.
 * An instance is uniquely identified by a space and an external ID.
 */
export interface InstanceId {
  /** The space containing the instance */
  readonly space: string;
  /** The external ID of the instance within the space */
  readonly externalId: string;
}

/**
 * Represents a reference to a node in CDF.
 * A node is uniquely identified by a space and external ID.
 */
export interface NodeReference {
  /** The space containing the node */
  readonly space: string;
  /** The external ID of the node within the space */
  readonly externalId: string;
}

/**
 * Represents a reference to an edge in CDF.
 * An edge connects two nodes and is uniquely identified by a space and external ID.
 */
export interface EdgeReference {
  /** The space containing the edge */
  readonly space: string;
  /** The external ID of the edge within the space */
  readonly externalId: string;
}

/**
 * Represents a reference to a view in CDF.
 * A view is uniquely identified by a space, external ID, and version.
 */
export interface ViewReference {
  /** The space containing the view */
  readonly space: string;
  /** The external ID of the view within the space */
  readonly externalId: string;
  /** The version of the view */
  readonly version: string;
}

/**
 * Represents a reference to a container in CDF.
 * A container is uniquely identified by a space and external ID.
 */
export interface ContainerReference {
  /** The space containing the container */
  readonly space: string;
  /** The external ID of the container within the space */
  readonly externalId: string;
}

/**
 * Represents a reference to a data model in CDF.
 * A data model is uniquely identified by a space, external ID, and version.
 */
export interface DataModelReference {
  /** The space containing the data model */
  readonly space: string;
  /** The external ID of the data model within the space */
  readonly externalId: string;
  /** The version of the data model */
  readonly version: string;
}

/**
 * Creates an InstanceId from space and external ID.
 *
 * @param space - The space containing the instance
 * @param externalId - The external ID of the instance
 * @returns An InstanceId object
 *
 * @example
 * ```typescript
 * const id = createInstanceId("my-space", "my-instance");
 * console.log(id); // { space: "my-space", externalId: "my-instance" }
 * ```
 */
export function createInstanceId(space: string, externalId: string): InstanceId {
  return { space, externalId };
}

/**
 * Creates a ViewReference from space, external ID, and version.
 *
 * @param space - The space containing the view
 * @param externalId - The external ID of the view
 * @param version - The version of the view
 * @returns A ViewReference object
 *
 * @example
 * ```typescript
 * const ref = createViewReference("my-space", "my-view", "v1");
 * console.log(ref); // { space: "my-space", externalId: "my-view", version: "v1" }
 * ```
 */
export function createViewReference(
  space: string,
  externalId: string,
  version: string
): ViewReference {
  return { space, externalId, version };
}

/**
 * Creates a ContainerReference from space and external ID.
 *
 * @param space - The space containing the container
 * @param externalId - The external ID of the container
 * @returns A ContainerReference object
 *
 * @example
 * ```typescript
 * const ref = createContainerReference("my-space", "my-container");
 * console.log(ref); // { space: "my-space", externalId: "my-container" }
 * ```
 */
export function createContainerReference(space: string, externalId: string): ContainerReference {
  return { space, externalId };
}

/**
 * Creates a DataModelReference from space, external ID, and version.
 *
 * @param space - The space containing the data model
 * @param externalId - The external ID of the data model
 * @param version - The version of the data model
 * @returns A DataModelReference object
 *
 * @example
 * ```typescript
 * const ref = createDataModelReference("my-space", "my-model", "v1");
 * console.log(ref); // { space: "my-space", externalId: "my-model", version: "v1" }
 * ```
 */
export function createDataModelReference(
  space: string,
  externalId: string,
  version: string
): DataModelReference {
  return { space, externalId, version };
}

/**
 * Creates a NodeReference from space and external ID.
 *
 * @param space - The space containing the node
 * @param externalId - The external ID of the node
 * @returns A NodeReference object
 *
 * @example
 * ```typescript
 * const ref = createNodeReference("my-space", "my-node");
 * console.log(ref); // { space: "my-space", externalId: "my-node" }
 * ```
 */
export function createNodeReference(space: string, externalId: string): NodeReference {
  return { space, externalId };
}

/**
 * Creates an EdgeReference from space and external ID.
 *
 * @param space - The space containing the edge
 * @param externalId - The external ID of the edge
 * @returns An EdgeReference object
 *
 * @example
 * ```typescript
 * const ref = createEdgeReference("my-space", "my-edge");
 * console.log(ref); // { space: "my-space", externalId: "my-edge" }
 * ```
 */
export function createEdgeReference(space: string, externalId: string): EdgeReference {
  return { space, externalId };
}

/**
 * Checks if two InstanceIds are equal.
 *
 * @param a - First InstanceId
 * @param b - Second InstanceId
 * @returns true if both have the same space and externalId
 */
export function instanceIdEquals(a: InstanceId, b: InstanceId): boolean {
  return a.space === b.space && a.externalId === b.externalId;
}

/**
 * Converts an InstanceId to a string representation.
 *
 * @param id - The InstanceId to convert
 * @returns A string in the format "space:externalId"
 */
export function instanceIdToString(id: InstanceId): string {
  return `${id.space}:${id.externalId}`;
}

/**
 * Converts a ViewReference to a string representation.
 *
 * @param ref - The ViewReference to convert
 * @returns A string in the format "space:externalId(version=version)"
 */
export function viewReferenceToString(ref: ViewReference): string {
  return `${ref.space}:${ref.externalId}(version=${ref.version})`;
}
