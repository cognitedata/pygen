/**
 * Reference types for CDF Data Modeling.
 *
 * These types represent references to various CDF resources like
 * spaces, views, containers, and instances.
 *
 * @packageDocumentation
 */

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
