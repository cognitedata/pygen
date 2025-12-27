/**
 * Type definitions for CDF Data Modeling
 *
 * @module types
 */

/**
 * Represents a reference to an instance by its external ID and space.
 */
export interface InstanceId {
  /** The external ID of the instance */
  readonly externalId: string;
  /** The space the instance belongs to */
  readonly space: string;
}

/**
 * Represents a reference to a view.
 */
export interface ViewReference {
  /** The external ID of the view */
  readonly externalId: string;
  /** The space the view belongs to */
  readonly space: string;
  /** The version of the view */
  readonly version: string;
}

/**
 * Represents a reference to a container.
 */
export interface ContainerReference {
  /** The external ID of the container */
  readonly externalId: string;
  /** The space the container belongs to */
  readonly space: string;
}

/**
 * Creates an InstanceId from external ID and space.
 */
export function createInstanceId(externalId: string, space: string): InstanceId {
  return { externalId, space };
}

/**
 * Creates a ViewReference from external ID, space, and version.
 */
export function createViewReference(
  externalId: string,
  space: string,
  version: string
): ViewReference {
  return { externalId, space, version };
}

/**
 * Creates a ContainerReference from external ID and space.
 */
export function createContainerReference(externalId: string, space: string): ContainerReference {
  return { externalId, space };
}
