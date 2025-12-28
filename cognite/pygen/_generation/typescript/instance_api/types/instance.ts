/**
 * Core instance types for CDF Data Modeling.
 *
 * This module provides base classes and interfaces for working with
 * CDF instances (nodes and edges).
 *
 * @packageDocumentation
 */

import type { InstanceId, InstanceType, ViewReference, NodeReference } from "./references.js";
import { msToDate, dateToMs, toSnakeCaseKey } from "./utils.js";

// ============================================================================
// Data Record Types
// ============================================================================

/**
 * Metadata record for a read instance.
 *
 * Contains system-managed metadata like version and timestamps.
 */
export interface DataRecord {
  /** The version of the instance */
  readonly version: number;
  /** When the instance was last updated (as Date object) */
  readonly lastUpdatedTime: Date;
  /** When the instance was created (as Date object) */
  readonly createdTime: Date;
  /** When the instance was deleted, if applicable (as Date object) */
  readonly deletedTime?: Date;
}

/**
 * Metadata record for a write instance.
 *
 * Contains optional version constraint for optimistic locking.
 */
export interface DataRecordWrite {
  /**
   * Fail the ingestion request if the instance version is greater than or equal to this value.
   * - If not specified, the ingestion will always overwrite any existing data.
   * - If set to 0, the upsert will behave as an insert (fails if item exists).
   * - If skipOnVersionConflict is set on the request, the item will be skipped instead of failing.
   */
  readonly existingVersion?: number;
}

/**
 * Raw data record from CDF API response (with timestamps as milliseconds).
 */
export interface DataRecordRaw {
  version: number;
  lastUpdatedTime: number;
  createdTime: number;
  deletedTime?: number;
}

/**
 * Parses a raw data record from the CDF API into a DataRecord.
 *
 * @param raw - Raw data record with timestamps as milliseconds
 * @returns A DataRecord with Date objects
 */
export function parseDataRecord(raw: DataRecordRaw): DataRecord {
  const result: DataRecord = {
    version: raw.version,
    lastUpdatedTime: msToDate(raw.lastUpdatedTime),
    createdTime: msToDate(raw.createdTime),
  };
  if (raw.deletedTime !== undefined) {
    (result as { deletedTime?: Date }).deletedTime = msToDate(raw.deletedTime);
  }
  return result;
}

/**
 * Serializes a DataRecordWrite to CDF API format.
 *
 * @param record - The data record write to serialize
 * @returns Object ready for CDF API, or empty object if no existingVersion
 */
export function serializeDataRecordWrite(record: DataRecordWrite): Record<string, number> {
  if (record.existingVersion !== undefined) {
    return { existingVersion: record.existingVersion };
  }
  return {};
}

// ============================================================================
// Base Instance Types
// ============================================================================

/**
 * Base interface for all instance types (both read and write).
 *
 * An instance is a node or edge in the CDF data modeling graph.
 */
export interface InstanceModelBase {
  /** The type of instance: "node" or "edge" */
  readonly instanceType: InstanceType;
  /** The space containing this instance */
  readonly space: string;
  /** The unique identifier within the space */
  readonly externalId: string;
}

/**
 * A read instance with full metadata.
 *
 * This is the base interface for all instance types returned from the API.
 */
export interface Instance extends InstanceModelBase {
  /** System metadata for this instance */
  readonly dataRecord: DataRecord;
}

/**
 * A write instance for creating or updating.
 *
 * This is the base interface for all instance types sent to the API.
 */
export interface InstanceWrite extends InstanceModelBase {
  /** Optional metadata for optimistic locking */
  readonly dataRecord?: DataRecordWrite;
}

/**
 * A node instance (read version).
 */
export interface NodeInstance extends Instance {
  readonly instanceType: "node";
}

/**
 * A node instance for writing.
 */
export interface NodeInstanceWrite extends InstanceWrite {
  readonly instanceType: "node";
}

/**
 * An edge instance (read version).
 */
export interface EdgeInstance extends Instance {
  readonly instanceType: "edge";
  /** Reference to the start node */
  readonly startNode: NodeReference;
  /** Reference to the end node */
  readonly endNode: NodeReference;
}

/**
 * An edge instance for writing.
 */
export interface EdgeInstanceWrite extends InstanceWrite {
  readonly instanceType: "edge";
  /** Reference to the start node */
  readonly startNode: NodeReference;
  /** Reference to the end node */
  readonly endNode: NodeReference;
}

// ============================================================================
// Instance List
// ============================================================================

/**
 * A list of instances with utility methods.
 *
 * Provides array-like behavior with additional methods for common operations
 * like dumping to API format and converting to records.
 *
 * This class wraps an internal array rather than extending Array to avoid
 * issues with TypeScript transpilation and spread operators.
 *
 * @typeParam T - The instance type contained in this list
 */
export class InstanceList<T extends Instance> implements Iterable<T> {
  /** Internal array storage */
  private readonly _items: T[];
  /** The view reference this list is associated with */
  private readonly _viewId?: ViewReference;

  /**
   * Creates a new InstanceList.
   *
   * @param items - Initial items to populate the list
   * @param viewId - Optional view reference for serialization
   */
  constructor(items?: readonly T[], viewId?: ViewReference) {
    this._items = items ? [...items] : [];
    if (viewId !== undefined) {
      this._viewId = viewId;
    }
  }

  /**
   * Gets the number of items in the list.
   */
  get length(): number {
    return this._items.length;
  }

  /**
   * Gets the view reference associated with this list, or undefined if not set.
   */
  get viewId(): ViewReference | undefined {
    return this._viewId;
  }

  /**
   * Gets an item by index.
   */
  at(index: number): T | undefined {
    return this._items[index];
  }

  /**
   * Iterator implementation for for...of loops.
   */
  [Symbol.iterator](): Iterator<T> {
    return this._items[Symbol.iterator]();
  }

  /**
   * Pushes items to the list.
   */
  push(...items: T[]): number {
    return this._items.push(...items);
  }

  /**
   * Creates a new InstanceList from an array.
   *
   * @param items - Array of instances
   * @param viewId - Optional view reference
   * @returns A new InstanceList
   */
  static from<T extends Instance>(items: readonly T[], viewId?: ViewReference): InstanceList<T> {
    return new InstanceList(items, viewId);
  }

  /**
   * Dumps all instances to a list of objects.
   *
   * @param camelCase - Whether to use camelCase keys (default: true)
   * @returns Array of plain objects representing the instances
   */
  dump(camelCase = true): Record<string, unknown>[] {
    return this._items.map((item) => dumpInstance(item, camelCase));
  }

  /**
   * Gets all instance IDs in this list.
   *
   * @returns Array of InstanceId objects
   */
  getIds(): InstanceId[] {
    return this._items.map((item) => ({ space: item.space, externalId: item.externalId }));
  }

  /**
   * Converts the list to a record keyed by external ID.
   *
   * Note: If there are duplicate external IDs across spaces, later items will overwrite earlier ones.
   *
   * @returns Record mapping externalId to instance
   */
  toRecord(): Record<string, T> {
    const result: Record<string, T> = {};
    for (const item of this._items) {
      result[item.externalId] = item;
    }
    return result;
  }

  /**
   * Converts the list to a record keyed by "space:externalId".
   *
   * @returns Record mapping "space:externalId" to instance
   */
  toRecordByFullId(): Record<string, T> {
    const result: Record<string, T> = {};
    for (const item of this._items) {
      result[`${item.space}:${item.externalId}`] = item;
    }
    return result;
  }

  /**
   * Filters instances by space.
   *
   * @param space - The space to filter by
   * @returns A new InstanceList with only instances from the given space
   */
  filterBySpace(space: string): InstanceList<T> {
    return new InstanceList(
      this._items.filter((item) => item.space === space),
      this._viewId
    );
  }

  /**
   * Gets the first instance or undefined if empty.
   */
  first(): T | undefined {
    return this._items[0];
  }

  /**
   * Gets the last instance or undefined if empty.
   */
  last(): T | undefined {
    return this._items[this._items.length - 1];
  }

  /**
   * Converts to a plain array.
   *
   * @returns A new array containing all instances
   */
  toArray(): T[] {
    return [...this._items];
  }
}

// ============================================================================
// Serialization & Deserialization
// ============================================================================

/**
 * Raw instance data from CDF API response.
 */
export interface InstanceRaw {
  readonly instanceType: InstanceType;
  readonly space: string;
  readonly externalId: string;
  readonly version: number;
  readonly lastUpdatedTime: number;
  readonly createdTime: number;
  readonly deletedTime?: number;
  readonly properties?: Record<string, Record<string, Record<string, unknown>>>;
  readonly startNode?: NodeReference;
  readonly endNode?: NodeReference;
}

/**
 * Fields that are part of the data record (not properties).
 */
const DATA_RECORD_FIELDS = new Set([
  "version",
  "lastUpdatedTime",
  "createdTime",
  "deletedTime",
  "existingVersion",
]);

/**
 * Fields that are part of the instance model base (not properties).
 */
const INSTANCE_MODEL_FIELDS = new Set(["instanceType", "space", "externalId"]);

/**
 * Fields that are edge-specific (not properties).
 */
const EDGE_FIELDS = new Set(["startNode", "endNode"]);

function extractViewProperties(
  properties: InstanceRaw["properties"] | undefined,
  viewId?: ViewReference
): Record<string, unknown> {
  if (!properties || !viewId) {
    return {};
  }
  const spaceData = properties[viewId.space];
  if (!spaceData) {
    return {};
  }
  const viewKey = `${viewId.externalId}/${viewId.version}`;
  const viewData = spaceData[viewKey];
  return viewData ? { ...viewData } : {};
}

/**
 * Parses a raw instance from CDF API format.
 *
 * @param raw - Raw instance data from the API
 * @param viewId - The view reference to extract properties from
 * @returns Parsed instance with all properties
 *
 * @typeParam T - The specific instance type to return (for type inference)
 */
// eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters
export function parseInstance<T extends Instance>(raw: InstanceRaw, viewId?: ViewReference): T {
  // Extract data record
  const dataRecord = parseDataRecord({
    version: raw.version,
    lastUpdatedTime: raw.lastUpdatedTime,
    createdTime: raw.createdTime,
    ...(raw.deletedTime !== undefined ? { deletedTime: raw.deletedTime } : {}),
  });

  // Extract properties from nested structure
  const properties = extractViewProperties(raw.properties, viewId);

  // Build the instance
  const instance: Record<string, unknown> = {
    instanceType: raw.instanceType,
    space: raw.space,
    externalId: raw.externalId,
    dataRecord,
    ...properties,
  };

  // Add edge-specific fields
  if (raw.instanceType === "edge") {
    if (!raw.startNode || !raw.endNode) {
      throw new Error("Edge instance is missing startNode or endNode");
    }
    instance.startNode = raw.startNode;
    instance.endNode = raw.endNode;
  }

  return instance as T;
}

/**
 * Parses multiple raw instances from CDF API format.
 *
 * @param items - Array of raw instance data
 * @param viewId - The view reference to extract properties from
 * @returns InstanceList with parsed instances
 */
export function parseInstances<T extends Instance>(
  items: readonly InstanceRaw[],
  viewId?: ViewReference
): InstanceList<T> {
  const parsed = items.map((item) => parseInstance<T>(item, viewId));
  return new InstanceList(parsed, viewId);
}

function collectPropertyValues(
  instance: Instance | InstanceWrite,
  camelCase: boolean
): Record<string, unknown> {
  const propertyValues: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(instance)) {
    if (
      value === undefined ||
      key === "dataRecord" ||
      DATA_RECORD_FIELDS.has(key) ||
      INSTANCE_MODEL_FIELDS.has(key) ||
      EDGE_FIELDS.has(key)
    ) {
      continue;
    }
    const outputKey = camelCase ? key : toSnakeCaseKey(key);
    propertyValues[outputKey] = value instanceof Date ? dateToMs(value) : value;
  }
  return propertyValues;
}

/**
 * Dumps an instance to a plain object.
 *
 * @param instance - The instance to dump
 * @param camelCase - Whether to use camelCase keys (default: true)
 * @returns Plain object representation
 */
export function dumpInstance(
  instance: Instance | InstanceWrite,
  camelCase = true
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  const setKey = (key: string, value: unknown): void => {
    result[camelCase ? key : toSnakeCaseKey(key)] = value;
  };

  setKey("instanceType", instance.instanceType);
  setKey("space", instance.space);
  setKey("externalId", instance.externalId);

  if (instance.dataRecord) {
    for (const [recordKey, recordValue] of Object.entries(instance.dataRecord)) {
      if (recordValue === undefined) {
        continue;
      }
      const normalizedValue = recordValue instanceof Date ? dateToMs(recordValue) : recordValue;
      setKey(recordKey, normalizedValue);
    }
  }

  if (instance.instanceType === "edge") {
    const edgeInstance = instance as EdgeInstance | EdgeInstanceWrite;
    setKey("startNode", edgeInstance.startNode);
    setKey("endNode", edgeInstance.endNode);
  }

  const properties = collectPropertyValues(instance, camelCase);
  Object.assign(result, properties);

  return result;
}

/**
 * Dumps an instance to CDF API format for upsert operations.
 *
 * @param instance - The instance to dump
 * @param viewId - The view reference for property nesting
 * @returns Object in CDF API upsert format
 */
export function dumpInstanceForAPI(
  instance: InstanceWrite,
  viewId: ViewReference
): Record<string, unknown> {
  const result: Record<string, unknown> = {
    instanceType: instance.instanceType,
    space: instance.space,
    externalId: instance.externalId,
  };

  // Add data record fields at top level
  if (instance.dataRecord?.existingVersion !== undefined) {
    result.existingVersion = instance.dataRecord.existingVersion;
  }

  // Add edge-specific fields
  if (instance.instanceType === "edge") {
    const edgeInstance = instance as EdgeInstanceWrite;
    result.startNode = edgeInstance.startNode;
    result.endNode = edgeInstance.endNode;
  }

  // Collect property values
  const propertyValues = collectPropertyValues(instance, true);

  // Nest properties in the CDF format
  if (Object.keys(propertyValues).length > 0) {
    result.sources = [
      {
        source: {
          type: "view",
          space: viewId.space,
          externalId: viewId.externalId,
          version: viewId.version,
        },
        properties: propertyValues,
      },
    ];
  }

  return result;
}

/**
 * Type guard to check if an instance is a node.
 */
export function isNode(instance: Instance): instance is NodeInstance {
  return instance.instanceType === "node";
}

/**
 * Type guard to check if an instance is an edge.
 */
export function isEdge(instance: Instance): instance is EdgeInstance {
  return instance.instanceType === "edge";
}

/**
 * Type guard to check if a write instance is a node.
 */
export function isNodeWrite(instance: InstanceWrite): instance is NodeInstanceWrite {
  return instance.instanceType === "node";
}

/**
 * Type guard to check if a write instance is an edge.
 */
export function isEdgeWrite(instance: InstanceWrite): instance is EdgeInstanceWrite {
  return instance.instanceType === "edge";
}
