/**
 * Data classes for RelatesTo Edge.
 *
 * @packageDocumentation
 */

import type {
  EdgeInstance, EdgeInstanceWrite,
} from "../instance_api/types/instance.ts";
import { InstanceList } from "../instance_api/types/instance.ts";
import type { NodeReference, ViewReference } from "../instance_api/types/references.ts";
import {
  DateTimeFilter,
  FilterContainer,
  FloatFilter,
  TextFilter,
} from "../instance_api/types/dtypeFilters.ts";

/** View reference for RelatesTo */
export const RELATES_TO_VIEW: ViewReference = {
  space: "pygen_example",
  externalId: "RelatesTo",
  version: "v1",
};

/**
 * Write class for RelatesTo Edge instances.
 *
 * Edge view for relating products or other nodes
 */
export interface RelatesToWrite extends EdgeInstanceWrite {
  readonly instanceType: "edge";
  startNode: NodeReference;
  endNode: NodeReference;
  relationType: string;
  strength: number;
  createdAt: Date;
}

/**
 * Read class for RelatesTo Edge instances.
 *
 * Contains all properties including system metadata.
 */
export interface RelatesTo extends EdgeInstance {
  readonly instanceType: "edge";
  readonly relationType: string;
  readonly strength: number;
  readonly createdAt: Date;
}

/**
 * Converts a RelatesTo read instance to a write instance.
 *
 * @param instance - The RelatesTo to convert
 * @returns A RelatesToWrite instance
 */
export function relatesToAsWrite(instance: RelatesTo): RelatesToWrite {
  const { dataRecord, ...rest } = instance;
  const write: RelatesToWrite = {
    ...rest,
  };
  if (dataRecord) {
    (write as { dataRecord?: { existingVersion: number } }).dataRecord = {
      existingVersion: dataRecord.version,
    };
  }
  return write;
}

/**
 * List of RelatesTo Edge instances.
 */
export class RelatesToList extends InstanceList<RelatesTo> {
  /**
   * Creates a new RelatesToList.
   *
   * @param items - Initial items to populate the list
   */
  constructor(items?: readonly RelatesTo[]) {
    super(items, RELATES_TO_VIEW);
  }

  /**
   * Converts all instances in the list to write instances.
   *
   * @returns Array of RelatesToWrite instances
   */
  asWrite(): RelatesToWrite[] {
    return this.map(relatesToAsWrite);
  }

  /**
   * Creates a new RelatesToList from an array.
   *
   * @param items - Array of RelatesTo instances
   * @returns A new RelatesToList
   */
  static fromArray(items: readonly RelatesTo[]): RelatesToList {
    return new RelatesToList(items);
  }
}

/**
 * Filter container for RelatesTo Edge instances.
 *
 * Provides type-safe filters for all RelatesTo properties.
 */
export class RelatesToFilter extends FilterContainer {
  /** Filter for the relationType property */
  readonly relationType: TextFilter;
  /** Filter for the strength property */
  readonly strength: FloatFilter;
  /** Filter for the createdAt property */
  readonly createdAt: DateTimeFilter;

  /**
   * Creates a new RelatesToFilter.
   *
   * @param operator - How to combine filters ("and" or "or"). Defaults to "and"
   */
  constructor(operator: "and" | "or" = "and") {
    const relationType = new TextFilter(RELATES_TO_VIEW, "relation_type", operator);
    const strength = new FloatFilter(RELATES_TO_VIEW, "strength", operator);
    const createdAt = new DateTimeFilter(RELATES_TO_VIEW, "created_at", operator);

    super([relationType, strength, createdAt], operator, "edge");

    this.relationType = relationType;
    this.strength = strength;
    this.createdAt = createdAt;
  }
}