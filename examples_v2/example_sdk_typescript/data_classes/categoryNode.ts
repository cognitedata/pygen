/**
 * Data classes for Category Node.
 *
 * @packageDocumentation
 */

import type {
  NodeInstance, NodeInstanceWrite,
} from "../instance_api/types/instance.ts";
import { InstanceList } from "../instance_api/types/instance.ts";
import type { ViewReference } from "../instance_api/types/references.ts";
import {
  FilterContainer,
  TextFilter,
} from "../instance_api/types/dtypeFilters.ts";

/** View reference for CategoryNode */
export const CATEGORY_NODE_VIEW: ViewReference = {
  space: "pygen_example",
  externalId: "CategoryNode",
  version: "v1",
};

/**
 * Write class for Category Node instances.
 *
 * View for category nodes with reverse direct relation
 */
export interface CategoryNodeWrite extends NodeInstanceWrite {
  readonly instanceType: "node";
  categoryName: string;
}

/**
 * Read class for Category Node instances.
 *
 * Contains all properties including system metadata.
 */
export interface CategoryNode extends NodeInstance {
  readonly instanceType: "node";
  readonly categoryName: string;
}

/**
 * Converts a CategoryNode read instance to a write instance.
 *
 * @param instance - The CategoryNode to convert
 * @returns A CategoryNodeWrite instance
 */
export function categoryNodeAsWrite(instance: CategoryNode): CategoryNodeWrite {
  const { dataRecord, ...rest } = instance;
  const write: CategoryNodeWrite = {
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
 * List of Category Node instances.
 */
export class CategoryNodeList extends InstanceList<CategoryNode> {
  /**
   * Creates a new CategoryNodeList.
   *
   * @param items - Initial items to populate the list
   */
  constructor(items?: readonly CategoryNode[]) {
    super(items, CATEGORY_NODE_VIEW);
  }

  /**
   * Converts all instances in the list to write instances.
   *
   * @returns Array of CategoryNodeWrite instances
   */
  asWrite(): CategoryNodeWrite[] {
    return this.map(categoryNodeAsWrite);
  }

  /**
   * Creates a new CategoryNodeList from an array.
   *
   * @param items - Array of CategoryNode instances
   * @returns A new CategoryNodeList
   */
  static fromArray(items: readonly CategoryNode[]): CategoryNodeList {
    return new CategoryNodeList(items);
  }
}

/**
 * Filter container for Category Node instances.
 *
 * Provides type-safe filters for all CategoryNode properties.
 */
export class CategoryNodeFilter extends FilterContainer {
  /** Filter for the categoryName property */
  readonly categoryName: TextFilter;

  /**
   * Creates a new CategoryNodeFilter.
   *
   * @param operator - How to combine filters ("and" or "or"). Defaults to "and"
   */
  constructor(operator: "and" | "or" = "and") {
    const categoryName = new TextFilter(CATEGORY_NODE_VIEW, "category_name", operator);

    super([categoryName], operator, "node");

    this.categoryName = categoryName;
  }
}