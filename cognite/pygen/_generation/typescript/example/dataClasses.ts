/**
 * Data classes for the example SDK.
 *
 * This module contains the data classes for the example data model with:
 * - ProductNode: Node view with various property types and a direct relation to CategoryNode
 * - CategoryNode: Node view with a reverse direct relation to ProductNode
 * - RelatesTo: Edge view for relating nodes
 *
 * @packageDocumentation
 */

import type {
  EdgeInstance,
  EdgeInstanceWrite,
  NodeInstance,
  NodeInstanceWrite,
} from "../instance_api/types/instance.ts";
import { InstanceList } from "../instance_api/types/instance.ts";
import type { InstanceId, NodeReference, ViewReference } from "../instance_api/types/references.ts";
import {
  BooleanFilter,
  DateFilter,
  DateTimeFilter,
  DirectRelationFilter,
  FilterContainer,
  FloatFilter,
  IntegerFilter,
  TextFilter,
} from "../instance_api/types/dtypeFilters.ts";

// ============================================================================
// Constants
// ============================================================================

/** The space for the example data model */
export const EXAMPLE_SPACE = "pygen_example";

/** The version for the example data model */
export const EXAMPLE_VERSION = "v1";

// ============================================================================
// View References
// ============================================================================

/** View reference for ProductNode */
export const PRODUCT_NODE_VIEW: ViewReference = {
  space: EXAMPLE_SPACE,
  externalId: "ProductNode",
  version: EXAMPLE_VERSION,
};

/** View reference for CategoryNode */
export const CATEGORY_NODE_VIEW: ViewReference = {
  space: EXAMPLE_SPACE,
  externalId: "CategoryNode",
  version: EXAMPLE_VERSION,
};

/** View reference for RelatesTo */
export const RELATES_TO_VIEW: ViewReference = {
  space: EXAMPLE_SPACE,
  externalId: "RelatesTo",
  version: EXAMPLE_VERSION,
};

// ============================================================================
// ProductNode
// ============================================================================

/**
 * Write class for ProductNode instances.
 *
 * Contains all required and optional properties for creating or updating a ProductNode.
 */
export interface ProductNodeWrite extends NodeInstanceWrite {
  readonly instanceType: "node";

  // Required properties
  /** The name of the product */
  readonly name: string;
  /** The price of the product */
  readonly price: number;
  /** The quantity in stock */
  readonly quantity: number;
  /** When the product was created (as Date) */
  readonly createdDate: Date;

  // Optional properties
  /** Optional description of the product */
  readonly description?: string;
  /** Optional list of tags */
  readonly tags?: readonly string[];
  /** Optional historical prices */
  readonly prices?: readonly number[];
  /** Optional historical quantities */
  readonly quantities?: readonly number[];
  /** Whether the product is active */
  readonly active?: boolean;
  /** When the product was last updated */
  readonly updatedTimestamp?: Date;
  /** Reference to the category */
  readonly category?: InstanceId | readonly [string, string];
}

/**
 * Read class for ProductNode instances.
 *
 * Contains all properties including system metadata.
 */
export interface ProductNode extends NodeInstance {
  readonly instanceType: "node";

  // Required properties
  /** The name of the product */
  readonly name: string;
  /** The price of the product */
  readonly price: number;
  /** The quantity in stock */
  readonly quantity: number;
  /** When the product was created (as Date) */
  readonly createdDate: Date;

  // Optional properties
  /** Optional description of the product */
  readonly description?: string;
  /** Optional list of tags */
  readonly tags?: readonly string[];
  /** Optional historical prices */
  readonly prices?: readonly number[];
  /** Optional historical quantities */
  readonly quantities?: readonly number[];
  /** Whether the product is active */
  readonly active?: boolean;
  /** When the product was last updated */
  readonly updatedTimestamp?: Date;
  /** Reference to the category */
  readonly category?: InstanceId;
}

/**
 * Converts a ProductNode read instance to a write instance.
 *
 * @param node - The ProductNode to convert
 * @returns A ProductNodeWrite instance
 */
export function productNodeAsWrite(node: ProductNode): ProductNodeWrite {
  const result: ProductNodeWrite = {
    instanceType: node.instanceType,
    space: node.space,
    externalId: node.externalId,
    name: node.name,
    price: node.price,
    quantity: node.quantity,
    createdDate: node.createdDate,
  };

  if (node.dataRecord) {
    (result as { dataRecord?: { existingVersion: number } }).dataRecord = {
      existingVersion: node.dataRecord.version,
    };
  }
  if (node.description !== undefined) {
    (result as { description?: string }).description = node.description;
  }
  if (node.tags !== undefined) {
    (result as { tags?: readonly string[] }).tags = node.tags;
  }
  if (node.prices !== undefined) {
    (result as { prices?: readonly number[] }).prices = node.prices;
  }
  if (node.quantities !== undefined) {
    (result as { quantities?: readonly number[] }).quantities = node.quantities;
  }
  if (node.active !== undefined) {
    (result as { active?: boolean }).active = node.active;
  }
  if (node.updatedTimestamp !== undefined) {
    (result as { updatedTimestamp?: Date }).updatedTimestamp = node.updatedTimestamp;
  }
  if (node.category !== undefined) {
    (result as { category?: InstanceId }).category = node.category;
  }

  return result;
}

/**
 * List of ProductNode instances.
 */
export class ProductNodeList extends InstanceList<ProductNode> {
  /**
   * Creates a new ProductNodeList.
   *
   * @param items - Initial items to populate the list
   */
  constructor(items?: readonly ProductNode[]) {
    super(items, PRODUCT_NODE_VIEW);
  }

  /**
   * Converts all nodes in the list to write instances.
   *
   * @returns Array of ProductNodeWrite instances
   */
  asWrite(): ProductNodeWrite[] {
    return this.map(productNodeAsWrite);
  }

  /**
   * Creates a new ProductNodeList from an array.
   *
   * @param items - Array of ProductNode instances
   * @returns A new ProductNodeList
   */
  static fromArray(items: readonly ProductNode[]): ProductNodeList {
    return new ProductNodeList(items);
  }
}

/**
 * Filter container for ProductNode instances.
 *
 * Provides type-safe filters for all ProductNode properties.
 */
export class ProductNodeFilter extends FilterContainer {
  /** Filter for the name property */
  readonly name: TextFilter;
  /** Filter for the description property */
  readonly description: TextFilter;
  /** Filter for the price property */
  readonly price: FloatFilter;
  /** Filter for the quantity property */
  readonly quantity: IntegerFilter;
  /** Filter for the active property */
  readonly active: BooleanFilter;
  /** Filter for the createdDate property */
  readonly createdDate: DateFilter;
  /** Filter for the category property */
  readonly category: DirectRelationFilter;

  /**
   * Creates a new ProductNodeFilter.
   *
   * @param operator - How to combine filters ("and" or "or"). Defaults to "and"
   */
  constructor(operator: "and" | "or" = "and") {
    const dataTypeFilters: (
      | TextFilter
      | FloatFilter
      | IntegerFilter
      | BooleanFilter
      | DateFilter
      | DirectRelationFilter
    )[] = [];

    super(dataTypeFilters, operator, "node");

    this.name = new TextFilter(PRODUCT_NODE_VIEW, "name", operator);
    this.description = new TextFilter(PRODUCT_NODE_VIEW, "description", operator);
    this.price = new FloatFilter(PRODUCT_NODE_VIEW, "price", operator);
    this.quantity = new IntegerFilter(PRODUCT_NODE_VIEW, "quantity", operator);
    this.active = new BooleanFilter(PRODUCT_NODE_VIEW, "active", operator);
    this.createdDate = new DateFilter(PRODUCT_NODE_VIEW, "createdDate", operator);
    this.category = new DirectRelationFilter(PRODUCT_NODE_VIEW, "category", operator);

    dataTypeFilters.push(
      this.name,
      this.description,
      this.price,
      this.quantity,
      this.active,
      this.createdDate,
      this.category,
    );
  }
}

// ============================================================================
// CategoryNode
// ============================================================================

/**
 * Write class for CategoryNode instances.
 */
export interface CategoryNodeWrite extends NodeInstanceWrite {
  readonly instanceType: "node";
  /** The name of the category */
  readonly categoryName: string;
}

/**
 * Read class for CategoryNode instances.
 */
export interface CategoryNode extends NodeInstance {
  readonly instanceType: "node";
  /** The name of the category */
  readonly categoryName: string;
}

/**
 * Converts a CategoryNode read instance to a write instance.
 *
 * @param node - The CategoryNode to convert
 * @returns A CategoryNodeWrite instance
 */
export function categoryNodeAsWrite(node: CategoryNode): CategoryNodeWrite {
  const result: CategoryNodeWrite = {
    instanceType: node.instanceType,
    space: node.space,
    externalId: node.externalId,
    categoryName: node.categoryName,
  };

  if (node.dataRecord) {
    (result as { dataRecord?: { existingVersion: number } }).dataRecord = {
      existingVersion: node.dataRecord.version,
    };
  }

  return result;
}

/**
 * List of CategoryNode instances.
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
   * Converts all nodes in the list to write instances.
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
 * Filter container for CategoryNode instances.
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
    const dataTypeFilters: TextFilter[] = [];

    super(dataTypeFilters, operator, "node");

    this.categoryName = new TextFilter(CATEGORY_NODE_VIEW, "categoryName", operator);

    dataTypeFilters.push(this.categoryName);
  }
}

// ============================================================================
// RelatesTo
// ============================================================================

/**
 * Write class for RelatesTo edge instances.
 */
export interface RelatesToWrite extends EdgeInstanceWrite {
  readonly instanceType: "edge";
  readonly startNode: NodeReference;
  readonly endNode: NodeReference;
  /** The type of relation */
  readonly relationType: string;
  /** The strength of the relation */
  readonly strength?: number;
  /** When the relation was created */
  readonly createdAt: Date;
}

/**
 * Read class for RelatesTo edge instances.
 */
export interface RelatesTo extends EdgeInstance {
  readonly instanceType: "edge";
  /** The type of relation */
  readonly relationType: string;
  /** The strength of the relation */
  readonly strength?: number;
  /** When the relation was created */
  readonly createdAt: Date;
}

/**
 * Converts a RelatesTo read instance to a write instance.
 *
 * @param edge - The RelatesTo to convert
 * @returns A RelatesToWrite instance
 */
export function relatesToAsWrite(edge: RelatesTo): RelatesToWrite {
  const result: RelatesToWrite = {
    instanceType: edge.instanceType,
    space: edge.space,
    externalId: edge.externalId,
    startNode: edge.startNode,
    endNode: edge.endNode,
    relationType: edge.relationType,
    createdAt: edge.createdAt,
  };

  if (edge.dataRecord) {
    (result as { dataRecord?: { existingVersion: number } }).dataRecord = {
      existingVersion: edge.dataRecord.version,
    };
  }
  if (edge.strength !== undefined) {
    (result as { strength?: number }).strength = edge.strength;
  }

  return result;
}

/**
 * List of RelatesTo edge instances.
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
   * Converts all edges in the list to write instances.
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
 * Filter container for RelatesTo edge instances.
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
    const dataTypeFilters: (TextFilter | FloatFilter | DateTimeFilter)[] = [];

    super(dataTypeFilters, operator, "edge");

    this.relationType = new TextFilter(RELATES_TO_VIEW, "relationType", operator);
    this.strength = new FloatFilter(RELATES_TO_VIEW, "strength", operator);
    this.createdAt = new DateTimeFilter(RELATES_TO_VIEW, "createdAt", operator);

    dataTypeFilters.push(this.relationType, this.strength, this.createdAt);
  }
}

