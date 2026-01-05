/**
 * Data classes for Product Node.
 *
 * @packageDocumentation
 */

import type {
  NodeInstance, NodeInstanceWrite,
} from "../instance_api/types/instance.ts";
import { InstanceList } from "../instance_api/types/instance.ts";
import type { InstanceId, ViewReference } from "../instance_api/types/references.ts";
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

/** View reference for ProductNode */
export const PRODUCT_NODE_VIEW: ViewReference = {
  space: "pygen_example",
  externalId: "ProductNode",
  version: "v1",
};

/**
 * Write class for Product Node instances.
 *
 * View for product nodes with various data types and relations
 */
export interface ProductNodeWrite extends NodeInstanceWrite {
  readonly instanceType: "node";
  name: string;
  description: string;
  tags: readonly string[];
  price: number;
  prices: readonly number[];
  quantity: number;
  quantities: readonly number[];
  active: boolean;
  createdDate: Date;
  updatedTimestamp: Date;
  category: InstanceId | readonly [string, string];
}

/**
 * Read class for Product Node instances.
 *
 * Contains all properties including system metadata.
 */
export interface ProductNode extends NodeInstance {
  readonly instanceType: "node";
  readonly name: string;
  readonly description: string;
  readonly tags: readonly string[];
  readonly price: number;
  readonly prices: readonly number[];
  readonly quantity: number;
  readonly quantities: readonly number[];
  readonly active: boolean;
  readonly createdDate: Date;
  readonly updatedTimestamp: Date;
  readonly category: InstanceId;
}

/**
 * Converts a ProductNode read instance to a write instance.
 *
 * @param instance - The ProductNode to convert
 * @returns A ProductNodeWrite instance
 */
export function productNodeAsWrite(instance: ProductNode): ProductNodeWrite {
  const { dataRecord, ...rest } = instance;
  const write: ProductNodeWrite = {
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
 * List of Product Node instances.
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
   * Converts all instances in the list to write instances.
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
 * Filter container for Product Node instances.
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
  /** Filter for the updatedTimestamp property */
  readonly updatedTimestamp: DateTimeFilter;
  /** Filter for the category property */
  readonly category: DirectRelationFilter;

  /**
   * Creates a new ProductNodeFilter.
   *
   * @param operator - How to combine filters ("and" or "or"). Defaults to "and"
   */
  constructor(operator: "and" | "or" = "and") {
    const name = new TextFilter(PRODUCT_NODE_VIEW, "name", operator);
    const description = new TextFilter(PRODUCT_NODE_VIEW, "description", operator);
    const price = new FloatFilter(PRODUCT_NODE_VIEW, "price", operator);
    const quantity = new IntegerFilter(PRODUCT_NODE_VIEW, "quantity", operator);
    const active = new BooleanFilter(PRODUCT_NODE_VIEW, "active", operator);
    const createdDate = new DateFilter(PRODUCT_NODE_VIEW, "created_date", operator);
    const updatedTimestamp = new DateTimeFilter(PRODUCT_NODE_VIEW, "updated_timestamp", operator);
    const category = new DirectRelationFilter(PRODUCT_NODE_VIEW, "category", operator);

    super([name, description, price, quantity, active, createdDate, updatedTimestamp, category], operator, "node");

    this.name = name;
    this.description = description;
    this.price = price;
    this.quantity = quantity;
    this.active = active;
    this.createdDate = createdDate;
    this.updatedTimestamp = updatedTimestamp;
    this.category = category;
  }
}