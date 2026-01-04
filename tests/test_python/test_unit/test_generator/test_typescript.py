import pytest

from cognite.pygen._client.models import ViewReference
from cognite.pygen._generator.typescript import TypeScriptDataClassGenerator
from cognite.pygen._pygen_model import (
    DataClass,
    DataClassFile,
    Field,
    FilterClass,
    ListDataClass,
)


@pytest.fixture(scope="session")
def ts_data_class_file() -> DataClassFile:
    """TypeScript data class file fixture with camelCase field names and TypeScript types."""
    return DataClassFile(
        filename="exampleView.ts",
        view_id=ViewReference(
            space="example_space",
            external_id="example_view",
            version="v1",
        ),
        instance_type="node",
        read=DataClass(
            name="ExampleView",
            fields=[
                Field(
                    cdf_prop_id="propertyOne",
                    name="propertyOne",
                    type_hint="string | undefined",
                    filter_name="TextFilter",
                    description="The first property.",
                    default_value="undefined",
                    dtype="string",
                ),
                Field(
                    cdf_prop_id="propertyTwo",
                    name="propertyTwo",
                    type_hint="number",
                    filter_name="IntegerFilter",
                    description="The second property.",
                    dtype="number",
                ),
            ],
            display_name="Example View",
            description="An example view for testing.",
        ),
        read_list=ListDataClass(
            name="ExampleViewList",
        ),
        filter=FilterClass(
            name="ExampleViewFilter",
        ),
        write=DataClass(
            name="ExampleViewWrite",
            fields=[
                Field(
                    cdf_prop_id="propertyOne",
                    name="propertyOne",
                    type_hint="string | undefined",
                    filter_name=None,
                    description="The first property.",
                    default_value="undefined",
                    dtype="string",
                ),
                Field(
                    cdf_prop_id="propertyTwo",
                    name="propertyTwo",
                    type_hint="number",
                    filter_name=None,
                    description="The second property.",
                    dtype="number",
                ),
            ],
            display_name="Example View",
            description="An example write view for testing.",
        ),
    )


EXPECTED_TS_IMPORT_STATEMENTS = """/**
 * Data classes for Example View.
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
  IntegerFilter,
  TextFilter,
} from "../instance_api/types/dtypeFilters.ts";"""

EXPECTED_TS_VIEW_CONSTANT = """/** View reference for ExampleView */
export const EXAMPLE_VIEW_VIEW: ViewReference = {
  space: "example_space",
  externalId: "example_view",
  version: "v1",
};"""

EXPECTED_TS_WRITE_INTERFACE = """/**
 * Write class for Example View instances.
 *
 * An example write view for testing.
 */
export interface ExampleViewWrite extends NodeInstanceWrite {
  readonly instanceType: "node";
  /** The first property. */
  propertyOne?: string;
  /** The second property. */
  propertyTwo: number;
}"""

EXPECTED_TS_READ_INTERFACE = """/**
 * Read class for Example View instances.
 *
 * Contains all properties including system metadata.
 */
export interface ExampleView extends NodeInstance {
  readonly instanceType: "node";
  /** The first property. */
  readonly propertyOne?: string;
  /** The second property. */
  readonly propertyTwo: number;
}"""

EXPECTED_TS_AS_WRITE_FUNCTION = """/**
 * Converts a ExampleView read instance to a write instance.
 *
 * @param instance - The ExampleView to convert
 * @returns A ExampleViewWrite instance
 */
export function exampleViewAsWrite(instance: ExampleView): ExampleViewWrite {
  const { dataRecord, ...rest } = instance;
  const write: ExampleViewWrite = {
    ...rest,
  };
  if (dataRecord) {
    (write as { dataRecord?: { existingVersion: number } }).dataRecord = {
      existingVersion: dataRecord.version,
    };
  }
  return write;
}"""

EXPECTED_TS_LIST_CLASS = """/**
 * List of Example View instances.
 */
export class ExampleViewList extends InstanceList<ExampleView> {
  /**
   * Creates a new ExampleViewList.
   *
   * @param items - Initial items to populate the list
   */
  constructor(items?: readonly ExampleView[]) {
    super(items, EXAMPLE_VIEW_VIEW);
  }

  /**
   * Converts all instances in the list to write instances.
   *
   * @returns Array of ExampleViewWrite instances
   */
  asWrite(): ExampleViewWrite[] {
    return this.map(exampleViewAsWrite);
  }

  /**
   * Creates a new ExampleViewList from an array.
   *
   * @param items - Array of ExampleView instances
   * @returns A new ExampleViewList
   */
  static fromArray(items: readonly ExampleView[]): ExampleViewList {
    return new ExampleViewList(items);
  }
}"""

EXPECTED_TS_FILTER_CLASS = """/**
 * Filter container for Example View instances.
 *
 * Provides type-safe filters for all ExampleView properties.
 */
export class ExampleViewFilter extends FilterContainer {
  /** Filter for the propertyOne property */
  readonly propertyOne: TextFilter;
  /** Filter for the propertyTwo property */
  readonly propertyTwo: IntegerFilter;

  /**
   * Creates a new ExampleViewFilter.
   *
   * @param operator - How to combine filters ("and" or "or"). Defaults to "and"
   */
  constructor(operator: "and" | "or" = "and") {
    const propertyOne = new TextFilter(EXAMPLE_VIEW_VIEW, "propertyOne", operator);
    const propertyTwo = new IntegerFilter(EXAMPLE_VIEW_VIEW, "propertyTwo", operator);

    super([propertyOne, propertyTwo], operator, "node");

    this.propertyOne = propertyOne;
    this.propertyTwo = propertyTwo;
  }
}"""


@pytest.fixture(scope="session")
def ts_data_class_generator(ts_data_class_file: DataClassFile) -> TypeScriptDataClassGenerator:
    return TypeScriptDataClassGenerator(ts_data_class_file)


class TestTypeScriptDataClassGenerator:
    def test_create_import_statements(self, ts_data_class_generator: TypeScriptDataClassGenerator) -> None:
        import_statements = ts_data_class_generator.create_import_statements()
        assert import_statements.strip() == EXPECTED_TS_IMPORT_STATEMENTS.strip()

    def test_create_view_reference_constant(self, ts_data_class_generator: TypeScriptDataClassGenerator) -> None:
        view_constant = ts_data_class_generator.create_view_reference_constant()
        assert view_constant.strip() == EXPECTED_TS_VIEW_CONSTANT.strip()

    def test_generate_write_interface(self, ts_data_class_generator: TypeScriptDataClassGenerator) -> None:
        write_interface = ts_data_class_generator.generate_write_interface()
        assert write_interface.strip() == EXPECTED_TS_WRITE_INTERFACE.strip()

    def test_generate_read_interface(self, ts_data_class_generator: TypeScriptDataClassGenerator) -> None:
        read_interface = ts_data_class_generator.generate_read_interface()
        assert read_interface.strip() == EXPECTED_TS_READ_INTERFACE.strip()

    def test_generate_as_write_function(self, ts_data_class_generator: TypeScriptDataClassGenerator) -> None:
        as_write_func = ts_data_class_generator.generate_as_write_function()
        assert as_write_func.strip() == EXPECTED_TS_AS_WRITE_FUNCTION.strip()

    def test_generate_list_class(self, ts_data_class_generator: TypeScriptDataClassGenerator) -> None:
        list_class = ts_data_class_generator.generate_list_class()
        assert list_class.strip() == EXPECTED_TS_LIST_CLASS.strip()

    def test_generate_filter_class(self, ts_data_class_generator: TypeScriptDataClassGenerator) -> None:
        filter_class = ts_data_class_generator.generate_filter_class()
        assert filter_class.strip() == EXPECTED_TS_FILTER_CLASS.strip()


# ============================================================================
# Edge Type Tests
# ============================================================================


@pytest.fixture(scope="session")
def ts_edge_data_class_file() -> DataClassFile:
    """TypeScript edge data class file fixture."""
    return DataClassFile(
        filename="relatesTo.ts",
        view_id=ViewReference(
            space="example_space",
            external_id="RelatesTo",
            version="v1",
        ),
        instance_type="edge",
        read=DataClass(
            name="RelatesTo",
            fields=[
                Field(
                    cdf_prop_id="relationType",
                    name="relationType",
                    type_hint="string",
                    filter_name="TextFilter",
                    description="The type of relation.",
                    dtype="string",
                ),
                Field(
                    cdf_prop_id="strength",
                    name="strength",
                    type_hint="number | undefined",
                    filter_name="FloatFilter",
                    description="The strength of the relation.",
                    default_value="undefined",
                    dtype="number",
                ),
            ],
            display_name="Relates To",
            description="An edge view for testing.",
        ),
        read_list=ListDataClass(
            name="RelatesToList",
        ),
        filter=FilterClass(
            name="RelatesToFilter",
        ),
        write=DataClass(
            name="RelatesToWrite",
            fields=[
                Field(
                    cdf_prop_id="relationType",
                    name="relationType",
                    type_hint="string",
                    filter_name=None,
                    description="The type of relation.",
                    dtype="string",
                ),
                Field(
                    cdf_prop_id="strength",
                    name="strength",
                    type_hint="number | undefined",
                    filter_name=None,
                    description="The strength of the relation.",
                    default_value="undefined",
                    dtype="number",
                ),
            ],
            display_name="Relates To",
            description="An edge write view for testing.",
        ),
    )


EXPECTED_TS_EDGE_IMPORT_STATEMENTS = """/**
 * Data classes for Relates To.
 *
 * @packageDocumentation
 */

import type {
  EdgeInstance, EdgeInstanceWrite,
} from "../instance_api/types/instance.ts";
import { InstanceList } from "../instance_api/types/instance.ts";
import type { NodeReference, ViewReference } from "../instance_api/types/references.ts";
import {
  FilterContainer,
  FloatFilter,
  TextFilter,
} from "../instance_api/types/dtypeFilters.ts";"""

EXPECTED_TS_EDGE_WRITE_INTERFACE = """/**
 * Write class for Relates To instances.
 *
 * An edge write view for testing.
 */
export interface RelatesToWrite extends EdgeInstanceWrite {
  readonly instanceType: "edge";
  startNode: NodeReference;
  endNode: NodeReference;
  /** The type of relation. */
  relationType: string;
  /** The strength of the relation. */
  strength?: number;
}"""

EXPECTED_TS_EDGE_READ_INTERFACE = """/**
 * Read class for Relates To instances.
 *
 * Contains all properties including system metadata.
 */
export interface RelatesTo extends EdgeInstance {
  readonly instanceType: "edge";
  /** The type of relation. */
  readonly relationType: string;
  /** The strength of the relation. */
  readonly strength?: number;
}"""


@pytest.fixture(scope="session")
def ts_edge_data_class_generator(ts_edge_data_class_file: DataClassFile) -> TypeScriptDataClassGenerator:
    return TypeScriptDataClassGenerator(ts_edge_data_class_file)


class TestTypeScriptEdgeDataClassGenerator:
    def test_create_import_statements(self, ts_edge_data_class_generator: TypeScriptDataClassGenerator) -> None:
        import_statements = ts_edge_data_class_generator.create_import_statements()
        assert import_statements.strip() == EXPECTED_TS_EDGE_IMPORT_STATEMENTS.strip()

    def test_generate_write_interface(self, ts_edge_data_class_generator: TypeScriptDataClassGenerator) -> None:
        write_interface = ts_edge_data_class_generator.generate_write_interface()
        assert write_interface.strip() == EXPECTED_TS_EDGE_WRITE_INTERFACE.strip()

    def test_generate_read_interface(self, ts_edge_data_class_generator: TypeScriptDataClassGenerator) -> None:
        read_interface = ts_edge_data_class_generator.generate_read_interface()
        assert read_interface.strip() == EXPECTED_TS_EDGE_READ_INTERFACE.strip()


# ============================================================================
# Direct Relation Tests
# ============================================================================


@pytest.fixture(scope="session")
def ts_direct_relation_data_class_file() -> DataClassFile:
    """TypeScript data class file with direct relation field."""
    return DataClassFile(
        filename="product.ts",
        view_id=ViewReference(
            space="example_space",
            external_id="Product",
            version="v1",
        ),
        instance_type="node",
        read=DataClass(
            name="Product",
            fields=[
                Field(
                    cdf_prop_id="name",
                    name="name",
                    type_hint="string",
                    filter_name="TextFilter",
                    description="The product name.",
                    dtype="string",
                ),
                Field(
                    cdf_prop_id="category",
                    name="category",
                    type_hint="InstanceId | undefined",
                    filter_name="DirectRelationFilter",
                    description="Reference to the category.",
                    default_value="undefined",
                    dtype="InstanceId",
                ),
            ],
            display_name="Product",
            description="A product with category relation.",
        ),
        read_list=ListDataClass(
            name="ProductList",
        ),
        filter=FilterClass(
            name="ProductFilter",
        ),
        write=DataClass(
            name="ProductWrite",
            fields=[
                Field(
                    cdf_prop_id="name",
                    name="name",
                    type_hint="string",
                    filter_name=None,
                    description="The product name.",
                    dtype="string",
                ),
                Field(
                    cdf_prop_id="category",
                    name="category",
                    type_hint="InstanceId | undefined",
                    filter_name=None,
                    description="Reference to the category.",
                    default_value="undefined",
                    dtype="InstanceId",
                ),
            ],
            display_name="Product",
            description="A product write view.",
        ),
    )


EXPECTED_TS_DIRECT_RELATION_IMPORT = """/**
 * Data classes for Product.
 *
 * @packageDocumentation
 */

import type {
  NodeInstance, NodeInstanceWrite,
} from "../instance_api/types/instance.ts";
import { InstanceList } from "../instance_api/types/instance.ts";
import type { InstanceId, ViewReference } from "../instance_api/types/references.ts";
import {
  DirectRelationFilter,
  FilterContainer,
  TextFilter,
} from "../instance_api/types/dtypeFilters.ts";"""

EXPECTED_TS_DIRECT_RELATION_WRITE = """/**
 * Write class for Product instances.
 *
 * A product write view.
 */
export interface ProductWrite extends NodeInstanceWrite {
  readonly instanceType: "node";
  /** The product name. */
  name: string;
  /** Reference to the category. */
  category?: InstanceId | readonly [string, string];
}"""


@pytest.fixture(scope="session")
def ts_direct_relation_generator(ts_direct_relation_data_class_file: DataClassFile) -> TypeScriptDataClassGenerator:
    return TypeScriptDataClassGenerator(ts_direct_relation_data_class_file)


class TestTypeScriptDirectRelationGenerator:
    def test_create_import_statements(self, ts_direct_relation_generator: TypeScriptDataClassGenerator) -> None:
        import_statements = ts_direct_relation_generator.create_import_statements()
        assert import_statements.strip() == EXPECTED_TS_DIRECT_RELATION_IMPORT.strip()

    def test_generate_write_interface_with_tuple(
        self, ts_direct_relation_generator: TypeScriptDataClassGenerator
    ) -> None:
        """Verify that write interface allows tuple format for direct relations."""
        write_interface = ts_direct_relation_generator.generate_write_interface()
        assert write_interface.strip() == EXPECTED_TS_DIRECT_RELATION_WRITE.strip()
