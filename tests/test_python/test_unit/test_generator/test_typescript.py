import pytest

from cognite.pygen._client.models import ViewReference
from cognite.pygen._generator.typescript import (
    TypeScriptAPIGenerator,
    TypeScriptDataClassGenerator,
    TypeScriptGenerator,
    TypeScriptPackageGenerator,
)
from cognite.pygen._pygen_model import (
    APIClassFile,
    DataClass,
    DataClassFile,
    Field,
    FilterClass,
    ListDataClass,
    PygenSDKModel,
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


# ============================================================================
# TypeScriptGenerator Tests
# ============================================================================


@pytest.fixture(scope="session")
def ts_pygen_sdk_model(
    ts_data_class_file: DataClassFile,
    ts_edge_data_class_file: DataClassFile,
) -> PygenSDKModel:
    """Create a PygenSDKModel for testing TypeScriptGenerator."""
    data_classes = [ts_data_class_file, ts_edge_data_class_file]
    api_classes = [
        APIClassFile(
            filename="exampleViewApi.ts",
            name="ExampleViewAPI",
            client_attribute_name="exampleView",
            data_class=ts_data_class_file,
        ),
        APIClassFile(
            filename="relatesToApi.ts",
            name="RelatesToAPI",
            client_attribute_name="relatesTo",
            data_class=ts_edge_data_class_file,
        ),
    ]
    return PygenSDKModel(data_classes=data_classes, api_classes=api_classes)


EXPECTED_TS_DATA_CLASS_INDEX = """/**
 * Data classes for the generated SDK.
 *
 * This module exports all data classes including read, write, list, and filter classes.
 *
 * @packageDocumentation
 */

export {
  EXAMPLE_VIEW_VIEW,
  ExampleViewWrite,
  ExampleView,
  exampleViewAsWrite,
  ExampleViewList,
  ExampleViewFilter,
} from "./exampleView.ts";
export {
  RELATES_TO_VIEW,
  RelatesToWrite,
  RelatesTo,
  relatesToAsWrite,
  RelatesToList,
  RelatesToFilter,
} from "./relatesTo.ts";"""


class TestTypeScriptGenerator:
    def test_create_data_class_index(self, ts_pygen_sdk_model: PygenSDKModel) -> None:
        """Test that create_data_class_index generates correct exports."""
        # Create a mock generator with the model
        # We need to patch the model directly since TypeScriptGenerator requires a DataModelResponse
        generator = TypeScriptGenerator.__new__(TypeScriptGenerator)
        generator.model = ts_pygen_sdk_model

        index_code = generator.create_data_class_index()
        assert index_code.strip() == EXPECTED_TS_DATA_CLASS_INDEX.strip()

    @pytest.mark.parametrize(
        "input_name,expected",
        [
            ("ProductNode", "PRODUCT_NODE_VIEW"),
            ("CategoryNode", "CATEGORY_NODE_VIEW"),
            ("RelatesTo", "RELATES_TO_VIEW"),
            ("SimpleView", "SIMPLE_VIEW_VIEW"),
            ("ABCView", "ABC_VIEW_VIEW"),
        ],
    )
    def test_to_view_const_name(self, input_name: str, expected: str) -> None:
        """Test conversion of PascalCase to UPPER_SNAKE_CASE view constant name."""
        assert TypeScriptGenerator._to_view_const_name(input_name) == expected


# ============================================================================
# API Class Generator Tests
# ============================================================================


@pytest.fixture(scope="session")
def ts_api_class_file(ts_data_class_file: DataClassFile) -> APIClassFile:
    return APIClassFile(
        filename="exampleViewApi.ts",
        name="ExampleViewAPI",
        client_attribute_name="exampleView",
        data_class=ts_data_class_file,
    )


@pytest.fixture(scope="session")
def ts_api_class_generator(ts_api_class_file: APIClassFile) -> TypeScriptAPIGenerator:
    return TypeScriptAPIGenerator(ts_api_class_file)


EXPECTED_TS_API_IMPORT_STATEMENTS = """/**
 * API class for ExampleView instances.
 *
 * @packageDocumentation
 */

import { InstanceAPI } from "../instance_api/api.ts";
import type { PygenClientConfig } from "../instance_api/auth/index.ts";
import type { ViewReference } from "../instance_api/types/references.ts";
import type { Aggregation, PropertySort, SortDirection } from "../instance_api/types/query.ts";
import type { AggregateResponse, Page } from "../instance_api/types/responses.ts";
import { InstanceList } from "../instance_api/types/instance.ts";

import {
  EXAMPLE_VIEW_VIEW,
  ExampleView,
  ExampleViewFilter,
  ExampleViewList,
} from "../data_classes/index.ts";"""

EXPECTED_TS_API_HELPER_FUNCTION = """
/**
 * Creates a property reference for sorting.
 *
 * @param viewRef - The view reference
 * @param propertyName - The property name
 * @returns A property path array
 */
function createPropertyRef(
  viewRef: ViewReference,
  propertyName: string,
): [string, string, string] {
  return [viewRef.space, `${viewRef.externalId}/${viewRef.version}`, propertyName];
}"""

EXPECTED_TS_API_CLASS = """
/**
 * API for ExampleView instances with type-safe filter methods.
 */
export class ExampleViewAPI extends InstanceAPI<ExampleView> {
  /**
   * Creates a new ExampleViewAPI.
   *
   * @param config - Client configuration for API access
   */
  constructor(config: PygenClientConfig) {
    super(config, EXAMPLE_VIEW_VIEW, "node");
  }

  /**
   * Builds a ExampleViewFilter from the given options.
   *
   * @param options - Filter options
   * @returns A configured ExampleViewFilter
   */
  private _buildFilter(options: {
    propertyOne?: string | readonly string[];
    propertyOnePrefix?: string;
    minPropertyTwo?: number;
    maxPropertyTwo?: number;
    externalIdPrefix?: string;
    space?: string | readonly string[];
  }): ExampleViewFilter {
    const filter = new ExampleViewFilter("and");
    filter.propertyOne.equalsOrIn(options.propertyOne ?? null);
    filter.propertyOne.prefix(options.propertyOnePrefix ?? null);
    filter.propertyTwo.greaterThanOrEquals(options.minPropertyTwo ?? null);
    filter.propertyTwo.lessThanOrEquals(options.maxPropertyTwo ?? null);
    filter.externalId.prefix(options.externalIdPrefix ?? null);
    filter.space.equalsOrIn(options.space ?? null);

    return filter;
  }

  /**
   * Retrieve ExampleView instances by ID.
   *
   * @param id - Instance identifier. Can be a string, InstanceId, tuple, or array
   * @param options - Additional options
   * @returns For single id: The ExampleView if found, undefined otherwise.
   *          For array of ids: A ExampleViewList of found instances.
   */
  async retrieve(
    id: string | InstanceId | readonly [string, string],
    options?: { space?: string },
  ): Promise<ExampleView | undefined>;
  async retrieve(
    id: readonly (string | InstanceId | readonly [string, string])[],
    options?: { space?: string },
  ): Promise<ExampleViewList>;
  async retrieve(
    id:
      | string
      | InstanceId
      | readonly [string, string]
      | readonly (string | InstanceId | readonly [string, string])[],
    options: { space?: string } = {},
  ): Promise<ExampleView | ExampleViewList | undefined> {
    const isSingle = !Array.isArray(id) ||
      (id.length === 2 && typeof id[0] === "string" && typeof id[1] === "string");

    if (isSingle) {
      return await this._retrieve(
        id as string | InstanceId | readonly [string, string],
        options,
      ) as ExampleView | undefined;
    }
    const result = await this._retrieve(
      id as readonly (string | InstanceId | readonly [string, string])[],
      options,
    );
    return new ExampleViewList([...(result as InstanceList<ExampleView>)]);
  }

  /**
   * Iterate over instances with pagination.
   *
   * @param options - Filter and pagination options
   * @returns A Page containing items and optional next cursor.
   */
  async iterate(options: {
    propertyOne?: string | readonly string[];
    propertyOnePrefix?: string;
    minPropertyTwo?: number;
    maxPropertyTwo?: number;
    externalIdPrefix?: string;
    space?: string | readonly string[];
    cursor?: string;
    limit?: number;
  } = {}): Promise<Page<ExampleViewList>> {
    const filter = this._buildFilter(options);
    const page = await this._iterate({
      cursor: options.cursor,
      limit: options.limit,
      filter: filter.asFilter(),
    });

    return { ...page, items: new ExampleViewList([...page.items]) };
  }

  /**
   * Search instances using full-text search.
   *
   * @param options - Search and filter options
   * @returns A ExampleViewList of matching instances.
   */
  async search(options: {
    query?: string;
    properties?: string | readonly string[];
    propertyOne?: string | readonly string[];
    propertyOnePrefix?: string;
    minPropertyTwo?: number;
    maxPropertyTwo?: number;
    externalIdPrefix?: string;
    space?: string | readonly string[];
    limit?: number;
  } = {}): Promise<ExampleViewList> {
    const filter = this._buildFilter(options);

    const result = await this._search({
      query: options.query,
      properties: options.properties,
      limit: options.limit,
      filter: filter.asFilter(),
    });

    return new ExampleViewList([...result.items]);
  }

  /**
   * Aggregate instances.
   *
   * @param aggregate - Aggregation(s) to perform.
   * @param options - Filter and grouping options.
   * @returns AggregateResponse with aggregated values.
   */
  async aggregate(
    aggregate: Aggregation | readonly Aggregation[],
    options: {
      groupBy?: string | readonly string[];
    propertyOne?: string | readonly string[];
    propertyOnePrefix?: string;
    minPropertyTwo?: number;
    maxPropertyTwo?: number;
    externalIdPrefix?: string;
    space?: string | readonly string[];
    } = {},
  ): Promise<AggregateResponse> {
    const filter = this._buildFilter(options);

    return this._aggregate(aggregate, {
      groupBy: options.groupBy,
      filter: filter.asFilter(),
    });
  }

  /**
   * List instances with type-safe filtering.
   *
   * @param options - Filter, sort, and pagination options.
   * @returns A ExampleViewList of matching instances.
   */
  async list(options: {
    propertyOne?: string | readonly string[];
    propertyOnePrefix?: string;
    minPropertyTwo?: number;
    maxPropertyTwo?: number;
    externalIdPrefix?: string;
    space?: string | readonly string[];
    sortBy?: string;
    sortDirection?: SortDirection;
    limit?: number;
  } = {}): Promise<ExampleViewList> {
    const filter = this._buildFilter(options);
    const sort: PropertySort | undefined = options.sortBy !== undefined
      ? {
        property: createPropertyRef(EXAMPLE_VIEW_VIEW, options.sortBy),
        direction: options.sortDirection,
      }
      : undefined;

    const result = await this._list({
      limit: options.limit,
      filter: filter.asFilter(),
      sort,
    });

    return new ExampleViewList([...result]);
  }
}"""


class TestTypeScriptAPIGenerator:
    def test_create_import_statements(self, ts_api_class_generator: TypeScriptAPIGenerator) -> None:
        import_statements = ts_api_class_generator.create_import_statements()
        assert import_statements.strip() == EXPECTED_TS_API_IMPORT_STATEMENTS.strip()

    def test_create_helper_function(self, ts_api_class_generator: TypeScriptAPIGenerator) -> None:
        helper_function = ts_api_class_generator.create_helper_function()
        assert helper_function.strip() == EXPECTED_TS_API_HELPER_FUNCTION.strip()

    def test_create_api_class(self, ts_api_class_generator: TypeScriptAPIGenerator) -> None:
        api_class = ts_api_class_generator.create_api_class()
        assert api_class.strip() == EXPECTED_TS_API_CLASS.strip()


# ============================================================================
# Package Generator Tests
# ============================================================================


@pytest.fixture(scope="session")
def ts_package_generator(ts_pygen_sdk_model: PygenSDKModel) -> TypeScriptPackageGenerator:
    return TypeScriptPackageGenerator(ts_pygen_sdk_model, "MyClient")


EXPECTED_TS_API_INDEX = """/**
 * API classes for the generated SDK.
 *
 * This module exports all view-specific API classes.
 *
 * @packageDocumentation
 */

export { ExampleViewAPI } from "./exampleViewApi.ts";
export { RelatesToAPI } from "./relatesToApi.ts";"""

EXPECTED_TS_CLIENT_CODE = """/**
 * Client for the generated SDK.
 *
 * This module contains the MyClient that composes view-specific APIs.
 *
 * @packageDocumentation
 */

import type { PygenClientConfig } from "./instance_api/auth/index.ts";
import { InstanceClient } from "./instance_api/client.ts";

import { ExampleViewAPI, RelatesToAPI } from "./_api/index.ts";

/**
 * Generated client for interacting with the data model.
 *
 * This client provides access to the following views:
 * - exampleView: ExampleViewAPI
 * - relatesTo: RelatesToAPI
 */
export class MyClient extends InstanceClient {
  /** API for ExampleView instances */
  readonly exampleView: ExampleViewAPI;
  /** API for RelatesTo instances */
  readonly relatesTo: RelatesToAPI;

  /**
   * Creates a new MyClient.
   *
   * @param config - Configuration for the client including URL, project, and credentials.
   * @param writeWorkers - Number of concurrent workers for write operations. Default is 5.
   * @param deleteWorkers - Number of concurrent workers for delete operations. Default is 3.
   * @param retrieveWorkers - Number of concurrent workers for retrieve operations. Default is 10.
   */
  constructor(
    config: PygenClientConfig,
    writeWorkers = 5,
    deleteWorkers = 3,
    retrieveWorkers = 10,
  ) {
    super(config, writeWorkers, deleteWorkers, retrieveWorkers);

    // Initialize view-specific APIs
    this.exampleView = new ExampleViewAPI(config);
    this.relatesTo = new RelatesToAPI(config);
  }
}"""

EXPECTED_TS_PACKAGE_INDEX = """/**
 * Generated SDK package.
 *
 * This package provides the MyClient for interacting with the data model.
 *
 * @packageDocumentation
 */

export { MyClient } from "./_client.ts";
export * from "./data_classes/index.ts";
"""


class TestTypeScriptPackageGenerator:
    def test_create_api_index(self, ts_package_generator: TypeScriptPackageGenerator) -> None:
        api_index = ts_package_generator.create_api_index()
        assert api_index.strip() == EXPECTED_TS_API_INDEX.strip()

    def test_create_client(self, ts_package_generator: TypeScriptPackageGenerator) -> None:
        client_code = ts_package_generator.create_client()
        assert client_code.strip() == EXPECTED_TS_CLIENT_CODE.strip()

    def test_create_package_index(self, ts_package_generator: TypeScriptPackageGenerator) -> None:
        package_index = ts_package_generator.create_package_index()
        assert package_index.strip() == EXPECTED_TS_PACKAGE_INDEX.strip()
