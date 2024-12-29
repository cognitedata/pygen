from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import DataModelId

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_MODELS = REPO_ROOT / "tests" / "data" / "models"
DATA_WRITE_DIR = REPO_ROOT / "tests" / "data" / "write"
JSON_DIR = REPO_ROOT / "tests" / "data" / "json"
EXAMPLES_DIR = REPO_ROOT / "examples"


@dataclass
class ExampleSDK:
    data_model_ids: list[DataModelId]
    client_name: str
    _top_level_package: str
    generate_sdk: bool
    instance_space: str | None
    download_nodes: bool = False
    is_typed: bool = False
    dataset_external_id: str | None = None
    typed_classes: set[str] = field(default_factory=set)
    manual_files: list[Path] = field(default_factory=list, init=False)

    @property
    def top_level_package(self) -> str:
        return self._top_level_package

    @property
    def client_dir(self) -> Path:
        return EXAMPLES_DIR / self.top_level_package.replace(".", "/")

    @staticmethod
    def model_dir(model_id: DataModelId) -> Path:
        return DATA_MODELS / model_id.external_id

    @classmethod
    def read_model_path(cls, model_id: DataModelId) -> Path:
        return cls.model_dir(model_id) / "model.yaml"

    @classmethod
    def read_node_path(cls, model_id: DataModelId) -> Path:
        return cls.model_dir(model_id) / "nodes.yaml"

    def load_data_models(self) -> dm.DataModelList[dm.View]:
        models = []
        for model_id in self.data_model_ids:
            data_model_file = self.read_model_path(model_id)

            if not data_model_file.is_file():
                raise FileNotFoundError(f"Data model file {data_model_file} not found")
            data_model = dm.DataModel[dm.View].load(data_model_file.read_text())
            models.append(data_model)
        return dm.DataModelList[dm.View](models)

    def load_data_model(self) -> dm.DataModel[dm.View]:
        models = self.load_data_models()
        if len(models) == 1:
            return models[0]
        raise ValueError(f"Expected exactly one data model, got {len(models)}")

    def load_read_nodes(self, data_model_id: dm.DataModelId) -> dm.NodeList:
        return dm.NodeList.load(self.read_node_path(data_model_id).read_text())


OMNI_SDK = ExampleSDK(
    data_model_ids=[DataModelId("sp_pygen_models", "Omni", "1")],
    _top_level_package="omni",
    client_name="OmniClient",
    generate_sdk=True,
    instance_space="sp_omni_instances",
    download_nodes=True,
    dataset_external_id="ds_omni_data",
)

OMNI_SUB_SDK = ExampleSDK(
    data_model_ids=[DataModelId("sp_pygen_models", "OmniSub", "1")],
    _top_level_package="omni_sub",
    client_name="OmniSubClient",
    generate_sdk=True,
    instance_space=None,
)

OMNI_TYPED = ExampleSDK(
    data_model_ids=[DataModelId("sp_pygen_models", "Omni", "1")],
    _top_level_package="omni_typed",
    client_name="DoesNotMatter",
    instance_space="omni-instances",
    generate_sdk=True,
    is_typed=True,
    # Done above
    download_nodes=False,
    typed_classes={
        "CDFExternalReferencesListed",
        "PrimitiveNullable",
        "PrimitiveRequiredListed",
        "MainInterface",
        "Implementation1",
        "Implementation2",
        "SubInterface",
        "Implementation1NonWritable",
        "DependentOnNonWritable",
    },
)

OMNI_MULTI_SDK = ExampleSDK(
    data_model_ids=[
        DataModelId("sp_pygen_models", "OmniMultiA", "1"),
        DataModelId("sp_pygen_models", "OmniMultiB", "1"),
        DataModelId("sp_pygen_models", "OmniMultiC", "1"),
    ],
    _top_level_package="omni_multi",
    client_name="OmniMultiClient",
    generate_sdk=True,
    # Omni multi is generated without instance space.
    instance_space=None,
    download_nodes=False,
)


PUMP_SDK = ExampleSDK(
    data_model_ids=[DataModelId("IntegrationTestsImmutable", "Pumps", "1")],
    _top_level_package="pump.client",
    client_name="PumpClient",
    generate_sdk=False,
    instance_space=None,
)

CORE_SDK = ExampleSDK(
    data_model_ids=[DataModelId("cdf_cdm", "CogniteCore", "v1")],
    _top_level_package="cognite_core",
    client_name="CogniteCoreClient",
    generate_sdk=True,
    instance_space="springfield_instances",
)

WIND_ENTERPRISE = ExampleSDK(
    data_model_ids=[DataModelId("sp_pygen_power_enterprise", "WindDomain", "v1")],
    _top_level_package="wind_enterprise",
    client_name="WindEnterpriseClient",
    generate_sdk=False,
    instance_space="sp_wind",
)

WIND_TURBINE = ExampleSDK(
    data_model_ids=[DataModelId("sp_pygen_power", "WindTurbine", "1")],
    _top_level_package="wind_turbine",
    client_name="WindTurbineClient",
    generate_sdk=True,
    instance_space="sp_wind",
)


class OmniFiles:
    client_dir = OMNI_SDK.client_dir
    client = client_dir / "_api_client.py"
    config = client_dir / "config.py"
    data_classes = client_dir / "data_classes"
    core_data = data_classes / "_core"
    data_core_base = core_data / "base.py"
    data_core_constants = core_data / "constants.py"
    data_core_init = core_data / "__init__.py"
    data_core_helpers = core_data / "helpers.py"
    data_core_cdf_external = core_data / "cdf_external.py"
    core_query_data = core_data / "query"
    data_core_query_init = core_query_data / "__init__.py"
    data_core_query_filter_classes = core_query_data / "filter_classes.py"
    data_core_query_select = core_query_data / "select.py"

    data_init = data_classes / "__init__.py"
    api = client_dir / "_api"
    core_api = api / "_core.py"
    core_init = api / "__init__.py"
    client_init = client_dir / "__init__.py"
    cdf_external_data = data_classes / "_cdf_external_references.py"
    cdf_external_timeseries_api = api / "cdf_external_references_timeseries.py"
    cdf_external_list_data = data_classes / "_cdf_external_references_listed.py"
    primitive_nullable_data = data_classes / "_primitive_nullable.py"
    primitive_nullable_list_data = data_classes / "_primitive_nullable_listed.py"
    primitive_with_defaults_data = data_classes / "_primitive_with_defaults.py"
    primitive_required_data = data_classes / "_primitive_required.py"
    primitive_required_list_data = data_classes / "_primitive_required_listed.py"
    implementation_1_data = data_classes / "_implementation_1.py"
    implementation_1_non_writeable_data = data_classes / "_implementation_1_non_writeable.py"
    connection_item_a_data = data_classes / "_connection_item_a.py"
    connection_item_c_node_data = data_classes / "_connection_item_c_node.py"
    connection_item_c_edge_data = data_classes / "_connection_item_c_edge.py"
    connection_item_d_data = data_classes / "_connection_item_d.py"
    connection_item_e_data = data_classes / "_connection_item_e.py"
    connection_item_f_data = data_classes / "_connection_item_f.py"
    connection_edge_a = data_classes / "_connection_edge_a.py"

    cdf_external_api = api / "cdf_external_references.py"
    cdf_external_list_api = api / "cdf_external_references_listed.py"
    primitive_nullable_api = api / "primitive_nullable.py"
    primitive_nullable_list_api = api / "primitive_nullable_listed.py"
    primitive_with_defaults_api = api / "primitive_with_defaults.py"
    primitive_required_api = api / "primitive_required.py"
    primitive_required_list_api = api / "primitive_required_listed.py"
    implementation_1_api = api / "implementation_1.py"
    implementation_1_non_writeable_api = api / "implementation_1_non_writeable.py"
    sub_interface = api / "sub_interface.py"

    connection_item_a_api = api / "connection_item_a.py"
    connection_item_b_api = api / "connection_item_b.py"
    connection_item_c_api = api / "connection_item_c_node.py"
    connection_item_e_api = api / "connection_item_e.py"
    connection_item_f_api = api / "connection_item_f.py"
    connection_item_g_api = api / "connection_item_g.py"

    connection_item_a_edge_apis = (api / "connection_item_a_outwards.py",)
    connection_item_b_edge_apis = (api / "connection_item_b_inwards.py", api / "connection_item_b_self_edge.py")
    connection_item_c_edge_apis = (
        api / "connection_item_c_node_connection_item_a.py",
        api / "connection_item_c_node_connection_item_b.py",
    )
    connection_item_a_query = api / "connection_item_a_query.py"
    connection_item_b_query = api / "connection_item_b_query.py"
    connection_item_c_query = api / "connection_item_c_node_query.py"
    connection_item_d_query = api / "connection_item_d_query.py"
    connection_item_f_query = api / "connection_item_f_query.py"
    connection_item_g_query = api / "connection_item_g_query.py"


class OmniTypedFiles:
    client_dir = OMNI_TYPED.client_dir
    typed = client_dir / "typed.py"


class OmniSubFiles:
    client_dir = OMNI_SUB_SDK.client_dir

    data_classes = client_dir / "data_classes"
    core_data = data_classes / "_core"
    data_core_base = core_data / "base.py"
    data_core_constants = core_data / "constants.py"
    data_core_init = core_data / "__init__.py"
    data_core_helpers = core_data / "helpers.py"
    data_core_query = core_data / "query.py"
    connection_item_a_data = data_classes / "_connection_item_a.py"
    connection_item_c_edge_data = data_classes / "_connection_item_c_edge.py"

    api = client_dir / "_api"
    core_api = api / "_core.py"
    connection_item_a_api = api / "connection_item_a.py"
    connection_item_a_edge_apis = (api / "connection_item_a_outwards.py",)

    api_client = client_dir / "_api_client.py"


class OmniMultiFiles:
    client_dir = OMNI_MULTI_SDK.client_dir
    api_client = client_dir / "_api_client.py"


class CogniteCoreFiles:
    client_dir = CORE_SDK.client_dir
    data_classes = client_dir / "data_classes"
    data_cognite_asset = data_classes / "_cognite_asset.py"


class WindTurbineFiles:
    client_dir = WIND_TURBINE.client_dir
    data_classes = client_dir / "data_classes"
    data_sensor_time_series = data_classes / "_sensor_time_series.py"
    data_metmast = data_classes / "_metmast.py"


EXAMPLE_SDKS = [var for var in locals().values() if isinstance(var, ExampleSDK)]
