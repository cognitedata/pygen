from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import DataModelId

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_MODELS = REPO_ROOT / "tests" / "data" / "models"

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


WINDMILL_SDK = ExampleSDK(
    data_model_ids=[DataModelId("power-models", "Windmill", "1")],
    _top_level_package="windmill",
    client_name="WindmillClient",
    generate_sdk=True,
    instance_space="windmill-instances",
)

OMNI_SDK = ExampleSDK(
    data_model_ids=[DataModelId("sp_pygen_models", "Omni", "1")],
    _top_level_package="omni",
    client_name="OmniClient",
    generate_sdk=True,
    instance_space="omni-instances",
    download_nodes=True,
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
        DataModelId("pygen-models", "OmniMultiA", "1"),
        DataModelId("pygen-models", "OmniMultiB", "1"),
        DataModelId("pygen-models", "OmniMultiC", "1"),
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

SCENARIO_INSTANCE_SDK = ExampleSDK(
    data_model_ids=[DataModelId("IntegrationTestsImmutable", "ScenarioInstance", "1")],
    _top_level_package="scenario_instance.client",
    client_name="ScenarioInstanceClient",
    generate_sdk=True,
    instance_space="IntegrationTestsImmutable",
)

EQUIPMENT_UNIT_SDK = ExampleSDK(
    data_model_ids=[DataModelId("IntegrationTestsImmutable", "EquipmentUnit", "2")],
    _top_level_package="equipment_unit",
    client_name="EquipmentUnitClient",
    generate_sdk=True,
    instance_space="IntegrationTestsImmutable",
)


class EquipmentSDKFiles:
    client_dir = EQUIPMENT_UNIT_SDK.client_dir
    client = client_dir / "_api_client.py"
    data_classes = client_dir / "data_classes"
    core_data = data_classes / "_core.py"
    start_end_time_data = data_classes / "_start_end_time.py"
    unit_procedure_data = data_classes / "_unit_procedure.py"
    equipment_module_data = data_classes / "_equipment_module.py"

    api = client_dir / "_api"
    equipment_api = api / "equipment_module.py"
    equipment_module_sensor_value_api = api / "equipment_module_sensor_value.py"

    unit_procedure_api = api / "unit_procedure.py"
    unit_procedure_query = api / "unit_procedure_query.py"
    unit_procedure_work_units = api / "unit_procedure_work_units.py"
    core_api = api / "_core.py"

    data_init = data_classes / "__init__.py"


class ScenarioInstanceFiles:
    client_dir = SCENARIO_INSTANCE_SDK.client_dir

    api = client_dir / "_api"
    scenario_instance_api = api / "scenario_instance.py"


class OmniFiles:
    client_dir = OMNI_SDK.client_dir
    client = client_dir / "_api_client.py"
    data_classes = client_dir / "data_classes"
    core_data = data_classes / "_core"
    data_core_base = core_data / "base.py"
    data_core_constants = core_data / "constants.py"
    data_core_init = core_data / "__init__.py"
    data_core_helpers = core_data / "helpers.py"
    data_core_query = core_data / "query.py"
    data_core_cdf_external = core_data / "cdf_external.py"
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


class WindMillFiles:
    class Data:
        wind_mill_json = DATA_MODELS / "WindMill" / "data" / "data.json"


EXAMPLE_SDKS = [var for var in locals().values() if isinstance(var, ExampleSDK)]
