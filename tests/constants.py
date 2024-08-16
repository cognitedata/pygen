from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from cognite.client import data_modeling as dm
from cognite.client.data_classes import FileMetadataList, SequenceList, TimeSeriesList
from cognite.client.data_classes.data_modeling import DataModelId, SpaceApply, SpaceApplyList

from cognite.pygen.utils.helper import get_pydantic_version

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_MODELS = REPO_ROOT / "tests" / "data_models"

_pydantic_version = get_pydantic_version()
IS_PYDANTIC_V1 = _pydantic_version == "v1"
IS_PYDANTIC_V2 = _pydantic_version == "v2"

EXAMPLES_DIR = {
    "v1": REPO_ROOT / "examples-pydantic-v1",
    "v2": REPO_ROOT / "examples",
}[_pydantic_version]

_EXAMPLES_DIR_V2 = REPO_ROOT / "examples"


@dataclass
class ExampleSDK:
    data_model_ids: list[DataModelId]
    client_name: str
    _top_level_package: str
    generate_sdk: bool
    download_nodes: bool = False
    _instance_space: str | None = None
    is_typed: bool = False
    typed_classes: set[str] = field(default_factory=set)
    manual_files: list[Path] = field(default_factory=list, init=False)

    @property
    def instance_space(self) -> str:
        return self._instance_space or self.data_model_ids[0].space

    @property
    def top_level_package(self) -> str:
        if IS_PYDANTIC_V1:
            if "." in self._top_level_package:
                first, *rest = self._top_level_package.split(".")
                return f"{first}_pydantic_v1." + ".".join(rest)
            else:
                return f"{self._top_level_package}_pydantic_v1"
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

    def append_manual_files(self, manual_files_cls: type):
        for var in vars(manual_files_cls).values():
            if isinstance(var, Path) and var.is_file():
                self.manual_files.append(var)

    def load_spaces(self) -> SpaceApplyList:
        spaces = {model.space for model in self.data_model_ids}
        for model in self.data_model_ids:
            views = self.load_views(model)
            spaces |= {view.space for view in views}

        if self.instance_space not in spaces:
            spaces.add(self.instance_space)
        return SpaceApplyList([SpaceApply(space) for space in spaces])

    @classmethod
    def load_views(cls, data_model_id: dm.DataModelId) -> dm.ViewApplyList:
        view_files = list((cls.model_dir(data_model_id) / "views").glob("*.view.yaml"))
        return dm.ViewApplyList([dm.ViewApply.load(view_file.read_text()) for view_file in view_files])

    @classmethod
    def load_containers(cls, data_model_id: dm.DataModelId) -> dm.ContainerApplyList:
        container_files = list((cls.model_dir(data_model_id) / "containers").glob("*.container.yaml"))
        return dm.ContainerApplyList(
            [dm.ContainerApply.load(container_file.read_text()) for container_file in container_files]
        )

    def load_write_model(self, data_model_id: dm.DataModelId) -> dm.DataModelApply:
        views = self.load_views(data_model_id).as_ids()
        return dm.DataModelApply(
            space=data_model_id.space,
            external_id=data_model_id.external_id,
            version=data_model_id.version,
            description="",
            name=data_model_id.external_id,
            views=views,
        )

    def load_timeseries(self, data_model_id: dm.DataModelId) -> TimeSeriesList:
        timeseries_files = list(self.model_dir(data_model_id).glob("**/*timeseries.yaml"))
        return TimeSeriesList([ts for filepath in timeseries_files for ts in TimeSeriesList.load(filepath.read_text())])

    def load_sequences(self, data_model_id: dm.DataModelId) -> SequenceList:
        sequence_files = list(self.model_dir(data_model_id).glob("**/*sequence.yaml"))
        return SequenceList([seq for filepath in sequence_files for seq in SequenceList.load(filepath.read_text())])

    def load_filemetadata(self, data_model_id: dm.DataModelId) -> FileMetadataList:
        filemetadata_files = list(self.model_dir(data_model_id).glob("**/*file.yaml"))
        return FileMetadataList(
            [f for filepath in filemetadata_files for f in FileMetadataList.load(filepath.read_text())]
        )

    def load_nodes(self, data_model_id: dm.DataModelId, isoformat_dates: bool = False) -> dm.NodeApplyList:
        node_files = list(self.model_dir(data_model_id).glob("**/*node.yaml"))
        nodes = dm.NodeApplyList([n for filepath in node_files for n in dm.NodeApplyList.load(filepath.read_text())])
        if not isoformat_dates:
            return nodes
        for node in nodes:
            for source in node.sources or []:
                for name in list(source.properties):
                    if isinstance(source.properties[name], date):
                        source.properties[name] = source.properties[name].isoformat()
                    elif isinstance(source.properties[name], datetime):
                        source.properties[name] = source.properties[name].isoformat(timespec="milliseconds")
                    if isinstance(source.properties[name], list):
                        for i, value in enumerate(source.properties[name]):
                            if isinstance(value, date):
                                source.properties[name][i] = value.isoformat()
                            elif isinstance(value, datetime):
                                source.properties[name][i] = value.isoformat(timespec="milliseconds")
        return nodes

    def load_edges(self, data_model_id: dm.DataModelId) -> dm.EdgeApplyList:
        edge_files = list(self.model_dir(data_model_id).glob("**/*edge.yaml"))
        return dm.EdgeApplyList([e for filepath in edge_files for e in dm.EdgeApplyList.load(filepath.read_text())])

    def load_read_nodes(self, data_model_id: dm.DataModelId) -> dm.NodeList:
        return dm.NodeList.load(self.read_node_path(data_model_id).read_text())


WINDMILL_SDK = ExampleSDK(
    data_model_ids=[DataModelId("power-models", "Windmill", "1")],
    _top_level_package="windmill",
    client_name="WindmillClient",
    generate_sdk=True,
    _instance_space="windmill-instances",
)

OMNI_SDK = ExampleSDK(
    data_model_ids=[DataModelId("pygen-models", "Omni", "1")],
    _top_level_package="omni",
    client_name="OmniClient",
    generate_sdk=True,
    _instance_space="omni-instances",
    download_nodes=True,
)

OMNI_TYPED = ExampleSDK(
    data_model_ids=[DataModelId("pygen-models", "Omni", "1")],
    _top_level_package="omni_typed",
    client_name="DoesNotMatter",
    _instance_space="omni-instances",
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
    _instance_space="omni-instances",
    download_nodes=False,
)


# This uses connections that are not supported by the UI, so it will not be shown there.
OMNIUM_CONNECTION_SDK = ExampleSDK(
    data_model_ids=[DataModelId("pygen-models", "OmniConnection", "1")],
    _top_level_package="omni_connection",
    client_name="OmniConnectionClient",
    generate_sdk=False,
    _instance_space="omni-instances",
)

APM_SDK = ExampleSDK(
    data_model_ids=[DataModelId("tutorial_apm_simple", "ApmSimple", "6")],
    _top_level_package="tutorial_apm_simple.client",
    client_name="ApmSimpleClient",
    generate_sdk=False,
)

PUMP_SDK = ExampleSDK(
    data_model_ids=[DataModelId("IntegrationTestsImmutable", "Pumps", "1")],
    _top_level_package="pump.client",
    client_name="PumpClient",
    generate_sdk=False,
)

SCENARIO_INSTANCE_SDK = ExampleSDK(
    data_model_ids=[DataModelId("IntegrationTestsImmutable", "ScenarioInstance", "1")],
    _top_level_package="scenario_instance.client",
    client_name="ScenarioInstanceClient",
    generate_sdk=True,
)

APM_APP_DATA_SOURCE = ExampleSDK(
    data_model_ids=[DataModelId("APM_AppData_4", "APM_AppData_4", "7")],
    _top_level_package="apm_domain.client",
    client_name="ApmClient",
    generate_sdk=False,
)

APM_APP_DATA_SINK = ExampleSDK(
    data_model_ids=[DataModelId("IntegrationTestsImmutable", "ApmAppData", "v3")],
    _top_level_package="sysdm_domain.client",
    client_name="SysDMClient",
    generate_sdk=False,
)

EQUIPMENT_UNIT_SDK = ExampleSDK(
    data_model_ids=[DataModelId("IntegrationTestsImmutable", "EquipmentUnit", "2")],
    _top_level_package="equipment_unit",
    client_name="EquipmentUnitClient",
    generate_sdk=True,
)


# The following files are manually maintained (they are used to implement new functionality,
# and are thus nod overwritten by the `python dev.py generate` command)


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


EQUIPMENT_UNIT_SDK.append_manual_files(EquipmentSDKFiles)


class ScenarioInstanceFiles:
    client_dir = SCENARIO_INSTANCE_SDK.client_dir

    api = client_dir / "_api"
    scenario_instance_api = api / "scenario_instance.py"


SCENARIO_INSTANCE_SDK.append_manual_files(ScenarioInstanceFiles)


class OmniFiles:
    client_dir = OMNI_SDK.client_dir
    client = client_dir / "_api_client.py"
    data_classes = client_dir / "data_classes"
    core_data = data_classes / "_core.py"
    data_init = data_classes / "__init__.py"
    api = client_dir / "_api"
    core_api = api / "_core.py"
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


OMNI_SDK.append_manual_files(OmniFiles)


class OmniTypedFiles:
    client_dir = OMNI_TYPED.client_dir
    typed = client_dir / "typed.py"


OMNI_TYPED.append_manual_files(OmniTypedFiles)


class OmniMultiFiles:
    client_dir = OMNI_MULTI_SDK.client_dir
    api_client = client_dir / "_api_client.py"


OMNI_MULTI_SDK.append_manual_files(OmniMultiFiles)


class WindMillFiles:
    class Data:
        wind_mill_json = DATA_MODELS / "WindMill" / "data" / "data.json"


EXAMPLE_SDKS = [var for var in locals().values() if isinstance(var, ExampleSDK)]
