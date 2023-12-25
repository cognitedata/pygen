from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from cognite.client import data_modeling as dm
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
    _instance_space: str | None = None
    manual_files: list[Path] = field(default_factory=list, init=False)

    @property
    def instance_space(self) -> str:
        return self._instance_space or self.data_model_ids[0].space

    @property
    def top_level_package(self) -> str:
        if IS_PYDANTIC_V1:
            first, *rest = self._top_level_package.split(".")
            return f"{first}_pydantic_v1." + ".".join(rest)
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
        spaces = list({model.space for model in self.data_model_ids})
        if self.instance_space not in spaces:
            spaces.append(self.instance_space)
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


WINDMILL_SDK = ExampleSDK(
    data_model_ids=[DataModelId("power-models", "Windmill", "1")],
    _top_level_package="windmill.client",
    client_name="WindmillClient",
    generate_sdk=True,
    _instance_space="windmill-instances",
)

MARKET_SDK = ExampleSDK(
    data_model_ids=[DataModelId("market", "CogPool", "3"), DataModelId("market", "PygenPool", "3")],
    _top_level_package="markets.client",
    client_name="MarketClient",
    generate_sdk=True,
)

SHOP_SDK = ExampleSDK(
    data_model_ids=[DataModelId("IntegrationTestsImmutable", "SHOP_Model", "2")],
    _top_level_package="shop.client",
    client_name="ShopClient",
    generate_sdk=True,
)

MOVIE_SDK = ExampleSDK(
    data_model_ids=[DataModelId("IntegrationTestsImmutable", "Movie", "4")],
    _top_level_package="movie_domain.client",
    client_name="MovieClient",
    generate_sdk=True,
)

OSDU_SDK = ExampleSDK(
    data_model_ids=[DataModelId("IntegrationTestsImmutable", "OSDUWells", "1")],
    _top_level_package="osdu_wells.client",
    client_name="OSDUClient",
    generate_sdk=False,
)

APM_SDK = ExampleSDK(
    data_model_ids=[DataModelId("tutorial_apm_simple", "ApmSimple", "6")],
    _top_level_package="tutorial_apm_simple.client",
    client_name="ApmSimpleClient",
    generate_sdk=True,
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
    _top_level_package="equipment_unit.client",
    client_name="EquipmentUnitClient",
    generate_sdk=True,
)


# The following files are manually maintained (they are used to implement new functionality,
# and are thus nod overwritten by the `python dev.py generate` command)


class MarketSDKFiles:
    client_dir = MARKET_SDK.client_dir
    client = client_dir / "_api_client.py"
    date_transformation_pair_data = client_dir / "data_classes" / "_date_transformation_pair.py"
    date_transformation_pair_api = client_dir / "_api" / "date_transformation_pair.py"
    date_transformation_pair_query_api = client_dir / "_api" / "date_transformation_pair_query.py"


MARKET_SDK.append_manual_files(MarketSDKFiles)


class ShopSDKFiles:
    client_dir = SHOP_SDK.client_dir
    data_classes = client_dir / "data_classes"
    api = client_dir / "_api"
    cases_data = data_classes / "_case.py"
    command_configs_data = data_classes / "_command_config.py"
    data_init = data_classes / "__init__.py"
    command_configs_api = api / "command_config.py"


SHOP_SDK.append_manual_files(ShopSDKFiles)


class MovieSDKFiles:
    client_dir = MOVIE_SDK.client_dir

    data_classes = client_dir / "data_classes"
    persons_data = data_classes / "_person.py"
    actors_data = data_classes / "_actor.py"

    api = client_dir / "_api"
    core_api = api / "_core.py"
    persons_api = api / "person.py"
    actors_api = api / "actor.py"
    actor_query_api = api / "actor_query.py"
    actor_movies_api = api / "actor_movies.py"

    client = client_dir / "_api_client.py"
    client_init = client_dir / "__init__.py"
    data_init = data_classes / "__init__.py"
    api_init = api / "__init__.py"


MOVIE_SDK.append_manual_files(MovieSDKFiles)


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


class WindMillFiles:
    class Data:
        wind_mill_json = DATA_MODELS / "WindMill" / "data" / "data.json"


EXAMPLE_SDKS = [var for var in locals().values() if isinstance(var, ExampleSDK)]
