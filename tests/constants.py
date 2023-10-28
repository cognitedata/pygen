from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import DataModelId
from yaml import safe_load

from cognite.pygen.utils.helper import get_pydantic_version

REPO_ROOT = Path(__file__).resolve().parent.parent
DMS_DATA_MODELS = REPO_ROOT / "tests" / "dms_data_models"

_pydantic_version = get_pydantic_version()
IS_PYDANTIC_V1 = _pydantic_version == "v1"

EXAMPLES_DIR = {
    "v1": REPO_ROOT / "examples-pydantic-v1",
    "v2": REPO_ROOT / "examples",
}[_pydantic_version]


@dataclass
class ExampleSDK:
    data_models: list[DataModelId]
    client_name: str
    _top_level_package: str
    download_only: bool = False  # Used to example SDK that only trigger validation error.
    manual_files: list[Path] = field(default_factory=list, init=False)

    @property
    def dms_files(self) -> list[Path]:
        return [
            DMS_DATA_MODELS / f"{model_id.space}-{model_id.external_id}-{model_id.version}.yaml"
            for model_id in self.data_models
        ]

    @property
    def top_level_package(self) -> str:
        if IS_PYDANTIC_V1:
            first, *rest = self._top_level_package.split(".")
            return f"{first}_pydantic_v1." + ".".join(rest)
        return self._top_level_package

    @property
    def client_dir(self) -> Path:
        return EXAMPLES_DIR / self.top_level_package.replace(".", "/")

    def load_data_models(self) -> list[dm.DataModel[dm.View]]:
        models = []
        for dms_file in self.dms_files:
            models.append(dm.DataModel.load(safe_load(dms_file.read_text())[0]))
        return models

    def load_data_model(self) -> dm.DataModel[dm.View]:
        models = self.load_data_models()
        if len(models) == 1:
            return models[0]
        raise ValueError(f"Expected exactly one data model, got {len(models)}")

    def append_manual_files(self, manual_files_cls: type):
        for var in vars(manual_files_cls).values():
            if isinstance(var, Path) and var.is_file():
                self.manual_files.append(var)


MARKET_SDK = ExampleSDK(
    data_models=[DataModelId("market", "CogPool", "3"), DataModelId("market", "PygenPool", "3")],
    _top_level_package="markets.client",
    client_name="MarketClient",
)

SHOP_SDK = ExampleSDK(
    data_models=[DataModelId("IntegrationTestsImmutable", "SHOP_Model", "2")],
    _top_level_package="shop.client",
    client_name="ShopClient",
)

MOVIE_SDK = ExampleSDK(
    data_models=[DataModelId("IntegrationTestsImmutable", "Movie", "2")],
    _top_level_package="movie_domain.client",
    client_name="MovieClient",
)

OSDU_SDK = ExampleSDK(
    data_models=[DataModelId("IntegrationTestsImmutable", "OSDUWells", "1")],
    _top_level_package="osdu.client",
    client_name="OSDUClient",
)

APM_SDK = ExampleSDK(
    data_models=[DataModelId("tutorial_apm_simple", "ApmSimple", "6")],
    _top_level_package="tutorial_apm_simple.client",
    client_name="ApmSimpleClient",
)

PUMP_SDK = ExampleSDK(
    data_models=[DataModelId("IntegrationTestsImmutable", "Pumps", "1")],
    _top_level_package="pump.client",
    client_name="PumpClient",
    download_only=True,
)

SCENARIO_INSTANCE_SDK = ExampleSDK(
    data_models=[DataModelId("IntegrationTestsImmutable", "ScenarioInstance", "1")],
    _top_level_package="scenario_instance.client",
    client_name="ScenarioInstanceClient",
)

# The following files are manually controlled and should not be overwritten by the generator.


class MarketSDKFiles:
    client_dir = MARKET_SDK.client_dir
    client = client_dir / "_api_client.py"
    date_transformation_pair_data = client_dir / "data_classes" / "_date_transformation_pair.py"
    date_transformation_pair_api = client_dir / "_api" / "date_transformation_pair.py"


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
    core_data = data_classes / "_core.py"

    api = client_dir / "_api"
    persons_api = api / "person.py"
    actors_api = api / "actor.py"
    core_api = api / "_core.py"

    client = client_dir / "_api_client.py"
    client_init = client_dir / "__init__.py"
    data_init = data_classes / "__init__.py"
    api_init = api / "__init__.py"


MOVIE_SDK.append_manual_files(MovieSDKFiles)


class ScenarioInstanceFiles:
    client_dir = SCENARIO_INSTANCE_SDK.client_dir

    api = client_dir / "_api"
    scenario_instance_api = api / "scenario_instance.py"


EXAMPLE_SDKS = [var for var in locals().values() if isinstance(var, ExampleSDK)]
