from collections.abc import Iterable

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.testing import monkeypatch_cognite_client

from cognite.pygen._core.generators import MultiAPIGenerator, SDKGenerator
from cognite.pygen._generator import CodeFormatter
from cognite.pygen.config import PygenConfig
from tests.constants import CORE_SDK, OMNI_SDK, OMNI_SUB_SDK, WIND_TURBINE


@pytest.fixture(scope="session")
def code_formatter() -> CodeFormatter:
    return CodeFormatter(format_code=True, logger=print, default_line_length=120)


@pytest.fixture(scope="session")
def pygen_config() -> PygenConfig:
    return PygenConfig()


@pytest.fixture()
def mock_cognite_client() -> Iterable[CogniteClient]:
    with monkeypatch_cognite_client() as m:
        yield m


@pytest.fixture(scope="session")
def omni_sdk_generator(omni_data_model: dm.DataModel[dm.View]) -> SDKGenerator:
    return SDKGenerator(
        OMNI_SDK.top_level_package,
        OMNI_SDK.client_name,
        omni_data_model,
        OMNI_SDK.instance_space,
    )


@pytest.fixture(scope="session")
def omni_multi_api_generator(omni_data_model: dm.DataModel[dm.View]) -> MultiAPIGenerator:
    return MultiAPIGenerator(
        OMNI_SDK.top_level_package,
        OMNI_SDK.client_name,
        [omni_data_model],
        OMNI_SDK.instance_space,
    )


@pytest.fixture(scope="session")
def omni_multi_api_generator_composition(omni_data_model: dm.DataModel[dm.View]) -> MultiAPIGenerator:
    return MultiAPIGenerator(
        OMNI_SDK.top_level_package,
        OMNI_SDK.client_name,
        [omni_data_model],
        OMNI_SDK.instance_space,
        implements="composition",
    )


@pytest.fixture(scope="session")
def omnisub_multi_api_generator(omnisub_data_model: dm.DataModel[dm.View]) -> MultiAPIGenerator:
    return MultiAPIGenerator(
        OMNI_SUB_SDK.top_level_package,
        OMNI_SUB_SDK.client_name,
        [omnisub_data_model],
        OMNI_SUB_SDK.instance_space,
    )


@pytest.fixture(scope="session")
def core_multi_api_generator(core_data_model: dm.DataModel[dm.View]) -> MultiAPIGenerator:
    return MultiAPIGenerator(
        CORE_SDK.top_level_package,
        CORE_SDK.client_name,
        [core_data_model],
        CORE_SDK.instance_space,
    )


@pytest.fixture(scope="session")
def turbine_multi_api_generator(turbine_data_model: dm.DataModel[dm.View]) -> MultiAPIGenerator:
    return MultiAPIGenerator(
        WIND_TURBINE.top_level_package,
        WIND_TURBINE.client_name,
        [turbine_data_model],
        WIND_TURBINE.instance_space,
    )
