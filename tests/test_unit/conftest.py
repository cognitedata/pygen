import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.testing import monkeypatch_cognite_client

from cognite.pygen._generator import CodeFormatter
from cognite.pygen.config import PygenConfig
from tests.constants import OMNI_SDK


@pytest.fixture(scope="session")
def code_formatter() -> CodeFormatter:
    return CodeFormatter(format_code=True, logger=print, default_line_length=120)


@pytest.fixture(scope="session")
def pygen_config() -> PygenConfig:
    return PygenConfig()


@pytest.fixture()
def mock_cognite_client() -> CogniteClient:
    with monkeypatch_cognite_client() as m:
        yield m


@pytest.fixture(scope="session")
def omnium_data_model() -> dm.DataModel[dm.View]:
    return OMNI_SDK.load_data_model()
