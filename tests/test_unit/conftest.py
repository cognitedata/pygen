import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.testing import monkeypatch_cognite_client

from cognite.pygen._generator import CodeFormatter
from cognite.pygen.config import PygenConfig
from cognite.pygen.utils.text import to_pascal
from tests.constants import IS_PYDANTIC_V2, OMNI_SDK
from tests.omni_constants import OmniClassPair

if IS_PYDANTIC_V2:
    from omni import data_classes
else:
    from omni_pydantic_v1 import data_classes


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
def omni_data_model() -> dm.DataModel[dm.View]:
    return OMNI_SDK.load_data_model()


@pytest.fixture(scope="session")
def omni_data_classes(omni_data_model: dm.DataModel[dm.View]) -> dict[dm.ViewId, OmniClassPair]:
    output = {}
    available_data_classes = vars(data_classes)
    for view in omni_data_model.views:
        read_name = to_pascal(view.external_id)
        write_name = read_name + "Apply"
        read_class = available_data_classes[read_name]
        write_class = available_data_classes[write_name]
        output[view.as_id()] = OmniClassPair(read_class, write_class, view)
    return output
