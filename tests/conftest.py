import pytest
from cognite.client import data_modeling as dm

from cognite.pygen.utils.text import to_pascal, to_snake
from tests.constants import IS_PYDANTIC_V2, OMNI_MULTI_SDK, OMNI_SDK
from tests.omni_constants import OmniClasses

if IS_PYDANTIC_V2:
    from omni import data_classes as omni_classes
else:
    from omni_pydantic_v1 import data_classes as omni_classes


@pytest.fixture(scope="session")
def omni_data_model() -> dm.DataModel[dm.View]:
    return OMNI_SDK.load_data_model()


@pytest.fixture(scope="session")
def omni_views(omni_data_model: dm.DataModel[dm.View]) -> dict[str, dm.View]:
    return {view.external_id: view for view in omni_data_model.views}


@pytest.fixture(scope="session")
def omni_data_classes(omni_data_model: dm.DataModel[dm.View]) -> dict[str, OmniClasses]:
    output = {}
    available_data_classes = vars(omni_classes)
    for view in omni_data_model.views:
        read_name = to_pascal(view.external_id)
        write_name = read_name + "Write"
        api_name = to_snake(view.external_id)
        read_class = available_data_classes[read_name]
        write_class = available_data_classes.get(write_name)
        output[view.external_id] = OmniClasses(read_class, write_class, api_name, view)
    return output


@pytest.fixture(scope="session")
def omni_multi_data_models() -> list[dm.DataModel[dm.View]]:
    return OMNI_MULTI_SDK.load_data_models()
