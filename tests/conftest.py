import pytest
from cognite.client import data_modeling as dm

from cognite.pygen.utils.text import to_pascal, to_snake
from tests.constants import IS_PYDANTIC_V2, OMNI_SDK
from tests.omni_constants import OmniClasses

if IS_PYDANTIC_V2:
    from omni import data_classes as omni_classes
else:
    from omni_pydantic_v1 import data_classes as omni_classes


@pytest.fixture(scope="session")
def omni_data_model() -> dm.DataModel[dm.View]:
    return OMNI_SDK.load_data_model()


@pytest.fixture(scope="session")
def omni_data_classes(omni_data_model: dm.DataModel[dm.View]) -> dict[dm.ViewId, OmniClasses]:
    output = {}
    available_data_classes = vars(omni_classes)
    for view in omni_data_model.views:
        read_name = to_pascal(view.external_id)
        write_name = read_name + "Apply"
        api_name = to_snake(view.external_id)
        read_class = available_data_classes[read_name]
        write_class = available_data_classes[write_name]
        output[view.as_id()] = OmniClasses(read_class, write_class, api_name, view)
    return output