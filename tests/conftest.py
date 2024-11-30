import pytest
from cognite.client import data_modeling as dm
from omni import data_classes as omni_classes

from cognite.pygen.utils.text import to_pascal, to_snake
from tests.constants import CORE_SDK, OMNI_MULTI_SDK, OMNI_SDK, OMNI_SUB_SDK, WIND_TURBINE
from tests.omni_constants import OmniClasses


@pytest.fixture(scope="session")
def omni_data_model() -> dm.DataModel[dm.View]:
    return OMNI_SDK.load_data_model()


@pytest.fixture(scope="session")
def omnisub_data_model() -> dm.DataModel[dm.View]:
    return OMNI_SUB_SDK.load_data_model()


@pytest.fixture(scope="session")
def core_data_model() -> dm.DataModel[dm.View]:
    return CORE_SDK.load_data_model()


@pytest.fixture(scope="session")
def turbine_data_model() -> dm.DataModel[dm.View]:
    return WIND_TURBINE.load_data_model()


@pytest.fixture(scope="session")
def omni_views(omni_data_model: dm.DataModel[dm.View]) -> dict[str, dm.View]:
    return {view.external_id: view for view in omni_data_model.views}


@pytest.fixture(scope="session")
def omni_data_classes(omni_data_model: dm.DataModel[dm.View]) -> dict[str, OmniClasses]:
    output = {}
    available_data_classes = vars(omni_classes)
    for view in omni_data_model.views:
        read_name = to_pascal(view.external_id)
        if view.used_for == "all":
            read_name = read_name + "Node"
        write_name = read_name + "Write"
        api_name = to_snake(view.external_id)
        if read_name not in available_data_classes:
            continue
        read_class = available_data_classes[read_name]
        write_class = available_data_classes.get(write_name)
        key = view.external_id
        if view.used_for == "all":
            # We only include the node class.
            key = f"{view.external_id}Node"
        output[key] = OmniClasses(read_class, write_class, api_name, view)
    return output


@pytest.fixture(scope="session")
def omni_multi_data_models() -> dm.DataModelList[dm.View]:
    return OMNI_MULTI_SDK.load_data_models()
