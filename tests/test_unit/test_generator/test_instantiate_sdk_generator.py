import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import SDKGenerator
from cognite.pygen.config import APIClassNaming, Case, Naming, Number, PygenConfig
from cognite.pygen.exceptions import NameConflict
from tests.constants import OMNI_MULTI_SDK


def test_generate_sdk_raises_name_conflict(pump_model: dm.DataModel[dm.View]) -> None:
    # Arrange
    invalid_config = PygenConfig()
    invalid_config.naming.api_class = APIClassNaming(file_name=Naming(Case.snake, Number.plural))

    # Act
    with pytest.raises(NameConflict) as e:
        SDKGenerator("cognite", "pump", pump_model, config=invalid_config)

    # Assert
    assert "Name conflict detected" in str(e.value)


def test_generate_sdk_no_name_conflict(pump_model: dm.DataModel[dm.View]) -> None:
    # Act
    generator = SDKGenerator("cognite", "pump", pump_model)

    # Assert
    assert generator


def test_create_multi_sdk_generator(omni_multi_data_models: list[dm.DataModel[dm.View]]) -> None:
    # Act
    generator = SDKGenerator(OMNI_MULTI_SDK.top_level_package, OMNI_MULTI_SDK.client_name, omni_multi_data_models)

    # Assert
    assert generator
