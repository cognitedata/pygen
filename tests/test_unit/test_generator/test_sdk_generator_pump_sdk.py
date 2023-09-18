import pytest
from cognite.client import data_modeling as dm

from cognite.pygen import SDKGenerator
from cognite.pygen.config import APIClassNaming, Case, Naming, Number, PygenConfig
from cognite.pygen.exceptions import NameConflict


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
