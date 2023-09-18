import pytest
from cognite.client import data_modeling as dm

from cognite.pygen import SDKGenerator
from cognite.pygen.exceptions import NameConflict


def test_generate_sdk_raises_name_conflict(pump_model: dm.DataModel[dm.View]) -> None:
    # Act
    with pytest.raises(NameConflict) as e:
        SDKGenerator("cognite", "pump", pump_model)

    # Assert
    assert "Name conflict detected." in str(e.value)
