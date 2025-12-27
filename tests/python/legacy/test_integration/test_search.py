import pytest
from omni import OmniClient
from omni import data_classes as dc


@pytest.fixture(scope="module")
def selected_item(omni_client: OmniClient) -> dc.PrimitiveRequired:
    items = omni_client.primitive_required.list(limit=1)
    assert len(items) > 0
    return items[0]


def test_search(omni_client: OmniClient, selected_item: dc.PrimitiveRequired) -> None:
    # Arrange
    word = selected_item.text.split(" ")[0]

    # Act
    result = omni_client.primitive_required.search(word)

    # Assert
    assert len(result) > 0
    assert selected_item.external_id in {item.external_id for item in result}


def test_search_on_external_id(omni_client: OmniClient, selected_item: dc.PrimitiveRequired) -> None:
    # Arrange
    external_id = selected_item.external_id

    # Act
    result = omni_client.primitive_required.search(external_id, properties=["external_id"])

    # Assert
    assert len(result) == 1
    assert result[0].external_id == external_id
