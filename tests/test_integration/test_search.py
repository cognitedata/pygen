from __future__ import annotations

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
else:
    from omni_pydantic_v1 import OmniClient


def test_search(omni_client: OmniClient) -> None:
    # Arrange
    items = omni_client.primitive_required.list(limit=5)
    assert len(items) > 0
    word = items[0].text.split(" ")[0]

    # Act
    result = omni_client.primitive_required.search(word)

    # Assert
    assert len(result) > 0
    assert items[0].external_id in [item.external_id for item in result]
