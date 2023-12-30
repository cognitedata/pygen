from __future__ import annotations

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
else:
    from omni_pydantic_v1 import OmniClient


def test_retrieve_missing(omni_client: OmniClient) -> None:
    # Arrange
    external_id = "non-existing"

    # Act
    item = omni_client.primitive_required.retrieve(external_id=external_id)

    # Assert
    assert item is None
