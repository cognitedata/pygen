from __future__ import annotations

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
    from omni import data_classes as dc
else:
    from omni_pydantic_v1 import OmniClient
    from omni_pydantic_v1 import data_classes as dc


def test_retrieve_missing(omni_client: OmniClient) -> None:
    # Arrange
    external_id = "non-existing"

    # Act
    item = omni_client.primitive_required.retrieve(external_id=external_id)

    # Assert
    assert item is None


def test_retrieve_and_as_write(omni_client: OmniClient) -> None:
    external_id = omni_client.primitive_nullable.list(limit=1)[0].external_id

    item = omni_client.primitive_nullable.retrieve(external_id=external_id)

    assert item is not None

    def raise_(*args, **kwargs):
        raise ValueError("This should not be called")

    dc.DomainModelWrite.external_id_factory = raise_
    write = item.as_write()
    dc.DomainModelWrite.external_id_factory = None
    assert write.external_id == item.external_id
