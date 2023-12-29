from __future__ import annotations

from cognite.client import CogniteClient

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
    from omni import data_classes as dc
else:
    from omni_pydantic_v1 import OmniClient
    from omni_pydantic_v1 import data_classes as dc


def test_list_empty_to_pandas(omni_client: OmniClient, cognite_client: CogniteClient) -> None:
    # Act
    empty_df = omni_client.empty.list().to_pandas()

    # Assert
    assert empty_df.empty
    if IS_PYDANTIC_V2:
        assert sorted(empty_df.columns) == sorted(
            set(dc.Empty.model_fields) - (set(dc.DomainModel.model_fields) - {"external_id"})
        )

    else:
        assert sorted(empty_df.columns) == sorted(
            set(dc.Empty.__fields__) - (set(dc.DomainModel.__fields__) - {"external_id"})
        )
