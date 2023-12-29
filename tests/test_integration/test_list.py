from __future__ import annotations

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
    from omni import data_classes as dc
else:
    from omni_pydantic_v1 import OmniClient
    from omni_pydantic_v1 import data_classes as dc


def test_list_empty_to_pandas(omni_client: OmniClient) -> None:
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


def test_filter_on_boolean(omni_client: OmniClient) -> None:
    items = omni_client.primitive_required.list(boolean=False, limit=-1)

    assert len(items) > 0
    assert not (is_true := [item for item in items if item.boolean]), f"Found items with boolean=True: {is_true}"


def test_filter_on_direct_edge(omni_client: OmniClient) -> None:
    all_items = omni_client.connection_item_a.list(limit=5)
    expected = next((item for item in all_items if item.other_direct), None)
    assert expected is not None, "No item with self_direct set"

    items = omni_client.connection_item_a.list(other_direct=expected.other_direct, limit=-1)

    assert len(items) > 0
    assert expected.external_id in [item.external_id for item in items]
