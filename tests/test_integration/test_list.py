from __future__ import annotations

import sys

from cognite.client.data_classes.data_modeling import InstanceSort

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
    from omni import data_classes as dc
    from omni.data_classes._core import DEFAULT_INSTANCE_SPACE
else:
    from omni_pydantic_v1 import OmniClient
    from omni_pydantic_v1 import data_classes as dc
    from omni_pydantic_v1.data_classes._core import DEFAULT_INSTANCE_SPACE


def test_list_empty_to_pandas(omni_client: OmniClient) -> None:
    # Act
    empty_df = omni_client.empty.list().to_pandas()

    # Assert
    assert empty_df.empty
    if IS_PYDANTIC_V2:
        assert sorted(empty_df.columns) == sorted(set(dc.Empty.model_fields) | (set(dc.DomainModel.model_fields)))

    else:
        assert sorted(empty_df.columns) == sorted(set(dc.Empty.__fields__) | (set(dc.DomainModel.__fields__)))


def test_filter_on_boolean(omni_client: OmniClient) -> None:
    items = omni_client.primitive_required.list(boolean=False, limit=-1)

    assert len(items) > 0
    is_true = [item for item in items if item.boolean]
    assert not is_true, f"Found items with boolean=True: {is_true}"


def test_filter_on_minimum_0(omni_client: OmniClient) -> None:
    items = omni_client.primitive_required.list(min_int_32=0, min_float_32=0.0, limit=-1)

    assert len(items) > 0
    is_not_0 = [item for item in items if item.int_32 < 0 or item.float_32 < 0.0]
    assert not is_not_0, f"Found items that are below 0: {is_not_0}"


def test_filter_on_direct_edge(omni_client: OmniClient) -> None:
    all_items = omni_client.connection_item_a.list(limit=5)
    expected = next((item for item in all_items if item.other_direct), None)
    assert expected is not None, "No item with self_direct set"

    items = omni_client.connection_item_a.list(other_direct=expected.other_direct, limit=-1, retrieve_edges=False)

    assert len(items) > 0
    assert expected.external_id in [item.external_id for item in items]


def test_filter_on_space(omni_client: OmniClient) -> None:
    # Act
    no_items = []  # omni_client.primitive_nullable.list(space="Non-existing space")
    some_items = omni_client.primitive_nullable.list(space=DEFAULT_INSTANCE_SPACE)

    # Assert
    assert len(no_items) == 0
    assert len(some_items) > 0


def test_filter_range(omni_client: OmniClient) -> None:
    # Arrange
    items = omni_client.primitive_required.list(limit=5)
    items = sorted(items, key=lambda item: item.int_32)

    # Act
    filtered_items = omni_client.primitive_required.list(min_int_32=items[2].int_32, limit=-1)

    # Assert
    assert len(filtered_items) > 0
    is_below = [item for item in filtered_items if item.int_32 < items[2].int_32]
    assert not is_below, f"Fount items below: {is_below}"


def test_list_above_5000_items(omni_client: OmniClient) -> None:
    # Arrange
    items = [
        dc.Implementation2Apply(external_id=f"implementation2_5000:{i}", mainValue=f"Implementation2 {i}")
        for i in range(5001)
    ]
    omni_client.implementation_2.apply(items)

    # Act
    items = omni_client.implementation_2.list(limit=-1, external_id_prefix="implementation2_5000:")

    # Assert
    assert len(items) == 5001


def test_list_and_sort(omni_client: OmniClient) -> None:
    # Act
    sorted_items = omni_client.primitive_required.list(limit=10, sort_by="int_32", direction="descending")

    # Assert
    assert len(sorted_items) > 1
    assert list(sorted_items) == sorted(sorted_items, key=lambda item: item.int_32, reverse=True)


def test_list_advanced_sort(omni_client: OmniClient) -> None:
    sort = [
        InstanceSort(property=["int_32"], direction="ascending", nulls_first=False),
        InstanceSort(property=["float_32"], direction="descending", nulls_first=True),
    ]

    def key(item: dc.PrimitiveNullable) -> tuple[int, float]:
        first = item.int_32 if item.int_32 is not None else sys.maxsize
        second = -item.float_32 if item.float_32 is not None else -sys.maxsize
        return first, second

    sorted_items = omni_client.primitive_nullable.list(limit=10, sort=sort)

    assert len(sorted_items) > 1
    assert list(sorted_items) == sorted(sorted_items, key=key)
