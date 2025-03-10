from __future__ import annotations

from omni import OmniClient


def test_aggregate_count(omni_client: OmniClient) -> None:
    # Act
    result = omni_client.primitive_required.aggregate("count")

    # Assert
    assert isinstance(result.value, float | int)
    assert result.value > 0


def test_aggregate_count_with_group_by(omni_client: OmniClient) -> None:
    # Act
    result = omni_client.primitive_required.aggregate("count", group_by="boolean")

    # Assert
    assert len(result) == 2
    assert isinstance(result[0].aggregates[0].value, int)
    assert result[0].aggregates[0].value > 0
    assert isinstance(result[1].aggregates[0].value, int)
    assert result[1].aggregates[0].value > 0


def test_histogram(omni_client: OmniClient) -> None:
    # Act
    result = omni_client.primitive_required.histogram("float_32", interval=10.0)

    # Assert
    assert len(result.buckets) > 0
    assert result.buckets[0].count > 0
