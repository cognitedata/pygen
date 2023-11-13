import pytest

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from equipment_unit.client.data_classes import StartEndTimeList

    from examples.equipment_unit.client import EquipmentUnitClient
else:
    raise NotImplementedError()


@pytest.fixture
def start_end_time_edges(equipment_client: EquipmentUnitClient) -> StartEndTimeList:
    edges = equipment_client.unit_procedure.work_units.list(limit=-1)
    assert len(edges) > 2, "There should be at least three edge in the list"
    assert isinstance(edges, StartEndTimeList)
    return edges


def test_filter_start_end_time_edges(
    start_end_time_edges: StartEndTimeList, equipment_client: EquipmentUnitClient
) -> None:
    sorted_by_start_time = sorted(start_end_time_edges, key=lambda x: x.start_time)

    filtered = equipment_client.unit_procedure.work_units.list(
        min_start_time=sorted_by_start_time[1].start_time, limit=-1
    )

    assert len(filtered) == len(sorted_by_start_time) - 1
