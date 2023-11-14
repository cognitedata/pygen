import pytest

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from equipment_unit.client.data_classes import (
        EquipmentModuleApply,
        StartEndTimeApply,
        StartEndTimeList,
        UnitProcedureApply,
    )

    from examples.equipment_unit.client import EquipmentUnitClient
else:
    raise NotImplementedError()


@pytest.fixture
def start_end_time_edges(workorder: EquipmentUnitClient) -> StartEndTimeList:
    edges = workorder.unit_procedure.work_units.list(limit=-1)
    assert len(edges) > 2, "There should be at least three edge in the list"
    assert isinstance(edges, StartEndTimeList)
    return edges


def test_filter_start_end_time_edges(start_end_time_edges: StartEndTimeList, workorder: EquipmentUnitClient) -> None:
    sorted_by_start_time = sorted(start_end_time_edges, key=lambda x: x.start_time)

    filtered = workorder.unit_procedure.work_units.list(min_start_time=sorted_by_start_time[1].start_time, limit=-1)

    assert len(filtered) == len(sorted_by_start_time) - 1


def test_apply_unit_procedure_with_edge(workorder: EquipmentUnitClient) -> None:
    new_procedure = UnitProcedureApply(
        external_id="procedure:new_procedure",
        name="New procedure",
        type_="New type",
        work_units=[
            StartEndTimeApply(
                start_time="2021-01-01T00:00:00Z",
                end_time="2021-01-01T00:00:00Z",
                equipment_module=EquipmentModuleApply(
                    external_id="module:new_module",
                    name="New module",
                    type_="New type",
                    sensor_value="timeseries:123",
                    description="New description",
                ),
            ),
        ],
    )

    workorder.unit_procedure.apply(new_procedure)
