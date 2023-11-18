import pytest

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from equipment_unit.client.data_classes import (
        EquipmentModule,
        EquipmentModuleApply,
        StartEndTime,
        StartEndTimeApply,
        StartEndTimeList,
        UnitProcedureApply,
        UnitProcedureList,
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


@pytest.fixture
def unit_procedure_list(workorder: EquipmentUnitClient) -> UnitProcedureList:
    nodes = workorder.unit_procedure.list(limit=5)
    assert len(nodes) > 2, "There should be at least three node in the list"
    assert isinstance(nodes, UnitProcedureList)
    return nodes


def test_edges_to_pandas(start_end_time_edges: StartEndTimeList) -> None:
    df = start_end_time_edges.to_pandas()
    assert len(df) == len(start_end_time_edges)


def test_unit_procedure_list_to_pandas(unit_procedure_list: UnitProcedureList) -> None:
    df = unit_procedure_list.to_pandas()
    assert len(df) == len(unit_procedure_list)


def test_filter_start_end_time_edges(start_end_time_edges: StartEndTimeList, workorder: EquipmentUnitClient) -> None:
    sorted_by_start_time = sorted(start_end_time_edges, key=lambda x: x.start_time)

    filtered = workorder.unit_procedure.work_units.list(min_start_time=sorted_by_start_time[1].start_time, limit=-1)

    assert len(filtered) == len(sorted_by_start_time) - 1


def test_filter_unit_procedure_through_edge(workorder: EquipmentUnitClient) -> None:
    unit_procedures = workorder.unit_procedure.work_units(type_="red", limit=3).list(
        retrieve_equipment_module=True, limit=-1
    )

    assert 1 <= len(unit_procedures) <= 3
    assert all(procedure.type_ == "red" for procedure in unit_procedures)
    for unit_procedure in unit_procedures:
        for work_unit in unit_procedure.work_units:
            assert isinstance(work_unit, StartEndTime)
            assert isinstance(work_unit.equipment_module, EquipmentModule)


@pytest.mark.skip(reason="Not implemented")
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
    try:
        workorder.unit_procedure.apply(new_procedure)
    finally:
        workorder.unit_procedure.delete("procedure:new_procedure")
        workorder.equipment_module.delete("module:new_module")
