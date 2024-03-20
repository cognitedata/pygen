import pytest
from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from equipment_unit.client.data_classes import (
        EquipmentModule,
        EquipmentModuleWrite,
        StartEndTime,
        StartEndTimeList,
        StartEndTimeWrite,
        UnitProcedureList,
        UnitProcedureWrite,
    )

    from examples.equipment_unit.client import EquipmentUnitClient
else:
    from equipment_unit_pydantic_v1.client import EquipmentUnitClient
    from equipment_unit_pydantic_v1.client.data_classes import (
        EquipmentModule,
        EquipmentModuleWrite,
        StartEndTime,
        StartEndTimeList,
        StartEndTimeWrite,
        UnitProcedureList,
        UnitProcedureWrite,
    )


@pytest.fixture
def start_end_time_edges(workorder: EquipmentUnitClient) -> StartEndTimeList:
    edges = workorder.unit_procedure.work_units_edge.list(limit=-1)
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


def test_single_unit_procedure_to_pandas(unit_procedure_list: UnitProcedureList) -> None:
    procedure = unit_procedure_list[0]

    series = procedure.to_pandas()
    assert len(series) == 8, "We only have the four properties + the external id, space, node type, and data_record"


def test_filter_start_end_time_edges(start_end_time_edges: StartEndTimeList, workorder: EquipmentUnitClient) -> None:
    sorted_by_start_time = sorted(start_end_time_edges, key=lambda x: x.start_time)

    filtered = workorder.unit_procedure.work_units_edge.list(
        min_start_time=sorted_by_start_time[1].start_time, limit=-1
    )

    assert len(filtered) == len(sorted_by_start_time) - 1


def test_filter_unit_procedure_through_edge(workorder: EquipmentUnitClient) -> None:
    unit_procedures = workorder.unit_procedure(type_="red", limit=3).work_units(limit=5).query()

    assert 1 <= len(unit_procedures) <= 3
    assert all(procedure.type_ == "red" for procedure in unit_procedures)
    for unit_procedure in unit_procedures:
        for work_unit in unit_procedure.work_units:
            assert isinstance(work_unit, StartEndTime)
            assert isinstance(work_unit.end_node, EquipmentModule)


def test_apply_unit_procedure_with_edge(workorder: EquipmentUnitClient, cognite_client: CogniteClient) -> None:
    new_procedure = UnitProcedureWrite(
        external_id="procedure:new_procedure",
        name="New procedure",
        type_="New type",
        work_units=[
            StartEndTimeWrite(
                start_time="2021-01-01T00:00:00Z",
                end_time="2021-01-01T00:00:00Z",
                end_node=EquipmentModuleWrite(
                    external_id="module:new_module",
                    name="New module",
                    type_="New type",
                    sensor_value=TimeSeries(
                        external_id="timeseries:123",
                        is_step=True,
                        description="This is a test timeseries, it should not persist",
                    ),
                    description="New description",
                ),
            ),
        ],
    )

    instances = new_procedure.to_instances_write()
    try:
        created = workorder.unit_procedure.apply(new_procedure)

        assert len(created.nodes) == 2
        assert len(created.edges) == 1
        assert len(created.time_series) == 1
    finally:
        cognite_client.data_modeling.instances.delete(instances.nodes.as_ids(), instances.edges.as_ids())
        cognite_client.time_series.delete(external_id=instances.time_series.as_external_ids(), ignore_unknown_ids=True)
