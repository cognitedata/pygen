from cognite.client import data_modeling as dm

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from equipment_unit.client._api._core import QueryBuilder, QueryStep
    from equipment_unit.client.data_classes import (
        StartEndTime,
        UnitProcedure,
        UnitProcedureList,
    )

else:
    from equipment_unit_pydantic_v1.client._api._core import QueryBuilder, QueryStep
    from equipment_unit_pydantic_v1.client.data_classes import (
        StartEndTime,
        UnitProcedure,
        UnitProcedureList,
    )


class TestQueryBuilderT:
    def test_unpack(self):
        edges = dm.EdgeList.load(
            """- instanceType: edge
  space: IntegrationTestsImmutable
  externalId: unit_procedure:Matthew Gonzalez:equipment_module:Hannah Mcgee
  version: 1
  lastUpdatedTime: 1699904583123
  createdTime: 1699904583123
  properties:
    IntegrationTestsImmutable:
      StartEndTime/d416e0ed98186b:
        end_time: '2023-11-11T14:33:45+00:00'
        start_time: '2023-11-11T06:48:59+00:00'
  type:
    space: IntegrationTestsImmutable
    externalId: UnitProcedure.equipment_module
  startNode:
    space: IntegrationTestsImmutable
    externalId: unit_procedure:Matthew Gonzalez
  endNode:
    space: IntegrationTestsImmutable
    externalId: equipment_module:Hannah Mcgee
- instanceType: edge
  space: IntegrationTestsImmutable
  externalId: unit_procedure:Matthew Gonzalez:equipment_module:Arthur Simon
  version: 1
  lastUpdatedTime: 1699904583123
  createdTime: 1699904583123
  properties:
    IntegrationTestsImmutable:
      StartEndTime/d416e0ed98186b:
        end_time: '2023-11-09T21:28:35+00:00'
        start_time: '2023-11-07T19:10:55+00:00'
  type:
    space: IntegrationTestsImmutable
    externalId: UnitProcedure.equipment_module
  startNode:
    space: IntegrationTestsImmutable
    externalId: unit_procedure:Matthew Gonzalez
  endNode:
    space: IntegrationTestsImmutable
    externalId: equipment_module:Arthur Simon
- instanceType: edge
  space: IntegrationTestsImmutable
  externalId: unit_procedure:Matthew Gonzalez:equipment_module:Rachel Moore
  version: 1
  lastUpdatedTime: 1699904583123
  createdTime: 1699904583123
  properties:
    IntegrationTestsImmutable:
      StartEndTime/d416e0ed98186b:
        end_time: '2023-10-25T06:17:41+00:00'
        start_time: '2023-10-23T12:17:00+00:00'
  type:
    space: IntegrationTestsImmutable
    externalId: UnitProcedure.equipment_module
  startNode:
    space: IntegrationTestsImmutable
    externalId: unit_procedure:Matthew Gonzalez
  endNode:
    space: IntegrationTestsImmutable
    externalId: equipment_module:Rachel Moore
- instanceType: edge
  space: IntegrationTestsImmutable
  externalId: unit_procedure:Matthew Gonzalez:equipment_module:Veronica Brewer
  version: 1
  lastUpdatedTime: 1699904583123
  createdTime: 1699904583123
  properties:
    IntegrationTestsImmutable:
      StartEndTime/d416e0ed98186b:
        end_time: '2023-11-11T01:58:51+00:00'
        start_time: '2023-10-26T14:18:04+00:00'
  type:
    space: IntegrationTestsImmutable
    externalId: UnitProcedure.equipment_module
  startNode:
    space: IntegrationTestsImmutable
    externalId: unit_procedure:Matthew Gonzalez
  endNode:
    space: IntegrationTestsImmutable
    externalId: equipment_module:Veronica Brewer
"""
        )
        node = dm.Node.load(
            """instanceType: node
space: IntegrationTestsImmutable
externalId: unit_procedure:Matthew Gonzalez
version: 1
lastUpdatedTime: 1699904583123
createdTime: 1699904583123
properties:
  IntegrationTestsImmutable:
    UnitProcedure/f16810a7105c44:
      name: Matthew Gonzalez
      type: red
"""
        )

        builder = QueryBuilder(
            UnitProcedureList,
            [
                QueryStep(
                    name="unit_procedure",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=None,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                dm.ViewId("IntegrationTestsImmutable", "UnitProcedure", "f16810a7105c44"), []
                            )
                        ]
                    ),
                    result_cls=UnitProcedure,
                    max_retrieve_limit=-1,
                ),
                QueryStep(
                    name="work_units",
                    expression=dm.query.EdgeResultSetExpression(from_="unit_procedure", filter=None),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                dm.ViewId("IntegrationTestsImmutable", "StartEndTime", "d416e0ed98186b"), []
                            )
                        ]
                    ),
                    result_cls=StartEndTime,
                    max_retrieve_limit=-1,
                ),
            ],
        )
        builder[0].results.append(node)
        builder[1].results.extend(edges)

        unpacked = builder.unpack()

        assert isinstance(unpacked, UnitProcedureList)
        assert len(unpacked) == 1
        for procedure in unpacked:
            assert isinstance(procedure, UnitProcedure)
            for work_unit in procedure.work_units:
                assert isinstance(work_unit, StartEndTime)
