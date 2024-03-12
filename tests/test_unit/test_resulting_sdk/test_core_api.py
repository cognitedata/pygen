from typing import Any

import pytest
from cognite.client import data_modeling as dm

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from equipment_unit.client._api._core import QueryBuilder, QueryStep
    from equipment_unit.client.data_classes import (
        StartEndTime,
        UnitProcedure,
        UnitProcedureList,
    )
    from windmill import data_classes as wdc
    from windmill._api._core import GraphQLQueryResponse
else:
    from equipment_unit_pydantic_v1.client._api._core import QueryBuilder, QueryStep
    from equipment_unit_pydantic_v1.client.data_classes import (
        StartEndTime,
        UnitProcedure,
        UnitProcedureList,
    )
    from windmill_pydantic_v1 import data_classes as wdc
    from windmill_pydantic_v1._api._core import GraphQLQueryResponse


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


def parse_graphql_query():
    result = {"listBlade": {"items": [{"__typename": "Blade", "name": "A"}]}}

    yield pytest.param(result, wdc.GraphQLList([wdc.BladeGraphQL(name="A")]), id="listBlade")

    result = {
        "listWindmill": {
            "items": [
                {
                    "__typename": "Windmill",
                    "name": "hornsea_1_mill_3",
                    "capacity": 7,
                    "blades": {
                        "items": [
                            {
                                "__typename": "Blade",
                                "name": "A",
                                "is_damaged": False,
                                "sensor_positions": {
                                    "items": [
                                        {
                                            "__typename": "SensorPosition",
                                            "position": 19.6,
                                            "flapwise_bend_mom_offset": None,
                                        }
                                    ]
                                },
                            },
                            {
                                "__typename": "Blade",
                                "name": "B",
                                "is_damaged": False,
                                "sensor_positions": {
                                    "items": [
                                        {
                                            "__typename": "SensorPosition",
                                            "position": 1.5,
                                            "flapwise_bend_mom_offset": None,
                                        }
                                    ]
                                },
                            },
                        ]
                    },
                }
            ]
        }
    }

    yield pytest.param(
        result,
        wdc.GraphQLList(
            [
                wdc.WindmillGraphQL(
                    name="hornsea_1_mill_3",
                    capacity=7,
                    blades=[
                        wdc.BladeGraphQL(
                            name="A",
                            is_damaged=False,
                            sensor_positions=[
                                wdc.SensorPositionGraphQL(
                                    position=19.6,
                                    flapwise_bend_mom_offset=None,
                                )
                            ],
                        ),
                        wdc.BladeGraphQL(
                            name="B",
                            is_damaged=False,
                            sensor_positions=[
                                wdc.SensorPositionGraphQL(
                                    position=1.5,
                                    flapwise_bend_mom_offset=None,
                                )
                            ],
                        ),
                    ],
                )
            ]
        ),
        id="listWindmill",
    )

    result = {
        "listWindmill": {
            "items": [
                {
                    "__typename": "Windmill",
                    "name": "hornsea_1_mill_3",
                    "capacity": 7,
                    "nacelle": {"externalId": "nacellewrite:1"},
                }
            ]
        }
    }
    yield pytest.param(
        result,
        wdc.GraphQLList(
            [
                wdc.WindmillGraphQL(
                    name="hornsea_1_mill_3",
                    capacity=7,
                    nacelle=wdc.NacelleGraphQL(external_id="nacellewrite:1"),
                )
            ]
        ),
        id="listWindmill",
    )


class TestGraphQLQuery:
    @pytest.mark.parametrize("result, expected", parse_graphql_query())
    def test_parse_query(self, result: dict[str, Any], expected: wdc.GraphQLList) -> None:
        actual = GraphQLQueryResponse(dm.DataModelId("power-models", "Windmill", "1")).parse(result)
        assert actual == expected

    def test_parse_query_with_error(self) -> None:
        result = {"errors": [{"message": "Error message"}]}
        with pytest.raises(RuntimeError) as exc_info:
            GraphQLQueryResponse(dm.DataModelId("power-models", "Windmill", "1")).parse(result)
        assert exc_info.match("Error message")

    def test_parse_query_without_typename(self) -> None:
        result = {"listWindmill": {"items": [{"name": "hornsea_1_mill_3", "capacity": 7}]}}
        with pytest.raises(RuntimeError) as exc_info:
            GraphQLQueryResponse(dm.DataModelId("power-models", "Windmill", "1")).parse(result)
        assert exc_info.match("Missing '__typename' in GraphQL response. Cannot determine the type of the response.")
