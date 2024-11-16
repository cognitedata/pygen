from typing import Any

import pytest
from cognite.client import data_modeling as dm
from wind_turbine import data_classes as wdc
from wind_turbine._api._core import GraphQLQueryResponse


def parse_graphql_query():
    result = {"listBlade": {"items": [{"__typename": "Blade", "name": "A"}]}}

    yield pytest.param(result, wdc.GraphQLList([wdc.BladeGraphQL(name="A")]), id="listBlade")

    result = {
        "listWindTurbine": {
            "items": [
                {
                    "__typename": "WindTurbine",
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
                wdc.WindTurbineGraphQL(
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
        id="listWindTurbine with nested blades and sensor_positions",
    )

    result = {
        "listWindTurbine": {
            "items": [
                {
                    "__typename": "WindTurbine",
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
                wdc.WindTurbineGraphQL(
                    name="hornsea_1_mill_3",
                    capacity=7,
                    nacelle=wdc.NacelleGraphQL(external_id="nacellewrite:1"),
                )
            ]
        ),
        id="listWindTurbine with direct relation",
    )
    result = {
        "listWindTurbine": {
            "items": [
                {
                    "__typename": "WindTurbine",
                    "name": "hornsea_1_mill_3",
                    "capacity": 7,
                    "createdTime": "2023-12-25T07:47:50.040Z",
                }
            ]
        }
    }
    yield pytest.param(
        result,
        wdc.GraphQLList(
            [
                wdc.WindTurbineGraphQL(
                    name="hornsea_1_mill_3",
                    capacity=7,
                    dataRecord=wdc.DataRecordGraphQL(created_time="2023-12-25T07:47:50.040Z"),
                )
            ]
        ),
        id="listWindmill with createdTime",
    )


class TestGraphQLQuery:
    @pytest.mark.parametrize("result, expected", parse_graphql_query())
    def test_parse_query(self, result: dict[str, Any], expected: wdc.GraphQLList) -> None:
        actual = GraphQLQueryResponse(dm.DataModelId("sp_pygen_power", "WindTurbine", "1")).parse(result)
        assert actual == expected

    def test_parse_query_with_error(self) -> None:
        result = {"errors": [{"message": "Error message"}]}
        with pytest.raises(RuntimeError) as exc_info:
            GraphQLQueryResponse(dm.DataModelId("sp_pygen_power", "WindTurbine", "1")).parse(result)
        assert exc_info.match("Error message")

    def test_parse_query_without_typename(self) -> None:
        result = {"listWindmill": {"items": [{"name": "hornsea_1_mill_3", "capacity": 7}]}}
        with pytest.raises(RuntimeError) as exc_info:
            GraphQLQueryResponse(dm.DataModelId("sp_pygen_power", "WindTurbine", "1")).parse(result)
        assert exc_info.match("Missing '__typename' in GraphQL response. Cannot determine the type of the response.")
