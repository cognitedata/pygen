from typing import Any

import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Properties
from cognite.client.testing import monkeypatch_cognite_client
from cognite_core import CogniteCoreClient
from omni import OmniClient
from omni import data_classes as dc
from omni._api._core import instantiate_classes
from omni.config import global_config
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
        id="listWindTurbine with createdTime",
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
        result = {"listWindTurbine": {"items": [{"name": "hornsea_1_mill_3", "capacity": 7}]}}
        with pytest.raises(RuntimeError) as exc_info:
            GraphQLQueryResponse(dm.DataModelId("sp_pygen_power", "WindTurbine", "1")).parse(result)
        assert exc_info.match("Missing '__typename' in GraphQL response. Cannot determine the type of the response.")


class TestAPIClass:
    def test_skip_validation(self) -> None:
        with monkeypatch_cognite_client() as mock_client:
            mock_client.data_modeling.instances.list.return_value = dm.NodeList[dm.Node](
                [
                    dm.Node(
                        space="my_space",
                        external_id="invalid_node",
                        version=1,
                        last_updated_time=1,
                        created_time=1,
                        deleted_time=None,
                        type=None,
                        properties=Properties(
                            {
                                dm.ViewId("sp_pygen_models", "PrimitiveRequired", "1"): {
                                    # Invalid as multiple required fields are missing
                                    # And the type of int64 is wrong
                                    "boolean": True,
                                    "int32": 10,
                                    "int64": "invalid_value",
                                }
                            }
                        ),
                    )
                ]
            )
            pygen = OmniClient(mock_client)
            with pytest.raises(ValueError):
                _ = pygen.primitive_required.list()
            try:
                global_config.validate_retrieve = False
                invalid = pygen.primitive_required.list()
            finally:
                global_config.validate_retrieve = True
            # Skipping data record and node type to focus on the properties:
            assert invalid[0].model_dump(exclude={"data_record", "node_type"}) == {
                "boolean": True,
                "external_id": "invalid_node",
                "int_32": 10,
                "int_64": "invalid_value",
                "space": "my_space",
            }

    def test_retrieve_extra_arguments(self) -> None:
        with monkeypatch_cognite_client() as mock_client:
            mock_client.data_modeling.instances.list.return_value = dm.NodeList[dm.Node](
                [
                    dm.Node(
                        space="my_space",
                        external_id="my_node",
                        version=1,
                        last_updated_time=1,
                        created_time=1,
                        deleted_time=None,
                        type=None,
                        properties=Properties(
                            {
                                dm.ViewId("sp_pygen_models", "PrimitiveNullable", "1"): {
                                    "boolean": True,
                                    "int32": 10,
                                    "new_field": "extra_field",
                                }
                            }
                        ),
                    )
                ]
            )
            pygen = OmniClient(mock_client)
            listed = pygen.primitive_nullable.list()
            assert len(listed) == 1
            node = listed[0]
            assert node.model_dump(exclude={"data_record", "node_type"}, exclude_unset=True) == {
                "boolean": True,
                "external_id": "my_node",
                "int_32": 10,
                "space": "my_space",
                "new_field": "extra_field",
            }

    def test_retrieve_unknown_enum(self) -> None:
        with monkeypatch_cognite_client() as mock_client:
            mock_client.data_modeling.instances.list.return_value = dm.NodeList[dm.Node](
                [
                    dm.Node(
                        space="my_space",
                        external_id="my_node",
                        version=1,
                        last_updated_time=1,
                        created_time=1,
                        deleted_time=None,
                        type=None,
                        properties=Properties(
                            {
                                dm.ViewId("cdf_cdm", "CogniteTimeSeries", "v1"): {
                                    "name": "my_ts",
                                    "isStep": False,
                                    "type": "new-status",
                                }
                            }
                        ),
                    )
                ]
            )
            pygen = CogniteCoreClient(mock_client)
            listed = pygen.cognite_time_series.list()
            assert len(listed) == 1
            node = listed[0]
            assert node.model_dump(exclude={"data_record", "node_type"}, exclude_unset=True, by_alias=True) == {
                "externalId": "my_node",
                "name": "my_ts",
                "isStep": False,
                "type": "new-status",
                "space": "my_space",
            }


class TestInstantiateClasses:
    def test_raise_multiple(self) -> None:
        raw = [
            {
                "space": "my_space",
                "externalId": "first-invalid",
                "float32": 1.0,
            },
            {
                "space": "my_space",
                "externalId": "second-invalid",
                "float32": 2.0,
                "float64": "not_valid_float",
            },
            {
                "space": "my_space",
                "externalId": "third-valid",
                "data_record": {
                    "version": 3,
                    "lastUpdatedTime": 3,
                    "createdTime": 3,
                },
                "float32": 3.0,
                "float64": 3.0,
                "int32": 3,
                "int64": 3,
                "text": "text",
                "boolean": True,
                "date": "2025-01-01",
                "timestamp": "2025-01-01T00:00:00Z",
                "json": {"key": "value"},
            },
        ]

        with pytest.raises(ValueError) as exc_info:
            instantiate_classes(dc.PrimitiveRequired, raw, "retrieve")
        assert exc_info.match("Failed to retrieve 'PrimitiveRequired', 2 out of 3 instances failed validation.")
