from typing import Any

import pytest
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import Token
from cognite.client.data_classes.aggregations import (
    AggregatedNumberedValue,
    CountValue,
    MaxValue,
    MinValue,
)
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResult, InstanceAggregationResultList

from cognite.pygen._query.interface import QueryExecutor
from cognite.pygen._version import __version__


class TestQueryInterface:
    @pytest.mark.parametrize(
        "results, expected_merged",
        [
            pytest.param([[CountValue("externalId", 5)]], {"count": {"externalId": 5}}, id="Single aggregated results"),
            pytest.param(
                [[CountValue("externalId", 5)], [CountValue("externalId", 10)]],
                {"count": {"externalId": 15}},
                id="Two count results",
            ),
            pytest.param(
                [[MinValue("maxPressure", 10.0)], [MinValue("maxPressure", 20.0)]],
                {"min": {"maxPressure": 10.0}},
                id="Two min results",
            ),
            pytest.param(
                [[MaxValue("maxPressure", 10.0)], [MaxValue("maxPressure", 20.0)]],
                {"max": {"maxPressure": 20.0}},
                id="Two max results",
            ),
            pytest.param(
                [
                    [MaxValue("maxPressure", 10.0), CountValue("externalId", 5)],
                    [MaxValue("maxPressure", 20.0), CountValue("externalId", 10)],
                ],
                {"max": {"maxPressure": 20.0}, "count": {"externalId": 15}},
                id="Multiple max and count results",
            ),
        ],
    )
    def test_merge_aggregate_results(
        self, results: list[list[AggregatedNumberedValue]], expected_merged: dict[str, dict[str, float | int | None]]
    ) -> None:
        actual = QueryExecutor._merge_aggregate_results(results)

        assert actual == expected_merged, f"Expected: {expected_merged}, but got: {actual}"

    @pytest.mark.parametrize(
        "results, expected_merged",
        [
            pytest.param(
                [
                    InstanceAggregationResultList(
                        [
                            InstanceAggregationResult([CountValue("externalId", 5)], group={"myBoolProperty": True}),
                            InstanceAggregationResult([CountValue("externalId", 10)], group={"myBoolProperty": False}),
                        ]
                    )
                ],
                [
                    {"count": {"externalId": 5}, "group": {"myBoolProperty": True}},
                    {"count": {"externalId": 10}, "group": {"myBoolProperty": False}},
                ],
                id="Single groupby aggregated results",
            ),
            pytest.param(
                [
                    InstanceAggregationResultList(
                        [
                            InstanceAggregationResult([CountValue("externalId", 5)], group={"myBoolProperty": True}),
                            InstanceAggregationResult([CountValue("externalId", 10)], group={"myBoolProperty": False}),
                        ]
                    ),
                    InstanceAggregationResultList(
                        [InstanceAggregationResult([CountValue("externalId", 20)], group={"myBoolProperty": True})]
                    ),
                ],
                [
                    {"count": {"externalId": 25}, "group": {"myBoolProperty": True}},
                    {"count": {"externalId": 10}, "group": {"myBoolProperty": False}},
                ],
                id="Two groupby count results",
            ),
        ],
    )
    def test_merge_groupby_aggregate_results(
        self, results: list[InstanceAggregationResultList], expected_merged: list[dict[str, Any]]
    ) -> None:
        actual = QueryExecutor._merge_groupby_aggregate_results(results)

        assert actual == expected_merged, f"Expected: {expected_merged}, but got: {actual}"

    def test_not_prefix_existing_client(self) -> None:
        client_name = f"CognitePygen:{__version__}:QueryExecutor"
        client = CogniteClient(
            ClientConfig(
                client_name,
                project="test_project",
                credentials=Token("223ienie"),
            )
        )

        _ = QueryExecutor(client)

        assert client.config.client_name == client_name, "Client name should not be prefixed again."
