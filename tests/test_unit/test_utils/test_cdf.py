from __future__ import annotations

from typing import Any

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen.utils.cdf import _find_first_node_type, _reduce_model, _unpack_filter
from tests.constants import CORE_SDK


@pytest.mark.parametrize(
    "filter_, expected",
    [
        pytest.param(
            dm.filters.Equals(["node", "type"], {"space": "some_space", "externalId": "someId"}),
            [([], dm.filters.Equals(["node", "type"], {"space": "some_space", "externalId": "someId"}))],
            id="Single Equals filter",
        ),
        pytest.param(
            dm.filters.And(
                dm.filters.Equals(["node", "type"], {"space": "some_space", "externalId": "someId"}),
                dm.filters.HasData(containers=[("containerSpace", "containerId")]),
            ),
            [
                (
                    [dm.filters.And],
                    dm.filters.Equals(["node", "type"], {"space": "some_space", "externalId": "someId"}),
                ),
                ([dm.filters.And], dm.filters.HasData(containers=[("containerSpace", "containerId")])),
            ],
            id="And filter",
        ),
        pytest.param(
            dm.filters.And(
                dm.filters.Not(dm.filters.Equals(["node", "type"], {"space": "some_space"})),
                dm.filters.Or(
                    dm.filters.Prefix("externalId", "start_with"),
                    dm.filters.Range(["mySpace", "myView/1", "value"], gt=3),
                ),
            ),
            [
                (
                    [dm.filters.And, dm.filters.Not],
                    dm.filters.Equals(["node", "type"], {"space": "some_space"}),
                ),
                (
                    [dm.filters.And, dm.filters.Or],
                    dm.filters.Prefix("externalId", "start_with"),
                ),
                (
                    [dm.filters.And, dm.filters.Or],
                    dm.filters.Range(["mySpace", "myView/1", "value"], gt=3),
                ),
            ],
            id="Complex filter (nested with And, Or and Not)",
        ),
    ],
)
def test__unpack_filter(filter_: dm.Filter, expected) -> None:
    actual = _unpack_filter(filter_)

    assert len(actual) == len(expected)
    for actual_item, expected_item in zip(actual, expected, strict=False):
        assert actual_item[0] == expected_item[0]
        assert actual_item[1].dump() == expected_item[1].dump()


@pytest.mark.parametrize(
    "filter_, expected",
    [
        (None, None),
        (
            dm.filters.Equals(["node", "type"], {"space": "mySpace", "externalId": "myType"}),
            dm.DirectRelationReference("mySpace", "myType"),
        ),
        (
            dm.filters.And(dm.filters.Equals(["node", "type"], {"space": "mySpace", "externalId": "myType"})),
            dm.DirectRelationReference("mySpace", "myType"),
        ),
        (
            dm.filters.Equals(["node", "space"], "mySpace"),
            None,
        ),
        (dm.filters.Not(dm.filters.Equals(["node", "type"], {"space": "mySpace", "externalId": "myType"})), None),
    ],
)
def test_find_node_type(filter_: dm.Filter | None, expected: dm.DirectRelationReference | None) -> None:
    # Act
    actual = _find_first_node_type(filter_)

    # Assert
    assert actual == expected


class TestReduceModel:
    def test_reduce_model_exclude_views(self) -> None:
        core = CORE_SDK.load_data_model()
        view_by_external_id = {view.external_id: view for view in core.views}
        exclude_views: set[str | dm.ViewId] = {
            view.external_id for view in core.views if view.external_id not in {"CogniteAsset", "CogniteActivity"}
        }

        reduced_model = _reduce_model(core, exclude_views=exclude_views)

        actual_by_external_id = {view.external_id: view for view in reduced_model.views}
        assert set(actual_by_external_id.keys()) == {"CogniteAsset", "CogniteActivity"}
        describable_props = set(view_by_external_id["CogniteDescribable"].properties.keys())
        sourceable_props = set(view_by_external_id["CogniteSourceable"].properties.keys()) - {"source"}
        assert set(actual_by_external_id["CogniteAsset"].properties.keys()) == (
            describable_props
            | sourceable_props
            | {
                "activities",
                "parent",
                "path",
                "pathLastUpdatedTime",
                "root",
                "children",
            }
        )
        assert actual_by_external_id["CogniteActivity"].properties.keys() == describable_props | sourceable_props | {
            "assets",
            "endTime",
            "startTime",
            "scheduledStartTime",
            "scheduledEndTime",
        }

    def test_reduce_model_exclude_space(self) -> None:
        default_prop_args: dict[str, Any] = dict(
            nullable=True,
            immutable=False,
            auto_increment=False,
            default_value=None,
            name=None,
            description=None,
        )
        default_view_args: dict[str, Any] = dict(
            last_updated_time=1,
            created_time=1,
            name=None,
            description=None,
            implements=None,
            writable=True,
            used_for="node",
            is_global=False,
            filter=None,
        )
        first = dm.View(
            space="mySpace",
            external_id="FirstView",
            version="v1",
            properties={
                "prop1": dm.MappedProperty(
                    dm.ContainerId("mySpace", "myContainer"),
                    "myProperty",
                    dm.Text(),
                    source=None,
                    **default_prop_args,
                ),
                "prop2": dm.MappedProperty(
                    dm.ContainerId("mySpace", "myContainer"),
                    "myProperty",
                    dm.DirectRelation(),
                    source=dm.ViewId("otherSpace", "SecondView", "v1"),
                    **default_prop_args,
                ),
            },
            **default_view_args,
        )
        second = dm.View(
            space="otherSpace",
            external_id="SecondView",
            version="v1",
            properties={
                "prop1": dm.MappedProperty(
                    dm.ContainerId("otherSpace", "myContainer"),
                    "myProperty",
                    dm.Text(),
                    source=None,
                    **default_prop_args,
                ),
            },
            **default_view_args,
        )
        model = dm.DataModel[dm.View](
            space="mySpace",
            external_id="myModel",
            version="v1",
            is_global=False,
            last_updated_time=1,
            created_time=1,
            name=None,
            description=None,
            views=[first, second],
        )

        reduced = _reduce_model(model=model, exclude_spaces={"otherSpace"})

        assert len(reduced.views) == 1
        assert reduced.views[0].as_id() == first.as_id()
        assert set(reduced.views[0].properties.keys()) == {"prop1"}
