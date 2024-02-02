import pytest
from cognite.client import data_modeling as dm

from cognite.pygen.utils.cdf import _unpack_filter


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
    for _i, (actual_item, expected_item) in enumerate(zip(actual, expected)):
        assert actual_item[0] == expected_item[0]
        assert actual_item[1].dump() == expected_item[1].dump()
