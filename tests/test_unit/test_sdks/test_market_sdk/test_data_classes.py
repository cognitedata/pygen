from datetime import datetime, timezone

from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Properties

from tests.constants import IS_PYDANTIC_V1

if IS_PYDANTIC_V1:
    from markets_pydantic_v1.client.data_classes import ValueTransformation
else:
    from markets.client.data_classes import ValueTransformation


def test_from_node_with_json() -> None:
    # Arrange
    node = dm.Node(
        space="market",
        external_id="myExternalId",
        version=1,
        last_updated_time=0,
        created_time=0,
        properties=Properties({dm.ViewId("market", "myView", "1"): {"arguments": {"a": 1}, "method": "myMethod"}}),
        deleted_time=None,
        type=None,
    )
    expected = ValueTransformation(
        external_id="myExternalId",
        version=1,
        last_updated_time=datetime.fromtimestamp(0, tz=timezone.utc),
        created_time=datetime.fromtimestamp(0, tz=timezone.utc),
        arguments={"a": 1},
        method="myMethod",
    )

    # Act
    actual = ValueTransformation.from_node(node)

    # Assert
    assert actual == expected
