import pytest

from cognite.pygen._client.models import DataModelResponseWithViews
from cognite.pygen._example_datamodel import (
    EXTERNAL_ID,
    SPACE,
    VERSION,
    category_container,
    category_view,
    example_data_model,
    product_container,
    product_view,
    relates_to_container,
    relates_to_view,
)


@pytest.fixture(scope="module")
def example_data_model_response() -> DataModelResponseWithViews:
    """Provides an example DataModelResponseWithViews for testing."""
    model = example_data_model.model_dump(by_alias=True)
    views = [
        category_view.model_dump(by_alias=True),
        product_view.model_dump(by_alias=True),
        relates_to_view.model_dump(by_alias=True),
    ]
    for item in [model, *views]:
        # Add server set properties
        item["createdTime"] = 1625247600000
        item["lastUpdatedTime"] = 1625247600000
        item["isGlobal"] = False
    for view, container in zip(views, [category_container, product_container, relates_to_container], strict=False):
        # Convert ViewRequests to ViewResponses
        view["writable"] = True
        view["queryable"] = True
        view["usedFor"] = container.used_for
        view["mappedContainers"] = [container.as_reference().model_dump(by_alias=True)]
        for prop in view["properties"].values():
            container_prop_id = prop.get("containerPropertyIdentifier")
            if container_prop_id:
                container_prop = container.properties[container_prop_id]
                prop["type"] = container_prop.type.model_dump(by_alias=True)
                prop["constraintState"] = {
                    "nullability": "current",
                    "maxListSize": "current",
                    "maxTextSize": "current",
                }
            elif prop.get("connectionType") in ("single_reverse_direct_relation", "multi_reverse_direct_relation"):
                prop["targetsList"] = True
    return DataModelResponseWithViews.model_validate({**model, "views": views})


def test_example_data_model(example_data_model_response: DataModelResponseWithViews) -> None:
    """Test to ensure the example data model fixture works correctly."""
    assert example_data_model.external_id == EXTERNAL_ID
    assert example_data_model.space == SPACE
    assert example_data_model.version == VERSION
    assert len(example_data_model.views or []) == 3
