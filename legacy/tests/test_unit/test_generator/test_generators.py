from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import MultiAPIGenerator, to_unique_parents_by_view_id


def test_to_parents_by_view_id_omni(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected: dict[str, list[str]] = {
        "Implementation1": ["SubInterface"],
        "Implementation1NonWriteable": ["SubInterface"],
        "Implementation2": ["SubInterface"],
        "SubInterface": ["MainInterface"],
    }
    views = [api.view for api in omni_multi_api_generator.api_by_type_by_view_id["node"].values()]

    # Act
    unique_parents_by_view_id = to_unique_parents_by_view_id(views)
    # Simplification of output for easy assertion.
    actual = {
        view_id.external_id: parent_names
        for view_id, parents in unique_parents_by_view_id.items()
        if (parent_names := [parent.external_id for parent in parents])
    }

    # Assert
    assert actual == expected


def test_view_implementing_view_outside_model() -> None:
    views = [
        dm.View(
            space="some_space",
            external_id="View1",
            version="1",
            properties={},
            last_updated_time=0,
            created_time=0,
            description="",
            name="",
            filter=None,
            implements=[dm.ViewId("some_space", "Unknown_to_model", "1")],
            writable=True,
            used_for="node",
            is_global=False,
        )
    ]

    actual = to_unique_parents_by_view_id(views)

    assert actual == {views[0].as_id(): []}
