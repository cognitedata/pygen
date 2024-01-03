from cognite.pygen._core.generators import MultiAPIGenerator, to_parents_by_view_id


def test_to_parents_by_view_id(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected: dict[str, list[str]] = {
        "Implementation1": ["SubInterface"],
        "Implementation1NonWriteable": ["SubInterface"],
        "Implementation2": ["SubInterface"],
        "SubInterface": ["MainInterface"],
    }
    views = [api.view for api in omni_multi_api_generator.api_by_view_id.values()]
    data_class_by_view_id = {api.view_id: api.data_class for api in omni_multi_api_generator.api_by_view_id.values()}
    interfaces = {parent for view in views for parent in view.implements or []}

    # Act
    actual = to_parents_by_view_id(views, data_class_by_view_id, interfaces)
    # Simplification of output for easy assertion.
    actual = {
        view_id.external_id: parent_names
        for view_id, parents in actual.items()
        if (parent_names := [parent.read_name for parent in parents])
    }

    # Assert
    assert actual == expected
