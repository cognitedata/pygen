from cognite.pygen._core.generators import MultiAPIGenerator, to_unique_parents_by_view_id


def test_to_parents_by_view_id_omni(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    expected: dict[str, list[str]] = {
        "Implementation1": ["SubInterface"],
        "Implementation1NonWriteable": ["SubInterface"],
        "Implementation2": ["SubInterface"],
        "SubInterface": ["MainInterface"],
    }
    views = [api.view for api in omni_multi_api_generator.api_by_view_id.values()]

    # Act
    actual = to_unique_parents_by_view_id(views)
    # Simplification of output for easy assertion.
    actual = {
        view_id.external_id: parent_names
        for view_id, parents in actual.items()
        if (parent_names := [parent.external_id for parent in parents])
    }

    # Assert
    assert actual == expected
