from cognite.pygen._generation.example_model import EXTERNAL_ID, SPACE, VERSION, example_data_model


def test_example_data_model_is_valid() -> None:
    assert example_data_model.space == SPACE
    assert example_data_model.external_id == EXTERNAL_ID
    assert example_data_model.version == VERSION
    assert len(example_data_model.views or []) == 3
