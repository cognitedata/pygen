from cognite.client import CogniteClient

from cognite.pygen import SDKGenerator


def test_generate_movie_sdk(cognite_client: CogniteClient) -> None:
    # Arrange
    data_models = cognite_client.data_modeling.data_models.retrieve(("IntegrationTestsImmutable", "Movie", "2"))
    views = cognite_client.data_modeling.views.retrieve(list(data_models[0].views))
    data_model = data_models[0]
    view_by_id = {view.as_id(): view for view in views}
    data_model.views = [view_by_id[view] for view in data_model.views]
    generator = SDKGenerator("movie_domain", "Movie")

    # Act
    sdk = generator.data_model_to_sdk(data_model)

    # Assert
    assert sdk
