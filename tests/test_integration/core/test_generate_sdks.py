from cognite.client import CogniteClient

from cognite.pygen import SDKGenerator


def test_generate_movie_sdk(cognite_client: CogniteClient) -> None:
    # Arrange
    data_models = cognite_client.data_modeling.data_models.retrieve(
        ("IntegrationTestsImmutable", "Movie", "2"), inline_views=True
    )
    assert data_models, "Please add a data model with id (IntegrationTestsImmutable, Movie, 2) to the test environment"
    data_model = data_models[0]
    generator = SDKGenerator("movie_domain", "Movie")

    # Act
    sdk = generator.generate_sdk(data_model)

    # Assert
    assert sdk


def test_generate_shop_sdk(cognite_client: CogniteClient) -> None:
    # Arrange
    data_model = cognite_client.data_modeling.data_models.retrieve(
        ("IntegrationTestsImmutable", "SHOP_Model", "2"), inline_views=True
    )
    assert (
        data_model
    ), "Please add a data model with id (IntegrationTestsImmutable, SHOP_Model, 2) to the test environment"
    data_model = data_model[0]
    generator = SDKGenerator("shop_domain", "Shop")

    # Act
    sdk = generator.generate_sdk(data_model)

    # Assert
    assert sdk
