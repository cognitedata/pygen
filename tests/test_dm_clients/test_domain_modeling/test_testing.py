from faker import Faker

from cognite.dm_clients.domain_modeling.testing import ApplyResponse, create_test_client_factory
from examples.cinematography_domain.client import CineClient
from examples.cinematography_domain.schema import Person, cine_schema


def test_create_test_client():
    # Arrange
    fake = Faker()
    person = Person(name=fake.first_name())
    response = ApplyResponse(external_id="Person", was_modified=True)
    responses = [[response.dict(by_alias=True)]]

    # Act
    with create_test_client_factory(CineClient, cine_schema, responses) as test_client:
        created_persons = test_client.apply([person])

    # Assert
    assert created_persons
    assert created_persons[0].name == person.name
