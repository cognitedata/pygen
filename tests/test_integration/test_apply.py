from __future__ import annotations

import pytest
from cognite.client import CogniteClient

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
    from omni import data_classes as dc
else:
    from omni_pydantic_v1 import OmniClient
    from omni_pydantic_v1 import data_classes as dc


def test_node_without_properties(omni_client: OmniClient, cognite_client: CogniteClient) -> None:
    # Arrange
    test_name = "integration_test:NodeWithoutProperties"
    new_connection_c = dc.ConnectionItemCApply(
        external_id=f"{test_name}:ConnectionPair",
        connection_item_a=[
            dc.ConnectionItemAApply(
                external_id=f"{test_name}:ConnectionPair:A",
                name="ConnectionPair:A",
            )
        ],
        connection_item_b=[
            dc.ConnectionItemBApply(
                external_id=f"{test_name}:ConnectionPair:B",
                name="ConnectionPair:B",
            )
        ],
    )
    created: dc.ResourcesApplyResult | None = None
    try:
        # Act
        created = omni_client.connection_item_c.apply(new_connection_c)

        # Assert
        assert len(created.nodes) == 3
        assert len(created.edges) == 2

        # Act
        retrieved = omni_client.connection_item_c.retrieve(new_connection_c.external_id)

        # Assert
        assert retrieved.external_id == new_connection_c.external_id

        pytest.skip("Edge case not supported yet")
        # The issue is that there are two edges of the same type. The way we could distinguish between them
        # is to use a hasData filter on the end node.
        assert retrieved.connection_item_a[0] == new_connection_c.connection_item_a[0].external_id
        assert retrieved.connection_item_b[0] == new_connection_c.connection_item_b[0].external_id
    finally:
        if created is not None:
            cognite_client.data_modeling.instances.delete(
                created.nodes.as_ids(),
                created.edges.as_ids(),
            )


@pytest.mark.skip("Known bug, logged as an issue")
def test_person_apply_multiple_requests(movie_client: OmniClient) -> None:
    # Arrange
    person = dc.PersonApply(
        external_id="person1",
        name="Person 1",
        birth_year=1990,
        roles=[
            dc.RoleApply(
                external_id="actor1",
                person="person1",
                won_oscar=True,
                movies=[
                    dc.MovieApply(
                        external_id="movie1",
                        title="Movie 1",
                        release_year=2020,
                        actors=["actor1"],
                        run_time_minutes=120,
                    )
                ],
            ),
        ],
    )

    limit = movie_client.person._client.data_modeling.instances._CREATE_LIMIT
    try:
        movie_client.person._client.data_modeling.instances._CREATE_LIMIT = 1

        # Act
        movie_client.person.apply(person)
    finally:
        movie_client.person._client.data_modeling.instances._CREATE_LIMIT = limit

    instances = person.to_instances_apply()
    movie_client.person._client.data_modeling.instances.delete(instances.nodes.as_ids(), instances.edges.as_ids())


def test_apply_recursive(omni_client: OmniClient, cognite_client: CogniteClient) -> None:
    # Arrange
    test_name = "integration_test:ApplyRecursive"
    new_connection_a = dc.ConnectionItemAApply(
        external_id=f"{test_name}:Connection:A",
        name="Connection:A",
        other_direct=dc.ConnectionItemCApply(
            external_id=f"{test_name}:Connection:C",
            connection_item_a=[],
            connection_item_b=[],
        ),
        self_direct=dc.ConnectionItemAApply(
            external_id=f"{test_name}:Connection:OtherA",
            name="Connection:OtherA",
        ),
        outwards=[
            dc.ConnectionItemBApply(
                external_id=f"{test_name}:Connection:B1",
                name="Connection:B1",
                self_edge=[
                    dc.ConnectionItemBApply(
                        external_id=f"{test_name}:Connection:B3",
                        name="Connection:B3",
                    ),
                ],
            ),
            dc.ConnectionItemBApply(
                external_id=f"{test_name}:Connection:B2",
                name="Connection:B2",
            ),
        ],
    )

    resources = new_connection_a.to_instances_apply()
    node_ids = resources.nodes.as_ids()
    edge_ids = resources.edges.as_ids()

    try:
        # Act
        created = omni_client.connection_item_a.apply(new_connection_a)

        # Assert
        assert len(created.nodes) == 6
        assert len(created.edges) == 3

        # Act
        retrieved = omni_client.connection_item_a.retrieve(new_connection_a.external_id)

        # Assert
        assert retrieved.external_id == new_connection_a.external_id
        assert retrieved.name == new_connection_a.name
        assert retrieved.other_direct == new_connection_a.other_direct.external_id
        assert retrieved.self_direct == new_connection_a.self_direct.external_id
        assert len(retrieved.outwards) == 2
    finally:
        cognite_client.data_modeling.instances.delete(nodes=node_ids, edges=edge_ids)
