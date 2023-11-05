from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._movie import MovieApply
    from ._nomination import NominationApply
    from ._person import PersonApply

__all__ = ["Role", "RoleApply", "RoleList", "RoleApplyList", "RoleFields"]
RoleFields = Literal["won_oscar"]

_ROLE_PROPERTIES_BY_FIELD = {
    "won_oscar": "wonOscar",
}


class Role(DomainModel):
    """This represent a read version of role.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the role.
        movies: The movie field.
        nomination: The nomination field.
        person: The person field.
        won_oscar: The won oscar field.
        created_time: The created time of the role node.
        last_updated_time: The last updated time of the role node.
        deleted_time: If present, the deleted time of the role node.
        version: The version of the role node.
    """

    space: str = "IntegrationTestsImmutable"
    movies: Optional[list[str]] = None
    nomination: Optional[list[str]] = None
    person: Optional[str] = None
    won_oscar: Optional[bool] = Field(None, alias="wonOscar")

    def as_apply(self) -> RoleApply:
        """Convert this read version of role to a write version."""
        return RoleApply(
            space=self.space,
            external_id=self.external_id,
            movies=self.movies,
            nomination=self.nomination,
            person=self.person,
            won_oscar=self.won_oscar,
        )


class RoleApply(DomainModelApply):
    """This represent a write version of role.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the role.
        movies: The movie field.
        nomination: The nomination field.
        person: The person field.
        won_oscar: The won oscar field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    movies: Union[list[MovieApply], list[str], None] = Field(default=None, repr=False)
    nomination: Union[list[NominationApply], list[str], None] = Field(default=None, repr=False)
    person: Union[PersonApply, str, None] = Field(None, repr=False)
    won_oscar: Optional[bool] = Field(None, alias="wonOscar")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.person is not None:
            properties["person"] = {
                "space": "IntegrationTestsImmutable",
                "externalId": self.person if isinstance(self.person, str) else self.person.external_id,
            }
        if self.won_oscar is not None:
            properties["wonOscar"] = self.won_oscar
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Role", "2"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        for movie in self.movies or []:
            edge = self._create_movie_edge(movie)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(movie, DomainModelApply):
                instances = movie._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for nomination in self.nomination or []:
            edge = self._create_nomination_edge(nomination)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(nomination, DomainModelApply):
                instances = nomination._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.person, DomainModelApply):
            instances = self.person._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_movie_edge(self, movie: Union[str, MovieApply]) -> dm.EdgeApply:
        if isinstance(movie, str):
            end_space, end_node_ext_id = self.space, movie
        elif isinstance(movie, DomainModelApply):
            end_space, end_node_ext_id = movie.space, movie.external_id
        else:
            raise TypeError(f"Expected str or MovieApply, got {type(movie)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "Role.movies"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )

    def _create_nomination_edge(self, nomination: Union[str, NominationApply]) -> dm.EdgeApply:
        if isinstance(nomination, str):
            end_space, end_node_ext_id = self.space, nomination
        elif isinstance(nomination, DomainModelApply):
            end_space, end_node_ext_id = nomination.space, nomination.external_id
        else:
            raise TypeError(f"Expected str or NominationApply, got {type(nomination)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "Role.nomination"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )


class RoleList(TypeList[Role]):
    """List of roles in read version."""

    _NODE = Role

    def as_apply(self) -> RoleApplyList:
        """Convert this read version of role to a write version."""
        return RoleApplyList([node.as_apply() for node in self.data])


class RoleApplyList(TypeApplyList[RoleApply]):
    """List of roles in write version."""

    _NODE = RoleApply
