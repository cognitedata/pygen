from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._movie import Movie, MovieApply
    from ._nomination import Nomination, NominationApply
    from ._person import Person, PersonApply


__all__ = ["Actor", "ActorApply", "ActorList", "ActorApplyList", "ActorFields"]
ActorFields = Literal["won_oscar"]

_ACTOR_PROPERTIES_BY_FIELD = {
    "won_oscar": "wonOscar",
}


class Actor(DomainModel):
    """This represents the reading version of actor.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the actor.
        movies: The movie field.
        nomination: The nomination field.
        person: The person field.
        won_oscar: The won oscar field.
        created_time: The created time of the actor node.
        last_updated_time: The last updated time of the actor node.
        deleted_time: If present, the deleted time of the actor node.
        version: The version of the actor node.
    """

    space: str = "IntegrationTestsImmutable"
    movies: Union[list[Movie], list[str], None] = Field(default=None, repr=False)
    nomination: Union[list[Nomination], list[str], None] = Field(default=None, repr=False)
    person: Union[Person, str, None] = Field(None, repr=False)
    won_oscar: Optional[bool] = Field(None, alias="wonOscar")

    def as_apply(self) -> ActorApply:
        """Convert this read version of actor to the writing version."""
        return ActorApply(
            space=self.space,
            external_id=self.external_id,
            movies=[movie.as_apply() if isinstance(movie, DomainModel) else movie for movie in self.movies or []],
            nomination=[
                nomination.as_apply() if isinstance(nomination, DomainModel) else nomination
                for nomination in self.nomination or []
            ],
            person=self.person.as_apply() if isinstance(self.person, DomainModel) else self.person,
            won_oscar=self.won_oscar,
        )


class ActorApply(DomainModelApply):
    """This represents the writing version of actor.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the actor.
        movies: The movie field.
        nomination: The nomination field.
        person: The person field.
        won_oscar: The won oscar field.
        existing_version: Fail the ingestion request if the actor version is greater than or equal to this value.
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
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "Actor", "2"
        )

        properties = {}
        if self.person is not None:
            properties["person"] = {
                "space": self.space if isinstance(self.person, str) else self.person.space,
                "externalId": self.person if isinstance(self.person, str) else self.person.external_id,
            }
        if self.won_oscar is not None:
            properties["wonOscar"] = self.won_oscar

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "Role.movies")
        for movie in self.movies or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, movie, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "Role.nomination")
        for nomination in self.nomination or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, nomination, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        if isinstance(self.person, DomainModelApply):
            other_resources = self.person._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        return resources


class ActorList(DomainModelList[Actor]):
    """List of actors in the read version."""

    _INSTANCE = Actor

    def as_apply(self) -> ActorApplyList:
        """Convert these read versions of actor to the writing versions."""
        return ActorApplyList([node.as_apply() for node in self.data])


class ActorApplyList(DomainModelApplyList[ActorApply]):
    """List of actors in the writing version."""

    _INSTANCE = ActorApply


def _create_actor_filter(
    view_id: dm.ViewId,
    person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    won_oscar: bool | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if person and isinstance(person, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("person"), value={"space": "IntegrationTestsImmutable", "externalId": person}
            )
        )
    if person and isinstance(person, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("person"), value={"space": person[0], "externalId": person[1]})
        )
    if person and isinstance(person, list) and isinstance(person[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("person"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in person],
            )
        )
    if person and isinstance(person, list) and isinstance(person[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("person"), values=[{"space": item[0], "externalId": item[1]} for item in person]
            )
        )
    if won_oscar and isinstance(won_oscar, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("wonOscar"), value=won_oscar))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
