from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from ._movies import MovieApply
    from ._nominations import NominationApply
    from ._persons import PersonApply

__all__ = ["Actor", "ActorApply", "ActorList"]


class Actor(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    movies: list[str] = []
    nomination: list[str] = []
    person: Optional[str] = None
    won_oscar: Optional[bool] = Field(None, alias="wonOscar")


class ActorApply(DomainModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    movies: Union[list[MovieApply], list[str]] = Field(default_factory=list, repr=False)
    nomination: Union[list[NominationApply], list[str]] = Field(default_factory=list, repr=False)
    person: Union[PersonApply, str, None] = Field(None, repr=False)
    won_oscar: Optional[bool] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
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
                source=dm.ContainerId("IntegrationTestsImmutable", "Role"),
                properties=properties,
            )
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=sources,
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        for movie in self.movies:
            edge = self._create_movie_edge(movie)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(movie, DomainModelApply):
                instances = movie._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for nomination in self.nomination:
            edge = self._create_nomination_edge(nomination)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(nomination, DomainModelApply):
                instances = nomination._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.person, DomainModelApply):
            instances = self.person._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_movie_edge(self, movie: Union[str, MovieApply]) -> dm.EdgeApply:
        if isinstance(movie, str):
            end_node_ext_id = movie
        elif isinstance(movie, DomainModelApply):
            end_node_ext_id = movie.external_id
        else:
            raise TypeError(f"Expected str or MovieApply, got {type(movie)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "Role.movies"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_nomination_edge(self, nomination: Union[str, NominationApply]) -> dm.EdgeApply:
        if isinstance(nomination, str):
            end_node_ext_id = nomination
        elif isinstance(nomination, DomainModelApply):
            end_node_ext_id = nomination.external_id
        else:
            raise TypeError(f"Expected str or NominationApply, got {type(nomination)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "Role.nomination"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )


class ActorList(TypeList[Actor]):
    _NODE = Actor
