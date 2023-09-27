from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._actor import ActorApply
    from ._director import DirectorApply
    from ._rating import RatingApply

__all__ = ["Movie", "MovieApply", "MovieList", "MovieApplyList"]


class Movie(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    actors: Optional[list[str]] = None
    directors: Optional[list[str]] = None
    meta: Optional[dict] = None
    rating: Optional[str] = None
    release_year: Optional[int] = Field(None, alias="releaseYear")
    run_time_minutes: Optional[float] = Field(None, alias="runTimeMinutes")
    title: Optional[str] = None

    def as_apply(self) -> MovieApply:
        return MovieApply(
            external_id=self.external_id,
            actors=self.actors,
            directors=self.directors,
            meta=self.meta,
            rating=self.rating,
            release_year=self.release_year,
            run_time_minutes=self.run_time_minutes,
            title=self.title,
        )


class MovieApply(DomainModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    actors: Union[list[ActorApply], list[str], None] = Field(default_factory=None, repr=False)
    directors: Union[list[DirectorApply], list[str], None] = Field(default_factory=None, repr=False)
    meta: Optional[dict] = None
    rating: Union[RatingApply, str, None] = Field(None, repr=False)
    release_year: Optional[int] = None
    run_time_minutes: Optional[float] = None
    title: str

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.meta is not None:
            properties["meta"] = self.meta
        if self.rating is not None:
            properties["rating"] = {
                "space": "IntegrationTestsImmutable",
                "externalId": self.rating if isinstance(self.rating, str) else self.rating.external_id,
            }
        if self.release_year is not None:
            properties["releaseYear"] = self.release_year
        if self.run_time_minutes is not None:
            properties["runTimeMinutes"] = self.run_time_minutes
        if self.title is not None:
            properties["title"] = self.title
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "Movie"),
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

        for actor in self.actors:
            edge = self._create_actor_edge(actor)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(actor, DomainModelApply):
                instances = actor._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for director in self.directors:
            edge = self._create_director_edge(director)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(director, DomainModelApply):
                instances = director._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.rating, DomainModelApply):
            instances = self.rating._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_actor_edge(self, actor: Union[str, ActorApply]) -> dm.EdgeApply:
        if isinstance(actor, str):
            end_node_ext_id = actor
        elif isinstance(actor, DomainModelApply):
            end_node_ext_id = actor.external_id
        else:
            raise TypeError(f"Expected str or ActorApply, got {type(actor)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "Movie.actors"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_director_edge(self, director: Union[str, DirectorApply]) -> dm.EdgeApply:
        if isinstance(director, str):
            end_node_ext_id = director
        elif isinstance(director, DomainModelApply):
            end_node_ext_id = director.external_id
        else:
            raise TypeError(f"Expected str or DirectorApply, got {type(director)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "Movie.directors"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )


class MovieList(TypeList[Movie]):
    _NODE = Movie

    def as_apply(self) -> MovieApplyList:
        return MovieApplyList([node.as_apply() for node in self.data])


class MovieApplyList(TypeApplyList[MovieApply]):
    _NODE = MovieApply
