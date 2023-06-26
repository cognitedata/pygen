from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import CircularModelApply, DomainModel, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._actors import ActorApply
    from ._directors import DirectorApply
    from ._ratings import RatingApply

__all__ = ["Movie", "MovieApply", "MovieList"]


class Movie(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    title: Optional[str] = None
    release_year: Optional[int] = Field(None, alias="releaseYear")
    rating: Optional[str] = None
    run_time_minutes: Optional[float] = Field(None, alias="runTimeMinutes")
    meta: Optional[dict] = None
    actors: list[str] = []
    directors: list[str] = []


class MovieApply(CircularModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    title: str
    release_year: Optional[int] = None
    rating: Optional[Union[str, "RatingApply"]] = None
    run_time_minutes: Optional[float] = None
    meta: Optional[dict] = None
    actors: list[Union[str, "ActorApply"]] = []
    directors: list[Union[str, "DirectorApply"]] = []

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ContainerId("IntegrationTestsImmutable", "Movie"),
                    properties={
                        "title": self.title,
                        "releaseYear": self.release_year,
                        "runTimeMinutes": self.run_time_minutes,
                        "meta": self.meta,
                    },
                ),
            ],
        )
        nodes = [this_node]
        edges = []

        if self.rating is not None:
            edge = self._create_rating_edge(self.rating)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(self.rating, CircularModelApply):
                instances = self.rating._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for actor in self.actors:
            edge = self._create_actor_edge(actor)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(actor, CircularModelApply):
                instances = actor._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for director in self.directors:
            edge = self._create_director_edge(director)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(director, CircularModelApply):
                instances = director._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_rating_edge(self, rating: Union[str, "RatingApply"]) -> dm.EdgeApply:
        if isinstance(rating, str):
            end_node_ext_id = rating
        elif isinstance(rating, CircularModelApply):
            end_node_ext_id = rating.external_id
        else:
            raise TypeError(f"Expected str or RatingApply, got {type(rating)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "Movie.rating"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_actor_edge(self, actor: Union[str, "ActorApply"]) -> dm.EdgeApply:
        if isinstance(actor, str):
            end_node_ext_id = actor
        elif isinstance(actor, CircularModelApply):
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

    def _create_director_edge(self, director: Union[str, "DirectorApply"]) -> dm.EdgeApply:
        if isinstance(director, str):
            end_node_ext_id = director
        elif isinstance(director, CircularModelApply):
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
