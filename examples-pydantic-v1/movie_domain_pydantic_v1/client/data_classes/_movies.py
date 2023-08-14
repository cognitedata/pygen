from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from movie_domain_pydantic_v1.client.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from movie_domain_pydantic_v1.client.data_classes._actors import ActorApply
    from movie_domain_pydantic_v1.client.data_classes._directors import DirectorApply
    from movie_domain_pydantic_v1.client.data_classes._ratings import RatingApply

__all__ = ["Movie", "MovieApply", "MovieList"]


class Movie(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    actors: list[str] = []
    directors: list[str] = []
    meta: Optional[dict] = None
    rating: Optional[str] = None
    release_year: Optional[int] = Field(None, alias="releaseYear")
    run_time_minutes: Optional[float] = Field(None, alias="runTimeMinutes")
    title: Optional[str] = None


class MovieApply(DomainModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    actors: list[Union["ActorApply", str]] = Field(default_factory=list, repr=False)
    directors: list[Union["DirectorApply", str]] = Field(default_factory=list, repr=False)
    meta: Optional[dict] = None
    rating: Optional[Union["RatingApply", str]] = Field(None, repr=False)
    release_year: Optional[int] = None
    run_time_minutes: Optional[float] = None
    title: str

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("IntegrationTestsImmutable", "Movie"),
            properties={
                "meta": self.meta,
                "rating": {
                    "space": "IntegrationTestsImmutable",
                    "externalId": self.rating if isinstance(self.rating, str) else self.rating.external_id,
                },
                "releaseYear": self.release_year,
                "runTimeMinutes": self.run_time_minutes,
                "title": self.title,
            },
        )
        sources.append(source)

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=sources,
        )
        nodes = [this_node]
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

    def _create_actor_edge(self, actor: Union[str, "ActorApply"]) -> dm.EdgeApply:
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

    def _create_director_edge(self, director: Union[str, "DirectorApply"]) -> dm.EdgeApply:
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
