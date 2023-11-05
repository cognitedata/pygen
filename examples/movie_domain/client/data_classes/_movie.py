from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._actor import ActorApply
    from ._director import DirectorApply
    from ._rating import RatingApply

__all__ = ["Movie", "MovieApply", "MovieList", "MovieApplyList", "MovieFields", "MovieTextFields"]


MovieTextFields = Literal["title"]
MovieFields = Literal["meta", "release_year", "run_time_minutes", "title"]

_MOVIE_PROPERTIES_BY_FIELD = {
    "meta": "meta",
    "release_year": "releaseYear",
    "run_time_minutes": "runTimeMinutes",
    "title": "title",
}


class Movie(DomainModel):
    """This represent a read version of movie.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the movie.
        actors: The actor field.
        directors: The director field.
        meta: The meta field.
        rating: The rating field.
        release_year: The release year field.
        run_time_minutes: The run time minute field.
        title: The title field.
        created_time: The created time of the movie node.
        last_updated_time: The last updated time of the movie node.
        deleted_time: If present, the deleted time of the movie node.
        version: The version of the movie node.
    """

    space: str = "IntegrationTestsImmutable"
    actors: Optional[list[str]] = None
    directors: Optional[list[str]] = None
    meta: Optional[dict] = None
    rating: Optional[str] = None
    release_year: Optional[int] = Field(None, alias="releaseYear")
    run_time_minutes: Optional[float] = Field(None, alias="runTimeMinutes")
    title: Optional[str] = None

    def as_apply(self) -> MovieApply:
        """Convert this read version of movie to a write version."""
        return MovieApply(
            space=self.space,
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
    """This represent a write version of movie.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the movie.
        actors: The actor field.
        directors: The director field.
        meta: The meta field.
        rating: The rating field.
        release_year: The release year field.
        run_time_minutes: The run time minute field.
        title: The title field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    actors: Union[list[ActorApply], list[str], None] = Field(default=None, repr=False)
    directors: Union[list[DirectorApply], list[str], None] = Field(default=None, repr=False)
    meta: Optional[dict] = None
    rating: Union[RatingApply, str, None] = Field(None, repr=False)
    release_year: Optional[int] = Field(None, alias="releaseYear")
    run_time_minutes: Optional[float] = Field(None, alias="runTimeMinutes")
    title: str

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

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
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Movie", "2"),
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

        for actor in self.actors or []:
            edge = self._create_actor_edge(actor)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(actor, DomainModelApply):
                instances = actor._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for director in self.directors or []:
            edge = self._create_director_edge(director)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(director, DomainModelApply):
                instances = director._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.rating, DomainModelApply):
            instances = self.rating._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_actor_edge(self, actor: Union[str, ActorApply]) -> dm.EdgeApply:
        if isinstance(actor, str):
            end_space, end_node_ext_id = self.space, actor
        elif isinstance(actor, DomainModelApply):
            end_space, end_node_ext_id = actor.space, actor.external_id
        else:
            raise TypeError(f"Expected str or ActorApply, got {type(actor)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "Movie.actors"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )

    def _create_director_edge(self, director: Union[str, DirectorApply]) -> dm.EdgeApply:
        if isinstance(director, str):
            end_space, end_node_ext_id = self.space, director
        elif isinstance(director, DomainModelApply):
            end_space, end_node_ext_id = director.space, director.external_id
        else:
            raise TypeError(f"Expected str or DirectorApply, got {type(director)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "Movie.directors"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )


class MovieList(TypeList[Movie]):
    """List of movies in read version."""

    _NODE = Movie

    def as_apply(self) -> MovieApplyList:
        """Convert this read version of movie to a write version."""
        return MovieApplyList([node.as_apply() for node in self.data])


class MovieApplyList(TypeApplyList[MovieApply]):
    """List of movies in write version."""

    _NODE = MovieApply
