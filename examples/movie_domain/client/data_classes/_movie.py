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
    from ._actor import Actor, ActorApply
    from ._director import Director, DirectorApply
    from ._rating import Rating, RatingApply


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
    """This represents the reading version of movie.

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
    actors: Union[list[Actor], list[str], None] = Field(default=None, repr=False)
    directors: Union[list[Director], list[str], None] = Field(default=None, repr=False)
    meta: Optional[dict] = None
    rating: Union[Rating, str, None] = Field(None, repr=False)
    release_year: Optional[int] = Field(None, alias="releaseYear")
    run_time_minutes: Optional[float] = Field(None, alias="runTimeMinutes")
    title: Optional[str] = None

    def as_apply(self) -> MovieApply:
        """Convert this read version of movie to the writing version."""
        return MovieApply(
            space=self.space,
            external_id=self.external_id,
            actors=[actor.as_apply() if isinstance(actor, DomainModel) else actor for actor in self.actors or []],
            directors=[
                director.as_apply() if isinstance(director, DomainModel) else director
                for director in self.directors or []
            ],
            meta=self.meta,
            rating=self.rating.as_apply() if isinstance(self.rating, DomainModel) else self.rating,
            release_year=self.release_year,
            run_time_minutes=self.run_time_minutes,
            title=self.title,
        )


class MovieApply(DomainModelApply):
    """This represents the writing version of movie.

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
        existing_version: Fail the ingestion request if the movie version is greater than or equal to this value.
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
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "Movie", "2"
        )

        properties = {}
        if self.meta is not None:
            properties["meta"] = self.meta
        if self.rating is not None:
            properties["rating"] = {
                "space": self.space if isinstance(self.rating, str) else self.rating.space,
                "externalId": self.rating if isinstance(self.rating, str) else self.rating.external_id,
            }
        if self.release_year is not None:
            properties["releaseYear"] = self.release_year
        if self.run_time_minutes is not None:
            properties["runTimeMinutes"] = self.run_time_minutes
        if self.title is not None:
            properties["title"] = self.title

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

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "Movie.actors")
        for actor in self.actors or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, actor, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "Movie.directors")
        for director in self.directors or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, director, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        if isinstance(self.rating, DomainModelApply):
            other_resources = self.rating._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        return resources


class MovieList(DomainModelList[Movie]):
    """List of movies in the read version."""

    _INSTANCE = Movie

    def as_apply(self) -> MovieApplyList:
        """Convert these read versions of movie to the writing versions."""
        return MovieApplyList([node.as_apply() for node in self.data])


class MovieApplyList(DomainModelApplyList[MovieApply]):
    """List of movies in the writing version."""

    _INSTANCE = MovieApply


def _create_movie_filter(
    view_id: dm.ViewId,
    rating: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_release_year: int | None = None,
    max_release_year: int | None = None,
    min_run_time_minutes: float | None = None,
    max_run_time_minutes: float | None = None,
    title: str | list[str] | None = None,
    title_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if rating and isinstance(rating, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("rating"), value={"space": "IntegrationTestsImmutable", "externalId": rating}
            )
        )
    if rating and isinstance(rating, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("rating"), value={"space": rating[0], "externalId": rating[1]})
        )
    if rating and isinstance(rating, list) and isinstance(rating[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("rating"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in rating],
            )
        )
    if rating and isinstance(rating, list) and isinstance(rating[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("rating"), values=[{"space": item[0], "externalId": item[1]} for item in rating]
            )
        )
    if min_release_year or max_release_year:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("releaseYear"), gte=min_release_year, lte=max_release_year)
        )
    if min_run_time_minutes or max_run_time_minutes:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("runTimeMinutes"), gte=min_run_time_minutes, lte=max_run_time_minutes
            )
        )
    if title and isinstance(title, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("title"), value=title))
    if title and isinstance(title, list):
        filters.append(dm.filters.In(view_id.as_property_ref("title"), values=title))
    if title_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("title"), value=title_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
