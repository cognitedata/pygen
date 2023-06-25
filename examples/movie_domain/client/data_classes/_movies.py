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

    def to_instances_apply(self) -> InstancesApply:
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
                        "rating": self.rating,
                        "runTimeMinutes": self.run_time_minutes,
                        "meta": self.meta,
                    },
                ),
            ],
        )
        nodes = [this_node]
        edges = []
        return InstancesApply(nodes, edges)


class MovieList(TypeList[Movie]):
    _NODE = Movie
