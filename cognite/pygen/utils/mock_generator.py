"""This module contains the mock generator for the pygen package. It can be used to generate mock nodes,
edges, timeseries, sequences, and files for a given data model/views.
"""
from __future__ import annotations

import random
import string
import typing
from collections import UserList
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    FileMetadataList,
    SequenceList,
    TimeSeriesList,
)
from cognite.client.data_classes.data_modeling.instances import PropertyValue


class MockGenerator:
    """Mock generator for the pygen package. It can be used to generate mock nodes, edges, timeseries,
    sequences, and files for a given data model/views.

    Args:
        views (List[View]): The views to generate mock data for.

        seed (int): The seed to use for the random number generator.
    """

    def __init__(
        self,
        views: typing.Sequence[dm.View],
        config: list[dm.ViewId] | None = None,
        data_set_id: int | None = None,
        seed: int | None = None,
    ):
        self.views = dm.ViewList(views)
        self.config = config or []
        self.data_set_id = data_set_id
        self.seed = seed

    @classmethod
    def from_data_model(cls, data_model: dm.DataModelId, client: CogniteClient) -> MockGenerator:
        raise NotImplementedError()

    def generate_mock_data(self) -> MockData:
        """Generates mock data for the given data model/views.

        Returns:
            MockData: The generated mock data.
        """
        raise NotImplementedError()


@dataclass
class ViewMockData:
    """Mock data for a given view."""

    view_id: dm.ViewId
    node: dm.NodeApplyList = field(default_factory=lambda: dm.NodeApplyList([]))
    edge: dm.EdgeApplyList = field(default_factory=lambda: dm.EdgeApplyList([]))
    timeseries: TimeSeriesList = field(default_factory=lambda: TimeSeriesList([]))
    sequence: SequenceList = field(default_factory=lambda: SequenceList([]))
    file: FileMetadataList = field(default_factory=lambda: FileMetadataList([]))

    def dump_to_folder(self) -> None:
        ...

    def deploy(self, client: CogniteClient) -> None:
        ...


class MockData(UserList[ViewMockData]):
    """Mock data for a given data model."""

    def dump_to_folder(self, folder: Path) -> None:
        """Dumps the mock data to a folder.

        Args:
            folder (Path): The folder to dump the mock data to.
        """
        raise NotImplementedError

    def deploy(self, client: CogniteClient) -> None:
        """Deploys the mock data to CDF.

        Args:
            client (CogniteClient): The client to use for deployment.
        """
        for view_mock_data in self:
            view_mock_data.deploy(client)


DEFAULT_PROPERTY_TYPES: typing.Mapping[type[dm.PropertyType], Callable] = {
    dm.Text: lambda: random.choices(string.ascii_lowercase + string.ascii_uppercase, k=10),
    dm.Int64: lambda: random.randint(-10_000, 10_000),
    dm.Int32: lambda: random.randint(-1000, 1000),
    dm.Float64: lambda: random.uniform(-10_000, 10_000),
    dm.Float32: lambda: random.uniform(-1000, 1000),
    dm.Boolean: lambda: random.choice([True, False]),
    dm.Timestamp: lambda: random.randint(0, 1000),
    dm.Date: lambda: random.randint(0, 1000),
    dm.TimeSeriesReference: lambda: f"timeseries_{uuid4()}",
    dm.FileReference: lambda: f"file_{uuid4()}",
    dm.SequenceReference: lambda: f"sequence_{uuid4()}",
    dm.Json: lambda: {"key": "value"},
}


@dataclass
class ViewMockConfig:
    view_id: dm.ViewId
    node_count: int = 5
    edge_count: int = 3
    allow_edge_reuse: bool = False
    properties: dict[str, Callable[[], PropertyValue]] = field(default_factory=lambda: {})
    property_types: dict[type[dm.PropertyType], Callable[[], PropertyValue]] = field(
        default_factory=lambda: dict(DEFAULT_PROPERTY_TYPES)
    )
