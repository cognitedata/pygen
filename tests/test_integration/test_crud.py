from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

import pytest
from _pytest.mark import ParameterSet
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from faker import Faker

from tests.constants import IS_PYDANTIC_V2, OMNI_SDK
from tests.data_models.Omni.generate_mock_data import generate_mock_data
from tests.omni_constants import OmniClasses

if IS_PYDANTIC_V2:
    from omni import OmniClient
    from omni._api._core import SequenceNotStr
    from omni.data_classes import DomainModelApply, ResourcesApplyResult
else:
    from omni_pydantic_v1 import OmniClient
    from omni_pydantic_v1._api._core import SequenceNotStr
    from omni_pydantic_v1.data_classes import DomainModelApply, ResourcesApplyResult


def omni_view_ids() -> list[ParameterSet]:
    return [
        pytest.param(view.as_id(), id=view.external_id)
        for view in dm.ViewList(OMNI_SDK.load_data_model().views)
        if view.writable and view.external_id != "SubInterface"
    ]


class DomainAPI(Protocol):
    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = "") -> Sequence:
        ...

    def delete(self, external_id: str | SequenceNotStr[str], space: str = ""):
        ...

    def apply(
        self, items: DomainModelApply | Sequence[DomainModelApply], replace: bool = False
    ) -> ResourcesApplyResult:
        ...

    def list(self, limit: int = 25, **kwargs) -> Sequence:
        ...


class TestCRUDOperations:
    @pytest.mark.parametrize("view_id", omni_view_ids())
    def test_create_retrieve_delete(
        self,
        view_id: dm.ViewId,
        omni_data_classes: dict[dm.ViewId, OmniClasses],
        omni_client: OmniClient,
        cognite_client: CogniteClient,
    ) -> None:
        # Arrange
        Faker.seed(42)
        faker = Faker()
        api_name = omni_data_classes[view_id].api_name
        write_class = omni_data_classes[view_id].write
        view = omni_data_classes[view_id].view
        mock_nodes = generate_mock_data(view, 2, 0, faker).node
        domain_nodes = [write_class.from_instance(node) for node in mock_nodes]
        external_ids = [node.external_id for node in domain_nodes]
        api: DomainAPI = getattr(omni_client, api_name)

        # Act
        try:
            created = api.apply(domain_nodes)

            assert len(created.nodes) == 2

            retrieved = api.retrieve(external_ids)

            assert len(retrieved) == 2
            assert sorted([n.external_id for n in retrieved]) == sorted(external_ids)

            api.delete(external_ids)

            retrieved_after_delete = api.retrieve(external_ids)
            assert len(retrieved_after_delete) == 0

        finally:
            cognite_client.data_modeling.instances.delete(nodes=mock_nodes.as_ids())

    @pytest.mark.parametrize("view_id", omni_view_ids())
    def test_list(self, view_id: dm.ViewId, omni_data_classes: dict[dm.ViewId, OmniClasses], omni_client: OmniClient):
        api_name = omni_data_classes[view_id].api_name
        api: DomainAPI = getattr(omni_client, api_name)

        retrieved = api.list(limit=5)

        assert 5 >= len(retrieved) >= 3
