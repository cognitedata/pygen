from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

import pytest
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


def omni_view_ids() -> list[dm.ViewId]:
    return dm.ViewList(OMNI_SDK.load_data_model().views).as_ids()


class DomainAPI(Protocol):
    def retrieve(self, external_id: str | SequenceNotStr[str], space: str):
        ...

    def delete(self, external_id: str | SequenceNotStr[str], space: str):
        ...

    def apply(
        self, items: DomainModelApply | Sequence[DomainModelApply], replace: bool = False
    ) -> ResourcesApplyResult:
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
        faker = Faker()
        api_name = omni_data_classes[view_id].api_name
        write_class = omni_data_classes[view_id].write
        view = omni_data_classes[view_id].view
        mock_nodes = generate_mock_data(view, 2, faker).node
        domain_nodes = write_class.from_instance(mock_nodes[0])
        api: DomainAPI = getattr(omni_client, api_name)

        # Act
        try:
            created = api.apply(domain_nodes)

            assert len(created) == 2

            retrieved = api.retrieve(domain_nodes.as_external_ids())

            assert len(retrieved) == 2
            assert sorted(retrieved.as_external_ids()) == sorted(domain_nodes.as_external_ids())

            api.delete(domain_nodes.as_external_ids())

            retrieved_after_delete = api.retrieve(domain_nodes.as_external_ids())
            assert len(retrieved_after_delete) == 0

        finally:
            cognite_client.data_modeling.instances.delete(nodes=mock_nodes.as_ids())
