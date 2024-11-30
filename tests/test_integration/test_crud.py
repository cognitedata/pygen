from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Protocol

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from omni import OmniClient
from omni import data_classes as odc
from omni._api._core import SequenceNotStr
from omni.data_classes import DomainModelWrite, ResourcesWriteResult

from cognite.pygen.utils.mock_generator import MockGenerator, _connected_views
from tests.constants import OMNI_SDK
from tests.omni_constants import OmniClasses


def omni_independent_view_ids() -> Iterable[dm.ViewId]:
    for connected in _connected_views(OMNI_SDK.load_data_model().views):
        if len(connected) != 1:
            continue
        view = connected[0]
        if view.writable and view.external_id not in {"SubInterface", "Empty", "ConnectionEdgeA"}:
            yield pytest.param(view.as_id(), id=view.external_id)  # type: ignore[misc]


class DomainAPI(Protocol):
    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = "") -> Sequence: ...

    def delete(self, external_id: str | SequenceNotStr[str], space: str = ""): ...

    def apply(
        self, items: DomainModelWrite | Sequence[DomainModelWrite], replace: bool = False
    ) -> ResourcesWriteResult: ...

    def list(self, limit: int = 25, **kwargs) -> Sequence: ...


class TestCRUDOperations:
    # The retrieve call fails frequently which is why the flaky decorator is used.
    @pytest.mark.parametrize("view_id", omni_independent_view_ids())
    @pytest.mark.flaky(reruns=3, reruns_delay=10, only_rerun=["AssertionError"])
    def test_create_retrieve_delete(
        self,
        view_id: dm.ViewId,
        omni_data_classes: dict[str, OmniClasses],
        omni_client: OmniClient,
        cognite_client: CogniteClient,
        omni_tmp_space: dm.Space,
    ) -> None:
        # Arrange
        api_name = omni_data_classes[view_id.external_id].api_name
        write_class = omni_data_classes[view_id.external_id].write
        assert write_class is not None
        view = omni_data_classes[view_id.external_id].view
        generator = MockGenerator([view], instance_space=omni_tmp_space.space, seed=42)
        mock_data = generator.generate_mock_data(2, 0)
        domain_nodes = [write_class.from_instance(node) for node in mock_data.nodes]
        external_ids = [node.external_id for node in domain_nodes]
        api: DomainAPI = getattr(omni_client, api_name)

        # Act
        try:
            created = omni_client.upsert(domain_nodes)

            assert len(created.nodes) == 2

            retrieved = api.retrieve(external_ids, space=omni_tmp_space.space)

            assert len(retrieved) == 2
            assert sorted([n.external_id for n in retrieved]) == sorted(external_ids)

            omni_client.delete(external_ids, space=omni_tmp_space.space)

            retrieved_after_delete = api.retrieve(external_ids, space=omni_tmp_space.space)
            assert len(retrieved_after_delete) == 0

        finally:
            cognite_client.data_modeling.instances.delete(nodes=mock_data.nodes.as_ids())

    @pytest.mark.parametrize("view_id", omni_independent_view_ids())
    def test_list(self, view_id: dm.ViewId, omni_data_classes: dict[str, OmniClasses], omni_client: OmniClient):
        api_name = omni_data_classes[view_id.external_id].api_name
        api: DomainAPI = getattr(omni_client, api_name)

        retrieved = api.list(limit=5)

        assert 5 >= len(retrieved) >= 3

    # The retrieve node frequently fails, likely due to eventual consistency.
    @pytest.mark.flaky(reruns=3, reruns_delay=10, only_rerun=["AssertionError"])
    def test_create_retrieve_delete_direct_listable(
        self, omni_client: OmniClient, cognite_client: CogniteClient
    ) -> None:
        item = odc.ConnectionItemFWrite(
            external_id="tmp_create_retrieve_delete_direct_listable",
            name="tmp_create_retrieve_delete_direct_listable",
            direct_list=[
                odc.ConnectionItemDWrite(
                    external_id="tmp_create_retrieve_delete_direct_listable_e",
                    name="tmp_create_retrieve_delete_direct_listable_e",
                ),
                odc.ConnectionItemDWrite(
                    external_id="tmp_create_retrieve_delete_direct_listable_e2",
                    name="tmp_create_retrieve_delete_direct_listable_e2",
                ),
            ],
        )

        try:
            created = omni_client.upsert(item)
            assert len(created.nodes) == 3
            assert len(created.edges) == 0

            retrieved = omni_client.connection_item_f.retrieve(item.external_id)
            assert retrieved is not None
            assert set(retrieved.direct_list or []) == {
                "tmp_create_retrieve_delete_direct_listable_e",
                "tmp_create_retrieve_delete_direct_listable_e2",
            }

            deleted = omni_client.delete(item)
            assert len(deleted.nodes) == 3
            assert len(deleted.edges) == 0
        finally:
            resources = item.to_instances_write()
            cognite_client.data_modeling.instances.delete(nodes=resources.nodes.as_ids())
