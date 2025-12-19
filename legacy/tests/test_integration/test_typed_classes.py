import inspect
import random
from collections.abc import Iterable

import pytest
from _pytest.mark import ParameterSet
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import TypedNode, TypedNodeApply
from omni_typed import typed

from cognite.pygen._core.generators import MultiAPIGenerator, SDKGenerator
from cognite.pygen._generator import _get_data_model
from cognite.pygen.config import PygenConfig
from cognite.pygen.utils import MockGenerator
from tests.constants import OMNI_TYPED


def omni_typed_view_ids() -> Iterable[ParameterSet]:
    for view in OMNI_TYPED.load_data_model().views:
        if view.external_id in OMNI_TYPED.typed_classes:
            if view.external_id == "SubInterface":
                # Due to the view filter on the SubInterface view, we cannot retrieve nodes
                # written to it.
                continue
            yield pytest.param(view, id=view.external_id)


@pytest.fixture(scope="session")
def typed_classe_by_view_id() -> dict[dm.ViewId, tuple[type[TypedNodeApply], type[TypedNode]]]:
    write_cls_by_id: dict[dm.ViewId, type[TypedNodeApply]] = {}
    read_cls_by_id: dict[dm.ViewId, type[TypedNode]] = {}
    for name, cls_ in vars(typed).items():
        if name.startswith("_") or not inspect.isclass(cls_):
            continue
        if issubclass(cls_, TypedNodeApply) and cls_ is not TypedNodeApply:
            write_cls_by_id[cls_.get_source()] = cls_
        elif issubclass(cls_, TypedNode) and cls_ is not TypedNode:
            read_cls_by_id[cls_.get_source()] = cls_
    output: dict[dm.ViewId, tuple[type[TypedNodeApply], type[TypedNode]]] = {}
    for view_id in set(write_cls_by_id.keys()) & set(read_cls_by_id.keys()):
        output[view_id] = write_cls_by_id[view_id], read_cls_by_id[view_id]
    return output


class TestTypedClasses:
    @pytest.mark.skip(reason="Missing test data")
    @pytest.mark.parametrize("view", omni_typed_view_ids())
    def test_create_retrieve_delete(
        self,
        view: dm.View,
        typed_classe_by_view_id: dict[dm.ViewId, tuple[type[TypedNodeApply], type[TypedNode]]],
        cognite_client: CogniteClient,
        omni_tmp_space: dm.Space,
    ) -> None:
        # Arrange
        generator = MockGenerator([view], instance_space=omni_tmp_space.space, seed=42)
        mock_data = generator.generate_mock_data(2, 0)
        node_ids = mock_data.nodes.as_ids()
        write_cls, read_cls = typed_classe_by_view_id[view.as_id()]
        write_nodes = [write_cls.load(node.dump()) for node in mock_data.nodes]

        # Act
        try:
            created = cognite_client.data_modeling.instances.apply(write_nodes)

            assert len(created.nodes) == 2

            retrieved = cognite_client.data_modeling.instances.retrieve_nodes(node_ids, node_cls=read_cls)
            assert len(retrieved) == 2
            assert set(retrieved.as_ids()) == set(node_ids)

            cognite_client.data_modeling.instances.delete(node_ids)

            retrieved_after_delete = cognite_client.data_modeling.instances.retrieve_nodes(node_ids, node_cls=read_cls)
            assert len(retrieved_after_delete) == 0

        finally:
            cognite_client.data_modeling.instances.delete(nodes=mock_data.nodes.as_ids())

    @staticmethod
    def create_core_multi_api_generator(client: CogniteClient) -> MultiAPIGenerator:
        data_model = _get_data_model(("cdf_cdm", "CogniteCore", "v1"), client, print)
        assert isinstance(data_model, dm.DataModel)
        # Ensure we have a random order of views
        # This should be sorted before generating the typed classes
        random.shuffle(data_model.views)
        generator = SDKGenerator(
            "cognite.pygen.typed",
            "Typed",
            data_model,
            None,
            "composition",
            print,
            PygenConfig(),
        )
        return generator._multi_api_generator

    def test_generate_deterministic_typed_class_file(self, cognite_client_alpha: CogniteClient) -> None:
        first = self.create_core_multi_api_generator(cognite_client_alpha)

        second = self.create_core_multi_api_generator(cognite_client_alpha)

        assert [d.read_name for d in first.data_classes_topological_order] == [
            d.read_name for d in second.data_classes_topological_order
        ]

        assert first.generate_typed_classes_file() == second.generate_typed_classes_file()
