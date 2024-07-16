import pytest
from _pytest.mark import ParameterSet
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import TypedNode, TypedNodeApply

from cognite.pygen.utils import MockGenerator
from tests.constants import IS_PYDANTIC_V2, OMNI_TYPED

if IS_PYDANTIC_V2:
    from omni_typed import typed
else:
    raise NotImplementedError("Only Pydantic v2 is supported")


def omni_typed_view_ids() -> list[ParameterSet]:
    for view in OMNI_TYPED.load_data_model().views:
        yield pytest.param(view, id=view.external_id)


@pytest.fixture(scope="session")
def typed_classe_by_view_id() -> dict[dm.ViewId, tuple[type[TypedNodeApply], type[TypedNode]]]:
    output: dict[dm.ViewId, tuple[type[TypedNodeApply], type[TypedNode]]] = {}
    for _ in vars(typed):
        ...
    return output


class TestCRUDOperations:
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

            cognite_client.data_modeling.instances.delete(node_ids, space=omni_tmp_space.space)

            retrieved_after_delete = cognite_client.data_modeling.instances.retrieve_nodes(node_ids, node_cls=read_cls)
            assert len(retrieved_after_delete) == 0

        finally:
            cognite_client.data_modeling.instances.delete(nodes=mock_data.nodes.as_ids())
