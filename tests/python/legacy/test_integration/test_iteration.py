import pytest
from omni import OmniClient
from omni import data_classes as dc


class TestIteration:
    def test_iterate_chunk_size_1(self, omni_client: OmniClient) -> None:
        retrieved = dc.ConnectionItemAList()
        for item in omni_client.connection_item_a.iterate(chunk_size=1, limit=3):
            assert len(item) == 1
            retrieved.extend(item)

        assert len(retrieved) == 3

    def test_iterate_chunk_size_2(self, omni_client: OmniClient) -> None:
        retrieved = dc.ConnectionItemAList()
        for item in omni_client.connection_item_a.iterate(chunk_size=2, limit=3):
            assert len(item) <= 2
            retrieved.extend(item)

        assert len(retrieved) == 3

    def test_iterate_stop_resume_iteration(self, omni_client: OmniClient) -> None:
        retrieved = dc.ConnectionItemAList()
        cursors: dict[str, str | None] | None = None
        for item in omni_client.connection_item_a.iterate(chunk_size=2):
            assert len(item) <= 2
            retrieved.extend(item)
            cursors = item.cursors
            break

        for item in omni_client.connection_item_a.iterate(chunk_size=2, limit=3, cursors=cursors):
            assert len(item) <= 2
            retrieved.extend(item)

        assert len(retrieved) == 3 + 2  # 2 from first iteration, 3 from the second iteration.
        assert len(set(retrieved.as_node_ids())) == len(retrieved)

    def test_iterate_stop_resume_twice_same_cursors_raise_value_error(self, omni_client: OmniClient) -> None:
        retrieved = dc.ConnectionItemAList()
        cursors: dict[str, str | None] | None = None
        for item in omni_client.connection_item_a.iterate(chunk_size=2):
            assert len(item) <= 2
            retrieved.extend(item)
            cursors = item.cursors
            break

        for item in omni_client.connection_item_a.iterate(chunk_size=2, limit=3, cursors=cursors):
            assert len(item) <= 2
            retrieved.extend(item)
            break

        with pytest.raises(ValueError) as e:
            for item in omni_client.connection_item_a.iterate(chunk_size=2, limit=3, cursors=cursors):
                assert len(item) <= 2
                break
        assert str(e.value) == (
            "Same cursors used twice. Please use a different set of cursors or start a new iteration. "
            "This is to avoid accidental infinite loops."
        )
