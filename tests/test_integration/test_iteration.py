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
