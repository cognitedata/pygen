from osdu.data_classes import AclApply, WellApply
from rich import print


def test_extid_pks(initialize_pygen_client):
    node = WellApply(id="6c60ceb0-3521-57b7-9bd8-e1d7c9f66230")
    print(node)


def test_extid_hash(initialize_pygen_client):
    node = AclApply(
        owners=["owner1@company.com", "owner2@company.com"],
        viewers=["viewer@company.com"],
    )
    print(node)
    assert node.external_id


def test_extid_hash_differnt(initialize_pygen_client):
    node = AclApply(
        owners=["someone@company.com"],
        viewers=["someone@company.com"],
    )
    node2 = AclApply(
        viewers=["viewer@company.com"],
        owners=["owner2@company.com", "owner1@company.com"],
    )
    assert node.external_id != node2.external_id


def test_extid_hash_same(initialize_pygen_client):
    def sort_values(d):
        if isinstance(d, dict):
            return {k: sort_values(v) for k, v in sorted(d.items())}
        elif isinstance(d, list):
            return [sort_values(v) for v in sorted(d)]
        else:
            return d

    node1 = AclApply(
        owners=["owner1@company.com", "owner2@company.com"],
        viewers=["viewer@company.com"],
    )
    node2 = AclApply(
        # just a bit resorted keys and values
        viewers=["viewer@company.com"],
        owners=["owner2@company.com", "owner1@company.com"],
    )
    assert node1.external_id == node2.external_id
