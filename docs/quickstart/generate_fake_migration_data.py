import random

from cognite.client import data_modeling as dm
from faker import Faker

from cognite.pygen import generate_sdk, load_cognite_client_from_toml


def generate():
    client = load_cognite_client_from_toml()
    generate_sdk(("APM_AppData_4", "APM_AppData_4", "7"), client, "apm_domain", "APMClient", overwrite=True)


def create_fake_data():
    _cwd_to_path()

    from apm_domain import APMClient
    from apm_domain.data_classes import APMTemplateApply, APMTemplateApplyList, APMTemplateItemApply

    space = "sourceSpace"
    load_cognite_client_from_toml().data_modeling.spaces.apply(
        dm.SpaceApply(space=space, name="Source Space", description="Used in the data migration tutorial")
    )

    fake = Faker()

    asset_external_ids = [fake.uuid4() for _ in range(50)]
    location_external_ids = [fake.uuid4() for _ in range(10)]
    user_external_ids = [fake.uuid4() for _ in range(50)]
    labels = [fake.color() for _ in range(5)]
    orders = [fake.pyint() for _ in range(10)]
    status = ["OnGoing", "Completed", "NotStarted", "OnHold", "Cancelled"]

    templates = []
    for _ in range(50):
        items = []
        external_id = fake.uuid4()
        for __ in range(random.randint(0, 4)):
            items.append(
                APMTemplateItemApply(
                    external_id=fake.uuid4(),
                    space=space,
                    asset_external_id=random.choice(asset_external_ids),
                    created_by_external_id=random.choice(user_external_ids),
                    labels=random.choices(labels, k=random.randint(0, 3)),
                    order=random.choice(orders),
                    template_external_id=external_id,
                    title=fake.word(),
                )
            )
        templates.append(
            APMTemplateApply(
                external_id=external_id,
                space=space,
                created_by_external_id=random.choice(user_external_ids),
                is_archived=random.choice([True, False]),
                root_location_external_id=random.choice(location_external_ids),
                assigned_to=random.choices(user_external_ids, k=random.randint(0, 3)),
                status=random.choice(status),
                title=fake.word(),
                template_items=items,
                updated_by_external_id=random.choice(user_external_ids),
            )
        )
    templates = APMTemplateApplyList(templates)

    client = APMClient(load_cognite_client_from_toml())
    result = client.apm_template.apply(templates)
    print(len(result.nodes), len(result.edges))


def _cwd_to_path():
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).parent))


def check_listing():
    _cwd_to_path()
    from apm_domain import APMClient

    client = APMClient(load_cognite_client_from_toml())
    templates = client.apm_template.list(space="sourceSpace", retrieve_edges=True, limit=-1)
    print(len(templates), sum(len(t.template_items or []) for t in templates))


if __name__ == "__main__":
    check_listing()
