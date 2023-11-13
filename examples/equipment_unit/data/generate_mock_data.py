from pprint import pprint

from cognite.pygen import load_cognite_client_from_toml
import random
from faker import Faker
from cognite.client import data_modeling as dm
from cognite.pygen.utils.helper import chdir
from tests.constants import REPO_ROOT


def main():
    fake = Faker()
    types = ["red", "blue", "green", "yellow", "orange"]

    unit_procedures = [
        {
            "name": fake.name(),
            "type": random.choice(types),
            "work_units": [
                {
                    "start_time": (start := fake.date_time_between(start_date="-30d", end_date="-1d")),
                    "end_time": fake.date_time_between(start_date=start, end_date="now"),
                    "name": fake.name(),
                    "description": fake.text(),
                    "sensor_value": fake.text(),
                    "type": random.choice(types),
                }
                for _ in range(random.randint(1, 5))
            ],
        }
        for _ in range(5)
    ]
    space = "IntegrationTestsImmutable"
    nodes = []
    edges = []
    for unit_procedure in unit_procedures:
        start_node = f"unit_procedure:{unit_procedure['name']}"
        nodes.append(
            dm.NodeApply(
                space=space,
                external_id=start_node,
                sources=[
                    dm.NodeOrEdgeData(
                        source=dm.ViewId(space, "UnitProcedure", "f16810a7105c44"),
                        properties={
                            "name": unit_procedure["name"],
                            "type": unit_procedure["type"],
                        },
                    )
                ],
            )
        )
        for edge in unit_procedure["work_units"]:
            end_node = f"equipment_module:{edge['name']}"
            nodes.append(
                dm.NodeApply(
                    space=space,
                    external_id=end_node,
                    sources=[
                        dm.NodeOrEdgeData(
                            source=dm.ViewId(space=space, external_id="EquipmentModule", version="b1cd4bf14a7a33"),
                            properties={
                                "name": edge["name"],
                                "type": edge["type"],
                                "description": edge["description"],
                                "sensor_value": edge["sensor_value"],
                            },
                        )
                    ],
                )
            )

            edges.append(
                dm.EdgeApply(
                    space=space,
                    external_id=f"{start_node}:{end_node}",
                    type=dm.DirectRelationReference(space, "UnitProcedure.equipment_module"),
                    start_node=dm.DirectRelationReference(space, start_node),
                    end_node=dm.DirectRelationReference(space, end_node),
                    sources=[
                        dm.NodeOrEdgeData(
                            source=dm.ViewId(space, "StartEndTime", "d416e0ed98186b"),
                            properties={
                                "start_time": edge["start_time"].isoformat(timespec="milliseconds"),
                                "end_time": edge["end_time"].isoformat(timespec="milliseconds"),
                            },
                        )
                    ],
                )
            )
        with chdir(REPO_ROOT):
            client = load_cognite_client_from_toml()
        client.data_modeling.instances.apply(nodes, edges, auto_create_end_nodes=True, auto_create_start_nodes=True)


if __name__ == "__main__":
    main()
