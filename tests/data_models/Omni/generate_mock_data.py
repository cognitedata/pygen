from pathlib import Path

import numpy as np
from cognite.client import data_modeling as dm
from faker import Faker

from cognite.pygen.utils.mock_generator import MockGenerator, ViewMockConfig
from tests.constants import OMNI_SDK

MODEL_DIR = Path(__file__).resolve().parent

MODEL_FILE = MODEL_DIR / "model.yaml"
DATA_DIR = MODEL_DIR / "data"


def main():
    Faker.seed(42)
    Faker()
    data_model = dm.DataModel[dm.View].load(MODEL_FILE.read_text())

    # The empty view is used for testing and should not have mock data, neither should interfaces
    views = [v for v in data_model.views if v.external_id != "Empty"]
    g = Generators(seed=42)
    config = ViewMockConfig(
        node_id_generator=g.id_generator,
        property_types={
            dm.Text: g.text,
            dm.Int64: g.int64,
            dm.Int32: g.int32,
            dm.Float64: g.float64,
            dm.Float32: g.float32,
            dm.Boolean: g.boolean,
            dm.Json: g.json,
            dm.Timestamp: g.timestamp,
            dm.Date: g.date,
            dm.FileReference: g.file,
            dm.SequenceReference: g.sequence,
            dm.TimeSeriesReference: g.timeseries,
        },
    )
    data = MockGenerator(
        views, OMNI_SDK.instance_space, default_config=config, seed=42, skip_interfaces=True
    ).generate_mock_data(node_count=5, max_edge_per_type=3, null_values=0.25)

    data.dump_yaml(DATA_DIR, exclude={("Implementation1NonWriteable", "node")})
    print(f"Generated {len(data.nodes)} nodes and {len(data.edges)} edges for {len(data)}")


class Generators:
    def __init__(self, seed):
        Faker.seed(seed)
        self.faker = Faker()

    def id_generator(self, view_id: dm.ViewId, node_count) -> list[str]:
        return [f"{view_id.external_id}:{self.faker.unique.first_name()}" for _ in range(node_count)]

    def text(self, count: int) -> list[str]:
        return [self.faker.sentence() for _ in range(count)]

    def int64(self, count: int) -> list[int]:
        info = np.iinfo(np.int64)
        return [self.faker.random.randint(int(info.min) + 1, int(info.max) - 1) for _ in range(count)]

    def int32(self, count: int) -> list[int]:
        info = np.iinfo(np.int32)
        return [self.faker.random.randint(int(info.min) + 1, int(info.max) - 1) for _ in range(count)]

    def float64(self, count: int) -> list[float]:
        info = np.finfo(np.float64)
        return [
            round(self.faker.random.uniform(float(info.min) / 2, float(info.max) / 2), info.precision)
            for _ in range(count)
        ]

    def float32(self, count: int) -> list[float]:
        return [round(float(np.float32(self.faker.random.uniform(-1000, 1000))), 2) for _ in range(count)]

    def boolean(self, count: int) -> list[bool]:
        return [self.faker.pybool() for _ in range(count)]

    def json(self, count: int) -> list[dict]:
        return [
            {self.text(1)[0]: self.text(1)[0] for _ in range(self.faker.random.randint(0, 5))} for _ in range(count)
        ]

    def timestamp(self, count: int) -> list[str]:
        return [self.faker.date_time_this_year() for _ in range(count)]

    def date(self, count: int) -> list[str]:
        return [self.faker.date_this_year() for _ in range(count)]

    def file(self, count: int) -> list[str]:
        return [f"file_{self.faker.unique.first_name().casefold()}" for _ in range(count)]

    def sequence(self, count: int) -> list[str]:
        return [f"sequence_{self.faker.unique.first_name().casefold()}" for _ in range(count)]

    def timeseries(self, count: int) -> list[str]:
        return [f"timeseries_{self.faker.unique.first_name().casefold()}" for _ in range(count)]


if __name__ == "__main__":
    main()
