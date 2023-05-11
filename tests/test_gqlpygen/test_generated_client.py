import importlib.util
import shutil
import sys
from collections import defaultdict
from contextlib import suppress
from pathlib import Path

import pytest
from faker import Faker

from cognite.dm_clients.domain_modeling import DomainModel, Schema
from cognite.dm_clients.domain_modeling.domain_client import DomainClient, DomainModelT
from cognite.dm_clients.misc import to_pascal
from cognite.gqlpygen.main import to_python
from tests.constants import REPO_ROOT, TestSchemas


@pytest.fixture()
def local_tmp_path():
    tmp_path = Path(__file__).parent / "tmp"
    tmp_path.mkdir(exist_ok=True)
    yield tmp_path
    with suppress(FileNotFoundError):
        shutil.rmtree(tmp_path)


def load_module_file(filepath: Path):
    # Ref https://docs.python.org/3/library/importlib.html#importing-a-source-file-directly
    relative = filepath.relative_to(REPO_ROOT)
    name = ".".join(relative.parent.parts + (relative.stem,))
    spec = importlib.util.spec_from_file_location(name, filepath)
    module = importlib.util.module_from_spec(spec)  # type:ignore[arg-type]
    sys.modules[name] = module
    spec.loader.exec_module(module)  # type:ignore[union-attr]
    return module


def create_instances(schema: Schema) -> dict[str, DomainModelT]:
    fake = Faker(locale="NO")
    Faker.seed(42)
    id_numbers = defaultdict(int)
    instances = {}
    for name, klass in schema.types_map.items():
        if name in instances:
            continue

        data = {}
        class_name = klass.__name__
        for field_name, field in klass.__fields__.items():
            if field_name == "externalId":
                id_numbers[class_name] += 1
                value = f"{class_name}_{id_numbers[class_name]}"
            elif field.type_ is str:
                value = fake.first_name()
            elif issubclass(field.type_, DomainModel):
                value = instances[field.type_.__name__.lower()]
            else:
                raise NotImplementedError()

            data[field_name] = value

        instances[name] = klass(**data)
    return instances


def test_domain_client(local_tmp_path):
    graphql_file = TestSchemas.foobar

    # Arrange
    name = graphql_file.stem
    to_python(graphql_file, local_tmp_path, name=name)

    # Act
    client_module = load_module_file(local_tmp_path / "client.py")
    schema_module = load_module_file(local_tmp_path / "schema.py")
    schema = getattr(schema_module, f"{name}_schema")
    instances_by_type = create_instances(schema)
    ClientClass = getattr(client_module, f"{to_pascal(name)}Client")  # noqa: N806

    # Assert
    assert issubclass(ClientClass, DomainClient)
    assert isinstance(schema, Schema)
    assert all(isinstance(instance, DomainModel) for instance in instances_by_type.values())
