import pytest

from cognite.dm_clients.domain_modeling import DomainModel, Schema


@pytest.fixture
def schema() -> Schema:
    return Schema()


def test_schema_1(schema: Schema):
    from . import schema_1

    attrs = [getattr(schema_1, attr) for attr in dir(schema_1)]
    clss = [
        attr for attr in attrs if isinstance(attr, type) and issubclass(attr, DomainModel) and attr is not DomainModel
    ]
    for cls in clss:
        if getattr(cls, "__root_model__", None):
            schema.register_type(root_type=True)(cls)
        else:
            schema.register_type(cls)

    schema.close()
    qgl_str = schema.as_str()
    assert qgl_str.strip() == schema_1.expected.strip()
