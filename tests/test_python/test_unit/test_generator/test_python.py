import pytest

from cognite.pygen._client.models import ViewReference
from cognite.pygen._generator.python import PythonDataClassGenerator
from cognite.pygen._pygen_model import DataClass, DataClassFile, Field, FilterClass, ListDataClass


@pytest.fixture(scope="session")
def data_class_file() -> DataClassFile:
    return DataClassFile(
        filename="example.py",
        view_id=ViewReference(
            space="example_space",
            external_id="example_view",
            version="v1",
        ),
        instance_type="node",
        read=DataClass(
            name="ExampleView",
            fields=[
                Field(
                    cdf_prop_id="prop1",
                    name="property_one",
                    type_hint="str | None",
                    filter_name="TextFilter",
                    description="The first property.",
                    default_value="None",
                ),
                Field(
                    cdf_prop_id="prop2",
                    name="property_two",
                    type_hint="int",
                    filter_name="IntegerFilter",
                    description="The second property.",
                ),
            ],
            display_name="Example View",
            description="An example view for testing.",
        ),
        read_list=ListDataClass(
            name="ExampleViewList",
        ),
        filter=FilterClass(
            name="ExampleViewFilter",
        ),
        write=DataClass(
            name="ExampleViewWrite",
            fields=[
                Field(
                    cdf_prop_id="prop1",
                    name="property_one",
                    type_hint="str | None",
                    filter_name=None,
                    description="The first property.",
                    default_value="None",
                ),
                Field(
                    cdf_prop_id="prop2",
                    name="property_two",
                    type_hint="int",
                    filter_name=None,
                    description="The second property.",
                ),
            ],
            display_name="Example View",
            description="An example write view for testing.",
        ),
    )


EXPECTED_WRITE_CLASS_CODE = '''class ExampleViewWrite(InstanceWrite):
    """Write class for Example View instances."""
    _view_id: ClassVar[ViewReference] = ViewReference(
        space="example_space", external_id="example_view", version="v1"
    )
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    property_one: str | None = Field(default=None, alias="prop1")
    property_two: int = Field(alias="prop2")
'''

EXPECTED_READ_CLASS_CODE = '''class ExampleView(Instance):
    """Read class for Example View instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(
        space="example_space", external_id="example_view", version="v1"
    )
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    property_one: str | None = Field(default=None, alias="prop1")
    property_two: int = Field(alias="prop2")

    def as_write(self) -> ExampleViewWrite:
        """Convert to write representation."""
        return ExampleViewWrite.model_validate(self.model_dump(by_alias=True))
'''

EXPECTED_READ_LIST_CLASS_CODE = '''class ExampleViewList(InstanceList[ExampleView]):
    """List of Example View instances."""
    _INSTANCE: ClassVar[type[ExampleView]] = ExampleView
'''

EXPECTED_FILTER_CLASS_CODE = """class ExampleViewFilter(FilterContainer):
    def __init__(self, operator: Literal["and", "or"] = "and") -> None:
        view_id = ExampleView._view_id
        self.property_one = TextFilter(view_id, "prop1", operator)
        self.property_two = IntegerFilter(view_id, "prop2", operator)
        super().__init__(
            data_type_filters=[
                self.property_one,
                self.property_two,
            ],
            operator=operator,
            instance_type="node",
        )
"""


@pytest.fixture(scope="session")
def data_class_generator(data_class_file: DataClassFile) -> PythonDataClassGenerator:
    return PythonDataClassGenerator(data_class_file)


class TestPythonDataClassGenerator:
    def test_generate_write_class(self, data_class_generator: PythonDataClassGenerator) -> None:
        write_class_code = data_class_generator.generate_write_class()
        assert write_class_code.strip() == EXPECTED_WRITE_CLASS_CODE.strip()

    def test_generate_read_class(self, data_class_generator: PythonDataClassGenerator) -> None:
        read_class_code = data_class_generator.generate_read_class()
        assert read_class_code.strip() == EXPECTED_READ_CLASS_CODE.strip()

    def test_generate_read_list_class(self, data_class_generator: PythonDataClassGenerator) -> None:
        read_list_class_code = data_class_generator.generate_read_list_class()
        assert read_list_class_code.strip() == EXPECTED_READ_LIST_CLASS_CODE.strip()

    def test_generate_filter_class(self, data_class_generator: PythonDataClassGenerator) -> None:
        filter_class_code = data_class_generator.generate_filter_class()
        assert filter_class_code.strip() == EXPECTED_FILTER_CLASS_CODE.strip()
