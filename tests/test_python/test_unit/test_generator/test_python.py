import pytest

from cognite.pygen._client.models import ViewReference
from cognite.pygen._generator.python import PythonDataClassGenerator
from cognite.pygen._pygen_model import DataClass, DataClassFile, Field, FilterClass, ListDataClass, ReadDataClass


@pytest.fixture(scope="session")
def data_class_file() -> DataClassFile:
    view_id = ViewReference(
        space="example_space",
        external_id="example_view",
        version="v1",
    )
    return DataClassFile(
        filename="example.py",
        read=ReadDataClass(
            view_id=view_id,
            name="ExampleView",
            fields=[
                Field(
                    cdf_prop_id="prop1",
                    name="property_one",
                    type_hint="str | None",
                    filter_name="TextFilter",
                    description="The first property.",
                ),
                Field(
                    cdf_prop_id="prop2",
                    name="property_two",
                    type_hint="int",
                    filter_name="IntegerFilter",
                    description="The second property.",
                ),
            ],
            instance_type="node",
            display_name="Example View",
            description="An example view for testing.",
            write_class_name="ExampleViewWrite",
        ),
        read_list=ListDataClass(
            view_id=view_id,
            name="ExampleViewList",
            read_class_name="ExampleView",
        ),
        filter=FilterClass(
            name="ExampleViewFilter",
            view_id=view_id,
            instance_type="node",
        ),
        write=DataClass(
            view_id=view_id,
            name="ExampleViewWrite",
            fields=[
                Field(
                    cdf_prop_id="prop1",
                    name="property_one",
                    type_hint="str | None",
                    filter_name=None,
                    description="The first property.",
                ),
                Field(
                    cdf_prop_id="prop2",
                    name="property_two",
                    type_hint="int",
                    filter_name=None,
                    description="The second property.",
                ),
            ],
            instance_type="node",
            display_name="Example View Write",
            description="An example write view for testing.",
        ),
    )


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


class TestPythonDataClassGenerator:
    def test_generate_filter_class(self, data_class_file: DataClassFile) -> None:
        generator = PythonDataClassGenerator(data_class_file)
        filter_class_code = generator.generate_filter_class()
        assert filter_class_code.strip() == EXPECTED_FILTER_CLASS_CODE.strip()
