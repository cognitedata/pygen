from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import MultiAPIGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import IS_PYDANTIC_V2, OmniFiles


def test_generate_primitive_nullable(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_view_id[dm.ViewId("pygen-models", "PrimitiveNullable", "1")]
    expected = OmniFiles.primitive_nullable_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_primitive_required(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_view_id[dm.ViewId("pygen-models", "PrimitiveRequired", "1")]
    expected = OmniFiles.primitive_required_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_primitive_nullable_list(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_view_id[dm.ViewId("pygen-models", "PrimitiveNullableListable", "1")]
    expected = OmniFiles.primitive_nullable_list_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_primitive_required_list(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_view_id[dm.ViewId("pygen-models", "PrimitiveRequiredListable", "1")]
    expected = OmniFiles.primitive_required_list_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_primitive_with_defaults(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_view_id[dm.ViewId("pygen-models", "PrimitiveWithDefaults", "1")]
    expected = OmniFiles.primitive_with_defaults_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_cdf_external_references(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_view_id[dm.ViewId("pygen-models", "CDFExternalReferences", "1")]
    expected = OmniFiles.cdf_external_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_cdf_external_references_list(
    omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter
):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_view_id[
        dm.ViewId("pygen-models", "CDFExternalReferencesListable", "1")
    ]
    expected = OmniFiles.cdf_external_list_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_implementation1(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_view_id[dm.ViewId("pygen-models", "Implementation1", "1")]
    expected = OmniFiles.implementation_1_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected
