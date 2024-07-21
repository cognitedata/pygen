from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import MultiAPIGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import OMNI_TYPED, OmniTypedFiles


def test_generate_typed_classes(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    view_external_ids = OMNI_TYPED.typed_classes
    expected = OmniTypedFiles.typed.read_text()

    # Act
    actual = omni_multi_api_generator.generate_typed_classes_file(
        include={dm.ViewId("pygen-models", external_id, "1") for external_id in view_external_ids}
    )
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected
