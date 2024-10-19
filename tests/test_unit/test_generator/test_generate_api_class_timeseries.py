import difflib

from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import MultiAPIGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import OmniFiles
from tests.omni_constants import OMNI_SPACE


def test_generate_cdf_external_references(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "CDFExternalReferences", "1")
    ]
    expected = OmniFiles.cdf_external_timeseries_api.read_text()

    # Act
    _, actual = next(
        api_generator.generate_timeseries_api_files(
            omni_multi_api_generator.top_level_package, omni_multi_api_generator.client_name
        )
    )
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected, "\n".join(difflib.unified_diff(expected.splitlines(), actual.splitlines()))
