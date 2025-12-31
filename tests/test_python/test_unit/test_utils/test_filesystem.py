import pytest

from cognite.pygen._utils.filesystem import sanitize


class TestSanitize:
    @pytest.mark.parametrize(
        "input_str, expected_output",
        [
            ("valid_filename.txt", "valid_filename.txt"),
            ("inva|lid:fi*le?name.txt", "invalid_filename.txt"),
            ("another<invalid>name.txt", "anotherinvalidname.txt"),
            ("file/name/with/slash.txt", "filenamewithslash.txt"),
            ("trailing_space .txt", "trailing_space_.txt"),
            ("trailing_dot..", "trailing_dot_."),
        ],
    )
    def test_sanitize_filename(self, input_str: str, expected_output: str):
        assert sanitize(name=input_str) == expected_output
