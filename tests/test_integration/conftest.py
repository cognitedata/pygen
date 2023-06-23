from __future__ import annotations

from pathlib import Path

import pytest
import toml


@pytest.fixture()
def client_config() -> dict[str, str]:
    return toml.load(Path(__file__).parent / "config.toml")["cognite"]
