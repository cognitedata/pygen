from __future__ import annotations

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
else:
    from omni_pydantic_v1 import OmniClient


def test_retrieve_datapoints(omni_client: OmniClient) -> None:
    # Act
    datapoints = omni_client.cdf_external_references.timeseries(limit=10).retrieve()

    # Assert
    assert datapoints
