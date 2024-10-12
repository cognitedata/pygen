from omni import OmniClient


def test_retrieve_datapoints(omni_client: OmniClient) -> None:
    # Act
    datapoints = omni_client.cdf_external_references.timeseries(limit=10).retrieve()

    # Assert
    assert datapoints
