"""Tests for the InstanceClient class."""

import gzip
import json
from unittest.mock import patch

import httpx
import pytest
import respx

from cognite.pygen._generation.python.instance_api import InstanceClient, InstanceId, UpsertResult
from cognite.pygen._generation.python.instance_api.auth.credentials import Credentials
from cognite.pygen._generation.python.instance_api.config import PygenClientConfig
from cognite.pygen._generation.python.instance_api.exceptions import MultiRequestError
from cognite.pygen._generation.python.instance_api.models.instance import InstanceWrite, ViewReference


class MockCredentials(Credentials):
    """Mock credentials for testing."""

    def authorization_header(self) -> tuple[str, str]:
        """Return a mock authorization header."""
        return ("Authorization", "Bearer mock-token")


@pytest.fixture
def config() -> PygenClientConfig:
    """Create a test configuration."""
    return PygenClientConfig(
        cdf_url="https://test.cognitedata.com",
        project="test-project",
        credentials=MockCredentials(),
    )


@pytest.fixture
def client(config: PygenClientConfig) -> InstanceClient:
    """Create an InstanceClient for testing."""
    return InstanceClient(config)


class PersonWrite(InstanceWrite):
    _view_id = ViewReference(space="test", external_id="Person", version="1")
    name: str
    age: int


@pytest.fixture
def sample_instance_write() -> InstanceWrite:
    """Create a sample InstanceWrite object."""
    return PersonWrite(
        instance_type="node",
        space="test",
        external_id="person-1",
        name="John Doe",
        age=30,
    )


@pytest.fixture
def sample_instance_writes() -> list[InstanceWrite]:
    """Create multiple sample InstanceWrite objects."""
    return [
        PersonWrite(
            instance_type="node",
            space="test",
            external_id=f"person-{i}",
            name=f"Person {i}",
            age=20 + i,
        )
        for i in range(1, 4)
    ]


@pytest.fixture
def upsert_url(config: PygenClientConfig) -> str:
    """Return the URL for upserting instances."""
    return config.create_api_url("/models/instances")


@pytest.fixture
def delete_url(config: PygenClientConfig) -> str:
    """Return the URL for deleting instances."""
    return config.create_api_url("/models/instances/delete")


class TestInstanceClientUpsert:
    """Tests for InstanceClient.upsert() method."""

    def test_upsert_single_instance(
        self,
        respx_mock: respx.MockRouter,
        client: InstanceClient,
        sample_instance_write: InstanceWrite,
        upsert_url: str,
    ) -> None:
        """Test upserting a single instance."""
        respx_mock.post(upsert_url).respond(
            json={
                "items": [
                    {
                        "instanceType": "node",
                        "space": "test",
                        "externalId": "person-1",
                        "version": 1,
                        "wasModified": True,
                        "createdTime": 1234567890000,
                        "lastUpdatedTime": 1234567890000,
                    }
                ]
            },
            status_code=200,
        )

        result = client.upsert(sample_instance_write)

        assert isinstance(result, UpsertResult)
        assert len(result.created) == 1
        assert result.created[0].external_id == "person-1"
        assert result.created[0].space == "test"
        assert result.created[0].was_modified is True
        assert len(result.updated) == 0
        assert len(result.unchanged) == 0
        assert len(result.deleted) == 0

    def test_upsert_multiple_instances(
        self,
        respx_mock: respx.MockRouter,
        client: InstanceClient,
        sample_instance_writes: list[InstanceWrite],
        upsert_url: str,
    ) -> None:
        """Test upserting multiple instances."""
        respx_mock.post(upsert_url).respond(
            json={
                "items": [
                    {
                        "instanceType": "node",
                        "space": "test",
                        "externalId": "person-1",
                        "version": 1,
                        "wasModified": True,
                        "createdTime": 1234567890000,
                        "lastUpdatedTime": 1234567890000,
                    },
                    {
                        "instanceType": "node",
                        "space": "test",
                        "externalId": "person-2",
                        "version": 1,
                        "wasModified": True,
                        "createdTime": 1234567890000,
                        "lastUpdatedTime": 2345678900000,
                    },
                    {
                        "instanceType": "node",
                        "space": "test",
                        "externalId": "person-3",
                        "version": 1,
                        "wasModified": False,
                        "createdTime": 1234567890000,
                        "lastUpdatedTime": 1234567890000,
                    },
                ]
            },
            status_code=200,
        )

        result = client.upsert(sample_instance_writes)

        assert isinstance(result, UpsertResult)
        assert len(result.created) == 1
        assert len(result.updated) == 1
        assert len(result.unchanged) == 1
        assert result.created[0].external_id == "person-1"
        assert result.updated[0].external_id == "person-2"
        assert result.unchanged[0].external_id == "person-3"

    def test_upsert_empty_list(self, client: InstanceClient) -> None:
        """Test upserting an empty list."""
        result = client.upsert([])

        assert isinstance(result, UpsertResult)
        assert len(result.created) == 0
        assert len(result.updated) == 0
        assert len(result.unchanged) == 0
        assert len(result.deleted) == 0

    def test_upsert_with_arguments(
        self,
        respx_mock: respx.MockRouter,
        client: InstanceClient,
        sample_instance_write: InstanceWrite,
        upsert_url: str,
    ) -> None:
        """Test upserting with replace mode."""
        route = respx_mock.post(upsert_url).respond(
            json={
                "items": [
                    {
                        "instanceType": "node",
                        "space": "test",
                        "externalId": "person-1",
                        "version": 1,
                        "wasModified": True,
                        "createdTime": 1234567890000,
                        "lastUpdatedTime": 1234567890000,
                    }
                ]
            },
            status_code=200,
        )

        result = client.upsert(sample_instance_write, mode="replace", skip_on_version_conflict=True)

        assert isinstance(result, UpsertResult)
        assert route.called
        request = respx_mock.calls[-1].request
        body = json.loads(gzip.decompress(request.content))
        assert body["replace"] is True
        assert body["skipOnVersionConflict"] is True

    def test_upsert_with_update_mode_not_implemented(
        self, client: InstanceClient, sample_instance_write: InstanceWrite
    ) -> None:
        """Test that update mode raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Update mode is not yet implemented"):
            client.upsert(sample_instance_write, mode="update")


class TestInstanceClientDelete:
    """Tests for InstanceClient.delete() method."""

    def test_delete_single_instance_by_id(
        self, respx_mock: respx.MockRouter, client: InstanceClient, delete_url: str
    ) -> None:
        """Test deleting a single instance by InstanceId."""
        instance_id = InstanceId(
            instance_type="node",
            space="test",
            external_id="person-1",
        )

        respx_mock.post(delete_url).respond(
            json={"items": [{"instanceType": "node", "space": "test", "externalId": "person-1"}]},
            status_code=200,
        )

        result = client.delete(instance_id)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].external_id == "person-1"
        assert result[0].space == "test"
        assert result[0].instance_type == "node"

    def test_delete_single_instance_by_string(
        self, respx_mock: respx.MockRouter, client: InstanceClient, delete_url: str
    ) -> None:
        """Test deleting a single instance by external_id string."""
        respx_mock.post(delete_url).respond(
            json={"items": [{"instanceType": "node", "space": "test", "externalId": "person-1"}]},
            status_code=200,
        )

        result = client.delete("person-1", space="test")

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].external_id == "person-1"
        assert result[0].instance_type == "node"

    def test_delete_string_without_space_raises_error(self, client: InstanceClient) -> None:
        """Test that deleting by string without space raises an error."""
        with pytest.raises(ValueError, match="space parameter is required"):
            client.delete("person-1")

    def test_delete_multiple_instances(
        self, respx_mock: respx.MockRouter, client: InstanceClient, delete_url: str
    ) -> None:
        """Test deleting multiple instances."""
        instance_ids = [InstanceId(instance_type="node", space="test", external_id=f"person-{i}") for i in range(1, 4)]

        respx_mock.post(delete_url).respond(
            json={
                "items": [
                    {"instanceType": "node", "space": "test", "externalId": "person-1"},
                    {"instanceType": "node", "space": "test", "externalId": "person-2"},
                    {"instanceType": "node", "space": "test", "externalId": "person-3"},
                ]
            },
            status_code=200,
        )

        result = client.delete(instance_ids)

        assert isinstance(result, list)
        assert len(result) == 3
        assert {item.external_id for item in result} == {"person-1", "person-2", "person-3"}

    def test_delete_empty_list(self, client: InstanceClient) -> None:
        """Test deleting an empty list."""
        result = client.delete([])

        assert isinstance(result, list)
        assert len(result) == 0

    def test_delete_instance_write(
        self,
        respx_mock: respx.MockRouter,
        client: InstanceClient,
        sample_instance_write: InstanceWrite,
        delete_url: str,
    ) -> None:
        """Test deleting using an InstanceWrite object."""
        respx_mock.post(delete_url).respond(
            json={"items": [{"instanceType": "node", "space": "test", "externalId": "person-1"}]},
            status_code=200,
        )

        result = client.delete(sample_instance_write)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].external_id == "person-1"
        assert result[0].instance_type == "node"


class TestInstanceClientChunking:
    """Tests for chunking behavior in InstanceClient."""

    def test_upsert_large_batch_is_chunked(
        self, respx_mock: respx.MockRouter, client: InstanceClient, upsert_url: str
    ) -> None:
        """Test that large batches are properly chunked."""

        # Create more items than the chunk size
        items = [
            PersonWrite(
                instance_type="node",
                space="test",
                external_id=f"person-{i}",
                name=f"Person {i}",
                age=20 + i,
            )
            for i in range(client._UPSERT_LIMIT + 500)
        ]

        route = respx_mock.post(upsert_url).respond(
            json={"items": []},
            status_code=200,
        )

        _ = client.upsert(items)

        # Should have made 2 requests (1000 + 500)
        assert route.call_count == 2

    def test_delete_large_batch_is_chunked(
        self, respx_mock: respx.MockRouter, client: InstanceClient, delete_url: str
    ) -> None:
        """Test that large delete batches are properly chunked."""
        # Create more items than the chunk size
        items = [InstanceId(instance_type="node", space="test", external_id=f"person-{i}") for i in range(1500)]

        route = respx_mock.post(delete_url).respond(
            json={"items": []},
            status_code=200,
        )

        _ = client.delete(items)

        # Should have made 2 requests (1000 + 500)
        assert route.call_count == 2

    def test_delete_batch_some_failing(
        self, respx_mock: respx.MockRouter, client: InstanceClient, delete_url: str
    ) -> None:
        """Test that partial failures are properly reported via MultiRequestError."""

        # Create 2500 items to trigger 3 batches (1000 + 1000 + 500)
        items = [InstanceId(instance_type="node", space="test", external_id=f"person-{i}") for i in range(2500)]

        def side_effect(request: httpx.Request) -> httpx.Response:
            content = gzip.decompress(request.content).decode("utf-8")
            if "person-500" in content:
                # First batch: connection timeout
                raise httpx.ConnectTimeout("Connection timed out")
            elif "person-1500" in content:
                return httpx.Response(
                    status_code=500,
                    json={"error": {"code": 500, "message": "Internal server error"}},
                )
            else:
                # Third batch: success
                return httpx.Response(
                    status_code=200,
                    json={
                        "items": [
                            {"instanceType": "node", "space": "test", "externalId": f"person-{i}"}
                            for i in range(2000, 2500)
                        ]
                    },
                )

        respx_mock.post(delete_url).mock(side_effect=side_effect)

        with patch("time.sleep"):
            with pytest.raises(MultiRequestError) as exc_info:
                client.delete(items)

        error = exc_info.value
        # One batch had connection timeout (FailedRequest)
        assert len(error.failed_requests) == 1
        # One batch had failed response (FailedResponse)
        assert len(error.failed_responses) == 1
        assert error.failed_responses[0].status_code == 500
        # One batch succeeded (500 items deleted)
        assert len(error.result.deleted) == 500
