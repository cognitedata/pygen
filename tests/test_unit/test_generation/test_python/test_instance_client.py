"""Tests for the InstanceClient class."""

from unittest.mock import patch

import pytest

from cognite.pygen._generation.python.instance_api import InstanceClient
from cognite.pygen._generation.python.instance_api._instance import (
    InstanceId,
    InstanceResult,
    InstanceWrite,
    ViewRef,
)
from cognite.pygen._generation.python.instance_api.auth.credentials import Credentials
from cognite.pygen._generation.python.instance_api.config import PygenClientConfig
from cognite.pygen._generation.python.instance_api.http_client import SuccessResponse


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


@pytest.fixture
def sample_instance_write() -> InstanceWrite:
    """Create a sample InstanceWrite object."""

    class PersonWrite(InstanceWrite):
        _view_id = ViewRef(space="test", external_id="Person", version="1")
        name: str
        age: int

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

    class PersonWrite(InstanceWrite):
        _view_id = ViewRef(space="test", external_id="Person", version="1")
        name: str
        age: int

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


class TestInstanceClientInit:
    """Tests for InstanceClient initialization."""

    def test_init(self, config: PygenClientConfig) -> None:
        """Test that InstanceClient can be initialized."""
        client = InstanceClient(config)
        assert client._config == config
        assert client._http_client is not None
        assert client._write_executor is not None
        assert client._delete_executor is not None
        assert client._retrieve_executor is not None

    def test_context_manager(self, client: InstanceClient) -> None:
        """Test that InstanceClient works as a context manager."""
        with client as c:
            assert c is client


class TestInstanceClientUpsert:
    """Tests for InstanceClient.upsert() method."""

    def test_upsert_single_instance(self, client: InstanceClient, sample_instance_write: InstanceWrite) -> None:
        """Test upserting a single instance."""
        mock_response = SuccessResponse(
            status_code=200,
            body='{"items": [{"instanceType": "node", "space": "test", '
            '"externalId": "person-1", "version": 1, "wasModified": true, '
            '"createdTime": 1234567890000, "lastUpdatedTime": 1234567890000}]}',
            content=b"",
        )

        with patch.object(client._http_client, "request_with_retries", return_value=mock_response):
            result = client.upsert(sample_instance_write)

        assert isinstance(result, InstanceResult)
        assert len(result.created) == 1
        assert result.created[0].external_id == "person-1"
        assert result.created[0].space == "test"
        assert result.created[0].was_modified is True
        assert len(result.updated) == 0
        assert len(result.unchanged) == 0
        assert len(result.deleted) == 0

    def test_upsert_multiple_instances(
        self, client: InstanceClient, sample_instance_writes: list[InstanceWrite]
    ) -> None:
        """Test upserting multiple instances."""
        mock_response = SuccessResponse(
            status_code=200,
            body='{"items": ['
            '{"instanceType": "node", "space": "test", "externalId": "person-1", '
            '"version": 1, "wasModified": true, "createdTime": 1234567890000, '
            '"lastUpdatedTime": 1234567890000},'
            '{"instanceType": "node", "space": "test", "externalId": "person-2", '
            '"version": 1, "wasModified": true, "createdTime": 1234567890000, '
            '"lastUpdatedTime": 1234567890000},'
            '{"instanceType": "node", "space": "test", "externalId": "person-3", '
            '"version": 1, "wasModified": true, "createdTime": 1234567890000,'
            ' "lastUpdatedTime": 1234567890000}'
            "]}",
            content=b"",
        )

        with patch.object(client._http_client, "request_with_retries", return_value=mock_response):
            result = client.upsert(sample_instance_writes)

        assert isinstance(result, InstanceResult)
        assert len(result.created) == 3
        assert {item.external_id for item in result.created} == {"person-1", "person-2", "person-3"}

    def test_upsert_empty_list(self, client: InstanceClient) -> None:
        """Test upserting an empty list."""
        result = client.upsert([])

        assert isinstance(result, InstanceResult)
        assert len(result.created) == 0
        assert len(result.updated) == 0
        assert len(result.unchanged) == 0
        assert len(result.deleted) == 0

    def test_upsert_with_replace_mode(self, client: InstanceClient, sample_instance_write: InstanceWrite) -> None:
        """Test upserting with replace mode."""
        mock_response = SuccessResponse(
            status_code=200,
            body='{"items": [{"instanceType": "node", "space": "test",'
            ' "externalId": "person-1", "version": 1, "wasModified": '
            'true, "createdTime": 1234567890000, "lastUpdatedTime": 1234567890000}]}',
            content=b"",
        )

        with patch.object(client._http_client, "request_with_retries", return_value=mock_response) as mock_request:
            result = client.upsert(sample_instance_write, mode="replace")

        assert isinstance(result, InstanceResult)
        # Verify that replace=True was passed in the request
        call_args = mock_request.call_args
        request_message = call_args[0][0]
        assert request_message.body_content["replace"] is True

    def test_upsert_with_skip_on_version_conflict(
        self, client: InstanceClient, sample_instance_write: InstanceWrite
    ) -> None:
        """Test upserting with skip_on_version_conflict."""
        mock_response = SuccessResponse(
            status_code=200,
            body='{"items": [{"instanceType": "node", "space": "test", "externalId": '
            '"person-1", "version": 1, "wasModified": true, "createdTime":'
            ' 1234567890000, "lastUpdatedTime": 1234567890000}]}',
            content=b"",
        )

        with patch.object(client._http_client, "request_with_retries", return_value=mock_response) as mock_request:
            result = client.upsert(sample_instance_write, skip_on_version_conflict=True)

        assert isinstance(result, InstanceResult)
        # Verify that skipOnVersionConflict=True was passed in the request
        call_args = mock_request.call_args
        request_message = call_args[0][0]
        assert request_message.body_content["skipOnVersionConflict"] is True

    def test_upsert_with_update_mode_not_implemented(
        self, client: InstanceClient, sample_instance_write: InstanceWrite
    ) -> None:
        """Test that update mode raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Update mode is not yet implemented"):
            client.upsert(sample_instance_write, mode="update")


class TestInstanceClientDelete:
    """Tests for InstanceClient.delete() method."""

    def test_delete_single_instance_by_id(self, client: InstanceClient) -> None:
        """Test deleting a single instance by InstanceId."""
        instance_id = InstanceId(
            instance_type="node",
            space="test",
            external_id="person-1",
        )

        mock_response = SuccessResponse(
            status_code=200,
            body='{"items": [{"instanceType": "node", "space": "test", "externalId": "person-1"}]}',
            content=b"",
        )

        with patch.object(client._http_client, "request_with_retries", return_value=mock_response):
            result = client.delete(instance_id)

        assert isinstance(result, InstanceResult)
        assert len(result.deleted) == 1
        assert result.deleted[0].external_id == "person-1"
        assert result.deleted[0].space == "test"
        assert result.deleted[0].instance_type == "node"

    def test_delete_single_instance_by_string(self, client: InstanceClient) -> None:
        """Test deleting a single instance by external_id string."""
        mock_response = SuccessResponse(
            status_code=200,
            body='{"items": [{"instanceType": "node", "space": "test", "externalId": "person-1"}]}',
            content=b"",
        )

        with patch.object(client._http_client, "request_with_retries", return_value=mock_response):
            result = client.delete("person-1", space="test")

        assert isinstance(result, InstanceResult)
        assert len(result.deleted) == 1
        assert result.deleted[0].external_id == "person-1"
        assert result.deleted[0].instance_type == "node"

    def test_delete_string_without_space_raises_error(self, client: InstanceClient) -> None:
        """Test that deleting by string without space raises an error."""
        with pytest.raises(ValueError, match="space parameter is required"):
            client.delete("person-1")

    def test_delete_multiple_instances(self, client: InstanceClient) -> None:
        """Test deleting multiple instances."""
        instance_ids = [InstanceId(instance_type="node", space="test", external_id=f"person-{i}") for i in range(1, 4)]

        mock_response = SuccessResponse(
            status_code=200,
            body='{"items": ['
            '{"instanceType": "node", "space": "test", "externalId": "person-1"},'
            '{"instanceType": "node", "space": "test", "externalId": "person-2"},'
            '{"instanceType": "node", "space": "test", "externalId": "person-3"}'
            "]}",
            content=b"",
        )

        with patch.object(client._http_client, "request_with_retries", return_value=mock_response):
            result = client.delete(instance_ids)

        assert isinstance(result, InstanceResult)
        assert len(result.deleted) == 3
        assert {item.external_id for item in result.deleted} == {"person-1", "person-2", "person-3"}

    def test_delete_empty_list(self, client: InstanceClient) -> None:
        """Test deleting an empty list."""
        result = client.delete([])

        assert isinstance(result, InstanceResult)
        assert len(result.deleted) == 0

    def test_delete_instance_write(self, client: InstanceClient, sample_instance_write: InstanceWrite) -> None:
        """Test deleting using an InstanceWrite object."""
        mock_response = SuccessResponse(
            status_code=200,
            body='{"items": [{"instanceType": "node", "space": "test", "externalId": "person-1"}]}',
            content=b"",
        )

        with patch.object(client._http_client, "request_with_retries", return_value=mock_response):
            result = client.delete(sample_instance_write)

        assert isinstance(result, InstanceResult)
        assert len(result.deleted) == 1
        assert result.deleted[0].external_id == "person-1"
        assert result.deleted[0].instance_type == "node"


class TestInstanceClientChunking:
    """Tests for chunking behavior in InstanceClient."""

    def test_upsert_large_batch_is_chunked(self, client: InstanceClient) -> None:
        """Test that large batches are properly chunked."""

        class PersonWrite(InstanceWrite):
            _view_id = ViewRef(space="test", external_id="Person", version="1")
            name: str

        # Create more items than the chunk size
        items = [
            PersonWrite(
                instance_type="node",
                space="test",
                external_id=f"person-{i}",
                name=f"Person {i}",
            )
            for i in range(1500)  # More than _UPSERT_LIMIT (1000)
        ]

        mock_response = SuccessResponse(
            status_code=200,
            body='{"items": []}',
            content=b"",
        )

        with patch.object(client._http_client, "request_with_retries", return_value=mock_response) as mock_request:
            _ = client.upsert(items)

        # Should have made 2 requests (1000 + 500)
        assert mock_request.call_count == 2

    def test_delete_large_batch_is_chunked(self, client: InstanceClient) -> None:
        """Test that large delete batches are properly chunked."""
        # Create more items than the chunk size
        items = [InstanceId(instance_type="node", space="test", external_id=f"person-{i}") for i in range(1500)]

        mock_response = SuccessResponse(
            status_code=200,
            body='{"items": []}',
            content=b"",
        )

        with patch.object(client._http_client, "request_with_retries", return_value=mock_response) as mock_request:
            _ = client.delete(items)

        # Should have made 2 requests (1000 + 500)
        assert mock_request.call_count == 2
