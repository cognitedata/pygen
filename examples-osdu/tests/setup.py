import pytest
from osdu.data_classes._core_patch import DomainModelApplyPatch
from osdu.data_classes.extid_factory import extid_factory


@pytest.fixture(scope="session")
def initialize_pygen_client():
    print("Setting up test suite...")

    # patch the DomainModelApply class
    DomainModelApplyPatch.set_external_id_factory(extid_factory)

    yield
    print("Tearing down test suite...")
    # perform teardown tasks here
