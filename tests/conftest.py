"""Test configuration and fixtures."""

from typing import AsyncGenerator, Generator, Protocol
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient

from src.clients.fhir_store import get_fhir_store_service
from src.clients.ms_converter import get_ms_converter_service
from src.clients.sentia import get_sentia_service
from src.clients.storage import get_storage_service
from src.core.auth import AuthenticatedUser, get_current_user
from src.main import app
from src.services.fhir_store_service import FHIRStoreService, PersistenceResult
from src.services.ms_converter_service import MSConverterService
from src.services.sentia_service import (
    OrganizationContext,
    PractitionerContext,
    PractitionerOrgContext,
    SentiaService,
)
from src.services.storage_service import StorageService

# Test UUIDs
TEST_PRACTITIONER_ID = UUID("00000000-0000-0000-0000-000000000001")
TEST_ORGANIZATION_ID = UUID("00000000-0000-0000-0000-000000000002")


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """Configure pytest-anyio to use asyncio."""
    return "asyncio"


@pytest.fixture
def mock_storage_service() -> MagicMock:
    """Mock storage service for testing."""
    mock = MagicMock(spec=StorageService)
    mock.upload_temp_file.return_value = "imports/test/file.xml"
    mock.get_temp_file.return_value = b"<test>content</test>"
    mock.delete_temp_file.return_value = True
    mock.exists.return_value = True
    mock.upload_export_file.return_value = "exports/test/bundle.json"
    mock.generate_export_download_url.return_value = (
        "https://storage.googleapis.com/test/exports/test/bundle.json?signed=true"
    )
    return mock


@pytest.fixture
def mock_ms_converter_service() -> AsyncMock:
    """Mock MS FHIR Converter service for testing."""
    mock = AsyncMock(spec=MSConverterService)

    # Default successful conversion response
    mock.convert_ccda.return_value = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [
            {
                "fullUrl": "urn:uuid:patient-1",
                "resource": {
                    "resourceType": "Patient",
                    "id": "patient-1",
                    "name": [{"family": "Test", "given": ["John"]}],
                },
            },
            {
                "fullUrl": "urn:uuid:condition-1",
                "resource": {
                    "resourceType": "Condition",
                    "id": "condition-1",
                    "clinicalStatus": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                                "code": "active",
                            }
                        ]
                    },
                },
            },
        ],
    }
    mock.convert_hl7v2.return_value = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [],
    }
    mock.health_check.return_value = True
    return mock


@pytest.fixture
def mock_fhir_store_service() -> AsyncMock:
    """Mock FHIR store service for testing."""
    mock = AsyncMock(spec=FHIRStoreService)

    # Default successful persistence response
    mock.persist_bundle.return_value = PersistenceResult(
        success=True,
        resources_created=2,
        resources_updated=0,
        errors=[],
        id_mapping={
            "urn:uuid:patient-1": "patient-1",
            "urn:uuid:condition-1": "condition-1",
        },
    )
    return mock


@pytest.fixture
def mock_sentia_service() -> AsyncMock:
    """Mock Sentia service for testing."""
    mock = AsyncMock(spec=SentiaService)

    # Default context response
    default_context = PractitionerOrgContext(
        practitioner=PractitionerContext(
            id=TEST_PRACTITIONER_ID,
            name="Test Practitioner",
            email="test@panova.health",
        ),
        organizations=[
            OrganizationContext(
                id=TEST_ORGANIZATION_ID,
                name="Test Organization",
            )
        ],
        default_organization=OrganizationContext(
            id=TEST_ORGANIZATION_ID,
            name="Test Organization",
        ),
    )

    mock.get_practitioner_context.return_value = default_context
    mock.validate_practitioner_org_access.return_value = default_context
    return mock


@pytest.fixture
def mock_authenticated_user() -> AuthenticatedUser:
    """Mock authenticated user for testing."""
    return AuthenticatedUser(
        auth_type="firebase",
        user_id="test-user-id",
        email="test@panova.health",
        raw_token="test-firebase-token",
    )


class ClientFactory(Protocol):
    """Protocol for client factory fixture."""

    def __call__(self) -> AsyncClient: ...


@pytest.fixture
def client_factory(
    mock_storage_service: MagicMock,
    mock_ms_converter_service: AsyncMock,
    mock_fhir_store_service: AsyncMock,
    mock_sentia_service: AsyncMock,
    mock_authenticated_user: AuthenticatedUser,
) -> Generator[ClientFactory, None, None]:
    """Factory for creating test clients with mocked dependencies."""

    def _create_client() -> AsyncClient:
        app.dependency_overrides[get_storage_service] = lambda: mock_storage_service
        app.dependency_overrides[get_ms_converter_service] = (
            lambda: mock_ms_converter_service
        )
        app.dependency_overrides[get_fhir_store_service] = (
            lambda: mock_fhir_store_service
        )
        app.dependency_overrides[get_sentia_service] = lambda: mock_sentia_service
        app.dependency_overrides[get_current_user] = lambda: mock_authenticated_user

        transport = ASGITransport(app=app)
        return AsyncClient(transport=transport, base_url="http://testserver")

    yield _create_client

    app.dependency_overrides.clear()


@pytest.fixture
async def client(
    client_factory: ClientFactory,
) -> AsyncGenerator[AsyncClient, None]:
    """Async client for testing endpoints."""
    async with client_factory() as c:
        yield c


# Sample C-CDA content for testing
SAMPLE_CCDA = """<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3">
  <templateId root="2.16.840.1.113883.10.20.22.1.2"/>
  <recordTarget>
    <patientRole>
      <patient>
        <name>
          <given>John</given>
          <family>Test</family>
        </name>
      </patient>
    </patientRole>
  </recordTarget>
  <component>
    <structuredBody>
      <component>
        <section>
          <title>Problems</title>
        </section>
      </component>
    </structuredBody>
  </component>
</ClinicalDocument>"""


@pytest.fixture
def sample_ccda() -> str:
    """Sample C-CDA document for testing."""
    return SAMPLE_CCDA
