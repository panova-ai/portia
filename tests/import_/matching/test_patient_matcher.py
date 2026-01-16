"""Tests for patient matcher."""

from datetime import date
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from src.import_.matching.patient_matcher import (
    MatchStatus,
    PatientDemographics,
    PatientMatcher,
    demographics_from_extraction,
)


@pytest.fixture
def mock_fhir_client() -> MagicMock:
    """Create a mock FHIR client."""
    client = MagicMock()

    # Mock the persons client
    client.persons = MagicMock()
    client.persons.client = MagicMock()
    client.persons.base_url = "https://healthcare.googleapis.com/v1/test/fhir"
    client.persons._get_auth_headers = MagicMock(
        return_value={"Authorization": "Bearer test"}
    )
    client.persons.create = AsyncMock()

    # Mock the patients client
    client.patients = MagicMock()
    client.patients.find_or_create_for_person_and_organization = AsyncMock()

    return client


@pytest.fixture
def sample_demographics() -> PatientDemographics:
    """Create sample patient demographics."""
    return PatientDemographics(
        given_name="Elkind",
        family_name="Testpatientone",
        birth_date=date(1980, 12, 23),
        gender="female",
        phone="555-123-4567",
        email="elkind@example.com",
        address_line="123 Test St",
        address_city="Carlsbad",
        address_state="CA",
        address_postal_code="92008",
    )


@pytest.fixture
def organization_id() -> UUID:
    """Create a test organization ID."""
    return uuid4()


class TestPatientMatcher:
    """Tests for PatientMatcher class."""

    @pytest.mark.anyio
    async def test_no_person_found_creates_both(
        self,
        mock_fhir_client: MagicMock,
        sample_demographics: PatientDemographics,
        organization_id: UUID,
    ) -> None:
        """Test that new Person and Patient are created when no match found."""
        # Mock empty search response
        mock_response = MagicMock()
        mock_response.json.return_value = {"entry": []}
        mock_response.raise_for_status = MagicMock()
        mock_fhir_client.persons.client.get = AsyncMock(return_value=mock_response)

        # Mock person creation
        person_id = uuid4()
        mock_person = MagicMock()
        mock_person.id = person_id
        mock_person.link = None
        mock_fhir_client.persons.create.return_value = mock_person

        # Mock patient creation
        patient_id = uuid4()
        mock_patient = MagicMock()
        mock_patient.id = patient_id
        mock_fhir_client.patients.find_or_create_for_person_and_organization.return_value = (
            mock_patient
        )

        matcher = PatientMatcher(mock_fhir_client)
        result = await matcher.match_or_create(sample_demographics, organization_id)

        assert result.status == MatchStatus.NEW_PERSON_NEW_PATIENT
        assert result.person_id == person_id
        assert result.patient_id == patient_id
        assert result.person_created is True
        assert result.patient_created is True

    @pytest.mark.anyio
    async def test_existing_person_existing_patient(
        self,
        mock_fhir_client: MagicMock,
        sample_demographics: PatientDemographics,
        organization_id: UUID,
    ) -> None:
        """Test matching existing Person with existing Patient."""
        person_id = uuid4()
        patient_id = uuid4()

        # Mock search returning existing person
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "entry": [
                {
                    "resource": {
                        "resourceType": "Person",
                        "id": str(person_id),
                        "name": [{"given": ["Elkind"], "family": "Testpatientone"}],
                        "link": [{"target": {"reference": f"Patient/{patient_id}"}}],
                    }
                }
            ]
        }
        mock_response.raise_for_status = MagicMock()
        mock_fhir_client.persons.client.get = AsyncMock(return_value=mock_response)

        # Mock existing patient
        mock_patient = MagicMock()
        mock_patient.id = patient_id
        mock_fhir_client.patients.find_or_create_for_person_and_organization.return_value = (
            mock_patient
        )

        matcher = PatientMatcher(mock_fhir_client)
        result = await matcher.match_or_create(sample_demographics, organization_id)

        assert result.status == MatchStatus.EXISTING_PERSON_EXISTING_PATIENT
        assert result.person_id == person_id
        assert result.patient_id == patient_id
        assert result.person_created is False
        assert result.patient_created is False

    @pytest.mark.anyio
    async def test_existing_person_new_patient(
        self,
        mock_fhir_client: MagicMock,
        sample_demographics: PatientDemographics,
        organization_id: UUID,
    ) -> None:
        """Test matching existing Person but creating new Patient for org."""
        person_id = uuid4()
        old_patient_id = uuid4()
        new_patient_id = uuid4()

        # Mock search returning existing person with patient from different org
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "entry": [
                {
                    "resource": {
                        "resourceType": "Person",
                        "id": str(person_id),
                        "name": [{"given": ["Elkind"], "family": "Testpatientone"}],
                        "link": [
                            {"target": {"reference": f"Patient/{old_patient_id}"}}
                        ],
                    }
                }
            ]
        }
        mock_response.raise_for_status = MagicMock()
        mock_fhir_client.persons.client.get = AsyncMock(return_value=mock_response)

        # Mock new patient created for this org
        mock_patient = MagicMock()
        mock_patient.id = new_patient_id  # Different ID than in link
        mock_fhir_client.patients.find_or_create_for_person_and_organization.return_value = (
            mock_patient
        )

        matcher = PatientMatcher(mock_fhir_client)
        result = await matcher.match_or_create(sample_demographics, organization_id)

        assert result.status == MatchStatus.EXISTING_PERSON_NEW_PATIENT
        assert result.person_id == person_id
        assert result.patient_id == new_patient_id
        assert result.person_created is False
        assert result.patient_created is True

    @pytest.mark.anyio
    async def test_multiple_matches_returns_error(
        self,
        mock_fhir_client: MagicMock,
        sample_demographics: PatientDemographics,
        organization_id: UUID,
    ) -> None:
        """Test that multiple Person matches returns error status."""
        person_id_1 = uuid4()
        person_id_2 = uuid4()

        # Mock search returning multiple persons
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "entry": [
                {
                    "resource": {
                        "resourceType": "Person",
                        "id": str(person_id_1),
                        "name": [{"given": ["Elkind"], "family": "Testpatientone"}],
                    }
                },
                {
                    "resource": {
                        "resourceType": "Person",
                        "id": str(person_id_2),
                        "name": [{"given": ["Elkind"], "family": "Testpatientone"}],
                    }
                },
            ]
        }
        mock_response.raise_for_status = MagicMock()
        mock_fhir_client.persons.client.get = AsyncMock(return_value=mock_response)

        matcher = PatientMatcher(mock_fhir_client)
        result = await matcher.match_or_create(sample_demographics, organization_id)

        assert result.status == MatchStatus.MULTIPLE_MATCHES
        assert result.person_id is None
        assert result.patient_id is None
        assert result.warnings is not None
        assert "2 Person resources" in result.warnings[0]


class TestDemographicsFromExtraction:
    """Tests for demographics_from_extraction function."""

    def test_extracts_demographics_from_charm_result(self) -> None:
        """Test extracting demographics from CharmExtractionResult."""
        from src.import_.charm.extractor import (
            CharmExtractionResult,
            PatientDemographicsData,
        )

        extraction = CharmExtractionResult(
            patient_id="PAT001",
            patient_name="Elkind Testpatientone",
            patient_demographics=PatientDemographicsData(
                given_name="Elkind",
                family_name="Testpatientone",
                birth_date=date(1980, 12, 23),
                gender="female",
                phone="555-123-4567",
                email="elkind@example.com",
                address_city="Carlsbad",
                address_state="CA",
                address_postal_code="92008",
            ),
            practitioner_name="Dr. Smith",
            organization_name="Test Clinic",
            encounters=[],
            problems=[],
            medications=[],
            notes=[],
        )

        demographics = demographics_from_extraction(extraction)

        assert demographics is not None
        assert demographics.given_name == "Elkind"
        assert demographics.family_name == "Testpatientone"
        assert demographics.birth_date == date(1980, 12, 23)
        assert demographics.gender == "female"
        assert demographics.email == "elkind@example.com"

    def test_returns_none_when_required_fields_missing(self) -> None:
        """Test that None is returned when required fields are missing."""
        from src.import_.charm.extractor import (
            CharmExtractionResult,
            PatientDemographicsData,
        )

        # Missing birth_date
        extraction = CharmExtractionResult(
            patient_id="PAT001",
            patient_name="Elkind Testpatientone",
            patient_demographics=PatientDemographicsData(
                given_name="Elkind",
                family_name="Testpatientone",
                birth_date=None,  # Missing required field
                gender="female",
            ),
            practitioner_name="Dr. Smith",
            organization_name="Test Clinic",
            encounters=[],
            problems=[],
            medications=[],
            notes=[],
        )

        demographics = demographics_from_extraction(extraction)

        assert demographics is None

    def test_returns_none_for_non_charm_result(self) -> None:
        """Test that None is returned for non-CharmExtractionResult."""
        demographics = demographics_from_extraction({"not": "a charm result"})

        assert demographics is None
