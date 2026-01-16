"""Tests for CHARM resource linker."""

from datetime import date
from typing import Any

import pytest

from src.import_.charm.extractor import (
    CharmExtractionResult,
    ClinicalNote,
    EncounterData,
    MedicationEntry,
    PatientDemographicsData,
    ProblemEntry,
)
from src.import_.charm.linker import link_resources_to_encounters


@pytest.fixture
def sample_fhir_bundle() -> dict[str, Any]:
    """Create a sample FHIR R4 bundle for testing."""
    return {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [
            {
                "resource": {
                    "resourceType": "Patient",
                    "id": "patient-123",
                    "name": [{"given": ["Test"], "family": "Patient"}],
                }
            },
            {
                "resource": {
                    "resourceType": "Practitioner",
                    "id": "practitioner-456",
                    "name": [{"given": ["Dr"], "family": "Smith"}],
                }
            },
            {
                "resource": {
                    "resourceType": "Organization",
                    "id": "org-789",
                    "name": "Test Clinic",
                }
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "condition-1",
                    "code": {
                        "coding": [
                            {"system": "http://snomed.info/sct", "code": "371631005"}
                        ]
                    },
                    "onsetDateTime": "2023-03-21T00:00:00Z",
                }
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "condition-2",
                    "code": {
                        "coding": [
                            {"system": "http://snomed.info/sct", "code": "371631005"}
                        ]
                    },
                    "onsetDateTime": "2023-03-28T00:00:00Z",
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationStatement",
                    "id": "med-1",
                    "effectiveDateTime": "2023-03-21T00:00:00Z",
                }
            },
        ],
    }


@pytest.fixture
def sample_extraction_result() -> CharmExtractionResult:
    """Create a sample extraction result."""
    return CharmExtractionResult(
        patient_id="PAT001",
        patient_name="Test Patient",
        patient_demographics=PatientDemographicsData(
            given_name="Test",
            family_name="Patient",
            birth_date=date(1980, 1, 1),
            gender="male",
        ),
        practitioner_name="Dr. Smith",
        organization_name="Test Clinic",
        encounters=[
            EncounterData(
                date=date(2023, 3, 21),
                notes=[
                    ClinicalNote(
                        date=date(2023, 3, 21),
                        note_type="History of Present Illness",
                        content="Patient presents with anxiety...",
                    )
                ],
                problem_ids=["problem-1"],
                medication_ids=["med-1"],
            ),
            EncounterData(
                date=date(2023, 3, 28),
                notes=[
                    ClinicalNote(
                        date=date(2023, 3, 28),
                        note_type="History of Present Illness",
                        content="Follow-up visit...",
                    )
                ],
                problem_ids=["problem-2"],
                medication_ids=[],
            ),
        ],
        problems=[
            ProblemEntry(
                code="371631005",
                display="Panic disorder",
                start_date=date(2023, 3, 21),
                end_date=date(2023, 3, 28),
                ccda_id="problem-1",
            ),
            ProblemEntry(
                code="371631005",
                display="Panic disorder",
                start_date=date(2023, 3, 28),
                end_date=None,
                ccda_id="problem-2",
            ),
        ],
        medications=[
            MedicationEntry(
                code="312938",
                display="Sertraline 100mg",
                start_date=date(2023, 3, 1),
                end_date=None,
                dosage="100mg",
                route="oral",
                ccda_id="med-1",
            )
        ],
        notes=[
            ClinicalNote(
                date=date(2023, 3, 21),
                note_type="History of Present Illness",
                content="Patient presents with anxiety...",
            ),
            ClinicalNote(
                date=date(2023, 3, 28),
                note_type="History of Present Illness",
                content="Follow-up visit...",
            ),
        ],
    )


class TestResourceLinker:
    """Tests for link_resources_to_encounters."""

    def test_creates_encounter_resources(
        self,
        sample_fhir_bundle: dict[str, Any],
        sample_extraction_result: CharmExtractionResult,
    ) -> None:
        """Test that Encounter resources are created."""
        result_bundle, warnings = link_resources_to_encounters(
            sample_fhir_bundle, sample_extraction_result
        )

        # Count encounters in result
        encounters = [
            e["resource"]
            for e in result_bundle["entry"]
            if e["resource"]["resourceType"] == "Encounter"
        ]

        assert len(encounters) == 2, "Should create 2 encounters"

    def test_encounters_have_correct_structure(
        self,
        sample_fhir_bundle: dict[str, Any],
        sample_extraction_result: CharmExtractionResult,
    ) -> None:
        """Test that created Encounters have correct FHIR structure."""
        result_bundle, _ = link_resources_to_encounters(
            sample_fhir_bundle, sample_extraction_result
        )

        encounters = [
            e["resource"]
            for e in result_bundle["entry"]
            if e["resource"]["resourceType"] == "Encounter"
        ]

        for enc in encounters:
            assert enc.get("status") == "completed"
            assert "class" in enc
            assert "subject" in enc
            assert "Patient/patient-123" in enc["subject"]["reference"]
            assert "actualPeriod" in enc

    def test_conditions_linked_to_encounters(
        self,
        sample_fhir_bundle: dict[str, Any],
        sample_extraction_result: CharmExtractionResult,
    ) -> None:
        """Test that Conditions are linked to their Encounters."""
        result_bundle, warnings = link_resources_to_encounters(
            sample_fhir_bundle, sample_extraction_result
        )

        conditions = [
            e["resource"]
            for e in result_bundle["entry"]
            if e["resource"]["resourceType"] == "Condition"
        ]

        # At least one condition should be linked
        linked_conditions = [c for c in conditions if "encounter" in c]
        assert (
            len(linked_conditions) > 0
        ), "Some conditions should be linked to encounters"

    def test_medications_linked_to_encounters(
        self,
        sample_fhir_bundle: dict[str, Any],
        sample_extraction_result: CharmExtractionResult,
    ) -> None:
        """Test that MedicationStatements are linked to Encounters."""
        result_bundle, warnings = link_resources_to_encounters(
            sample_fhir_bundle, sample_extraction_result
        )

        medications = [
            e["resource"]
            for e in result_bundle["entry"]
            if e["resource"]["resourceType"] == "MedicationStatement"
        ]

        # At least one medication should be linked
        linked_meds = [m for m in medications if "context" in m]
        assert len(linked_meds) > 0, "Some medications should be linked to encounters"

    def test_returns_warnings(
        self,
        sample_fhir_bundle: dict[str, Any],
        sample_extraction_result: CharmExtractionResult,
    ) -> None:
        """Test that informative warnings are returned."""
        _, warnings = link_resources_to_encounters(
            sample_fhir_bundle, sample_extraction_result
        )

        assert len(warnings) > 0, "Should return warnings"
        assert any(
            "Encounter" in w for w in warnings
        ), "Should mention encounters created"

    def test_encounter_references_practitioner(
        self,
        sample_fhir_bundle: dict[str, Any],
        sample_extraction_result: CharmExtractionResult,
    ) -> None:
        """Test that Encounters reference the practitioner."""
        result_bundle, _ = link_resources_to_encounters(
            sample_fhir_bundle, sample_extraction_result
        )

        encounters = [
            e["resource"]
            for e in result_bundle["entry"]
            if e["resource"]["resourceType"] == "Encounter"
        ]

        for enc in encounters:
            if "participant" in enc:
                participant_refs = [
                    p.get("actor", {}).get("reference", "") for p in enc["participant"]
                ]
                assert any("Practitioner" in ref for ref in participant_refs)

    def test_encounter_references_organization(
        self,
        sample_fhir_bundle: dict[str, Any],
        sample_extraction_result: CharmExtractionResult,
    ) -> None:
        """Test that Encounters reference the service provider organization."""
        result_bundle, _ = link_resources_to_encounters(
            sample_fhir_bundle, sample_extraction_result
        )

        encounters = [
            e["resource"]
            for e in result_bundle["entry"]
            if e["resource"]["resourceType"] == "Encounter"
        ]

        for enc in encounters:
            assert "serviceProvider" in enc
            assert "Organization" in enc["serviceProvider"]["reference"]
