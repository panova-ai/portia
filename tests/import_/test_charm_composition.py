"""Tests for CHARM composition builder."""

from datetime import date
from typing import Any

import pytest

from src.import_.charm.composition_builder import build_compositions
from src.import_.charm.extractor import (
    CharmExtractionResult,
    ClinicalNote,
    EncounterData,
    PatientDemographicsData,
)


@pytest.fixture
def sample_fhir_bundle_with_encounters() -> dict[str, Any]:
    """Create a sample FHIR R4 bundle with encounters for testing."""
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
                    "resourceType": "Encounter",
                    "id": "encounter-1",
                    "status": "completed",
                    "actualPeriod": {"start": "2023-03-21T00:00:00Z"},
                }
            },
            {
                "resource": {
                    "resourceType": "Encounter",
                    "id": "encounter-2",
                    "status": "completed",
                    "actualPeriod": {"start": "2023-03-28T00:00:00Z"},
                }
            },
        ],
    }


@pytest.fixture
def sample_extraction_result() -> CharmExtractionResult:
    """Create a sample extraction result with notes."""
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
                        content="Patient presents with anxiety. Therapy performed: CBT techniques.",
                    ),
                    ClinicalNote(
                        date=date(2023, 3, 21),
                        note_type="Past Medical History",
                        content="No significant past medical history.",
                    ),
                ],
                problem_ids=[],
                medication_ids=[],
            ),
            EncounterData(
                date=date(2023, 3, 28),
                notes=[
                    ClinicalNote(
                        date=date(2023, 3, 28),
                        note_type="History of Present Illness",
                        content="Follow-up visit. Patient reports improvement.",
                    ),
                ],
                problem_ids=[],
                medication_ids=[],
            ),
        ],
        problems=[],
        medications=[],
        notes=[
            ClinicalNote(
                date=date(2023, 3, 21),
                note_type="History of Present Illness",
                content="Patient presents with anxiety. Therapy performed: CBT techniques.",
            ),
            ClinicalNote(
                date=date(2023, 3, 21),
                note_type="Past Medical History",
                content="No significant past medical history.",
            ),
            ClinicalNote(
                date=date(2023, 3, 28),
                note_type="History of Present Illness",
                content="Follow-up visit. Patient reports improvement.",
            ),
        ],
    )


@pytest.fixture
def encounter_date_map() -> dict[date, str]:
    """Create a mapping of encounter dates to references."""
    return {
        date(2023, 3, 21): "Encounter/encounter-1",
        date(2023, 3, 28): "Encounter/encounter-2",
    }


class TestCompositionBuilder:
    """Tests for build_compositions."""

    def test_creates_compositions(
        self,
        sample_fhir_bundle_with_encounters: dict[str, Any],
        sample_extraction_result: CharmExtractionResult,
        encounter_date_map: dict[date, str],
    ) -> None:
        """Test that Composition resources are created."""
        result_bundle, warnings = build_compositions(
            sample_fhir_bundle_with_encounters,
            sample_extraction_result,
            encounter_date_map,
        )

        compositions = [
            e["resource"]
            for e in result_bundle["entry"]
            if e["resource"]["resourceType"] == "Composition"
        ]

        # Should create one composition per encounter date with notes
        assert len(compositions) == 2, "Should create 2 compositions"

    def test_composition_has_correct_structure(
        self,
        sample_fhir_bundle_with_encounters: dict[str, Any],
        sample_extraction_result: CharmExtractionResult,
        encounter_date_map: dict[date, str],
    ) -> None:
        """Test that Compositions have correct FHIR structure."""
        result_bundle, _ = build_compositions(
            sample_fhir_bundle_with_encounters,
            sample_extraction_result,
            encounter_date_map,
        )

        compositions = [
            e["resource"]
            for e in result_bundle["entry"]
            if e["resource"]["resourceType"] == "Composition"
        ]

        for comp in compositions:
            assert comp.get("status") == "final"
            assert "type" in comp
            assert "subject" in comp
            assert "encounter" in comp
            assert "date" in comp
            assert "title" in comp
            assert "section" in comp

    def test_composition_references_patient(
        self,
        sample_fhir_bundle_with_encounters: dict[str, Any],
        sample_extraction_result: CharmExtractionResult,
        encounter_date_map: dict[date, str],
    ) -> None:
        """Test that Compositions reference the patient."""
        result_bundle, _ = build_compositions(
            sample_fhir_bundle_with_encounters,
            sample_extraction_result,
            encounter_date_map,
        )

        compositions = [
            e["resource"]
            for e in result_bundle["entry"]
            if e["resource"]["resourceType"] == "Composition"
        ]

        for comp in compositions:
            assert "Patient/patient-123" in comp["subject"]["reference"]

    def test_composition_references_encounter(
        self,
        sample_fhir_bundle_with_encounters: dict[str, Any],
        sample_extraction_result: CharmExtractionResult,
        encounter_date_map: dict[date, str],
    ) -> None:
        """Test that Compositions reference their Encounter."""
        result_bundle, _ = build_compositions(
            sample_fhir_bundle_with_encounters,
            sample_extraction_result,
            encounter_date_map,
        )

        compositions = [
            e["resource"]
            for e in result_bundle["entry"]
            if e["resource"]["resourceType"] == "Composition"
        ]

        for comp in compositions:
            assert "encounter" in comp
            assert "Encounter/" in comp["encounter"]["reference"]

    def test_composition_has_sections(
        self,
        sample_fhir_bundle_with_encounters: dict[str, Any],
        sample_extraction_result: CharmExtractionResult,
        encounter_date_map: dict[date, str],
    ) -> None:
        """Test that Compositions have sections for each note type."""
        result_bundle, _ = build_compositions(
            sample_fhir_bundle_with_encounters,
            sample_extraction_result,
            encounter_date_map,
        )

        compositions = [
            e["resource"]
            for e in result_bundle["entry"]
            if e["resource"]["resourceType"] == "Composition"
        ]

        # First composition should have 2 sections (HPI and PMH)
        first_comp = [c for c in compositions if "March 21" in c.get("title", "")][0]
        assert (
            len(first_comp["section"]) == 2
        ), "First composition should have 2 sections"

        # Second composition should have 1 section (HPI only)
        second_comp = [c for c in compositions if "March 28" in c.get("title", "")][0]
        assert (
            len(second_comp["section"]) == 1
        ), "Second composition should have 1 section"

    def test_section_has_loinc_code(
        self,
        sample_fhir_bundle_with_encounters: dict[str, Any],
        sample_extraction_result: CharmExtractionResult,
        encounter_date_map: dict[date, str],
    ) -> None:
        """Test that sections have LOINC codes."""
        result_bundle, _ = build_compositions(
            sample_fhir_bundle_with_encounters,
            sample_extraction_result,
            encounter_date_map,
        )

        compositions = [
            e["resource"]
            for e in result_bundle["entry"]
            if e["resource"]["resourceType"] == "Composition"
        ]

        for comp in compositions:
            for section in comp["section"]:
                assert "code" in section
                assert "coding" in section["code"]
                assert section["code"]["coding"][0]["system"] == "http://loinc.org"

    def test_section_contains_narrative(
        self,
        sample_fhir_bundle_with_encounters: dict[str, Any],
        sample_extraction_result: CharmExtractionResult,
        encounter_date_map: dict[date, str],
    ) -> None:
        """Test that sections contain narrative text."""
        result_bundle, _ = build_compositions(
            sample_fhir_bundle_with_encounters,
            sample_extraction_result,
            encounter_date_map,
        )

        compositions = [
            e["resource"]
            for e in result_bundle["entry"]
            if e["resource"]["resourceType"] == "Composition"
        ]

        for comp in compositions:
            for section in comp["section"]:
                assert "text" in section
                assert "div" in section["text"]
                assert section["text"]["status"] == "generated"

    def test_html_content_escaped(
        self,
        sample_fhir_bundle_with_encounters: dict[str, Any],
        encounter_date_map: dict[date, str],
    ) -> None:
        """Test that HTML special characters are escaped in narrative."""
        # Create extraction result with HTML-like content
        extraction_result = CharmExtractionResult(
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
                            content='Patient says: "I feel <bad> & anxious"',
                        ),
                    ],
                    problem_ids=[],
                    medication_ids=[],
                ),
            ],
            problems=[],
            medications=[],
            notes=[
                ClinicalNote(
                    date=date(2023, 3, 21),
                    note_type="History of Present Illness",
                    content='Patient says: "I feel <bad> & anxious"',
                ),
            ],
        )

        result_bundle, _ = build_compositions(
            sample_fhir_bundle_with_encounters,
            extraction_result,
            encounter_date_map,
        )

        compositions = [
            e["resource"]
            for e in result_bundle["entry"]
            if e["resource"]["resourceType"] == "Composition"
        ]

        # Check that special characters are escaped
        for comp in compositions:
            for section in comp["section"]:
                div = section["text"]["div"]
                assert "&lt;" in div or "<bad>" not in div
                assert "&amp;" in div or "& " not in div
