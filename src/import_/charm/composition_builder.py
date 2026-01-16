"""
Composition builder for CHARM clinical notes.

Converts extracted clinical notes (HPI, Past Medical History, etc.) into
FHIR Composition resources with proper sections and linking to Encounters.
"""

from datetime import date
from typing import Any
from uuid import uuid4

from src.import_.charm.extractor import CharmExtractionResult, ClinicalNote

# LOINC codes for composition sections
LOINC_CODES = {
    "History of Present Illness": {
        "code": "10164-2",
        "display": "History of Present illness Narrative",
    },
    "Past Medical History": {
        "code": "11348-0",
        "display": "History of Past illness Narrative",
    },
    "Assessment": {
        "code": "51848-0",
        "display": "Evaluation note",
    },
    "Plan": {
        "code": "18776-5",
        "display": "Plan of care note",
    },
    "Subjective": {
        "code": "61150-9",
        "display": "Subjective Narrative",
    },
    "Objective": {
        "code": "61149-1",
        "display": "Objective Narrative",
    },
}

# Progress note type
PROGRESS_NOTE_TYPE = {
    "system": "http://loinc.org",
    "code": "11506-3",
    "display": "Progress note",
}


def build_compositions(
    fhir_bundle: dict[str, Any],
    extraction_result: CharmExtractionResult,
    encounter_date_to_ref: dict[date, str],
) -> tuple[dict[str, Any], list[str]]:
    """
    Build Composition resources from extracted clinical notes.

    Groups notes by date (encounter) and creates one Composition per encounter
    with sections for each note type.

    Args:
        fhir_bundle: The FHIR bundle to add Compositions to
        extraction_result: Extracted data from CHARM C-CDA
        encounter_date_to_ref: Mapping of encounter dates to FHIR references

    Returns:
        Tuple of (modified bundle, warnings)
    """
    warnings: list[str] = []

    # Get references from bundle
    patient_ref = _find_patient_reference(fhir_bundle)
    practitioner_ref = _find_practitioner_reference(fhir_bundle)
    organization_ref = _find_organization_reference(fhir_bundle)

    if not patient_ref:
        warnings.append("Cannot create Compositions: no Patient reference found")
        return fhir_bundle, warnings

    # Group notes by date
    notes_by_date: dict[date, list[ClinicalNote]] = {}
    for note in extraction_result.notes:
        if note.date not in notes_by_date:
            notes_by_date[note.date] = []
        notes_by_date[note.date].append(note)

    # Create a Composition for each encounter date that has notes
    composition_entries: list[dict[str, Any]] = []

    for note_date, notes in sorted(notes_by_date.items()):
        encounter_ref = encounter_date_to_ref.get(note_date)
        if not encounter_ref:
            warnings.append(f"No encounter found for notes dated {note_date}")
            continue

        composition = _create_composition(
            notes=notes,
            note_date=note_date,
            patient_ref=patient_ref,
            practitioner_ref=practitioner_ref,
            organization_ref=organization_ref,
            encounter_ref=encounter_ref,
        )

        composition_entries.append({"resource": composition})

    # Add Compositions to the bundle
    existing_entries = fhir_bundle.get("entry", [])
    fhir_bundle["entry"] = existing_entries + composition_entries

    warnings.append(f"Created {len(composition_entries)} Composition resources")

    return fhir_bundle, warnings


def _find_patient_reference(bundle: dict[str, Any]) -> str | None:
    """Find the Patient resource reference in the bundle."""
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Patient":
            patient_id = resource.get("id")
            if patient_id:
                return f"Patient/{patient_id}"
            full_url: str | None = entry.get("fullUrl")
            if full_url:
                return full_url
    return None


def _find_practitioner_reference(bundle: dict[str, Any]) -> str | None:
    """Find a Practitioner resource reference in the bundle."""
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Practitioner":
            pract_id = resource.get("id")
            if pract_id:
                return f"Practitioner/{pract_id}"
            full_url: str | None = entry.get("fullUrl")
            if full_url:
                return full_url
    return None


def _find_organization_reference(bundle: dict[str, Any]) -> str | None:
    """Find an Organization resource reference in the bundle."""
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Organization":
            org_id = resource.get("id")
            if org_id:
                return f"Organization/{org_id}"
            full_url: str | None = entry.get("fullUrl")
            if full_url:
                return full_url
    return None


def _create_composition(
    notes: list[ClinicalNote],
    note_date: date,
    patient_ref: str,
    practitioner_ref: str | None,
    organization_ref: str | None,
    encounter_ref: str,
) -> dict[str, Any]:
    """
    Create a FHIR Composition resource from clinical notes.

    Args:
        notes: List of clinical notes for this encounter
        note_date: The date of the encounter/notes
        patient_ref: Reference to the Patient resource
        practitioner_ref: Reference to the Practitioner resource
        organization_ref: Reference to the Organization resource
        encounter_ref: Reference to the Encounter resource

    Returns:
        FHIR Composition resource
    """
    composition_id = str(uuid4())
    date_str = note_date.isoformat()

    # Build sections from notes
    sections = []
    for note in notes:
        section = _create_section(note)
        if section:
            sections.append(section)

    # Create the Composition
    composition: dict[str, Any] = {
        "resourceType": "Composition",
        "id": composition_id,
        "status": "final",
        "type": {
            "coding": [PROGRESS_NOTE_TYPE],
            "text": "Progress note",
        },
        "subject": {"reference": patient_ref},
        "encounter": {"reference": encounter_ref},
        "date": f"{date_str}T00:00:00Z",
        "title": f"Clinical Note - {note_date.strftime('%B %d, %Y')}",
        "section": sections,
    }

    # Add author if we have a practitioner
    if practitioner_ref:
        composition["author"] = [{"reference": practitioner_ref}]

    # Add custodian if we have an organization
    if organization_ref:
        composition["custodian"] = {"reference": organization_ref}

    return composition


def _create_section(note: ClinicalNote) -> dict[str, Any] | None:
    """
    Create a Composition section from a clinical note.

    Args:
        note: The clinical note to convert

    Returns:
        FHIR CompositionSection or None if note type is unknown
    """
    # Get the LOINC code for this note type
    loinc_info = LOINC_CODES.get(note.note_type)

    # If we don't have a specific code, use a generic one
    if not loinc_info:
        # Try partial matching
        for key, value in LOINC_CODES.items():
            if key.lower() in note.note_type.lower():
                loinc_info = value
                break

    if not loinc_info:
        # Use a generic annotation code
        loinc_info = {
            "code": "48767-8",
            "display": "Annotation comment",
        }

    # Escape HTML entities in the content
    safe_content = _escape_html(note.content)

    section: dict[str, Any] = {
        "title": note.note_type,
        "code": {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": loinc_info["code"],
                    "display": loinc_info["display"],
                }
            ]
        },
        "text": {
            "status": "generated",
            "div": f'<div xmlns="http://www.w3.org/1999/xhtml"><p>{safe_content}</p></div>',
        },
    }

    return section


def _escape_html(text: str) -> str:
    """Escape HTML special characters in text."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )
