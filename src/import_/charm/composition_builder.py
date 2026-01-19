"""
Composition builder for CHARM clinical notes.

Converts extracted clinical notes (HPI, Past Medical History, etc.) into
FHIR Composition resources with proper sections and linking to Encounters.
"""

from datetime import date
from typing import Any
from uuid import uuid4

from src.import_.charm.extractor import CharmExtractionResult, ClinicalNote

# SOAP LOINC codes (matching Sentia's expected codes)
SOAP_LOINC_CODES = {
    "Subjective": {"code": "61150-9", "display": "Subjective Narrative"},
    "Objective": {"code": "61149-1", "display": "Objective Narrative"},
    "Assessment": {"code": "51848-0", "display": "Evaluation note"},
    "Plan": {"code": "18776-5", "display": "Plan of care note"},
    "Additional": {"code": "48767-8", "display": "Annotation comment"},
}

# Map C-CDA note types to SOAP sections
NOTE_TYPE_TO_SOAP = {
    # Subjective section
    "history of present illness": "Subjective",
    "hpi": "Subjective",
    "chief complaint": "Subjective",
    "reason for visit": "Subjective",
    "subjective": "Subjective",
    # Objective section
    "mental status exam": "Objective",
    "mse": "Objective",
    "physical examination": "Objective",
    "physical exam": "Objective",
    "vital signs": "Objective",
    "objective": "Objective",
    # Assessment section
    "assessment": "Assessment",
    "diagnosis": "Assessment",
    "impression": "Assessment",
    # Plan section
    "plan": "Plan",
    "plan of care": "Plan",
    "treatment plan": "Plan",
    "recommendations": "Plan",
    # Everything else goes to Additional
    "past medical history": "Additional",
    "social history": "Additional",
    "family history": "Additional",
    "medications": "Additional",
    "allergies": "Additional",
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

        composition, full_url = _create_composition(
            notes=notes,
            note_date=note_date,
            patient_ref=patient_ref,
            practitioner_ref=practitioner_ref,
            organization_ref=organization_ref,
            encounter_ref=encounter_ref,
        )

        composition_entries.append({"fullUrl": full_url, "resource": composition})

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
            # Prefer fullUrl for transaction bundle compatibility
            # GCP FHIR API resolves urn:uuid references within transaction bundles
            full_url: str | None = entry.get("fullUrl")
            if full_url and full_url.startswith("urn:uuid:"):
                return full_url
            patient_id = resource.get("id")
            if patient_id:
                return f"Patient/{patient_id}"
            if full_url:
                return full_url
    return None


def _find_practitioner_reference(bundle: dict[str, Any]) -> str | None:
    """Find a Practitioner resource reference in the bundle."""
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Practitioner":
            # Prefer fullUrl for transaction bundle compatibility
            full_url: str | None = entry.get("fullUrl")
            if full_url and full_url.startswith("urn:uuid:"):
                return full_url
            pract_id = resource.get("id")
            if pract_id:
                return f"Practitioner/{pract_id}"
            if full_url:
                return full_url
    return None


def _find_organization_reference(bundle: dict[str, Any]) -> str | None:
    """Find an Organization resource reference in the bundle."""
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Organization":
            # Prefer fullUrl for transaction bundle compatibility
            full_url: str | None = entry.get("fullUrl")
            if full_url and full_url.startswith("urn:uuid:"):
                return full_url
            org_id = resource.get("id")
            if org_id:
                return f"Organization/{org_id}"
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
) -> tuple[dict[str, Any], str]:
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
        Tuple of (FHIR Composition resource, fullUrl for bundle)
    """
    composition_id = str(uuid4())
    full_url = f"urn:uuid:{composition_id}"
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

    return composition, full_url


def _create_section(note: ClinicalNote) -> dict[str, Any] | None:
    """
    Create a Composition section from a clinical note.

    Args:
        note: The clinical note to convert

    Returns:
        FHIR CompositionSection or None if note type is unknown
    """
    # Map note type to SOAP section
    note_type_lower = note.note_type.lower()
    soap_section = NOTE_TYPE_TO_SOAP.get(note_type_lower)

    # Try partial matching if exact match not found
    if not soap_section:
        for key, mapped_section in NOTE_TYPE_TO_SOAP.items():
            if key in note_type_lower or note_type_lower in key:
                soap_section = mapped_section
                break

    # Default to Additional if no mapping found
    if not soap_section:
        soap_section = "Additional"

    loinc_info = SOAP_LOINC_CODES[soap_section]

    # Strip HTML tags and clean up the content
    clean_content = _strip_html(note.content)

    section: dict[str, Any] = {
        "title": soap_section,  # Use SOAP section name, not original note type
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
            # Note: FHIR spec requires XHTML div wrapper, but omnia frontend
            # renders text.div as plain text (no dangerouslySetInnerHTML).
            # Using plain text for UI compatibility.
            "div": clean_content,
        },
    }

    return section


def _strip_html(text: str) -> str:
    """Strip HTML tags from text, preserving content."""
    import re

    # Remove HTML tags
    clean = re.sub(r"<[^>]+>", "", text)
    # Decode common HTML entities
    clean = (
        clean.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
    )
    # Normalize whitespace
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def _escape_html(text: str) -> str:
    """Escape HTML special characters in text."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )
