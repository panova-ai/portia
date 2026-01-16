"""
Resource linker for CHARM imports.

Creates Encounter resources and links Conditions, Medications, and other
resources to their appropriate Encounters based on date matching.
"""

from datetime import date
from typing import Any
from uuid import uuid4

from src.import_.charm.extractor import CharmExtractionResult, EncounterData


def link_resources_to_encounters(
    fhir_bundle: dict[str, Any],
    extraction_result: CharmExtractionResult,
) -> tuple[dict[str, Any], list[str]]:
    """
    Create Encounter resources and link existing resources to them.

    Args:
        fhir_bundle: The FHIR R4 bundle from MS Converter
        extraction_result: Extracted data from CHARM C-CDA

    Returns:
        Tuple of (modified bundle, warnings)
    """
    warnings: list[str] = []

    # Get the patient reference from the bundle
    patient_ref = _find_patient_reference(fhir_bundle)
    if not patient_ref:
        warnings.append("Could not find Patient reference in bundle")
        return fhir_bundle, warnings

    # Get practitioner reference
    practitioner_ref = _find_practitioner_reference(fhir_bundle)

    # Get organization reference
    organization_ref = _find_organization_reference(fhir_bundle)

    # Build a mapping from C-CDA IDs to FHIR resource references
    ccda_to_fhir = _build_ccda_to_fhir_map(fhir_bundle)

    # Create Encounter resources
    encounter_entries: list[dict[str, Any]] = []
    encounter_date_to_ref: dict[date, str] = {}

    for enc_data in extraction_result.encounters:
        encounter, enc_ref = _create_encounter(
            enc_data,
            patient_ref,
            practitioner_ref,
            organization_ref,
        )
        encounter_entries.append({"resource": encounter})
        encounter_date_to_ref[enc_data.date] = enc_ref

    # Link Conditions to Encounters
    condition_links = 0
    for entry in fhir_bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Condition":
            linked = _link_condition_to_encounter(
                resource,
                extraction_result.problems,
                ccda_to_fhir,
                encounter_date_to_ref,
            )
            if linked:
                condition_links += 1

    if condition_links:
        warnings.append(f"Linked {condition_links} Conditions to Encounters")

    # Link MedicationStatements to Encounters
    med_links = 0
    for entry in fhir_bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "MedicationStatement":
            linked = _link_medication_to_encounter(
                resource,
                extraction_result.medications,
                ccda_to_fhir,
                encounter_date_to_ref,
            )
            if linked:
                med_links += 1

    if med_links:
        warnings.append(f"Linked {med_links} MedicationStatements to Encounters")

    # Add Encounter entries to the bundle
    existing_entries = fhir_bundle.get("entry", [])
    fhir_bundle["entry"] = encounter_entries + existing_entries

    warnings.append(f"Created {len(encounter_entries)} Encounter resources")

    return fhir_bundle, warnings


def _find_patient_reference(bundle: dict[str, Any]) -> str | None:
    """Find the Patient resource reference in the bundle."""
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Patient":
            patient_id = resource.get("id")
            if patient_id:
                return f"Patient/{patient_id}"
            # Try fullUrl
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


def _build_ccda_to_fhir_map(bundle: dict[str, Any]) -> dict[str, str]:
    """
    Build a mapping from C-CDA IDs to FHIR resource references.

    The MS Converter preserves C-CDA identifiers in the FHIR resource IDs
    or identifier fields.
    """
    mapping: dict[str, str] = {}

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")
        resource_id = resource.get("id")

        if not resource_type or not resource_id:
            continue

        # The resource ID is often derived from the C-CDA ID
        fhir_ref = f"{resource_type}/{resource_id}"

        # Check identifiers for C-CDA ID
        for identifier in resource.get("identifier", []):
            value = identifier.get("value", "")

            # Map both the full identifier and just the value
            if value:
                mapping[value] = fhir_ref
                # Also try with urn:uuid: prefix stripped
                if value.startswith("urn:uuid:"):
                    mapping[value[9:]] = fhir_ref

        # Also map the resource ID directly
        mapping[resource_id] = fhir_ref

    return mapping


def _create_encounter(
    enc_data: EncounterData,
    patient_ref: str,
    practitioner_ref: str | None,
    organization_ref: str | None,
) -> tuple[dict[str, Any], str]:
    """
    Create a FHIR Encounter resource from extracted encounter data.

    Returns tuple of (Encounter resource, reference string).
    """
    encounter_id = str(uuid4())
    enc_ref = f"Encounter/{encounter_id}"

    # Format date as FHIR datetime
    date_str = enc_data.date.isoformat()

    encounter: dict[str, Any] = {
        "resourceType": "Encounter",
        "id": encounter_id,
        "status": "completed",
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": "AMB",
            "display": "ambulatory",
        },
        "type": [
            {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "185463005",
                        "display": "Visit out of hours",
                    }
                ],
                "text": "Psychiatry visit",
            }
        ],
        "subject": {"reference": patient_ref},
        "actualPeriod": {
            "start": f"{date_str}T00:00:00Z",
            "end": f"{date_str}T23:59:59Z",
        },
    }

    # Add service provider if we have an organization
    if organization_ref:
        encounter["serviceProvider"] = {"reference": organization_ref}

    # Add participant if we have a practitioner
    if practitioner_ref:
        encounter["participant"] = [
            {
                "type": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                                "code": "PPRF",
                                "display": "primary performer",
                            }
                        ]
                    }
                ],
                "actor": {"reference": practitioner_ref},
            }
        ]

    # Add diagnosis references for linked conditions
    # (These will be updated after conditions are linked)

    return encounter, enc_ref


def _link_condition_to_encounter(
    condition: dict[str, Any],
    problems: list[Any],
    ccda_to_fhir: dict[str, str],
    encounter_date_to_ref: dict[date, str],
) -> bool:
    """
    Link a Condition resource to its appropriate Encounter.

    Uses the onset date to match with encounter dates.
    Returns True if linked.
    """
    # Get the onset date from the condition
    onset = condition.get("onsetDateTime") or condition.get("onsetPeriod", {}).get(
        "start"
    )
    if not onset:
        # Try recordedDate
        onset = condition.get("recordedDate")

    if not onset:
        return False

    # Parse the date
    try:
        # Handle various date formats
        if "T" in onset:
            onset_date = date.fromisoformat(onset.split("T")[0])
        else:
            onset_date = date.fromisoformat(onset[:10])
    except (ValueError, TypeError):
        return False

    # Find the matching encounter (exact date or closest prior date)
    matching_enc_ref = None
    for enc_date, enc_ref in sorted(encounter_date_to_ref.items()):
        if enc_date == onset_date:
            matching_enc_ref = enc_ref
            break
        elif enc_date < onset_date:
            matching_enc_ref = enc_ref  # Keep the most recent one before onset

    if matching_enc_ref:
        condition["encounter"] = {"reference": matching_enc_ref}
        return True

    return False


def _link_medication_to_encounter(
    medication: dict[str, Any],
    medications: list[Any],
    ccda_to_fhir: dict[str, str],
    encounter_date_to_ref: dict[date, str],
) -> bool:
    """
    Link a MedicationStatement resource to its appropriate Encounter.

    Uses the effective date to match with encounter dates.
    Returns True if linked.
    """
    # Get the effective date from the medication
    effective = medication.get("effectiveDateTime") or medication.get(
        "effectivePeriod", {}
    ).get("start")
    if not effective:
        # Try dateAsserted
        effective = medication.get("dateAsserted")

    if not effective:
        return False

    # Parse the date
    try:
        if "T" in effective:
            effective_date = date.fromisoformat(effective.split("T")[0])
        else:
            effective_date = date.fromisoformat(effective[:10])
    except (ValueError, TypeError):
        return False

    # Find the matching encounter
    matching_enc_ref = None
    for enc_date, enc_ref in sorted(encounter_date_to_ref.items()):
        if enc_date == effective_date:
            matching_enc_ref = enc_ref
            break
        elif enc_date < effective_date:
            matching_enc_ref = enc_ref

    if matching_enc_ref:
        # In FHIR R4, MedicationStatement uses 'context' for encounter
        medication["context"] = {"reference": matching_enc_ref}
        return True

    return False
