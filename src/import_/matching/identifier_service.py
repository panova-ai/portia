"""
Import service for handling re-imports.

Provides:
- Import source tagging (to identify import-created resources)
- Duplicate detection within a bundle
- Pre-import cleanup of existing import-created resources
"""

import logging
from typing import Any
from uuid import UUID

logger = logging.getLogger(__name__)

# Tag system for identifying import-created resources
IMPORT_SOURCE_TAG_SYSTEM = "https://panova.ai/import-source"


def tag_bundle_for_import(
    bundle: dict[str, Any],
    source_system: str,
    patient_id: UUID,
) -> dict[str, Any]:
    """
    Tag all resources in a bundle with import source metadata.

    This enables selective deletion on re-import - only resources
    with this tag will be deleted, preserving manually-created resources.

    Args:
        bundle: FHIR Bundle with resources
        source_system: Source system identifier (e.g., "charm")
        patient_id: The matched/created patient ID

    Returns:
        Modified bundle with tagged resources
    """
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if not resource:
            continue

        resource_type = resource.get("resourceType")

        # Skip Patient - we don't want to delete/recreate the patient
        if resource_type == "Patient":
            continue

        # Add import source tag to resource meta
        _add_import_tag(resource, source_system)

        # Update subject reference to use matched patient
        # Note: In R5, some resources like Composition have subject as an array
        if "subject" in resource:
            subject_ref = {"reference": f"Patient/{patient_id}"}
            if isinstance(resource["subject"], list):
                resource["subject"] = [subject_ref]
            else:
                resource["subject"] = subject_ref

    return bundle


def _add_import_tag(resource: dict[str, Any], source_system: str) -> None:
    """Add import source tag to a resource's meta.tag."""
    meta = resource.get("meta", {})
    tags = meta.get("tag", [])

    # Check if tag already exists
    for tag in tags:
        if tag.get("system") == IMPORT_SOURCE_TAG_SYSTEM:
            tag["code"] = source_system
            return

    # Add new tag
    tags.append(
        {
            "system": IMPORT_SOURCE_TAG_SYSTEM,
            "code": source_system,
        }
    )

    meta["tag"] = tags
    resource["meta"] = meta


def remove_duplicate_resources(bundle: dict[str, Any]) -> tuple[dict[str, Any], int]:
    """
    Remove duplicate resources from a bundle based on content.

    Duplicates are detected by matching:
    - Encounter: same date
    - Condition: same code + onset date
    - Composition: same date
    - Other resources: kept as-is

    Args:
        bundle: FHIR Bundle to deduplicate

    Returns:
        Tuple of (deduplicated bundle, count of removed duplicates)
    """
    seen_keys: set[str] = set()
    duplicates_removed = 0
    kept_entries: list[dict[str, Any]] = []

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")

        # Generate a dedup key based on resource type
        dedup_key = _get_dedup_key(resource, resource_type)

        if dedup_key:
            if dedup_key in seen_keys:
                duplicates_removed += 1
                logger.debug(f"Removing duplicate {resource_type}: {dedup_key}")
                continue
            seen_keys.add(dedup_key)

        kept_entries.append(entry)

    bundle["entry"] = kept_entries
    return bundle, duplicates_removed


def _get_dedup_key(resource: dict[str, Any], resource_type: str | None) -> str | None:
    """Generate a deduplication key for a resource."""
    if resource_type == "Encounter":
        # Dedupe by date
        period = resource.get("actualPeriod", {})
        start = period.get("start", "")
        if start:
            date_part = start.split("T")[0] if "T" in start else start[:10]
            return f"Encounter:{date_part}"

    elif resource_type == "Condition":
        # Dedupe by code + onset date
        code = _extract_code(resource)
        onset = resource.get("onsetDateTime") or resource.get("recordedDate", "")
        if code and onset:
            date_part = onset.split("T")[0] if "T" in onset else onset[:10]
            return f"Condition:{code}:{date_part}"

    elif resource_type == "Composition":
        # Dedupe by date
        date_str = resource.get("date", "")
        if date_str:
            date_part = date_str.split("T")[0] if "T" in date_str else date_str[:10]
            return f"Composition:{date_part}"

    return None


def _extract_code(resource: dict[str, Any]) -> str | None:
    """Extract primary code from a coded resource."""
    code_elem = resource.get("code", {})
    codings = code_elem.get("coding", [])
    if codings:
        code = codings[0].get("code")
        return str(code) if code is not None else None
    return None


# Resource types that should be deleted on re-import
IMPORTABLE_RESOURCE_TYPES = [
    "Encounter",
    "Condition",
    "MedicationStatement",
    "Composition",
    "Observation",
    "Procedure",
    "Immunization",
    "DiagnosticReport",
    "DocumentReference",
    # Note: Patient is NOT included - we keep the patient
    # Note: Practitioner and Organization are NOT included - they're shared
    # Note: Medication is NOT included - they're contained/referenced
]


def get_import_resource_types() -> list[str]:
    """Get the list of resource types that are managed by import."""
    return IMPORTABLE_RESOURCE_TYPES.copy()
