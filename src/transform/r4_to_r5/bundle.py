"""
Bundle transformer for R4 to R5 conversion.

Transforms an entire FHIR R4 Bundle to FHIR R5 format by
routing each entry to the appropriate resource transformer.
"""

from typing import Any

from src.schemas.import_schemas import ResourceCounts
from src.transform.r4_to_r5.allergy import transform_allergy_intolerance
from src.transform.r4_to_r5.composition import transform_composition
from src.transform.r4_to_r5.condition import transform_condition
from src.transform.r4_to_r5.encounter import transform_encounter
from src.transform.r4_to_r5.immunization import transform_immunization
from src.transform.r4_to_r5.medication import transform_medication_statement
from src.transform.r4_to_r5.observation import transform_observation
from src.transform.r4_to_r5.patient import transform_patient

# Map R4 resource types to their transformers
RESOURCE_TRANSFORMERS: dict[str, Any] = {
    "Patient": transform_patient,
    "Condition": transform_condition,
    "MedicationStatement": transform_medication_statement,
    "AllergyIntolerance": transform_allergy_intolerance,
    "Immunization": transform_immunization,
    "Observation": transform_observation,
    "Encounter": transform_encounter,
    "Composition": transform_composition,
}

# Fields that should always be arrays (0..*) in FHIR
ARRAY_FIELDS = {
    "identifier",
    "basedOn",
    "partOf",
    "category",
    "performer",
    "note",
}


def transform_bundle(
    r4_bundle: dict[str, Any],
) -> tuple[dict[str, Any], ResourceCounts, list[str]]:
    """
    Transform a FHIR R4 Bundle to FHIR R5.

    Args:
        r4_bundle: The FHIR R4 Bundle from MS Converter

    Returns:
        Tuple of (R5 Bundle, resource counts, warnings)
    """
    warnings: list[str] = []
    counts = ResourceCounts()

    r5_entries: list[dict[str, Any]] = []

    for entry in r4_bundle.get("entry", []):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")

        if not resource_type:
            warnings.append("Entry missing resourceType, skipping")
            continue

        # Get the transformer for this resource type
        transformer = RESOURCE_TRANSFORMERS.get(resource_type)

        if transformer:
            try:
                r5_resource = transformer(resource)

                # Normalize array fields
                r5_resource = _normalize_array_fields(r5_resource)

                # Map MedicationStatement to MedicationUsage for counting
                count_type = resource_type
                if resource_type == "MedicationStatement":
                    count_type = "MedicationUsage"

                # Update counts
                if hasattr(counts, count_type):
                    setattr(counts, count_type, getattr(counts, count_type) + 1)

                # Create R5 entry
                r5_entry = {
                    "fullUrl": entry.get("fullUrl"),
                    "resource": r5_resource,
                }

                # Preserve request if present (for transaction bundles)
                if "request" in entry:
                    r5_entry["request"] = _transform_request(
                        entry["request"], resource_type, r5_resource.get("resourceType")
                    )

                r5_entries.append(r5_entry)

            except Exception as e:
                warnings.append(f"Failed to transform {resource_type}: {e!s}")
        else:
            # Pass through resources without specific transformers
            # Still normalize array fields
            normalized_resource = _normalize_array_fields(resource.copy())
            r5_entries.append({**entry, "resource": normalized_resource})

            # Count known resource types
            if hasattr(counts, resource_type):
                setattr(counts, resource_type, getattr(counts, resource_type) + 1)

    # Build R5 Bundle
    r5_bundle: dict[str, Any] = {
        "resourceType": "Bundle",
        "type": r4_bundle.get("type", "collection"),
        "entry": r5_entries,
    }

    # Preserve bundle metadata if present
    if "id" in r4_bundle:
        r5_bundle["id"] = r4_bundle["id"]
    if "timestamp" in r4_bundle:
        r5_bundle["timestamp"] = r4_bundle["timestamp"]

    return r5_bundle, counts, warnings


def _normalize_array_fields(resource: dict[str, Any]) -> dict[str, Any]:
    """
    Ensure common array fields are always arrays.

    Many FHIR fields like 'identifier' are 0..* (arrays) but may come
    from MS Converter as single objects. This ensures compliance with
    FHIR R5 schema.
    """
    for field in ARRAY_FIELDS:
        if field in resource:
            value = resource[field]
            if not isinstance(value, list):
                resource[field] = [value]
    return resource


def _transform_request(
    request: dict[str, Any],
    r4_type: str,
    r5_type: str | None,
) -> dict[str, Any]:
    """Transform bundle entry request, updating resource type references."""
    new_request = request.copy()

    # Update URL if resource type changed (e.g., MedicationStatement -> MedicationUsage)
    if r5_type and r4_type != r5_type:
        url = request.get("url", "")
        if url.startswith(r4_type):
            new_request["url"] = url.replace(r4_type, r5_type, 1)

    return new_request
