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
from src.transform.r4_to_r5.organization import transform_organization
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
    "Organization": transform_organization,
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

                # Get the actual resource type from the transformed resource
                # (handles cases where transformer changes the type)
                count_type = r5_resource.get("resourceType", resource_type)

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

    # Clean up orphaned encounter references
    orphan_warnings = _clean_orphaned_encounter_refs(r5_bundle)
    warnings.extend(orphan_warnings)

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


def _clean_orphaned_encounter_refs(bundle: dict[str, Any]) -> list[str]:
    """
    Fix encounter references to use fullUrl format and remove orphaned ones.

    FHIR transaction bundles require urn:uuid format for local references.
    This converts Encounter/{id} references to the fullUrl format and removes
    references to Encounters not in the bundle.
    """
    import logging

    logger = logging.getLogger(__name__)
    warnings: list[str] = []

    # Build mappings of Encounter IDs to fullUrls
    enc_id_to_fullurl: dict[str, str] = {}
    valid_encounter_refs: set[str] = set()
    encounter_count = 0

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Encounter":
            encounter_count += 1
            enc_id = resource.get("id")
            full_url = entry.get("fullUrl")

            if full_url:
                valid_encounter_refs.add(full_url)
                if enc_id:
                    enc_id_to_fullurl[enc_id] = full_url
                    # Also map the Encounter/{id} format
                    enc_id_to_fullurl[f"Encounter/{enc_id}"] = full_url
            elif enc_id:
                # No fullUrl, just mark the Encounter/{id} as valid
                valid_encounter_refs.add(f"Encounter/{enc_id}")
                warnings.append(
                    f"Encounter {enc_id} has no fullUrl - will use Encounter/id format"
                )

    # Use warnings list instead of logging for visibility in response
    warnings.append(
        f"Encounter ref cleanup: {encounter_count} encounters, "
        f"{len(valid_encounter_refs)} valid refs, {len(enc_id_to_fullurl)//2} ID mappings"
    )

    orphaned_count = 0
    converted_count = 0
    total_refs_checked = 0
    refs_by_type: dict[str, list[str]] = {}

    def process_reference(
        ref_value: dict[str, Any], resource_type: str, field: str
    ) -> bool:
        """Process a single reference, converting or removing as needed.
        Returns True if the reference should be deleted."""
        nonlocal converted_count, orphaned_count, total_refs_checked
        ref_str = ref_value.get("reference", "")
        if not ref_str:
            return False

        # Track all encounter refs we see
        if ref_str.startswith(("Encounter/", "urn:uuid:")):
            total_refs_checked += 1
            key = f"{resource_type}.{field}"
            if key not in refs_by_type:
                refs_by_type[key] = []
            refs_by_type[key].append(ref_str[:50])  # Truncate for readability

        # Check if reference is in Encounter/{id} format that needs conversion
        if ref_str.startswith("Encounter/") and ref_str in enc_id_to_fullurl:
            logger.debug(
                "Converting %s.%s: %s -> %s",
                resource_type,
                field,
                ref_str,
                enc_id_to_fullurl[ref_str],
            )
            ref_value["reference"] = enc_id_to_fullurl[ref_str]
            converted_count += 1
            return False
        elif ref_str.startswith(("Encounter/", "urn:uuid:")):
            # Check if it's a valid reference
            if ref_str not in valid_encounter_refs:
                logger.warning(
                    "Removing orphaned %s.%s: %s (not in valid refs)",
                    resource_type,
                    field,
                    ref_str,
                )
                orphaned_count += 1
                return True
        return False

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")

        if resource_type == "Encounter":
            continue

        # Check top-level encounter and context fields
        for field in ["encounter", "context"]:
            if field in resource:
                ref_value = resource[field]
                if isinstance(ref_value, dict):
                    if process_reference(ref_value, resource_type, field):
                        del resource[field]

        # Check nested context.encounter (e.g., DocumentReference)
        if "context" in resource and isinstance(resource["context"], dict):
            context = resource["context"]
            if "encounter" in context:
                enc_refs = context["encounter"]
                # Can be a single reference or array of references
                if isinstance(enc_refs, list):
                    # Process each reference in the list
                    to_remove = []
                    for i, ref_value in enumerate(enc_refs):
                        if isinstance(ref_value, dict) and process_reference(
                            ref_value, resource_type, "context.encounter"
                        ):
                            to_remove.append(i)
                    # Remove orphaned refs in reverse order
                    for i in reversed(to_remove):
                        enc_refs.pop(i)
                elif isinstance(enc_refs, dict):
                    if process_reference(enc_refs, resource_type, "context.encounter"):
                        del context["encounter"]

        # Check Encounter.partOf (references another Encounter)
        if resource_type == "Encounter" and "partOf" in resource:
            ref_value = resource["partOf"]
            if isinstance(ref_value, dict):
                if process_reference(ref_value, resource_type, "partOf"):
                    del resource["partOf"]

    # Summary of references found
    warnings.append(f"Checked {total_refs_checked} encounter refs")
    for key, refs in refs_by_type.items():
        warnings.append(
            f"  {key}: {len(refs)} refs (sample: {refs[0] if refs else 'none'})"
        )

    if converted_count:
        warnings.append(
            f"Converted {converted_count} encounter references to fullUrl format"
        )
    if orphaned_count:
        warnings.append(f"Removed {orphaned_count} orphaned encounter references")

    return warnings
