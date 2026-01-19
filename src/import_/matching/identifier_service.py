"""
Identifier service for idempotent resource creation.

Generates stable, deterministic identifiers for imported resources to enable:
- Idempotent imports (same data → same identifiers → no duplicates)
- Conditional FHIR operations (PUT with identifier matching)
- Traceability back to source data
"""

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any
from uuid import UUID

logger = logging.getLogger(__name__)

# Identifier systems used for imported resources
IMPORT_IDENTIFIER_SYSTEM = "urn:panova:import"
ENCOUNTER_IDENTIFIER_SYSTEM = f"{IMPORT_IDENTIFIER_SYSTEM}:encounter"
CONDITION_IDENTIFIER_SYSTEM = f"{IMPORT_IDENTIFIER_SYSTEM}:condition"
COMPOSITION_IDENTIFIER_SYSTEM = f"{IMPORT_IDENTIFIER_SYSTEM}:composition"
MEDICATION_IDENTIFIER_SYSTEM = f"{IMPORT_IDENTIFIER_SYSTEM}:medication"
OBSERVATION_IDENTIFIER_SYSTEM = f"{IMPORT_IDENTIFIER_SYSTEM}:observation"


@dataclass
class ResourceIdentifier:
    """A FHIR identifier for an imported resource."""

    system: str
    value: str

    def to_fhir(self) -> dict[str, str]:
        """Convert to FHIR Identifier format."""
        return {"system": self.system, "value": self.value}

    def to_search_param(self) -> str:
        """Convert to FHIR search parameter format (system|value)."""
        return f"{self.system}|{self.value}"


class IdentifierService:
    """
    Generates stable identifiers for imported FHIR resources.

    Identifiers are deterministic based on:
    - Patient ID (ties resource to specific patient)
    - Date (ties resource to specific encounter/time)
    - Code (ties resource to specific clinical concept)
    - Source ID (ties resource to source system record)

    This enables idempotent imports where re-importing the same data
    will match existing resources rather than creating duplicates.
    """

    def __init__(self, source_system: str = "charm"):
        """
        Initialize the identifier service.

        Args:
            source_system: The source system name (e.g., "charm")
        """
        self.source_system = source_system

    def encounter_identifier(
        self,
        patient_id: UUID,
        encounter_date: date,
    ) -> ResourceIdentifier:
        """
        Generate identifier for an Encounter.

        Format: {patient_id}:{YYYY-MM-DD}

        This ensures one Encounter per patient per date.
        """
        value = f"{patient_id}:{encounter_date.isoformat()}"
        return ResourceIdentifier(
            system=ENCOUNTER_IDENTIFIER_SYSTEM,
            value=value,
        )

    def condition_identifier(
        self,
        patient_id: UUID,
        code: str,
        onset_date: date,
    ) -> ResourceIdentifier:
        """
        Generate identifier for a Condition.

        Format: {patient_id}:{code}:{YYYY-MM-DD}

        This ensures one Condition per patient per code per onset date.
        For CHARM imports where the same condition is documented per-encounter,
        this will create one Condition per encounter (which is the intended behavior).
        """
        value = f"{patient_id}:{code}:{onset_date.isoformat()}"
        return ResourceIdentifier(
            system=CONDITION_IDENTIFIER_SYSTEM,
            value=value,
        )

    def composition_identifier(
        self,
        patient_id: UUID,
        encounter_date: date,
    ) -> ResourceIdentifier:
        """
        Generate identifier for a Composition (clinical notes).

        Format: {patient_id}:{YYYY-MM-DD}

        This ensures one Composition per patient per encounter date.
        """
        value = f"{patient_id}:{encounter_date.isoformat()}"
        return ResourceIdentifier(
            system=COMPOSITION_IDENTIFIER_SYSTEM,
            value=value,
        )

    def medication_identifier(
        self,
        patient_id: UUID,
        code: str,
        start_date: date | None,
    ) -> ResourceIdentifier:
        """
        Generate identifier for a MedicationStatement.

        Format: {patient_id}:{code}:{YYYY-MM-DD}

        This ensures one MedicationStatement per patient per medication per start date.
        """
        date_part = start_date.isoformat() if start_date else "unknown"
        value = f"{patient_id}:{code}:{date_part}"
        return ResourceIdentifier(
            system=MEDICATION_IDENTIFIER_SYSTEM,
            value=value,
        )

    def observation_identifier(
        self,
        patient_id: UUID,
        code: str,
        effective_date: date,
    ) -> ResourceIdentifier:
        """
        Generate identifier for an Observation.

        Format: {patient_id}:{code}:{YYYY-MM-DD}
        """
        value = f"{patient_id}:{code}:{effective_date.isoformat()}"
        return ResourceIdentifier(
            system=OBSERVATION_IDENTIFIER_SYSTEM,
            value=value,
        )

    def add_identifier_to_resource(
        self,
        resource: dict[str, Any],
        identifier: ResourceIdentifier,
    ) -> dict[str, Any]:
        """
        Add an identifier to a FHIR resource.

        If the resource already has identifiers, appends to the list.
        If not, creates the identifier array.
        """
        identifiers = resource.get("identifier", [])

        # Check if this identifier already exists
        for existing in identifiers:
            if (
                existing.get("system") == identifier.system
                and existing.get("value") == identifier.value
            ):
                return resource  # Already has this identifier

        identifiers.append(identifier.to_fhir())
        resource["identifier"] = identifiers
        return resource

    def create_conditional_request(
        self,
        resource_type: str,
        identifier: ResourceIdentifier,
    ) -> dict[str, str]:
        """
        Create a FHIR Bundle request for conditional PUT.

        This enables idempotent writes where:
        - If resource with identifier exists → update it
        - If resource doesn't exist → create it
        """
        return {
            "method": "PUT",
            "url": f"{resource_type}?identifier={identifier.to_search_param()}",
        }


def add_identifiers_to_bundle(
    bundle: dict[str, Any],
    patient_id: UUID,
    identifier_service: IdentifierService,
) -> tuple[dict[str, Any], list[str]]:
    """
    Add stable identifiers to all resources in a bundle.

    This transforms a collection bundle into a transaction bundle
    with conditional PUT operations for idempotent imports.

    Args:
        bundle: FHIR Bundle with resources
        patient_id: The matched/created patient ID
        identifier_service: Service for generating identifiers

    Returns:
        Tuple of (modified bundle, warnings list)
    """
    id_warnings: list[str] = []

    # Log initial state - find ALL Encounter references BEFORE any processing
    initial_enc_refs = _find_all_encounter_refs(bundle)
    id_warnings.append(f"INITIAL: Found {len(initial_enc_refs)} Encounter refs")
    for ref_info in initial_enc_refs[:10]:  # First 10
        id_warnings.append(f"  INITIAL REF: {ref_info}")

    # Change bundle type to transaction for conditional operations
    bundle["type"] = "transaction"

    # Track assigned identifiers and their fullUrls to handle duplicates
    # Maps identifier search param -> fullUrl of the kept entry
    identifier_to_fullurl: dict[str, str] = {}
    # Maps duplicate fullUrl/ID -> kept fullUrl (for reference remapping)
    duplicate_refs: dict[str, str] = {}
    # Maps ResourceType/id -> fullUrl for ALL resources (to normalize references)
    id_to_fullurl: dict[str, str] = {}

    # First pass: collect all resource id -> fullUrl mappings
    encounter_count = 0
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")
        entry_fullurl = entry.get("fullUrl", "")
        resource_id = resource.get("id", "")

        if resource_type == "Encounter":
            encounter_count += 1
            id_warnings.append(
                f"Encounter #{encounter_count}: id={resource_id}, fullUrl={entry_fullurl[:50]}"
            )
            if not resource_id or not entry_fullurl:
                id_warnings.append(
                    f"  WARNING: Encounter #{encounter_count} missing id or fullUrl!"
                )

        if resource_type and resource_id and entry_fullurl:
            # Map "ResourceType/id" -> fullUrl for reference normalization
            id_to_fullurl[f"{resource_type}/{resource_id}"] = entry_fullurl

    id_warnings.append(
        f"Collected {len(id_to_fullurl)} id->fullUrl mappings, "
        f"{sum(1 for k in id_to_fullurl if k.startswith('Encounter/'))} are Encounters"
    )

    # Log sample Encounter mappings
    enc_mappings = [
        (k, v) for k, v in id_to_fullurl.items() if k.startswith("Encounter/")
    ]
    for k, v in enc_mappings[:3]:
        id_warnings.append(f"  Mapping: {k} -> {v[:50]}")

    # Second pass: process identifiers and mark duplicates
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")

        if not resource_type:
            continue

        identifier = None

        # Flag to skip conditional PUT for resources that are heavily referenced
        skip_conditional_put = False

        if resource_type == "Encounter":
            # Skip conditional PUT for Encounters - they are referenced by many other
            # resources (Conditions, MedicationStatements, Compositions) and using
            # conditional PUT breaks reference resolution in transaction bundles.
            # The FHIR server can't pre-resolve urn:uuid references to Encounters
            # when their final ID depends on the conditional evaluation.
            # We still generate identifiers for duplicate detection, but the
            # Encounters will use POST with urn:uuid fullUrl.
            skip_conditional_put = True
            enc_date = _extract_date_from_period(resource.get("actualPeriod", {}))
            if enc_date:
                identifier = identifier_service.encounter_identifier(
                    patient_id, enc_date
                )

        elif resource_type == "Condition":
            # Extract code and onset
            code = _extract_code(resource)
            onset_date = _extract_date(
                resource.get("onsetDateTime") or resource.get("recordedDate")
            )
            if code and onset_date:
                identifier = identifier_service.condition_identifier(
                    patient_id, code, onset_date
                )

        elif resource_type == "Composition":
            # Extract date
            comp_date = _extract_date(resource.get("date"))
            if comp_date:
                identifier = identifier_service.composition_identifier(
                    patient_id, comp_date
                )

        elif resource_type == "MedicationStatement":
            # Extract code and effective date
            code = _extract_medication_code(resource)
            start_date = _extract_date_from_period(
                resource.get("effectivePeriod", {})
            ) or _extract_date(resource.get("effectiveDateTime"))
            if code:
                identifier = identifier_service.medication_identifier(
                    patient_id, code, start_date
                )

        elif resource_type == "Observation":
            # Extract code and effective date
            code = _extract_code(resource)
            eff_date = _extract_date(resource.get("effectiveDateTime"))
            if code and eff_date:
                identifier = identifier_service.observation_identifier(
                    patient_id, code, eff_date
                )

        # Add identifier and conditional request (mark duplicates for removal)
        if identifier:
            search_param = identifier.to_search_param()
            if search_param in identifier_to_fullurl:
                # Mark for removal - another resource already has this identifier
                entry["_duplicate"] = True
                # Record reference mapping for this duplicate
                entry_fullurl = entry.get("fullUrl", "")
                entry_id = f"{resource_type}/{resource.get('id', '')}"
                kept_fullurl = identifier_to_fullurl[search_param]
                if entry_fullurl:
                    duplicate_refs[entry_fullurl] = kept_fullurl
                if resource.get("id"):
                    duplicate_refs[entry_id] = kept_fullurl
                logger.warning(
                    f"Duplicate {resource_type}: {entry_id} -> {kept_fullurl}"
                )
            else:
                entry_fullurl = entry.get("fullUrl", "")
                identifier_to_fullurl[search_param] = entry_fullurl
                identifier_service.add_identifier_to_resource(resource, identifier)
                # Only add conditional PUT for resources that aren't heavily referenced
                # Resources like Encounters are referenced by many others, and conditional
                # PUT breaks reference resolution in transaction bundles
                if not skip_conditional_put:
                    entry["request"] = identifier_service.create_conditional_request(
                        resource_type, identifier
                    )

        # Update subject reference to use matched patient
        # Note: In R5, some resources like Composition have subject as an array
        if "subject" in resource:
            subject_ref = {"reference": f"Patient/{patient_id}"}
            if isinstance(resource["subject"], list):
                resource["subject"] = [subject_ref]
            else:
                resource["subject"] = subject_ref

    # Build the complete reference map:
    # 1. Duplicate refs -> kept fullUrl
    # 2. ResourceType/id -> fullUrl (normalize all references)
    ref_map: dict[str, str] = {}

    # First add id_to_fullurl (for all resources)
    for id_ref, fullurl in id_to_fullurl.items():
        # Only add if not already a urn:uuid reference
        if id_ref != fullurl:
            ref_map[id_ref] = fullurl

    # Then add duplicate refs (overrides if needed)
    ref_map.update(duplicate_refs)

    # Log ref_map details
    enc_ref_map = {k: v for k, v in ref_map.items() if "Encounter" in k}
    id_warnings.append(
        f"ref_map has {len(ref_map)} total, {len(enc_ref_map)} Encounter mappings"
    )
    for k, v in list(enc_ref_map.items())[:5]:
        id_warnings.append(f"  ref_map: {k} -> {v[:50]}")

    # Find refs BEFORE remapping
    pre_remap_refs = _find_all_encounter_refs(bundle)
    id_warnings.append(f"PRE-REMAP: {len(pre_remap_refs)} Encounter refs")
    for ref_info in pre_remap_refs[:10]:
        id_warnings.append(f"  PRE-REMAP: {ref_info}")

    # Remap references to normalize all to fullUrl format
    if ref_map:
        id_warnings.append(f"Remapping {len(ref_map)} references")
        _remap_references(bundle, ref_map, id_warnings)

    # Find refs AFTER remapping
    post_remap_refs = _find_all_encounter_refs(bundle)
    id_warnings.append(f"POST-REMAP: {len(post_remap_refs)} Encounter refs")
    for ref_info in post_remap_refs[:10]:
        id_warnings.append(f"  POST-REMAP: {ref_info}")

    # Remove duplicate entries (marked earlier)
    duplicates_removed = sum(1 for e in bundle.get("entry", []) if e.get("_duplicate"))
    bundle["entry"] = [e for e in bundle.get("entry", []) if not e.get("_duplicate")]
    id_warnings.append(f"Removed {duplicates_removed} duplicate entries from bundle")

    # Check for unrewritten Encounter references (ResourceType/id format)
    unrewritten = _find_unrewritten_refs(bundle, "Encounter/")
    id_warnings.append(f"UNREWRITTEN: {len(unrewritten)} Encounter/ refs remaining")
    for ref_info in unrewritten[:20]:  # Show more to find the problem
        id_warnings.append(f"  UNREWRITTEN: {ref_info}")

    # Log final resource counts
    resource_counts: dict[str, int] = {}
    encounter_fullurls: list[str] = []
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        rtype = resource.get("resourceType", "Unknown")
        resource_counts[rtype] = resource_counts.get(rtype, 0) + 1
        if rtype == "Encounter":
            encounter_fullurls.append(entry.get("fullUrl", "NO_FULLURL"))
    id_warnings.append(f"Final bundle: {resource_counts}")
    id_warnings.append(f"Sample Encounter fullUrls: {encounter_fullurls[:3]}")

    # Final check - find ALL Encounter refs in final bundle
    final_refs = _find_all_encounter_refs(bundle)
    id_warnings.append(f"FINAL: {len(final_refs)} total Encounter refs")
    # Count how many are urn:uuid vs Encounter/
    urn_refs = [r for r in final_refs if "urn:uuid" in r]
    enc_id_refs = [r for r in final_refs if "Encounter/" in r and "urn:uuid" not in r]
    id_warnings.append(
        f"  urn:uuid refs: {len(urn_refs)}, Encounter/ refs: {len(enc_id_refs)}"
    )
    for ref_info in enc_id_refs[:10]:
        id_warnings.append(f"  BAD REF: {ref_info}")

    return bundle, id_warnings


def _find_all_encounter_refs(bundle: dict[str, Any]) -> list[str]:
    """Find ALL Encounter references in the bundle with context."""
    refs: list[str] = []
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType", "Unknown")
        resource_id = resource.get("id", "no-id")
        _collect_encounter_refs(resource, f"{resource_type}/{resource_id}", refs)
    return refs


def _collect_encounter_refs(obj: Any, context: str, refs: list[str]) -> None:
    """Recursively collect ALL Encounter references with context."""
    if isinstance(obj, dict):
        if "reference" in obj:
            ref_value = obj["reference"]
            if isinstance(ref_value, str) and (
                "Encounter" in ref_value or "urn:uuid" in ref_value
            ):
                # Check if it looks like an Encounter ref
                refs.append(f"{ref_value} in {context}")
            elif isinstance(ref_value, dict) and "reference" in ref_value:
                nested = ref_value["reference"]
                if isinstance(nested, str) and (
                    "Encounter" in nested or "urn:uuid" in nested
                ):
                    refs.append(f"{nested} (nested) in {context}")
        for key, value in obj.items():
            _collect_encounter_refs(value, f"{context}.{key}", refs)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            _collect_encounter_refs(item, f"{context}[{i}]", refs)


def _remap_references(
    bundle: dict[str, Any], ref_map: dict[str, str], warnings: list[str]
) -> None:
    """
    Update all references in the bundle using the reference map.

    Args:
        bundle: The FHIR bundle to update
        ref_map: Map of old reference -> new reference
        warnings: List to append warning messages to
    """
    remap_count = 0
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType", "Unknown")
        resource_id = resource.get("id", "no-id")
        count = _remap_refs_in_obj(
            resource, ref_map, f"{resource_type}/{resource_id}", warnings
        )
        remap_count += count
    warnings.append(f"Remapped {remap_count} references total")


def _remap_refs_in_obj(
    obj: Any, ref_map: dict[str, str], context: str, warnings: list[str]
) -> int:
    """Recursively update references in a FHIR object.

    Handles both simple Reference and R5 CodeableReference structures:
    - Simple: {"reference": "Encounter/xxx"}
    - CodeableReference: {"reference": {"reference": "Encounter/xxx"}}

    Returns count of remapped references.
    """
    count = 0
    if isinstance(obj, dict):
        # Check if this is a Reference (simple or nested)
        if "reference" in obj:
            ref_value = obj["reference"]
            if isinstance(ref_value, str):
                # Simple Reference: {"reference": "Encounter/xxx"}
                if ref_value in ref_map:
                    warnings.append(
                        f"  REMAP: {ref_value} -> {ref_map[ref_value][:40]} in {context}"
                    )
                    obj["reference"] = ref_map[ref_value]
                    count += 1
                elif ref_value.startswith("Encounter/"):
                    # This is an Encounter ref that's NOT in the map!
                    warnings.append(f"  NOT IN MAP: {ref_value} in {context}")
            elif isinstance(ref_value, dict) and "reference" in ref_value:
                # Nested CodeableReference: {"reference": {"reference": "Encounter/xxx"}}
                nested_ref = ref_value["reference"]
                if isinstance(nested_ref, str) and nested_ref in ref_map:
                    warnings.append(
                        f"  REMAP NESTED: {nested_ref} -> {ref_map[nested_ref][:40]} in {context}"
                    )
                    ref_value["reference"] = ref_map[nested_ref]
                    count += 1
                elif isinstance(nested_ref, str) and nested_ref.startswith(
                    "Encounter/"
                ):
                    # This is an Encounter ref that's NOT in the map!
                    warnings.append(f"  NOT IN MAP (nested): {nested_ref} in {context}")
        # Recurse into nested objects
        for key, value in obj.items():
            count += _remap_refs_in_obj(value, ref_map, f"{context}.{key}", warnings)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            count += _remap_refs_in_obj(item, ref_map, f"{context}[{i}]", warnings)
    return count


def _find_unrewritten_refs(bundle: dict[str, Any], prefix: str) -> list[str]:
    """Find references that still start with the given prefix."""
    refs: list[str] = []
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType", "Unknown")
        resource_id = resource.get("id", "no-id")
        # Track which resource contains the bad reference
        found_in_resource: list[str] = []
        _collect_refs(resource, prefix, found_in_resource)
        for ref in found_in_resource:
            refs.append(f"{ref} (in {resource_type}/{resource_id})")
    return refs


def _collect_refs(obj: Any, prefix: str, refs: list[str]) -> None:
    """Recursively collect references matching the prefix."""
    if isinstance(obj, dict):
        if "reference" in obj:
            ref_value = obj["reference"]
            if isinstance(ref_value, str) and ref_value.startswith(prefix):
                refs.append(ref_value)
            elif isinstance(ref_value, dict) and "reference" in ref_value:
                nested = ref_value["reference"]
                if isinstance(nested, str) and nested.startswith(prefix):
                    refs.append(nested)
        for value in obj.values():
            _collect_refs(value, prefix, refs)
    elif isinstance(obj, list):
        for item in obj:
            _collect_refs(item, prefix, refs)


def _extract_date(value: str | None) -> date | None:
    """Extract date from FHIR dateTime string."""
    if not value:
        return None
    try:
        if "T" in value:
            return date.fromisoformat(value.split("T")[0])
        return date.fromisoformat(value[:10])
    except (ValueError, TypeError):
        return None


def _extract_date_from_period(period: dict[str, Any]) -> date | None:
    """Extract start date from FHIR Period."""
    return _extract_date(period.get("start"))


def _extract_code(resource: dict[str, Any]) -> str | None:
    """Extract primary code from a coded resource."""
    code_elem = resource.get("code", {})
    codings = code_elem.get("coding", [])
    if codings:
        code: str | None = codings[0].get("code")
        return code
    return None


def _extract_medication_code(resource: dict[str, Any]) -> str | None:
    """Extract medication code from MedicationStatement."""
    # R4 uses medicationCodeableConcept or medicationReference
    med_concept = resource.get("medicationCodeableConcept", {})
    codings = med_concept.get("coding", [])
    if codings:
        code: str | None = codings[0].get("code")
        return code

    # R5 uses medication (CodeableReference)
    medication = resource.get("medication", {})
    if "concept" in medication:
        codings = medication["concept"].get("coding", [])
        if codings:
            code = codings[0].get("code")
            return code

    return None
