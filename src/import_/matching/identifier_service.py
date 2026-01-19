"""
Identifier service for idempotent resource creation.

Generates stable, deterministic identifiers for imported resources to enable:
- Idempotent imports (same data → same identifiers → no duplicates)
- Conditional FHIR operations (PUT with identifier matching)
- Traceability back to source data
"""

from dataclasses import dataclass
from datetime import date
from typing import Any
from uuid import UUID

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
) -> dict[str, Any]:
    """
    Add stable identifiers to all resources in a bundle.

    This transforms a collection bundle into a transaction bundle
    with conditional PUT operations for idempotent imports.

    Args:
        bundle: FHIR Bundle with resources
        patient_id: The matched/created patient ID
        identifier_service: Service for generating identifiers

    Returns:
        Modified bundle with identifiers and conditional requests
    """
    # Change bundle type to transaction for conditional operations
    bundle["type"] = "transaction"

    # Track assigned identifiers to avoid duplicates in the same bundle
    # (e.g., MS Converter and CHARM both create Encounters for same date)
    assigned_identifiers: set[str] = set()

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")

        if not resource_type:
            continue

        identifier = None

        if resource_type == "Encounter":
            # Extract date from actualPeriod
            enc_date = _extract_date_from_period(resource.get("actualPeriod", {}))
            if enc_date:
                identifier = identifier_service.encounter_identifier(
                    patient_id, enc_date
                )
                # Skip if we already assigned this identifier to another Encounter
                if identifier and identifier.to_search_param() in assigned_identifiers:
                    identifier = None

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

        # Add identifier and conditional request
        if identifier:
            # Track to avoid duplicates in the same bundle
            assigned_identifiers.add(identifier.to_search_param())
            identifier_service.add_identifier_to_resource(resource, identifier)
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

    return bundle


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
