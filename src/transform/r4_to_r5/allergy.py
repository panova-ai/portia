"""
AllergyIntolerance resource transformer (R4 to R5).

AllergyIntolerance has some changes between R4 and R5.
https://hl7.org/fhir/R5/allergyintolerance.html
"""

from typing import Any


def transform_allergy_intolerance(
    r4_allergy: dict[str, Any],
) -> dict[str, Any]:
    """
    Transform a FHIR R4 AllergyIntolerance to R5.

    Key changes in R5:
    - type is now required and codes changed
    - criticality codes same
    - reaction.severity codes same
    - encounter renamed from patient/encounter context

    Args:
        r4_allergy: FHIR R4 AllergyIntolerance resource

    Returns:
        FHIR R5 AllergyIntolerance resource
    """
    r5_allergy = r4_allergy.copy()
    r5_allergy["resourceType"] = "AllergyIntolerance"

    # Transform type if present (allergy | intolerance -> allergy | intolerance | biologic)
    if "type" in r5_allergy:
        r5_allergy["type"] = _transform_type(r5_allergy["type"])

    # Transform category if present
    # R4: food | medication | environment | biologic
    # R5: same values
    # (no transformation needed)

    # Transform clinicalStatus if present
    if "clinicalStatus" in r5_allergy:
        r5_allergy["clinicalStatus"] = _transform_clinical_status(
            r5_allergy["clinicalStatus"]
        )

    # Transform verificationStatus if present
    if "verificationStatus" in r5_allergy:
        r5_allergy["verificationStatus"] = _transform_verification_status(
            r5_allergy["verificationStatus"]
        )

    return r5_allergy


def _transform_type(r4_type: str) -> str:
    """Transform AllergyIntolerance.type from R4 to R5."""
    # R4 codes: allergy, intolerance
    # R5 codes: allergy, intolerance (same, but biologic added)
    return r4_type


def _transform_clinical_status(status: dict[str, Any]) -> dict[str, Any]:
    """Transform AllergyIntolerance.clinicalStatus CodeableConcept."""
    # R4 and R5 use same codes: active, inactive, resolved
    return status


def _transform_verification_status(status: dict[str, Any]) -> dict[str, Any]:
    """Transform AllergyIntolerance.verificationStatus CodeableConcept."""
    # R4: unconfirmed, confirmed, refuted, entered-in-error
    # R5: unconfirmed, presumed, confirmed, refuted, entered-in-error
    # "presumed" was added in R5
    return status
