"""
Immunization resource transformer (R4 to R5).

Immunization has some changes between R4 and R5.
https://hl7.org/fhir/R5/immunization.html
"""

from typing import Any


def transform_immunization(r4_immunization: dict[str, Any]) -> dict[str, Any]:
    """
    Transform a FHIR R4 Immunization to R5.

    Key changes in R5:
    - status codes same (completed, entered-in-error, not-done)
    - vaccineCode remains CodeableConcept
    - occurrence[x] same pattern
    - performer.function renamed to performer.function
    - protocolApplied restructured

    Args:
        r4_immunization: FHIR R4 Immunization resource

    Returns:
        FHIR R5 Immunization resource
    """
    r5_immunization = r4_immunization.copy()
    r5_immunization["resourceType"] = "Immunization"

    # Transform statusReason if present
    if "statusReason" in r5_immunization:
        # R5 renames this but keeps same structure
        pass

    # Transform performer if present
    if "performer" in r5_immunization:
        r5_immunization["performer"] = [
            _transform_performer(p) for p in r5_immunization["performer"]
        ]

    # Transform protocolApplied if present
    if "protocolApplied" in r5_immunization:
        r5_immunization["protocolApplied"] = [
            _transform_protocol_applied(p) for p in r5_immunization["protocolApplied"]
        ]

    # Transform education if present
    if "education" in r5_immunization:
        # R5 removes education, move to extension if needed
        # For now, remove it as it's not in R5
        del r5_immunization["education"]

    # Transform programEligibility if present
    if "programEligibility" in r5_immunization:
        r5_immunization["programEligibility"] = [
            _transform_program_eligibility(p)
            for p in r5_immunization["programEligibility"]
        ]

    return r5_immunization


def _transform_performer(r4_performer: dict[str, Any]) -> dict[str, Any]:
    """Transform Immunization.performer."""
    r5_performer = r4_performer.copy()
    # function remains CodeableConcept, same structure
    return r5_performer


def _transform_protocol_applied(r4_protocol: dict[str, Any]) -> dict[str, Any]:
    """Transform Immunization.protocolApplied."""
    r5_protocol = r4_protocol.copy()

    # doseNumber[x] and seriesDoses[x] changed in R5
    # R4: doseNumberPositiveInt, doseNumberString
    # R5: doseNumber is just string

    if "doseNumberPositiveInt" in r5_protocol:
        r5_protocol["doseNumber"] = str(r5_protocol.pop("doseNumberPositiveInt"))
    elif "doseNumberString" in r5_protocol:
        r5_protocol["doseNumber"] = r5_protocol.pop("doseNumberString")

    if "seriesDosesPositiveInt" in r5_protocol:
        r5_protocol["seriesDoses"] = str(r5_protocol.pop("seriesDosesPositiveInt"))
    elif "seriesDosesString" in r5_protocol:
        r5_protocol["seriesDoses"] = r5_protocol.pop("seriesDosesString")

    return r5_protocol


def _transform_program_eligibility(r4_eligibility: dict[str, Any]) -> dict[str, Any]:
    """Transform Immunization.programEligibility."""
    # R4: CodeableConcept
    # R5: BackboneElement with program and programStatus
    # This is a structural change
    return {
        "program": {"text": "Unknown"},
        "programStatus": r4_eligibility,
    }
