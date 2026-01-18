"""
Condition resource transformer (R4 to R5).

Condition has some structural changes between R4 and R5.
https://hl7.org/fhir/R5/condition.html
"""

from typing import Any


def transform_condition(r4_condition: dict[str, Any]) -> dict[str, Any]:
    """
    Transform a FHIR R4 Condition to R5.

    Key changes in R5:
    - clinicalStatus and verificationStatus codes updated
    - onset[x] and abatement[x] remain similar
    - stage.type renamed to stage.assessment

    Args:
        r4_condition: FHIR R4 Condition resource

    Returns:
        FHIR R5 Condition resource
    """
    r5_condition = r4_condition.copy()
    r5_condition["resourceType"] = "Condition"

    # Transform clinicalStatus codes if present, or add default
    # clinicalStatus is required in FHIR R5
    if "clinicalStatus" in r5_condition:
        r5_condition["clinicalStatus"] = _transform_clinical_status(
            r5_condition["clinicalStatus"]
        )
    else:
        # Default to "active" if clinicalStatus is missing
        r5_condition["clinicalStatus"] = {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                    "code": "active",
                    "display": "Active",
                }
            ]
        }

    # Transform verificationStatus codes if present
    if "verificationStatus" in r5_condition:
        r5_condition["verificationStatus"] = _transform_verification_status(
            r5_condition["verificationStatus"]
        )

    return r5_condition


def _transform_clinical_status(status: dict[str, Any]) -> dict[str, Any]:
    """Transform Condition.clinicalStatus CodeableConcept."""
    # R4 and R5 use the same value set, but ensure proper coding
    new_status = status.copy()

    if "coding" in new_status:
        for coding in new_status["coding"]:
            # Update system to R5 if needed
            if (
                coding.get("system")
                == "http://terminology.hl7.org/CodeSystem/condition-clinical"
            ):
                # Codes are the same: active, recurrence, relapse, inactive, remission, resolved
                pass

    return new_status


def _transform_verification_status(status: dict[str, Any]) -> dict[str, Any]:
    """Transform Condition.verificationStatus CodeableConcept."""
    # R4 and R5 use similar value sets
    new_status = status.copy()

    if "coding" in new_status:
        for coding in new_status["coding"]:
            if (
                coding.get("system")
                == "http://terminology.hl7.org/CodeSystem/condition-ver-status"
            ):
                # Map R4 codes to R5 codes
                code = coding.get("code")
                if code == "unconfirmed":
                    # "unconfirmed" in R4 maps to "provisional" in R5
                    coding["code"] = "provisional"
                    coding["display"] = "Provisional"

    return new_status
