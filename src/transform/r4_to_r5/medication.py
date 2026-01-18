"""
MedicationStatement to MedicationUsage transformer (R4 to R5).

This is one of the most significant changes between R4 and R5.
MedicationStatement was renamed to MedicationUsage in R5.
https://hl7.org/fhir/R5/medicationusage.html
"""

from typing import Any


def transform_medication_statement(
    r4_medication_statement: dict[str, Any],
) -> dict[str, Any]:
    """
    Transform a FHIR R4 MedicationStatement to R5 format.

    Note: GCP Healthcare API's R5 mode still uses "MedicationStatement" as the
    resource type (not "MedicationUsage" from the official R5 spec), so we keep
    the resource type unchanged but apply R5 structural changes.

    Key changes in R5:
    - status codes changed significantly
    - medication[x] becomes medication (CodeableReference)
    - basedOn renamed to relatedClinicalInformation (broader scope)
    - reasonCode/reasonReference consolidated to reason

    Args:
        r4_medication_statement: FHIR R4 MedicationStatement resource

    Returns:
        FHIR R5-compatible MedicationStatement resource
    """
    # Keep MedicationStatement as GCP Healthcare doesn't support MedicationUsage
    r5_medication_usage: dict[str, Any] = {
        "resourceType": "MedicationStatement",
    }

    # Copy over fields that are the same
    same_fields = [
        "id",
        "meta",
        "implicitRules",
        "language",
        "text",
        "contained",
        "extension",
        "modifierExtension",
        "identifier",
        "subject",
        "encounter",  # context -> encounter (renamed in R5)
        "effectiveDateTime",
        "effectivePeriod",
        "dateAsserted",
        "informationSource",
        "note",
        "dosage",
    ]

    for field in same_fields:
        if field in r4_medication_statement:
            r5_medication_usage[field] = r4_medication_statement[field]

    # Handle context -> encounter rename (R4 used both)
    if "context" in r4_medication_statement and "encounter" not in r5_medication_usage:
        r5_medication_usage["encounter"] = r4_medication_statement["context"]

    # Transform status (significant changes)
    if "status" in r4_medication_statement:
        r5_medication_usage["status"] = _transform_status(
            r4_medication_statement["status"]
        )

    # Transform medication[x] to medication (CodeableReference in R5)
    r5_medication_usage["medication"] = _transform_medication(r4_medication_statement)

    # Transform category if present
    if "category" in r4_medication_statement:
        # R5 changes category to a list
        category = r4_medication_statement["category"]
        if isinstance(category, dict):
            r5_medication_usage["category"] = [category]
        else:
            r5_medication_usage["category"] = category

    # Transform reasonCode and reasonReference to reason (CodeableReference[])
    reasons: list[dict[str, Any]] = []

    if "reasonCode" in r4_medication_statement:
        for code in r4_medication_statement["reasonCode"]:
            reasons.append({"concept": code})

    if "reasonReference" in r4_medication_statement:
        for ref in r4_medication_statement["reasonReference"]:
            reasons.append({"reference": ref})

    if reasons:
        r5_medication_usage["reason"] = reasons

    # Transform derivedFrom if present
    if "derivedFrom" in r4_medication_statement:
        r5_medication_usage["derivedFrom"] = r4_medication_statement["derivedFrom"]

    # Transform partOf if present
    if "partOf" in r4_medication_statement:
        r5_medication_usage["partOf"] = r4_medication_statement["partOf"]

    # Transform basedOn -> relatedClinicalInformation (broader in R5)
    if "basedOn" in r4_medication_statement:
        r5_medication_usage["relatedClinicalInformation"] = r4_medication_statement[
            "basedOn"
        ]

    # Transform statusReason if present
    if "statusReason" in r4_medication_statement:
        # R4 statusReason is a list, R5 is a list too
        r5_medication_usage["statusReason"] = r4_medication_statement["statusReason"]

    return r5_medication_usage


def _transform_status(r4_status: str) -> str:
    """
    Transform MedicationStatement.status to MedicationUsage.status.

    R4 codes: active, completed, entered-in-error, intended, stopped, on-hold, unknown, not-taken
    R5 codes: recorded, entered-in-error, draft
    """
    status_mapping = {
        "active": "recorded",
        "completed": "recorded",
        "entered-in-error": "entered-in-error",
        "intended": "draft",
        "stopped": "recorded",
        "on-hold": "recorded",
        "unknown": "recorded",
        "not-taken": "recorded",
    }
    return status_mapping.get(r4_status, "recorded")


def _transform_medication(
    r4_medication_statement: dict[str, Any],
) -> dict[str, Any]:
    """
    Transform medication[x] to medication (CodeableReference).

    R4 had medicationCodeableConcept or medicationReference.
    R5 uses a CodeableReference which can hold either.
    """
    medication: dict[str, Any] = {}

    if "medicationCodeableConcept" in r4_medication_statement:
        medication["concept"] = r4_medication_statement["medicationCodeableConcept"]
    elif "medicationReference" in r4_medication_statement:
        medication["reference"] = r4_medication_statement["medicationReference"]

    return medication
