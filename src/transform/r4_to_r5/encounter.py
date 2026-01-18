"""
Encounter transformer for R4 to R5 conversion.

Key changes in R5:
- 'period' renamed to 'actualPeriod'
- 'class' is now a single CodeableConcept (was Coding in R4)
- 'hospitalization' renamed to 'admission'
- 'reasonCode' and 'reasonReference' merged into 'reason'
- 'diagnosis.use' renamed to 'diagnosis.condition'
- 'subject' changed from single Reference to array (0..*)
- New 'plannedStartDate' and 'plannedEndDate' fields
"""

from typing import Any


def transform_encounter(r4_encounter: dict[str, Any]) -> dict[str, Any]:
    """
    Transform a FHIR R4 Encounter to R5 format.

    Args:
        r4_encounter: The R4 Encounter resource

    Returns:
        R5-compatible Encounter resource
    """
    r5_encounter = r4_encounter.copy()

    # Transform 'class' from Coding to CodeableConcept
    if "class" in r5_encounter:
        r4_class = r5_encounter["class"]
        # R4 class is a Coding, R5 expects CodeableConcept with coding array
        if isinstance(r4_class, dict) and "coding" not in r4_class:
            # It's a single Coding, wrap in CodeableConcept
            r5_encounter["class"] = {
                "coding": [r4_class],
            }

    # Transform 'subject' from single Reference to array (R5 change)
    if "subject" in r5_encounter:
        subject = r5_encounter["subject"]
        if isinstance(subject, dict):
            # Wrap single reference in array
            r5_encounter["subject"] = [subject]

    # Transform 'period' to 'actualPeriod' if not already present
    if "period" in r5_encounter and "actualPeriod" not in r5_encounter:
        r5_encounter["actualPeriod"] = r5_encounter.pop("period")

    # Transform 'hospitalization' to 'admission'
    if "hospitalization" in r5_encounter:
        r5_encounter["admission"] = r5_encounter.pop("hospitalization")

    # Transform reasonCode and reasonReference into reason array
    if "reasonCode" in r5_encounter or "reasonReference" in r5_encounter:
        reasons = []

        # Convert reasonCode entries
        for reason_code in r5_encounter.pop("reasonCode", []):
            reasons.append({"use": reason_code})

        # Convert reasonReference entries
        for reason_ref in r5_encounter.pop("reasonReference", []):
            reasons.append({"value": [{"reference": reason_ref}]})

        if reasons:
            r5_encounter["reason"] = reasons

    # Transform diagnosis entries
    if "diagnosis" in r5_encounter:
        r5_diagnoses = []
        for diag in r5_encounter["diagnosis"]:
            r5_diag = diag.copy()
            # 'condition' in R4 becomes part of 'condition' array in R5
            if "condition" in r5_diag:
                r5_diag["condition"] = [{"reference": r5_diag["condition"]}]
            # 'use' stays the same
            r5_diagnoses.append(r5_diag)
        r5_encounter["diagnosis"] = r5_diagnoses

    # Transform participant individual -> actor
    if "participant" in r5_encounter:
        for participant in r5_encounter["participant"]:
            if "individual" in participant:
                participant["actor"] = participant.pop("individual")

    # Transform serviceType from CodeableConcept to CodeableReference
    if "serviceType" in r5_encounter:
        service_type = r5_encounter["serviceType"]
        if isinstance(service_type, dict) and "concept" not in service_type:
            r5_encounter["serviceType"] = [{"concept": service_type}]

    return r5_encounter
