"""
Composition transformer for R4 to R5 conversion.

Key changes in R5:
- 'subject' changed from single Reference (0..1) to array (0..*)
- 'confidentiality' removed
- 'attester.mode' changed from code to CodeableConcept
- 'relatesTo.code' renamed to 'relatesTo.type'
- 'relatesTo.target[x]' becomes 'relatesTo.resourceReference'
- 'event' structure changed
- 'section.mode' removed (was deprecated)
"""

from typing import Any


def transform_composition(r4_composition: dict[str, Any]) -> dict[str, Any]:
    """
    Transform a FHIR R4 Composition to R5 format.

    Args:
        r4_composition: The R4 Composition resource

    Returns:
        R5-compatible Composition resource
    """
    r5_composition = r4_composition.copy()

    # Remove 'confidentiality' field (removed in R5)
    r5_composition.pop("confidentiality", None)

    # Transform 'subject' from single Reference to array (R5 change)
    if "subject" in r5_composition:
        subject = r5_composition["subject"]
        if isinstance(subject, dict):
            r5_composition["subject"] = [subject]

    # Transform attester entries
    if "attester" in r5_composition:
        r5_attesters = []
        for attester in r5_composition["attester"]:
            r5_attester = attester.copy()

            # Transform mode from code to CodeableConcept
            if "mode" in r5_attester and isinstance(r5_attester["mode"], str):
                mode_code = r5_attester["mode"]
                r5_attester["mode"] = {
                    "coding": [
                        {
                            "system": "http://hl7.org/fhir/composition-attestation-mode",
                            "code": mode_code,
                        }
                    ]
                }

            r5_attesters.append(r5_attester)
        r5_composition["attester"] = r5_attesters

    # Transform relatesTo entries
    if "relatesTo" in r5_composition:
        r5_relates_to = []
        for relates_to in r5_composition["relatesTo"]:
            r5_relates = relates_to.copy()

            # Rename 'code' to 'type'
            if "code" in r5_relates:
                r5_relates["type"] = r5_relates.pop("code")

            # Transform target[x] to resourceReference
            if "targetIdentifier" in r5_relates:
                r5_relates["resourceReference"] = {
                    "identifier": r5_relates.pop("targetIdentifier")
                }
            elif "targetReference" in r5_relates:
                r5_relates["resourceReference"] = r5_relates.pop("targetReference")

            r5_relates_to.append(r5_relates)
        r5_composition["relatesTo"] = r5_relates_to

    # Transform event entries
    if "event" in r5_composition:
        r5_events = []
        for event in r5_composition["event"]:
            r5_event = event.copy()

            # 'code' becomes 'detail' with CodeableReference
            if "code" in r5_event:
                r5_event["detail"] = [
                    {"concept": code} for code in r5_event.pop("code")
                ]

            r5_events.append(r5_event)
        r5_composition["event"] = r5_events

    # Transform sections recursively
    if "section" in r5_composition:
        r5_composition["section"] = [
            _transform_section(section) for section in r5_composition["section"]
        ]

    return r5_composition


def _transform_section(section: dict[str, Any]) -> dict[str, Any]:
    """Transform a Composition section recursively."""
    r5_section = section.copy()

    # Remove deprecated 'mode' field
    r5_section.pop("mode", None)

    # Transform nested sections
    if "section" in r5_section:
        r5_section["section"] = [_transform_section(s) for s in r5_section["section"]]

    return r5_section
