"""
Patient resource transformer (R4 to R5).

Patient resource has minimal changes between R4 and R5.
https://hl7.org/fhir/R5/patient.html
"""

from typing import Any


def transform_patient(r4_patient: dict[str, Any]) -> dict[str, Any]:
    """
    Transform a FHIR R4 Patient to R5.

    The Patient resource has very few breaking changes between R4 and R5.
    Main changes:
    - link.type codes changed (replaced → replaced-by, refer → seealso)

    Args:
        r4_patient: FHIR R4 Patient resource

    Returns:
        FHIR R5 Patient resource
    """
    r5_patient = r4_patient.copy()
    r5_patient["resourceType"] = "Patient"

    # Transform link.type if present
    if "link" in r5_patient:
        for link in r5_patient["link"]:
            if "type" in link:
                link["type"] = _transform_link_type(link["type"])

    return r5_patient


def _transform_link_type(r4_type: str) -> str:
    """Transform Patient.link.type from R4 to R5 values."""
    type_mapping = {
        "replaced-by": "replaced-by",  # Same
        "replaces": "replaces",  # Same
        "refer": "seealso",  # Changed in R5
        "seealso": "seealso",  # Same (R5 name)
    }
    return type_mapping.get(r4_type, r4_type)
