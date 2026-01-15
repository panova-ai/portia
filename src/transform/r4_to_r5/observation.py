"""
Observation resource transformer (R4 to R5).

Observation has relatively few changes between R4 and R5.
https://hl7.org/fhir/R5/observation.html
"""

from typing import Any


def transform_observation(r4_observation: dict[str, Any]) -> dict[str, Any]:
    """
    Transform a FHIR R4 Observation to R5.

    Key changes in R5:
    - status codes same
    - category same structure
    - component same structure
    - referenceRange similar
    - triggered renamed to triggeredBy

    Args:
        r4_observation: FHIR R4 Observation resource

    Returns:
        FHIR R5 Observation resource
    """
    r5_observation = r4_observation.copy()
    r5_observation["resourceType"] = "Observation"

    # Transform status if present
    if "status" in r5_observation:
        r5_observation["status"] = _transform_status(r5_observation["status"])

    # Transform hasMember references (no change needed)

    # Transform derivedFrom references (no change needed)

    # Handle component observations
    if "component" in r5_observation:
        r5_observation["component"] = [
            _transform_component(c) for c in r5_observation["component"]
        ]

    return r5_observation


def _transform_status(r4_status: str) -> str:
    """Transform Observation.status from R4 to R5."""
    # R4: registered, preliminary, final, amended, corrected, cancelled, entered-in-error, unknown
    # R5: registered, preliminary, final, amended, corrected, cancelled, entered-in-error, unknown
    # Same codes, no transformation needed
    return r4_status


def _transform_component(r4_component: dict[str, Any]) -> dict[str, Any]:
    """Transform Observation.component."""
    # Component structure is largely the same in R4 and R5
    return r4_component
