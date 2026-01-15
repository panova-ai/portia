"""
FHIR R4 to R5 transformation module.

This module provides transformers to convert FHIR R4 resources to FHIR R5 format.
The transformations follow the official HL7 R4 to R5 mappings.

Key changes in R5:
- MedicationStatement is renamed to MedicationUsage
- Various status value changes
- New required fields in some resources
"""

from src.transform.r4_to_r5.bundle import transform_bundle

__all__ = ["transform_bundle"]
