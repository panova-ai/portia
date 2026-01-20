"""
Resource matching and identity resolution for imports.

This module handles:
- Matching imported patients to existing Person/Patient resources
- Practitioner context resolution
- Import source tagging for re-import cleanup
- Duplicate detection within import bundles
"""

from src.import_.matching.identifier_service import (
    get_import_resource_types,
    remove_duplicate_resources,
    tag_bundle_for_import,
)
from src.import_.matching.patient_matcher import MatchResult, PatientMatcher

__all__ = [
    "PatientMatcher",
    "MatchResult",
    "get_import_resource_types",
    "remove_duplicate_resources",
    "tag_bundle_for_import",
]
