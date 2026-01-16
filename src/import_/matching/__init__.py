"""
Resource matching and identity resolution for imports.

This module handles:
- Matching imported patients to existing Person/Patient resources
- Practitioner context resolution
- Idempotent resource creation with stable identifiers
"""

from src.import_.matching.identifier_service import IdentifierService
from src.import_.matching.patient_matcher import MatchResult, PatientMatcher

__all__ = [
    "PatientMatcher",
    "MatchResult",
    "IdentifierService",
]
