"""
CHARM EHR-specific import processing.

This module handles special processing for C-CDA documents exported from CHARM EHR,
including:
- Extracting encounter dates from clinical notes
- Creating proper Encounter resources
- Linking Conditions, Medications, and Notes to Encounters
"""

from src.import_.charm.composition_builder import build_compositions
from src.import_.charm.extractor import CharmCcdaExtractor, EncounterData
from src.import_.charm.linker import link_resources_to_encounters

__all__ = [
    "CharmCcdaExtractor",
    "EncounterData",
    "link_resources_to_encounters",
    "build_compositions",
]
