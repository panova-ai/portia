"""
Import gateway - orchestrates the import pipeline.

This module handles the complete import flow:
1. Validate input format
2. Call MS Converter (C-CDA/HL7v2 → FHIR R4)
3. Apply source-specific post-processing (e.g., CHARM encounter linking)
4. Transform R4 → R5
5. Return the transformed bundle
"""

import base64
from datetime import date
from typing import Any
from uuid import uuid4

from src.exceptions import ConversionError, ValidationError
from src.import_.charm.composition_builder import build_compositions
from src.import_.charm.extractor import CharmCcdaExtractor
from src.import_.charm.linker import link_resources_to_encounters
from src.import_.validators.ccda_validator import validate_ccda
from src.schemas.import_schemas import (
    ImportFormat,
    ImportRequest,
    ImportResponse,
    ImportStatus,
)
from src.services.ms_converter_service import CcdaTemplate, MSConverterService
from src.transform.r4_to_r5 import transform_bundle

# Known source systems that require special processing
CHARM_SOURCE_SYSTEMS = {"charm", "charm_ehr", "charm-ehr"}


async def process_import(
    request: ImportRequest,
    ms_converter: MSConverterService,
) -> ImportResponse:
    """
    Process an import request through the full pipeline.

    Args:
        request: The import request with format and data
        ms_converter: MS FHIR Converter service client

    Returns:
        ImportResponse with the converted FHIR R5 bundle

    Raises:
        ValidationError: If input validation fails
        ConversionError: If conversion fails
    """
    import_id = uuid4()
    warnings: list[str] = []
    errors: list[str] = []

    # Decode base64 data
    try:
        raw_data = base64.b64decode(request.data)
        content = raw_data.decode("utf-8")
    except Exception as e:
        raise ValidationError(f"Failed to decode base64 data: {e}") from e

    # Check if this is from a known source system
    source_system = (request.metadata or {}).get("source_system", "").lower()
    is_charm = source_system in CHARM_SOURCE_SYSTEMS

    # Route to appropriate handler based on format
    if request.format == ImportFormat.CCDA:
        r4_bundle, format_warnings = await _process_ccda(
            content, ms_converter, is_charm=is_charm
        )
        warnings.extend(format_warnings)
    elif request.format == ImportFormat.HL7V2:
        raise ValidationError("HL7v2 import not yet implemented")
    elif request.format == ImportFormat.FHIR_R4:
        raise ValidationError("FHIR R4 direct import not yet implemented")
    else:
        raise ValidationError(f"Unsupported format: {request.format}")

    # Transform R4 to R5
    r5_bundle, counts, transform_warnings = transform_bundle(r4_bundle)
    warnings.extend(transform_warnings)

    # Determine final status
    status = ImportStatus.COMPLETED
    if errors:
        status = ImportStatus.FAILED
    elif warnings:
        status = ImportStatus.PARTIAL

    return ImportResponse(
        import_id=import_id,
        status=status,
        fhir_bundle=r5_bundle,
        resources_extracted=counts,
        warnings=warnings,
        errors=errors,
    )


async def _process_ccda(
    content: str,
    ms_converter: MSConverterService,
    is_charm: bool = False,
) -> tuple[dict[str, Any], list[str]]:
    """
    Process a C-CDA document.

    Args:
        content: The C-CDA XML content
        ms_converter: MS Converter service
        is_charm: Whether this is from CHARM EHR (enables special processing)

    Returns:
        Tuple of (FHIR R4 Bundle, warnings)
    """
    warnings: list[str] = []

    # Validate the C-CDA
    validation_result = validate_ccda(content)

    if not validation_result.is_valid:
        if validation_result.errors:
            for error in validation_result.errors:
                warnings.append(f"C-CDA validation: {error}")

    # Determine template based on document type
    template = _get_ccda_template(validation_result.document_type)

    # Convert using MS FHIR Converter
    try:
        r4_bundle = await ms_converter.convert_ccda(content, template)
    except Exception as e:
        raise ConversionError(f"MS Converter failed: {e}") from e

    # Apply CHARM-specific post-processing if applicable
    if is_charm or _detect_charm_source(content):
        r4_bundle, charm_warnings = _apply_charm_processing(content, r4_bundle)
        warnings.extend(charm_warnings)

    return r4_bundle, warnings


def _detect_charm_source(content: str) -> bool:
    """
    Auto-detect if a C-CDA is from CHARM EHR.

    CHARM documents have specific patterns we can identify.
    """
    # Check for CHARM-specific patterns
    charm_indicators = [
        # CHARM often has clinical summaries with therapy notes
        "History of Present Illness" in content and "Therapy performed" in content,
        # CHARM organization patterns
        "Sofia Elkind MD" in content,  # Known CHARM practice
        # Could add more CHARM-specific OIDs or patterns here
    ]
    return any(charm_indicators)


def _apply_charm_processing(
    original_ccda: str,
    r4_bundle: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    """
    Apply CHARM-specific processing to create Encounters and link resources.

    Args:
        original_ccda: The original C-CDA XML content
        r4_bundle: The FHIR R4 bundle from MS Converter

    Returns:
        Tuple of (modified R4 bundle, warnings)
    """
    warnings: list[str] = []

    try:
        # Extract encounter and note data from the C-CDA
        extractor = CharmCcdaExtractor(original_ccda)
        extraction_result = extractor.extract()

        # Log extraction summary
        warnings.append(
            f"CHARM extraction: {len(extraction_result.encounters)} encounters, "
            f"{len(extraction_result.problems)} problems, "
            f"{len(extraction_result.medications)} medications, "
            f"{len(extraction_result.notes)} notes"
        )

        # Create Encounters and link Conditions/Medications
        r4_bundle, link_warnings = link_resources_to_encounters(
            r4_bundle, extraction_result
        )
        warnings.extend(link_warnings)

        # Build encounter date to reference mapping for composition building
        encounter_date_to_ref = _build_encounter_date_map(r4_bundle)

        # Create Compositions from clinical notes
        r4_bundle, comp_warnings = build_compositions(
            r4_bundle, extraction_result, encounter_date_to_ref
        )
        warnings.extend(comp_warnings)

    except Exception as e:
        warnings.append(f"CHARM processing error (non-fatal): {e}")
        # Return original bundle if CHARM processing fails
        # The basic MS Converter output is still usable

    return r4_bundle, warnings


def _build_encounter_date_map(bundle: dict[str, Any]) -> dict[date, str]:
    """Build a mapping from encounter dates to FHIR references."""
    date_to_ref: dict[date, str] = {}

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Encounter":
            enc_id = resource.get("id")
            if not enc_id:
                continue

            enc_ref = f"Encounter/{enc_id}"

            # Get the date from actualPeriod.start
            actual_period = resource.get("actualPeriod", {})
            start = actual_period.get("start")

            if start:
                try:
                    if "T" in start:
                        enc_date = date.fromisoformat(start.split("T")[0])
                    else:
                        enc_date = date.fromisoformat(start[:10])
                    date_to_ref[enc_date] = enc_ref
                except (ValueError, TypeError):
                    pass

    return date_to_ref


def _get_ccda_template(document_type: str | None) -> CcdaTemplate:
    """Map C-CDA document type to MS Converter template."""
    template_map = {
        "CCD": CcdaTemplate.CCD,
        "ConsultationNote": CcdaTemplate.CONSULTATION_NOTE,
        "DischargeSummary": CcdaTemplate.DISCHARGE_SUMMARY,
        "HistoryAndPhysical": CcdaTemplate.HISTORY_AND_PHYSICAL,
        "OperativeNote": CcdaTemplate.OPERATIVE_NOTE,
        "ProcedureNote": CcdaTemplate.PROCEDURE_NOTE,
        "ProgressNote": CcdaTemplate.PROGRESS_NOTE,
        "ReferralNote": CcdaTemplate.REFERRAL_NOTE,
        "TransferSummary": CcdaTemplate.TRANSFER_SUMMARY,
    }
    return template_map.get(document_type or "", CcdaTemplate.CCD)
