"""
Import gateway - orchestrates the import pipeline.

This module handles the complete import flow:
1. Validate input format
2. Call MS Converter (C-CDA/HL7v2 → FHIR R4)
3. Transform R4 → R5
4. Return the transformed bundle
"""

import base64
from typing import Any
from uuid import uuid4

from src.exceptions import ConversionError, ValidationError
from src.import_.validators.ccda_validator import validate_ccda
from src.schemas.import_schemas import (
    ImportFormat,
    ImportRequest,
    ImportResponse,
    ImportStatus,
)
from src.services.ms_converter_service import CcdaTemplate, MSConverterService
from src.transform.r4_to_r5 import transform_bundle


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

    # Route to appropriate handler based on format
    if request.format == ImportFormat.CCDA:
        r4_bundle, format_warnings = await _process_ccda(content, ms_converter)
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
) -> tuple[dict[str, Any], list[str]]:
    """
    Process a C-CDA document.

    Args:
        content: The C-CDA XML content
        ms_converter: MS Converter service

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

    return r4_bundle, warnings


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
