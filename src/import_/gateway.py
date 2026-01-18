"""
Import gateway - orchestrates the import pipeline.

This module handles the complete import flow:
1. Validate input format
2. Call MS Converter (C-CDA/HL7v2 → FHIR R4)
3. Apply source-specific post-processing (e.g., CHARM encounter linking)
4. Transform R4 → R5
5. Match/create Patient for idempotency
6. Persist to FHIR store
7. Return the response with persistence info
"""

import base64
import logging
from datetime import date
from typing import Any
from uuid import UUID, uuid4

from src.exceptions import ConversionError, ValidationError
from src.import_.ccda_preprocessor import sanitize_ccda
from src.import_.charm.composition_builder import build_compositions
from src.import_.charm.extractor import CharmCcdaExtractor
from src.import_.charm.linker import link_resources_to_encounters
from src.import_.matching.patient_matcher import (
    MatchResult,
    MatchStatus,
    PatientDemographics,
    PatientMatcher,
)
from src.import_.validators.ccda_validator import validate_ccda
from src.schemas.import_schemas import (
    ImportFormat,
    ImportRequest,
    ImportResponse,
    ImportStatus,
    PersistenceInfo,
)
from src.services.fhir_store_service import FHIRStoreService
from src.services.ms_converter_service import CcdaTemplate, MSConverterService
from src.transform.r4_to_r5 import transform_bundle

logger = logging.getLogger(__name__)

# Known source systems that require special processing
CHARM_SOURCE_SYSTEMS = {"charm", "charm_ehr", "charm-ehr"}


async def process_import(
    request: ImportRequest,
    ms_converter: MSConverterService,
    fhir_store: FHIRStoreService | None = None,
    organization_id: UUID | None = None,
) -> ImportResponse:
    """
    Process an import request through the full pipeline.

    Args:
        request: The import request with format and data
        ms_converter: MS FHIR Converter service client
        fhir_store: Optional FHIR store service for persistence
        organization_id: Optional organization ID for tagging resources

    Returns:
        ImportResponse with the converted FHIR R5 bundle

    Raises:
        ValidationError: If input validation fails
        ConversionError: If conversion fails
    """
    import_id = uuid4()
    warnings: list[str] = []
    errors: list[str] = []
    persistence_info: PersistenceInfo | None = None

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
            content, ms_converter, is_charm=is_charm, organization_id=organization_id
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

    # Set organization on Patient resources
    if organization_id:
        _set_patient_organization(r5_bundle, organization_id)

    # Patient matching for idempotent imports
    if fhir_store and organization_id:
        match_result, match_warnings = await _match_patient(
            r5_bundle, fhir_store, organization_id
        )
        warnings.extend(match_warnings)

        if match_result and match_result.patient_id:
            # Update bundle to use the matched/created Patient
            r5_bundle = _update_patient_references(
                r5_bundle, str(match_result.patient_id)
            )

    # Persist to FHIR store if service is provided
    if fhir_store:
        result = await fhir_store.persist_bundle(r5_bundle, organization_id)
        persistence_info = PersistenceInfo(
            persisted=result.success,
            resources_created=result.resources_created,
            resources_updated=result.resources_updated,
        )
        if result.errors:
            errors.extend(result.errors)
            warnings.append(
                f"FHIR persistence completed with {len(result.errors)} errors"
            )
        else:
            warnings.append(
                f"Persisted {result.resources_created} resources to FHIR store"
            )

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
        persistence=persistence_info,
        warnings=warnings,
        errors=errors,
    )


async def _process_ccda(
    content: str,
    ms_converter: MSConverterService,
    is_charm: bool = False,
    organization_id: UUID | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """
    Process a C-CDA document.

    Args:
        content: The C-CDA XML content
        ms_converter: MS Converter service
        is_charm: Whether this is from CHARM EHR (enables special processing)
        organization_id: Target organization for the import

    Returns:
        Tuple of (FHIR R4 Bundle, warnings)
    """
    warnings: list[str] = []

    # Pre-process C-CDA to fix values that cause MS Converter failures
    content, sanitize_warnings = sanitize_ccda(content)
    warnings.extend(sanitize_warnings)

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
        r4_bundle, charm_warnings = _apply_charm_processing(
            content, r4_bundle, organization_id
        )
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
    organization_id: UUID | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """
    Apply CHARM-specific processing to create Encounters and link resources.

    Args:
        original_ccda: The original C-CDA XML content
        r4_bundle: The FHIR R4 bundle from MS Converter
        organization_id: Target organization for the import

    Returns:
        Tuple of (modified R4 bundle, warnings)
    """
    warnings: list[str] = []

    # Ensure Patient has urn:uuid fullUrl for proper transaction bundle references
    _ensure_patient_fullurl(r4_bundle)

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
            r4_bundle, extraction_result, organization_id
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
            # Prefer fullUrl for local bundle references (urn:uuid format)
            # This ensures transaction bundles can resolve references correctly
            full_url = entry.get("fullUrl")
            enc_id = resource.get("id")

            if full_url:
                enc_ref = full_url
            elif enc_id:
                enc_ref = f"Encounter/{enc_id}"
            else:
                continue

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


def _set_patient_organization(bundle: dict[str, Any], organization_id: UUID) -> None:
    """Set managingOrganization on all Patient resources in the bundle.

    This is required for patients to appear in the organization's patient list.
    """
    org_reference = f"Organization/{organization_id}"

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Patient":
            resource["managingOrganization"] = {"reference": org_reference}


def _ensure_patient_fullurl(bundle: dict[str, Any]) -> None:
    """Ensure Patient resource has urn:uuid fullUrl for transaction bundle references.

    MS Converter output may not have fullUrl set. For proper reference resolution
    within transaction bundles, we need to ensure Patient has a urn:uuid fullUrl
    that other resources (like Encounters) can reference.
    """
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Patient":
            full_url = entry.get("fullUrl")
            # Add urn:uuid fullUrl if missing or not in urn:uuid format
            if not full_url or not full_url.startswith("urn:uuid:"):
                patient_id = resource.get("id") or str(uuid4())
                # Ensure resource has an id
                if not resource.get("id"):
                    resource["id"] = patient_id
                entry["fullUrl"] = f"urn:uuid:{patient_id}"


async def _match_patient(
    bundle: dict[str, Any],
    fhir_store: FHIRStoreService,
    organization_id: UUID,
) -> tuple[MatchResult | None, list[str]]:
    """Match the Patient in the bundle to existing Person/Patient resources.

    Uses Panova's Person/Patient model for idempotent imports:
    - Searches for existing Person by demographics
    - Finds or creates Patient in the target organization
    - Returns the matched/created Patient ID

    Args:
        bundle: The FHIR R5 bundle with Patient resource
        fhir_store: FHIR store service (provides FHIRClient)
        organization_id: Target organization for the Patient

    Returns:
        Tuple of (MatchResult or None, warnings)
    """
    warnings: list[str] = []

    # Extract demographics from the bundle's Patient resource
    demographics = _extract_patient_demographics(bundle)
    if not demographics:
        warnings.append("Could not extract patient demographics for matching")
        return None, warnings

    # Use PatientMatcher to find/create the proper Person+Patient
    matcher = PatientMatcher(fhir_store.client)
    try:
        result = await matcher.match_or_create(demographics, organization_id)

        # Log the result
        if result.status == MatchStatus.EXISTING_PERSON_EXISTING_PATIENT:
            warnings.append(
                f"Matched existing Patient {result.patient_id} "
                f"(Person {result.person_id})"
            )
        elif result.status == MatchStatus.EXISTING_PERSON_NEW_PATIENT:
            warnings.append(
                f"Created Patient {result.patient_id} for existing "
                f"Person {result.person_id}"
            )
        elif result.status == MatchStatus.NEW_PERSON_NEW_PATIENT:
            warnings.append(
                f"Created new Person {result.person_id} and "
                f"Patient {result.patient_id}"
            )
        elif result.status == MatchStatus.MULTIPLE_MATCHES:
            warnings.append("Multiple Person matches found - skipping patient matching")
            return None, warnings
        elif result.status == MatchStatus.MATCH_FAILED:
            warnings.append("Patient matching failed")
            return None, warnings

        if result.warnings:
            warnings.extend(result.warnings)

        return result, warnings

    except Exception as e:
        logger.warning("Patient matching failed: %s", e)
        warnings.append(f"Patient matching error (non-fatal): {e}")
        return None, warnings


def _extract_patient_demographics(bundle: dict[str, Any]) -> PatientDemographics | None:
    """Extract patient demographics from the bundle's Patient resource."""
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Patient":
            # Extract name
            names = resource.get("name", [])
            if not names:
                return None
            name = names[0]
            given_names = name.get("given", [])
            given_name = given_names[0] if given_names else None
            family_name = name.get("family")

            # Extract birthDate
            birth_date_str = resource.get("birthDate")
            if not birth_date_str or not given_name or not family_name:
                return None

            try:
                birth_date = date.fromisoformat(birth_date_str[:10])
            except (ValueError, TypeError):
                return None

            # Extract optional fields
            gender = resource.get("gender")

            # Extract phone/email from telecom
            phone = None
            email = None
            for telecom in resource.get("telecom", []):
                system = telecom.get("system")
                value = telecom.get("value")
                if system == "phone" and not phone:
                    phone = value
                elif system == "email" and not email:
                    email = value

            # Extract address
            address_line = None
            address_city = None
            address_state = None
            address_postal_code = None
            addresses = resource.get("address", [])
            if addresses:
                addr = addresses[0]
                lines = addr.get("line", [])
                address_line = lines[0] if lines else None
                address_city = addr.get("city")
                address_state = addr.get("state")
                address_postal_code = addr.get("postalCode")

            return PatientDemographics(
                given_name=given_name,
                family_name=family_name,
                birth_date=birth_date,
                gender=gender,
                phone=phone,
                email=email,
                address_line=address_line,
                address_city=address_city,
                address_state=address_state,
                address_postal_code=address_postal_code,
            )

    return None


def _update_patient_references(
    bundle: dict[str, Any], patient_id: str
) -> dict[str, Any]:
    """Update the bundle to use the matched Patient and remove the old Patient entry.

    Args:
        bundle: The FHIR bundle
        patient_id: The matched/created Patient ID to use

    Returns:
        Modified bundle with updated Patient references
    """
    new_patient_ref = f"Patient/{patient_id}"

    # Find and remove the Patient entry, noting its old reference
    old_patient_ref = None
    new_entries = []

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Patient":
            # Capture the old reference before removing
            full_url = entry.get("fullUrl", "")
            old_id = resource.get("id", "")
            if full_url:
                old_patient_ref = full_url
            elif old_id:
                old_patient_ref = f"Patient/{old_id}"
            # Don't include this Patient in the new entries
            continue
        new_entries.append(entry)

    bundle["entry"] = new_entries

    if not old_patient_ref:
        logger.warning("Could not find old Patient reference to update")
        return bundle

    # Update all references to the old Patient to point to the new one
    _replace_references(bundle, old_patient_ref, new_patient_ref)

    return bundle


def _replace_references(obj: Any, old_ref: str, new_ref: str) -> None:
    """Recursively replace reference strings in a dict/list structure."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "reference" and value == old_ref:
                obj[key] = new_ref
            else:
                _replace_references(value, old_ref, new_ref)
    elif isinstance(obj, list):
        for item in obj:
            _replace_references(item, old_ref, new_ref)
