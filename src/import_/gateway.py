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
from src.import_.ccda_preprocessor import DoseRangeInfo, sanitize_ccda
from src.import_.charm.composition_builder import build_compositions
from src.import_.charm.extractor import CharmCcdaExtractor
from src.import_.charm.linker import link_resources_to_encounters
from src.import_.matching.identifier_service import (
    get_import_resource_types,
    remove_duplicate_resources,
    tag_bundle_for_import,
)
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
from src.services.fhir_store_service import (
    FHIRStoreService,
    PersistenceResult,
    delete_imported_resources,
)
from src.services.ms_converter_service import CcdaTemplate, MSConverterService
from src.transform.r4_to_r5 import transform_bundle

# Consent category for import-generated consents
IMPORT_CONSENT_CATEGORY = {
    "coding": [
        {
            "system": "https://panova.ai/consent-category",
            "code": "import-provisional",
            "display": "Provisional consent from data import",
        }
    ],
    "text": "Provisional consent generated during data import. Explicit patient consent should be obtained.",
}

logger = logging.getLogger(__name__)

# Known source systems that require special processing
CHARM_SOURCE_SYSTEMS = {"charm", "charm_ehr", "charm-ehr"}


async def process_import(
    request: ImportRequest,
    ms_converter: MSConverterService,
    fhir_store: FHIRStoreService | None = None,
    organization_id: UUID | None = None,
    practitioner_role_id: UUID | None = None,
) -> ImportResponse:
    """
    Process an import request through the full pipeline.

    Args:
        request: The import request with format and data
        ms_converter: MS FHIR Converter service client
        fhir_store: Optional FHIR store service for persistence
        organization_id: Optional organization ID for tagging resources
        practitioner_role_id: Optional PractitionerRole ID for encounter participant

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
    dose_ranges: list[DoseRangeInfo] = []
    if request.format == ImportFormat.CCDA:
        r4_bundle, format_warnings, dose_ranges = await _process_ccda(
            content,
            ms_converter,
            is_charm=is_charm,
            organization_id=organization_id,
            practitioner_role_id=practitioner_role_id,
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

    # Convert dose quantities to dose ranges where applicable
    if dose_ranges:
        _convert_dose_quantities_to_ranges(r5_bundle, dose_ranges)

    # Ensure all resources have urn:uuid fullUrls and remap references
    # This is critical for GCP FHIR transaction bundle reference resolution
    remap_warnings = _ensure_all_fullurls_and_remap_references(r5_bundle)
    warnings.extend(remap_warnings)

    # Inline medication concepts for UI compatibility
    _inline_medication_concepts(r5_bundle)

    # Filter out NKDA (No Known Drug Allergy) entries that have no actual substance
    nkda_count = _filter_nkda_allergies(r5_bundle)
    if nkda_count > 0:
        warnings.append(f"Filtered {nkda_count} 'No Known Drug Allergy' entries")
        # Update counts to reflect filtered allergies
        counts.AllergyIntolerance = max(0, counts.AllergyIntolerance - nkda_count)

    # Set organization on Patient resources
    if organization_id:
        _set_patient_organization(r5_bundle, organization_id)

    # Determine import source system
    source_system = (request.metadata or {}).get("source_system", "").lower()
    if not source_system:
        source_system = "charm" if is_charm else "unknown"

    # Patient matching for idempotent imports
    match_result: MatchResult | None = None
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

            # Tag resources with import source for selective re-import cleanup
            r5_bundle = tag_bundle_for_import(
                r5_bundle, source_system, match_result.patient_id
            )

            # Remove duplicates within the bundle
            r5_bundle, dups_removed = remove_duplicate_resources(r5_bundle)
            if dups_removed > 0:
                warnings.append(
                    f"Removed {dups_removed} duplicate resources from bundle"
                )

            # If patient already existed, delete their previous import-created resources
            # This ensures re-imports cleanly replace previous data
            if not match_result.patient_created:
                deletion_result = await delete_imported_resources(
                    fhir_store.client,
                    match_result.patient_id,
                    source_system,
                    get_import_resource_types(),
                )
                if deletion_result.resources_deleted > 0:
                    warnings.append(
                        f"Deleted {deletion_result.resources_deleted} existing "
                        f"import-created resources for re-import"
                    )
                if deletion_result.errors:
                    warnings.extend(
                        [f"Deletion warning: {e}" for e in deletion_result.errors]
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

        # Create provisional Consent for the imported patient
        if (
            result.success
            and match_result
            and match_result.patient_id
            and organization_id
        ):
            consent_result, consent_warnings = await _create_provisional_consent(
                fhir_store,
                match_result.patient_id,
                organization_id,
            )
            warnings.extend(consent_warnings)

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
    practitioner_role_id: UUID | None = None,
) -> tuple[dict[str, Any], list[str], list[DoseRangeInfo]]:
    """
    Process a C-CDA document.

    Args:
        content: The C-CDA XML content
        ms_converter: MS Converter service
        is_charm: Whether this is from CHARM EHR (enables special processing)
        organization_id: Target organization for the import
        practitioner_role_id: Target PractitionerRole for encounter participant

    Returns:
        Tuple of (FHIR R4 Bundle, warnings, dose_ranges)
    """
    warnings: list[str] = []

    # Pre-process C-CDA to fix values that cause MS Converter failures
    content, sanitize_warnings, dose_ranges = sanitize_ccda(content)
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
            content, r4_bundle, organization_id, practitioner_role_id
        )
        warnings.extend(charm_warnings)

    return r4_bundle, warnings, dose_ranges


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
    practitioner_role_id: UUID | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """
    Apply CHARM-specific processing to create Encounters and link resources.

    Args:
        original_ccda: The original C-CDA XML content
        r4_bundle: The FHIR R4 bundle from MS Converter
        organization_id: Target organization for the import
        practitioner_role_id: Target PractitionerRole for encounter participant

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
            r4_bundle, extraction_result, organization_id, practitioner_role_id
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


def _ensure_all_fullurls_and_remap_references(bundle: dict[str, Any]) -> list[str]:
    """Ensure all resources have urn:uuid fullUrls and update all references.

    GCP Healthcare FHIR API requires urn:uuid format for reference resolution
    within transaction bundles. This function:
    1. Assigns urn:uuid fullUrls to all resources that don't have them
    2. Builds a mapping from ResourceType/id to urn:uuid
    3. Updates all references in the bundle to use urn:uuid format

    Args:
        bundle: The FHIR bundle to process

    Returns:
        List of warnings
    """
    warnings: list[str] = []

    # Step 1: Build mapping and ensure all resources have urn:uuid fullUrls
    ref_map: dict[str, str] = {}

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")
        resource_id = resource.get("id")

        if not resource_type:
            continue

        # Ensure resource has an id
        if not resource_id:
            resource_id = str(uuid4())
            resource["id"] = resource_id

        # Build the standard reference format
        standard_ref = f"{resource_type}/{resource_id}"

        # Check current fullUrl
        full_url = entry.get("fullUrl")

        # Ensure fullUrl is in urn:uuid format
        if not full_url or not full_url.startswith("urn:uuid:"):
            full_url = f"urn:uuid:{resource_id}"
            entry["fullUrl"] = full_url

        # Map both ResourceType/id and the old fullUrl to the new urn:uuid
        ref_map[standard_ref] = full_url
        if entry.get("fullUrl") and entry["fullUrl"] != full_url:
            # Also map the original fullUrl if different
            ref_map[entry["fullUrl"]] = full_url

    # Step 2: Update all references in the bundle to use urn:uuid format
    remapped_count = 0

    def remap_reference(obj: Any) -> None:
        """Recursively remap references to urn:uuid format."""
        nonlocal remapped_count
        if isinstance(obj, dict):
            if "reference" in obj:
                ref_val = obj["reference"]
                if isinstance(ref_val, str) and ref_val in ref_map:
                    obj["reference"] = ref_map[ref_val]
                    remapped_count += 1
            # Recurse into all dict values
            for value in obj.values():
                remap_reference(value)
        elif isinstance(obj, list):
            for item in obj:
                remap_reference(item)

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        remap_reference(resource)

    if remapped_count > 0:
        warnings.append(f"Remapped {remapped_count} references to urn:uuid format")

    return warnings


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
                f"Created new Person {result.person_id} and Patient {result.patient_id}"
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

    # Find and remove the Patient entry, noting all possible old references
    old_refs_to_replace: list[str] = []
    new_entries = []

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Patient":
            # Capture ALL possible reference formats before removing
            full_url = entry.get("fullUrl", "")
            old_id = resource.get("id", "")

            # Both formats may be used in the bundle
            if full_url:
                old_refs_to_replace.append(full_url)
            if old_id:
                old_refs_to_replace.append(f"Patient/{old_id}")
                # Also handle urn:uuid format if it differs from fullUrl
                urn_ref = f"urn:uuid:{old_id}"
                if urn_ref != full_url:
                    old_refs_to_replace.append(urn_ref)

            # Don't include this Patient in the new entries
            continue
        new_entries.append(entry)

    bundle["entry"] = new_entries

    if not old_refs_to_replace:
        logger.warning("Could not find old Patient reference to update")
        return bundle

    # Update all references to the old Patient (all formats) to point to the new one
    for old_ref in old_refs_to_replace:
        _replace_references(bundle, old_ref, new_patient_ref)

    return bundle


def _inline_medication_concepts(bundle: dict[str, Any]) -> None:
    """
    Inline medication concepts from Medication resources into MedicationStatements.

    The omnia UI displays medication.concept.text directly and doesn't resolve
    medication.reference. This function copies the Medication's code into the
    MedicationStatement's medication.concept field.
    """
    # Build a map of Medication resources by their references
    medication_map: dict[str, dict[str, Any]] = {}
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Medication":
            med_id = resource.get("id")
            full_url = entry.get("fullUrl", "")
            if med_id:
                medication_map[f"Medication/{med_id}"] = resource
            if full_url:
                medication_map[full_url] = resource

    # Update MedicationStatements to inline the medication concept
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "MedicationStatement":
            medication = resource.get("medication", {})
            med_ref = medication.get("reference", {})

            # Get the reference string (handle both nested and flat formats)
            ref_str = None
            if isinstance(med_ref, dict):
                ref_str = med_ref.get("reference")
            elif isinstance(med_ref, str):
                ref_str = med_ref

            if ref_str and ref_str in medication_map:
                med_resource = medication_map[ref_str]
                med_code = med_resource.get("code", {})

                # Add concept with the medication name
                if med_code:
                    # Get display text from coding or use text field
                    display_text = med_code.get("text")
                    if not display_text:
                        codings = med_code.get("coding", [])
                        if codings:
                            display_text = codings[0].get("display")

                    medication["concept"] = {
                        "coding": med_code.get("coding", []),
                        "text": display_text,
                    }


def _convert_dose_quantities_to_ranges(
    bundle: dict[str, Any], dose_ranges: list[DoseRangeInfo]
) -> None:
    """
    Convert doseQuantity to doseRange for MedicationStatements with range dosages.

    When C-CDA contains dose ranges like "1-2 tablets", MS Converter can only
    handle single numeric values. We sanitize to the average for conversion,
    then post-process to restore the proper doseRange structure.

    Matching is done by medication code (RxNorm) to ensure we convert the
    correct medications, not ones that happen to have the same average dose.

    Args:
        bundle: The FHIR bundle to modify
        dose_ranges: List of DoseRangeInfo from C-CDA sanitization
    """
    if not dose_ranges:
        return

    # Build a map of medication codes to their range info
    # Track how many times we've used each code (for duplicates)
    ranges_by_code: dict[str, list[DoseRangeInfo]] = {}
    for dr in dose_ranges:
        if dr.medication_code:
            if dr.medication_code not in ranges_by_code:
                ranges_by_code[dr.medication_code] = []
            ranges_by_code[dr.medication_code].append(dr)

    # Track which ranges we've used
    used_ranges: set[int] = set()

    # Process each MedicationStatement
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") != "MedicationStatement":
            continue

        # Get medication code from the MedicationStatement
        med_code = _get_medication_code_from_statement(resource, bundle)
        if not med_code or med_code not in ranges_by_code:
            continue

        # Find an unused range for this medication code
        range_info: DoseRangeInfo | None = None
        for ri in ranges_by_code[med_code]:
            range_key = id(ri)
            if range_key not in used_ranges:
                range_info = ri
                used_ranges.add(range_key)
                break

        if range_info is None:
            continue

        # Convert doseQuantity to doseRange
        dosages = resource.get("dosage", [])
        for dosage in dosages:
            dose_and_rate_list = dosage.get("doseAndRate", [])
            for dose_and_rate in dose_and_rate_list:
                dose_quantity = dose_and_rate.get("doseQuantity")

                if dose_quantity:
                    # Convert to doseRange
                    unit = dose_quantity.get("unit") or range_info.unit

                    dose_and_rate["doseRange"] = {
                        "low": {"value": range_info.low},
                        "high": {"value": range_info.high},
                    }
                    if unit:
                        dose_and_rate["doseRange"]["low"]["unit"] = unit
                        dose_and_rate["doseRange"]["high"]["unit"] = unit

                    # Remove the doseQuantity
                    del dose_and_rate["doseQuantity"]
                    break  # Only convert once per MedicationStatement


def _get_medication_code_from_statement(
    med_statement: dict[str, Any], bundle: dict[str, Any]
) -> str | None:
    """
    Extract the medication code (RxNorm) from a MedicationStatement.

    The code can be inline in medication.concept or referenced via medication.reference.
    """
    medication = med_statement.get("medication", {})

    # Check inline concept first
    concept = medication.get("concept", {})
    for coding in concept.get("coding", []):
        code: str | None = coding.get("code")
        if code:
            return code

    # Check reference to Medication resource
    reference = medication.get("reference", {})
    ref_str = reference.get("reference") if isinstance(reference, dict) else reference

    if ref_str:
        # Find the referenced Medication in the bundle
        for entry in bundle.get("entry", []):
            resource = entry.get("resource", {})
            if resource.get("resourceType") != "Medication":
                continue

            # Check if this is the referenced medication
            med_id = resource.get("id")
            full_url = entry.get("fullUrl", "")

            if (
                ref_str == f"Medication/{med_id}"
                or ref_str == full_url
                or (med_id and ref_str.endswith(med_id))
            ):
                # Extract code from Medication resource
                med_resource_code = resource.get("code", {})
                for coding in med_resource_code.get("coding", []):
                    med_code: str | None = coding.get("code")
                    if med_code:
                        return med_code

    return None


def _filter_nkda_allergies(bundle: dict[str, Any]) -> int:
    """
    Filter out NKDA (No Known Drug Allergy) entries from the bundle.

    MS Converter creates AllergyIntolerance resources for NKDA statements
    in C-CDA (negationInd="true"), but these have no actual allergen code.
    These should not be displayed as allergies in the UI.

    Args:
        bundle: The FHIR bundle to filter

    Returns:
        Number of NKDA entries filtered out
    """
    filtered_count = 0
    new_entries = []

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})

        if resource.get("resourceType") == "AllergyIntolerance":
            # Check if this has a meaningful code (actual allergen)
            code = resource.get("code", {})
            codings = code.get("coding", [])
            text = code.get("text", "")

            # Filter out if no code/coding and no meaningful text
            has_meaningful_code = bool(codings) or (
                text and text.lower() not in ["", "unknown", "none", "n/a"]
            )

            if not has_meaningful_code:
                # This is likely an NKDA entry - filter it out
                filtered_count += 1
                continue

        new_entries.append(entry)

    bundle["entry"] = new_entries
    return filtered_count


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


async def _create_provisional_consent(
    fhir_store: FHIRStoreService,
    patient_id: UUID,
    organization_id: UUID,
) -> tuple[PersistenceResult | None, list[str]]:
    """
    Create a provisional Consent resource for an imported patient.

    This consent allows the organization to view the patient's data but is
    marked as provisional/import-generated. The patient should be asked for
    explicit consent after import.

    Args:
        fhir_store: FHIR store service
        patient_id: The Patient resource ID
        organization_id: The Organization to grant access to

    Returns:
        Tuple of (PersistenceResult or None, warnings)
    """
    from datetime import datetime, timezone

    warnings: list[str] = []

    # Build the provisional Consent resource
    now = datetime.now(timezone.utc)
    consent: dict[str, Any] = {
        "resourceType": "Consent",
        "status": "active",
        "category": [IMPORT_CONSENT_CATEGORY],
        "subject": {"reference": f"Patient/{patient_id}"},
        "date": now.strftime("%Y-%m-%d"),
        "grantor": [{"reference": f"Patient/{patient_id}"}],
        "grantee": [{"reference": f"Organization/{organization_id}"}],
        "decision": "permit",
        "period": {"start": now.isoformat()},
        "policyBasis": {
            "reference": {"display": "Provisional consent implied by data import"}
        },
    }

    # Create a bundle with just the Consent
    consent_bundle: dict[str, Any] = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [{"resource": consent}],
    }

    try:
        result = await fhir_store.persist_bundle(consent_bundle, organization_id)
        if result.success:
            warnings.append(
                f"Created provisional Consent for Patient/{patient_id} "
                f"(requires explicit patient consent)"
            )
        else:
            warnings.append(f"Failed to create provisional Consent: {result.errors}")
        return result, warnings
    except Exception as e:
        logger.warning("Failed to create provisional Consent: %s", e)
        warnings.append(f"Could not create provisional Consent: {e}")
        return None, warnings
