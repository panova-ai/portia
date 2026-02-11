"""
Appointment importer for Charm CSV exports.

Orchestrates the appointment import flow:
1. Parse CSV
2. Match/create Person+Patient using PatientMatcher
3. Look up organization's default Location
4. Create FHIR Encounter with import tags
5. Call Sentia API to create GCal event
"""

import base64
import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Optional
from uuid import UUID

from fhir_client.resources.location import Location

from src.import_.charm.appointment_csv_parser import (
    ParsedCharmAppointment,
    parse_appointment_csv,
)
from src.import_.matching.identifier_service import IMPORT_SOURCE_TAG_SYSTEM
from src.import_.matching.patient_matcher import (
    MatchStatus,
    PatientDemographics,
    PatientMatcher,
)
from src.services.fhir_store_service import FHIRStoreService
from src.services.sentia_service import SentiaService

logger = logging.getLogger(__name__)

CHARM_APPOINTMENTS_SOURCE = "charm-appointments"
CHARM_APPOINTMENT_ID_SYSTEM = "https://charm.com/appointment-id"
CHARM_RECORD_ID_SYSTEM = "https://charm.com/record-id"
CONFIRMATION_STATUS_SYSTEM = "https://panova.ai/confirmation-status"


@dataclass
class AppointmentImportResult:
    """Result of importing a single appointment."""

    success: bool
    charm_appointment_id: str
    patient_id: Optional[UUID] = None
    person_id: Optional[UUID] = None
    encounter_id: Optional[UUID] = None
    gcal_event_id: Optional[str] = None
    error: Optional[str] = None
    warnings: list[str] = field(default_factory=list)


@dataclass
class AppointmentImportResponse:
    """Response from appointment import operation."""

    total_rows: int
    successful: int
    failed: int
    skipped: int
    results: list[AppointmentImportResult]
    warnings: list[str]


async def import_appointments_from_csv(
    csv_data_base64: str,
    organization_id: UUID,
    practitioner_role_id: UUID,
    fhir_store: FHIRStoreService,
    sentia_service: SentiaService,
    auth_token: Optional[str] = None,
    service_token: Optional[str] = None,
) -> AppointmentImportResponse:
    """
    Import appointments from Charm CSV export.

    Flow:
    1. Decode and parse CSV
    2. Look up organization's default Location
    3. For each appointment row:
       a. Match/create Person+Patient using PatientMatcher
       b. Create FHIR Encounter with pending-import status
       c. Call Sentia API to create GCal event

    Args:
        csv_data_base64: Base64-encoded CSV data
        organization_id: Target organization ID
        practitioner_role_id: PractitionerRole ID for encounter participant
        fhir_store: FHIR store service for persistence
        sentia_service: Sentia service for GCal event creation
        auth_token: Firebase auth token for Sentia API calls

    Returns:
        AppointmentImportResponse with counts and results
    """
    results: list[AppointmentImportResult] = []
    warnings: list[str] = []

    # Decode and parse CSV
    try:
        csv_content = base64.b64decode(csv_data_base64).decode("utf-8")
    except Exception as e:
        raise ValueError(f"Failed to decode CSV data: {e}") from e

    try:
        appointments = parse_appointment_csv(csv_content)
    except ValueError as e:
        raise ValueError(f"Failed to parse CSV: {e}") from e

    if not appointments:
        return AppointmentImportResponse(
            total_rows=0,
            successful=0,
            failed=0,
            skipped=0,
            results=[],
            warnings=["CSV file is empty or contains no valid appointments"],
        )

    warnings.append(f"Parsed {len(appointments)} appointments from CSV")

    # Look up organization's default location
    default_location = await _get_default_location(fhir_store, organization_id)
    if not default_location:
        raise ValueError(f"No Location found for organization {organization_id}")
    assert default_location.id is not None
    warnings.append(f"Using default location: {default_location.name}")

    # Initialize patient matcher
    matcher = PatientMatcher(fhir_store.client)

    # Process each appointment
    successful = 0
    failed = 0
    skipped = 0

    for appointment in appointments:
        result = await _import_single_appointment(
            appointment=appointment,
            organization_id=organization_id,
            practitioner_role_id=practitioner_role_id,
            location_id=default_location.id,
            location_timezone=default_location.timezone or "America/Los_Angeles",
            matcher=matcher,
            fhir_store=fhir_store,
            sentia_service=sentia_service,
            auth_token=auth_token,
            service_token=service_token,
        )

        results.append(result)
        if result.success:
            successful += 1
        elif result.error and "skipped" in result.error.lower():
            skipped += 1
        else:
            failed += 1

        # Add per-appointment warnings to global warnings
        warnings.extend(result.warnings)

    return AppointmentImportResponse(
        total_rows=len(appointments),
        successful=successful,
        failed=failed,
        skipped=skipped,
        results=results,
        warnings=warnings,
    )


async def _import_single_appointment(
    appointment: ParsedCharmAppointment,
    organization_id: UUID,
    practitioner_role_id: UUID,
    location_id: UUID,
    location_timezone: str,
    matcher: PatientMatcher,
    fhir_store: FHIRStoreService,
    sentia_service: SentiaService,
    auth_token: Optional[str],
    service_token: Optional[str] = None,
) -> AppointmentImportResult:
    """Import a single appointment."""
    warnings: list[str] = []

    try:
        # Step 1: Match or create patient
        demographics = _to_patient_demographics(appointment)
        match_result = await matcher.match_or_create(demographics, organization_id)

        if match_result.status == MatchStatus.MULTIPLE_MATCHES:
            return AppointmentImportResult(
                success=False,
                charm_appointment_id=appointment.charm_appointment_id,
                error="Multiple patient matches found - manual resolution required",
                warnings=match_result.warnings or [],
            )

        if match_result.status == MatchStatus.MATCH_FAILED:
            return AppointmentImportResult(
                success=False,
                charm_appointment_id=appointment.charm_appointment_id,
                error="Patient matching failed",
                warnings=match_result.warnings or [],
            )

        assert match_result.patient_id is not None

        if match_result.patient_created:
            warnings.append(
                f"Created new patient {match_result.patient_id} for {appointment.given_name} {appointment.family_name}"
            )

        # Step 2: Create FHIR Encounter
        encounter_id = await _create_import_encounter(
            appointment=appointment,
            patient_id=match_result.patient_id,
            organization_id=organization_id,
            practitioner_role_id=practitioner_role_id,
            location_id=location_id,
            fhir_store=fhir_store,
        )

        # Step 3: Call Sentia to create GCal event (if auth_token or service_token provided)
        gcal_event_id: Optional[str] = None
        if auth_token or service_token:
            try:
                gcal_result = await sentia_service.create_imported_appointment(
                    auth_token=auth_token,
                    encounter_id=encounter_id,
                    patient_id=match_result.patient_id,
                    practitioner_role_id=practitioner_role_id,
                    location_id=location_id,
                    start=appointment.start,
                    end=appointment.end,
                    reason=appointment.reason or appointment.visit_type,
                    is_virtual=appointment.is_virtual,
                    timezone=location_timezone,
                    service_token=service_token,
                )
                gcal_event_id = gcal_result.gcal_event_id
                if gcal_result.warnings:
                    warnings.extend(gcal_result.warnings)
            except Exception as e:
                warnings.append(f"Failed to create GCal event: {e}")
                # Continue - encounter is still created
        else:
            warnings.append("Skipped GCal event creation (no auth token)")

        return AppointmentImportResult(
            success=True,
            charm_appointment_id=appointment.charm_appointment_id,
            patient_id=match_result.patient_id,
            person_id=match_result.person_id,
            encounter_id=encounter_id,
            gcal_event_id=gcal_event_id,
            warnings=warnings,
        )

    except Exception as e:
        logger.exception(
            "Failed to import appointment %s", appointment.charm_appointment_id
        )
        return AppointmentImportResult(
            success=False,
            charm_appointment_id=appointment.charm_appointment_id,
            error=str(e),
            warnings=warnings,
        )


def _to_patient_demographics(
    appointment: ParsedCharmAppointment,
) -> PatientDemographics:
    """Convert appointment data to PatientDemographics for matching."""
    return PatientDemographics(
        given_name=appointment.given_name,
        family_name=appointment.family_name,
        birth_date=appointment.birth_date or date(1900, 1, 1),  # Fallback for matching
        gender=appointment.gender,
        phone=appointment.phone,
        email=appointment.email,
        address_line=appointment.address_line,
        address_city=appointment.address_city,
        address_state=appointment.address_state,
        address_postal_code=appointment.address_postal_code,
    )


async def _get_default_location(
    fhir_store: FHIRStoreService, organization_id: UUID
) -> Optional[Location]:
    """Get the default (first) location for an organization."""
    # Search for locations in this organization
    # Note: Not filtering by status since many locations don't have it set
    search_params = {
        "organization": f"Organization/{organization_id}",
        "_count": "1",
    }

    response = await fhir_store.client.locations.client.get(
        f"{fhir_store.client.locations.base_url}/Location",
        headers=fhir_store.client.locations._get_auth_headers(),  # type: ignore[no-untyped-call]
        params=search_params,
    )
    response.raise_for_status()
    data = response.json()

    entries = data.get("entry", [])
    if entries:
        resource = entries[0].get("resource", {})
        if resource.get("resourceType") == "Location":
            return Location(**resource)

    return None


async def _create_import_encounter(
    appointment: ParsedCharmAppointment,
    patient_id: UUID,
    organization_id: UUID,
    practitioner_role_id: UUID,
    location_id: UUID,
    fhir_store: FHIRStoreService,
) -> UUID:
    """Create a FHIR Encounter for the imported appointment."""
    # Determine encounter class based on virtual flag
    encounter_class = "VR" if appointment.is_virtual else "AMB"

    encounter: dict[str, Any] = {
        "resourceType": "Encounter",
        "status": "planned",
        "plannedStartDate": appointment.start.isoformat(),
        "plannedEndDate": appointment.end.isoformat(),
        "actualPeriod": {
            "start": appointment.start.isoformat(),
            "end": appointment.end.isoformat(),
        },
        "subject": {"reference": f"Patient/{patient_id}"},
        "participant": [
            {"actor": {"reference": f"PractitionerRole/{practitioner_role_id}"}}
        ],
        "location": [{"location": {"reference": f"Location/{location_id}"}}],
        "serviceProvider": {"reference": f"Organization/{organization_id}"},
        "class": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                        "code": encounter_class,
                    }
                ]
            }
        ],
        "reason": [
            {
                "value": [
                    {"concept": {"text": appointment.reason or appointment.visit_type}}
                ]
            }
        ],
        "identifier": [
            {
                "system": CHARM_APPOINTMENT_ID_SYSTEM,
                "value": appointment.charm_appointment_id,
            },
        ],
        "meta": {
            "tag": [
                {"system": IMPORT_SOURCE_TAG_SYSTEM, "code": CHARM_APPOINTMENTS_SOURCE},
                {"system": CONFIRMATION_STATUS_SYSTEM, "code": "pending-import"},
            ]
        },
    }

    # Add Charm record ID if available
    if appointment.charm_record_id:
        encounter["identifier"].append(
            {
                "system": CHARM_RECORD_ID_SYSTEM,
                "value": appointment.charm_record_id,
            }
        )

    # Generate a UUID for fullUrl tracking
    import uuid as uuid_module

    encounter_uuid = uuid_module.uuid4()

    # Create a bundle with just this encounter
    # Include fullUrl so the ID mapping is populated after persist
    bundle: dict[str, Any] = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [
            {
                "fullUrl": f"urn:uuid:{encounter_uuid}",
                "resource": encounter,
            }
        ],
    }

    # Persist to FHIR store
    result = await fhir_store.persist_bundle(bundle, organization_id)

    if not result.success:
        raise ValueError(f"Failed to create encounter: {result.errors}")

    # Extract the created encounter ID from the id_mapping
    logger.info(
        "persist_bundle result: success=%s, created=%s, id_mapping=%s",
        result.success,
        result.resources_created,
        result.id_mapping,
    )

    for full_url, resource_id in result.id_mapping.items():
        logger.info(
            "Extracting encounter ID from full_url=%s, resource_id=%s",
            full_url,
            resource_id,
        )
        try:
            return UUID(resource_id)
        except ValueError as e:
            logger.error("Failed to parse resource_id '%s' as UUID: %s", resource_id, e)
            raise ValueError(f"Invalid encounter ID format: {resource_id}") from e

    raise ValueError("Encounter created but ID not returned")
