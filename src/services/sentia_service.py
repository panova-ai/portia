"""
Sentia API client for practitioner and organization resolution.

Portia calls Sentia to resolve the current practitioner and their
organizations from a Firebase token, avoiding duplication of
database tables and lookup logic.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

import httpx
from pydantic import BaseModel

from src.settings import settings


class AppointmentImportResult(BaseModel):
    """Result from Sentia appointment import endpoint."""

    encounter_id: UUID
    gcal_event_id: Optional[str] = None
    warnings: list[str] = []


class PractitionerContext(BaseModel):
    """Practitioner information from Sentia."""

    id: UUID
    name: str | None = None
    email: str | None = None
    npi: str | None = None


class OrganizationContext(BaseModel):
    """Organization information from Sentia."""

    id: UUID
    name: str | None = None


class PractitionerRoleContext(BaseModel):
    """PractitionerRole information from Sentia."""

    id: UUID
    practitioner_id: UUID
    organization_id: UUID


class PractitionerOrgContext(BaseModel):
    """Combined practitioner and organization context."""

    practitioner: PractitionerContext
    organizations: list[OrganizationContext]
    default_organization: OrganizationContext | None = None
    practitioner_role: PractitionerRoleContext | None = None


class SentiaService:
    """HTTP client for Sentia backend API."""

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float | None = None,
    ):
        self.base_url = (base_url or settings.sentia_url).rstrip("/")
        self.timeout = timeout or settings.sentia_timeout
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get_practitioner_context(
        self,
        auth_token: str,
    ) -> PractitionerOrgContext:
        """
        Get practitioner and organization context from Sentia.

        Args:
            auth_token: Firebase ID token to forward to Sentia

        Returns:
            PractitionerOrgContext with practitioner and their organizations

        Raises:
            httpx.HTTPStatusError: If Sentia returns an error
        """
        client = await self._get_client()
        headers = {"Authorization": f"Bearer {auth_token}"}

        # Get current practitioner
        practitioner_resp = await client.get(
            "/practitioner/current",
            headers=headers,
        )
        practitioner_resp.raise_for_status()
        practitioner_data = practitioner_resp.json()

        # Extract practitioner info
        practitioner_name = None
        if practitioner_data.get("name"):
            name_parts = practitioner_data["name"][0]
            given = name_parts.get("given", [])
            family = name_parts.get("family", "")
            practitioner_name = f"{' '.join(given)} {family}".strip()

        practitioner = PractitionerContext(
            id=practitioner_data["id"],
            name=practitioner_name,
            npi=practitioner_data.get("npi"),
        )

        # Get organizations for practitioner
        orgs_resp = await client.get(
            "/organizations",
            headers=headers,
        )
        orgs_resp.raise_for_status()
        orgs_data = orgs_resp.json()

        organizations = [
            OrganizationContext(
                id=entry["id"],
                name=entry.get("name"),
            )
            for entry in orgs_data.get("entries", [])
        ]

        # Use first organization as default if available
        default_org = organizations[0] if organizations else None

        return PractitionerOrgContext(
            practitioner=practitioner,
            organizations=organizations,
            default_organization=default_org,
        )

    async def validate_practitioner_org_access(
        self,
        auth_token: str,
        organization_id: UUID,
    ) -> PractitionerOrgContext:
        """
        Validate practitioner has access to a specific organization.

        Args:
            auth_token: Firebase ID token
            organization_id: Organization to validate access to

        Returns:
            PractitionerOrgContext with validated organization and PractitionerRole

        Raises:
            ValueError: If practitioner doesn't have access to organization
            httpx.HTTPStatusError: If Sentia returns an error
        """
        context = await self.get_practitioner_context(auth_token)

        # Check if practitioner has access to the requested organization
        org_ids = {org.id for org in context.organizations}
        if organization_id not in org_ids:
            raise ValueError(
                f"Practitioner does not have access to organization {organization_id}"
            )

        # Set the validated organization as default
        for org in context.organizations:
            if org.id == organization_id:
                context.default_organization = org
                break

        # Get the PractitionerRole for this practitioner in this organization
        practitioner_role = await self.get_practitioner_role(
            auth_token, organization_id
        )
        context.practitioner_role = practitioner_role

        return context

    async def get_practitioner_role(
        self,
        auth_token: str,
        organization_id: UUID,
    ) -> PractitionerRoleContext | None:
        """
        Get the PractitionerRole for the current practitioner in an organization.

        Args:
            auth_token: Firebase ID token
            organization_id: Organization to get role for

        Returns:
            PractitionerRoleContext if found, None otherwise
        """
        client = await self._get_client()
        headers = {"Authorization": f"Bearer {auth_token}"}

        try:
            # Call the /organizations/{org_id}/practitioner-roles/mine endpoint
            roles_resp = await client.get(
                f"/organizations/{organization_id}/practitioner-roles/mine",
                headers=headers,
                params={"count": 1},  # We only need the first role
            )
            roles_resp.raise_for_status()
            roles_data = roles_resp.json()

            entries = roles_data.get("entries", [])
            if not entries:
                return None

            # Get the first role
            role = entries[0]

            # Extract practitioner ID from the reference (format: "Practitioner/{id}")
            practitioner_id: UUID | None = None
            practitioner_ref = role.get("practitioner", {})
            if isinstance(practitioner_ref, dict):
                ref_str = practitioner_ref.get("reference", "")
                if ref_str.startswith("Practitioner/"):
                    practitioner_id = UUID(ref_str.replace("Practitioner/", ""))

            if not practitioner_id:
                return None

            return PractitionerRoleContext(
                id=role["id"],
                practitioner_id=practitioner_id,
                organization_id=organization_id,
            )
        except Exception:
            # If we can't get the role, return None and let the import continue
            return None

    async def create_imported_appointment(
        self,
        auth_token: str | None,
        encounter_id: UUID,
        patient_id: UUID,
        practitioner_role_id: UUID,
        location_id: UUID,
        start: datetime,
        end: datetime,
        reason: str,
        is_virtual: bool,
        timezone: str,
        service_token: str | None = None,
    ) -> AppointmentImportResult:
        """
        Create a GCal event for an imported appointment via Sentia.

        This endpoint creates the GCal event and links it to the encounter.
        It bypasses the 'start must be in future' validation for imports.

        Supports two authentication modes:
        - Firebase auth (auth_token): Uses /appointments/import endpoint
        - Service auth (service_token): Uses /appointments/import-service endpoint

        Args:
            auth_token: Firebase ID token (optional if service_token provided)
            encounter_id: FHIR Encounter ID (already created)
            patient_id: FHIR Patient ID
            practitioner_role_id: FHIR PractitionerRole ID
            location_id: FHIR Location ID
            start: Appointment start time
            end: Appointment end time
            reason: Appointment reason/description
            is_virtual: Whether this is a virtual appointment
            timezone: Timezone for the appointment
            service_token: Service JWT token for service-to-service auth

        Returns:
            AppointmentImportResult with GCal event ID if created
        """
        client = await self._get_client()

        # Determine endpoint and auth header based on token type
        if service_token:
            headers = {"Authorization": f"Bearer {service_token}"}
            endpoint = "/appointments/import-service"
        elif auth_token:
            headers = {"Authorization": f"Bearer {auth_token}"}
            endpoint = "/appointments/import"
        else:
            raise ValueError("Either auth_token or service_token must be provided")

        payload = {
            "encounter_id": str(encounter_id),
            "patient_id": str(patient_id),
            "practitioner_role_id": str(practitioner_role_id),
            "location_id": str(location_id),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "reason": reason,
            "is_virtual": is_virtual,
            "timezone": timezone,
        }

        response = await client.post(
            endpoint,
            headers=headers,
            json=payload,
        )
        response.raise_for_status()

        data = response.json()
        return AppointmentImportResult(
            encounter_id=UUID(data["encounter_id"]),
            gcal_event_id=data.get("gcal_event_id"),
            warnings=data.get("warnings", []),
        )
