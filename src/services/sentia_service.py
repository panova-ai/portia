"""
Sentia API client for practitioner and organization resolution.

Portia calls Sentia to resolve the current practitioner and their
organizations from a Firebase token, avoiding duplication of
database tables and lookup logic.
"""

from uuid import UUID

import httpx
from pydantic import BaseModel

from src.settings import settings


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


class PractitionerOrgContext(BaseModel):
    """Combined practitioner and organization context."""

    practitioner: PractitionerContext
    organizations: list[OrganizationContext]
    default_organization: OrganizationContext | None = None


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
            PractitionerOrgContext with validated organization

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

        return context
