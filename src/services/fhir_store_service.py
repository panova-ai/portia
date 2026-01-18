"""
FHIR Store service for persisting imported resources.

Uses Sentia's fhir_client to interact with GCP Healthcare FHIR API.
"""

import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from fhir_client.client import FHIRClient
from fhir_client.config import FHIRClientConfig

from src.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class PersistenceResult:
    """Result of persisting a FHIR bundle to the store."""

    success: bool
    resources_created: int
    resources_updated: int
    errors: list[str]
    # Map of original fullUrl -> assigned FHIR ID
    id_mapping: dict[str, str]


class FHIRStoreService:
    """Service for persisting FHIR resources to GCP Healthcare API."""

    def __init__(self, fhir_client: FHIRClient):
        self._client = fhir_client

    async def persist_bundle(
        self,
        bundle: dict[str, Any],
        organization_id: UUID | None = None,
    ) -> PersistenceResult:
        """
        Persist a FHIR bundle to the store.

        Converts a collection bundle to a transaction bundle and executes it.

        Args:
            bundle: FHIR Bundle (collection type from import)
            organization_id: Optional organization to tag resources with

        Returns:
            PersistenceResult with created/updated counts and any errors
        """
        errors: list[str] = []
        id_mapping: dict[str, str] = {}

        # Convert collection bundle to transaction bundle
        transaction_bundle = self._to_transaction_bundle(bundle, organization_id)

        entry_count = len(transaction_bundle.get("entry", []))
        logger.info("Persisting transaction bundle with %d entries", entry_count)

        try:
            # Use any resource client's _execute_bundle method
            response = await self._client.patients._execute_bundle(transaction_bundle)

            # Process response to extract IDs and count results
            created = 0
            updated = 0

            for i, entry in enumerate(response.get("entry", [])):
                response_info = entry.get("response", {})
                status = response_info.get("status", "")
                location = response_info.get("location", "")

                # Extract resource ID from location header
                if location:
                    # Location format: ResourceType/id/_history/version
                    parts = location.split("/")
                    if len(parts) >= 2:
                        resource_id = parts[1]
                        # Map original fullUrl to new ID
                        original_entries = transaction_bundle.get("entry", [])
                        if i < len(original_entries):
                            original_url = original_entries[i].get("fullUrl", "")
                            if original_url:
                                id_mapping[original_url] = resource_id

                if status.startswith("201"):
                    created += 1
                elif status.startswith("200"):
                    updated += 1
                elif not status.startswith("2"):
                    outcome = entry.get("resource", {})
                    if outcome.get("resourceType") == "OperationOutcome":
                        issues = outcome.get("issue", [])
                        for issue in issues:
                            errors.append(
                                f"{issue.get('severity')}: {issue.get('diagnostics', 'Unknown error')}"
                            )

            logger.info(
                "Persistence complete: %d created, %d updated, %d errors",
                created,
                updated,
                len(errors),
            )

            return PersistenceResult(
                success=len(errors) == 0,
                resources_created=created,
                resources_updated=updated,
                errors=errors,
                id_mapping=id_mapping,
            )

        except Exception as e:
            logger.exception("Failed to persist bundle: %s", e)
            return PersistenceResult(
                success=False,
                resources_created=0,
                resources_updated=0,
                errors=[str(e)],
                id_mapping={},
            )

    def _to_transaction_bundle(
        self,
        bundle: dict[str, Any],
        organization_id: UUID | None = None,
    ) -> dict[str, Any]:
        """
        Convert a collection bundle to a transaction bundle.

        Args:
            bundle: FHIR Bundle (collection type)
            organization_id: Optional organization ID to add to resources

        Returns:
            FHIR Bundle with type "transaction" and proper request entries
        """
        transaction_entries: list[dict[str, Any]] = []

        for entry in bundle.get("entry", []):
            resource = entry.get("resource", {})
            if not resource:
                continue

            resource_type = resource.get("resourceType")
            resource_id = resource.get("id")

            if not resource_type:
                continue

            # Add organization tag if provided
            if organization_id:
                resource = self._add_organization_tag(resource, organization_id)

            # Create transaction entry
            transaction_entry: dict[str, Any] = {
                "resource": resource,
                "request": {
                    "method": "PUT" if resource_id else "POST",
                    "url": (
                        f"{resource_type}/{resource_id}"
                        if resource_id
                        else resource_type
                    ),
                },
            }

            # Preserve fullUrl for ID mapping
            if entry.get("fullUrl"):
                transaction_entry["fullUrl"] = entry["fullUrl"]

            transaction_entries.append(transaction_entry)

        return {
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": transaction_entries,
        }

    def _add_organization_tag(
        self,
        resource: dict[str, Any],
        organization_id: UUID,
    ) -> dict[str, Any]:
        """Add organization ID as a tag to the resource meta."""
        meta = resource.get("meta", {})
        tags = meta.get("tag", [])

        # Check if org tag already exists
        org_tag_url = "https://panova.ai/organization-id"
        existing_tag = next((t for t in tags if t.get("system") == org_tag_url), None)

        if existing_tag:
            existing_tag["code"] = str(organization_id)
        else:
            tags.append(
                {
                    "system": org_tag_url,
                    "code": str(organization_id),
                }
            )

        meta["tag"] = tags
        resource["meta"] = meta
        return resource


def create_fhir_store_service() -> FHIRStoreService:
    """Create a FHIRStoreService with default configuration."""
    config = FHIRClientConfig(
        gcp_project_id=settings.gcp_project_id,
        gcp_region=settings.gcp_region,
        gcp_healthcare_dataset=settings.gcp_healthcare_dataset,
        gcp_fhir_store=settings.gcp_fhir_store,
    )
    fhir_client = FHIRClient(config)
    return FHIRStoreService(fhir_client)
