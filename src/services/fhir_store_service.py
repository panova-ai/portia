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
from httpx import HTTPStatusError

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

    @property
    def client(self) -> FHIRClient:
        """Expose the underlying FHIRClient for operations like patient matching."""
        return self._client

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

                # Debug: log response entry
                logger.warning(
                    "Bundle entry %d response: status=%s, location=%s, response_info=%s",
                    i,
                    status,
                    location,
                    response_info,
                )

                # Extract resource ID from location header
                if location:
                    # Location can be a full URL or ResourceType/id/_history/version
                    # Full URL format: https://.../ResourceType/uuid/_history/...
                    # We need to find the UUID before _history
                    parts = location.split("/")

                    # Find the index of _history and get the part before it
                    resource_id = ""
                    for idx, part in enumerate(parts):
                        if part == "_history" and idx > 0:
                            resource_id = parts[idx - 1]
                            break

                    # Fallback: if no _history found, try parts[1] for short format
                    if not resource_id and len(parts) >= 2:
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

        except HTTPStatusError as e:
            # Extract OperationOutcome from response body for detailed error info
            error_details = [str(e)]
            try:
                response_body = e.response.json()
                if response_body.get("resourceType") == "OperationOutcome":
                    issues = response_body.get("issue", [])
                    for issue in issues:
                        severity = issue.get("severity", "error")
                        diagnostics = issue.get("diagnostics", "")
                        details = issue.get("details", {}).get("text", "")
                        expression = issue.get("expression", [])
                        location = ", ".join(expression) if expression else ""
                        error_msg = (
                            f"{severity}: {diagnostics or details or 'Unknown error'}"
                        )
                        if location:
                            error_msg = f"{error_msg} at {location}"
                        error_details.append(error_msg)
                        logger.error("FHIR error: %s", error_msg)
                else:
                    logger.error("FHIR error response: %s", response_body)
            except Exception:
                logger.error("Could not parse error response body")

            return PersistenceResult(
                success=False,
                resources_created=0,
                resources_updated=0,
                errors=error_details,
                id_mapping={},
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

            full_url = entry.get("fullUrl", "")

            # For resources with urn:uuid fullUrl, always use POST to let server assign IDs
            # This is required for GCP Healthcare API's reference resolution
            # Do NOT use existing requests from MS Converter as they use ResourceType/id format
            use_post = full_url.startswith("urn:uuid:")

            if use_post:
                # POST to resource type endpoint - server will resolve urn:uuid refs
                request = {
                    "method": "POST",
                    "url": resource_type,
                }
            else:
                # For non-urn:uuid fullUrls, use existing request if present
                # or construct a PUT/POST request
                existing_request = entry.get("request")
                if (
                    existing_request
                    and existing_request.get("method")
                    and existing_request.get("url")
                ):
                    request = existing_request
                else:
                    request = {
                        "method": "PUT" if resource_id else "POST",
                        "url": (
                            f"{resource_type}/{resource_id}"
                            if resource_id
                            else resource_type
                        ),
                    }

            # Create transaction entry
            transaction_entry: dict[str, Any] = {
                "resource": resource,
                "request": request,
            }

            # Preserve fullUrl for ID mapping (required for local reference resolution)
            if full_url:
                transaction_entry["fullUrl"] = full_url

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


@dataclass
class DeletionResult:
    """Result of deleting imported resources."""

    success: bool
    resources_deleted: int
    errors: list[str]


# Import source tag system (must match identifier_service.py)
IMPORT_SOURCE_TAG_SYSTEM = "https://panova.ai/import-source"


async def delete_imported_resources(
    fhir_client: FHIRClient,
    patient_id: UUID,
    source_system: str,
    resource_types: list[str],
) -> DeletionResult:
    """
    Delete import-created resources for a patient.

    Only deletes resources tagged with the specified import source,
    preserving manually-created resources.

    Args:
        fhir_client: FHIR client for API calls
        patient_id: Patient whose imported resources should be deleted
        source_system: Import source tag to match (e.g., "charm")
        resource_types: List of resource types to delete

    Returns:
        DeletionResult with count and any errors
    """
    errors: list[str] = []
    total_deleted = 0

    tag_filter = f"{IMPORT_SOURCE_TAG_SYSTEM}|{source_system}"

    for resource_type in resource_types:
        try:
            # Build search parameters
            # Use subject for most resources, patient for Observation
            if resource_type == "Observation":
                patient_param = "patient"
            else:
                patient_param = "subject"

            search_params = {
                patient_param: f"Patient/{patient_id}",
                "_tag": tag_filter,
                "_count": "1000",  # Get all matching resources
            }

            # Search for resources to delete
            logger.info(
                "Searching for %s resources to delete (patient=%s, tag=%s)",
                resource_type,
                patient_id,
                source_system,
            )

            # Use BaseClient's _search_resource method (via patients client)
            response = await fhir_client.patients._search_resource(
                resource_type, search_params
            )

            entries = response.get("entry", [])
            if not entries:
                logger.debug("No %s resources found for deletion", resource_type)
                continue

            logger.info("Found %d %s resources to delete", len(entries), resource_type)

            # Delete each resource
            for entry in entries:
                resource = entry.get("resource", {})
                resource_id = resource.get("id")
                if resource_id:
                    try:
                        # Use BaseClient's _delete_resource method
                        await fhir_client.patients._delete_resource(
                            resource_type, UUID(resource_id)
                        )
                        total_deleted += 1
                    except HTTPStatusError as e:
                        if e.response.status_code == 404:
                            # Already deleted, ignore
                            pass
                        else:
                            errors.append(
                                f"Failed to delete {resource_type}/{resource_id}: {e}"
                            )
                    except Exception as e:
                        errors.append(
                            f"Failed to delete {resource_type}/{resource_id}: {e}"
                        )

        except HTTPStatusError as e:
            if e.response.status_code == 404:
                # Resource type might not exist or no results, ignore
                logger.debug("No %s resources found (404)", resource_type)
            else:
                errors.append(f"Failed to search {resource_type}: {e}")
        except Exception as e:
            errors.append(f"Failed to search {resource_type}: {e}")

    logger.info(
        "Deletion complete: %d resources deleted, %d errors",
        total_deleted,
        len(errors),
    )

    return DeletionResult(
        success=len(errors) == 0,
        resources_deleted=total_deleted,
        errors=errors,
    )


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
