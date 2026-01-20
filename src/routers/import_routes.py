"""Import endpoint for health data."""

import logging

from fastapi import APIRouter, HTTPException, status

from src.exceptions import ConversionError, ValidationError
from src.import_.gateway import process_import
from src.routers.deps import (
    CurrentUserDep,
    FHIRStoreServiceDep,
    MSConverterServiceDep,
    SentiaServiceDep,
)
from src.schemas.import_schemas import ImportRequest, ImportResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/import", tags=["Import"])


@router.post("", response_model=ImportResponse, status_code=status.HTTP_201_CREATED)
async def import_data(
    request: ImportRequest,
    ms_converter: MSConverterServiceDep,
    fhir_store: FHIRStoreServiceDep,
    current_user: CurrentUserDep,
    sentia_service: SentiaServiceDep,
) -> ImportResponse:
    """
    Import health data from various formats.

    Requires authentication via Firebase token or service token.

    For Firebase users, organization and practitioner context is resolved
    from Sentia. For service tokens, organization_id must be provided
    explicitly in the request.

    Supported formats:
    - C-CDA: Clinical documents from other EHRs
    - HL7v2: Lab results, ADT messages (coming soon)
    - FHIR R4: Data from R4-based systems (coming soon)

    The data is converted to FHIR R5 format and returned in the response.
    """
    # Resolve organization context
    organization_id = request.organization_id
    practitioner_id = request.practitioner_id
    practitioner_role_id = request.practitioner_role_id

    if current_user.auth_type == "firebase" and current_user.raw_token:
        # Resolve practitioner/org context from Sentia
        try:
            if organization_id:
                # Validate access to specified organization
                context = await sentia_service.validate_practitioner_org_access(
                    current_user.raw_token,
                    organization_id,
                )
            else:
                # Get default organization
                context = await sentia_service.get_practitioner_context(
                    current_user.raw_token
                )
                if context.default_organization:
                    organization_id = context.default_organization.id
                    # Get PractitionerRole for the default organization
                    context.practitioner_role = (
                        await sentia_service.get_practitioner_role(
                            current_user.raw_token, organization_id
                        )
                    )

            # Set practitioner_id if not provided
            if not practitioner_id:
                practitioner_id = context.practitioner.id

            # Set practitioner_role_id if not provided and available from context
            if not practitioner_role_id and context.practitioner_role:
                practitioner_role_id = context.practitioner_role.id

            logger.info(
                "Import request from practitioner %s (org=%s, role=%s)",
                context.practitioner.name or context.practitioner.id,
                (
                    context.default_organization.name
                    if context.default_organization
                    else organization_id
                ),
                practitioner_role_id,
            )
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=str(e),
            ) from e
        except Exception as e:
            logger.warning("Failed to resolve Sentia context: %s", e)
            # Continue without context - the import can still work
    else:
        # Service token - organization_id should be provided
        if not organization_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="organization_id is required for service token authentication",
            )
        logger.info(
            "Import request from service %s (org=%s)",
            current_user.service_name,
            organization_id,
        )

    try:
        response = await process_import(
            request,
            ms_converter,
            fhir_store=fhir_store,
            organization_id=organization_id,
            practitioner_role_id=practitioner_role_id,
        )
        return response

    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e

    except ConversionError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        ) from e
