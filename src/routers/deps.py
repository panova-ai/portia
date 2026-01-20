"""Shared dependencies for routers."""

from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, status
from httpx import HTTPStatusError

from src.clients.fhir_store import get_fhir_store_service
from src.clients.ms_converter import get_ms_converter_service
from src.clients.sentia import get_sentia_service
from src.clients.storage import get_storage_service
from src.core.auth import AuthenticatedUser, get_current_user
from src.services.fhir_store_service import FHIRStoreService
from src.services.ms_converter_service import MSConverterService
from src.services.sentia_service import PractitionerOrgContext, SentiaService
from src.services.storage_service import StorageService

# Typed dependency aliases for use in endpoint signatures
FHIRStoreServiceDep = Annotated[FHIRStoreService, Depends(get_fhir_store_service)]
MSConverterServiceDep = Annotated[MSConverterService, Depends(get_ms_converter_service)]
StorageServiceDep = Annotated[StorageService, Depends(get_storage_service)]
SentiaServiceDep = Annotated[SentiaService, Depends(get_sentia_service)]
CurrentUserDep = Annotated[AuthenticatedUser, Depends(get_current_user)]


async def get_practitioner_context(
    current_user: CurrentUserDep,
    sentia_service: SentiaServiceDep,
) -> PractitionerOrgContext:
    """
    Resolve practitioner and organization context from Sentia.

    Only works for Firebase-authenticated users. Service tokens
    must provide organization_id explicitly in requests.
    """
    if current_user.auth_type != "firebase":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Practitioner context requires Firebase authentication",
        )

    if not current_user.raw_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No token available for Sentia resolution",
        )

    try:
        return await sentia_service.get_practitioner_context(current_user.raw_token)
    except HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token not valid for Sentia",
            ) from e
        if e.response.status_code == 403:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Practitioner not found in Sentia",
            ) from e
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Sentia API error: {e.response.status_code}",
        ) from e


async def get_practitioner_context_with_org(
    organization_id: UUID,
    current_user: CurrentUserDep,
    sentia_service: SentiaServiceDep,
) -> PractitionerOrgContext:
    """
    Resolve practitioner context and validate access to specific organization.
    """
    if current_user.auth_type != "firebase":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Practitioner context requires Firebase authentication",
        )

    if not current_user.raw_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No token available for Sentia resolution",
        )

    try:
        return await sentia_service.validate_practitioner_org_access(
            current_user.raw_token,
            organization_id,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(e),
        ) from e
    except HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token not valid for Sentia",
            ) from e
        if e.response.status_code == 403:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Practitioner not found in Sentia",
            ) from e
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Sentia API error: {e.response.status_code}",
        ) from e


# Typed dependency for practitioner context
PractitionerContextDep = Annotated[
    PractitionerOrgContext, Depends(get_practitioner_context)
]
