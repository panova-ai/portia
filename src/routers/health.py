"""Health check endpoint."""

from fastapi import APIRouter

from src.routers.deps import MSConverterServiceDep
from src.schemas.health import HealthResponse

router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthResponse)
async def health_check(
    ms_converter: MSConverterServiceDep,
) -> HealthResponse:
    """Check service health including MS FHIR Converter connectivity."""
    ms_converter_healthy = await ms_converter.health_check()

    return HealthResponse(
        status="healthy" if ms_converter_healthy else "degraded",
        ms_converter=ms_converter_healthy,
    )
