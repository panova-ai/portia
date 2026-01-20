"""Sentia service dependency provider."""

from collections.abc import AsyncGenerator

from src.services.sentia_service import SentiaService

# Module-level singleton
_sentia_service: SentiaService | None = None


def get_sentia_service() -> SentiaService:
    """Get the Sentia service singleton."""
    global _sentia_service
    if _sentia_service is None:
        _sentia_service = SentiaService()
    return _sentia_service


async def get_sentia_service_async() -> AsyncGenerator[SentiaService, None]:
    """Async generator for Sentia service (for FastAPI Depends)."""
    yield get_sentia_service()
