"""Dependency injection provider for MS FHIR Converter client."""

from src.services.ms_converter_service import MSConverterService

_ms_converter_service: MSConverterService | None = None


def get_ms_converter_service() -> MSConverterService:
    """Get or create the MSConverterService singleton."""
    global _ms_converter_service
    if _ms_converter_service is None:
        _ms_converter_service = MSConverterService()
    return _ms_converter_service
