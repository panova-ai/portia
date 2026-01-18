"""Dependency provider for FHIR Store service."""

from functools import lru_cache

from src.services.fhir_store_service import FHIRStoreService, create_fhir_store_service


@lru_cache(maxsize=1)
def get_fhir_store_service() -> FHIRStoreService:
    """Get singleton FHIRStoreService instance."""
    return create_fhir_store_service()
