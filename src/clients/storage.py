"""Dependency injection provider for storage service."""

import os

from src.services.storage_service import StorageService

_storage_service: StorageService | None = None


def get_storage_service() -> StorageService:
    """Get or create the StorageService singleton."""
    global _storage_service
    if _storage_service is None:
        # In tests, we'll override this dependency
        if os.getenv("PYTEST_CURRENT_TEST"):
            raise RuntimeError(
                "StorageService should be mocked in tests via dependency override"
            )
        _storage_service = StorageService()
    return _storage_service
