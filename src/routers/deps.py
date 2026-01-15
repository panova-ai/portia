"""Shared dependencies for routers."""

from typing import Annotated

from fastapi import Depends

from src.clients.ms_converter import get_ms_converter_service
from src.clients.storage import get_storage_service
from src.services.ms_converter_service import MSConverterService
from src.services.storage_service import StorageService

# Typed dependency aliases for use in endpoint signatures
MSConverterServiceDep = Annotated[MSConverterService, Depends(get_ms_converter_service)]
StorageServiceDep = Annotated[StorageService, Depends(get_storage_service)]
