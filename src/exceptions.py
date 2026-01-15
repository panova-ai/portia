"""Custom exceptions for Portia service."""


class PortiaError(Exception):
    """Base exception for Portia errors."""

    pass


class ValidationError(PortiaError):
    """Error during input validation."""

    pass


class ConversionError(PortiaError):
    """Error during format conversion."""

    pass


class TransformError(PortiaError):
    """Error during FHIR version transformation."""

    pass


class StorageError(PortiaError):
    """Error during storage operations."""

    pass
