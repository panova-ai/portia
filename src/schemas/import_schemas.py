"""Schemas for import endpoints."""

from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class ImportFormat(str, Enum):
    """Supported import formats."""

    CCDA = "ccda"
    HL7V2 = "hl7v2"
    FHIR_R4 = "fhir-r4"


class ImportStatus(str, Enum):
    """Import job status values."""

    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"


class ImportRequest(BaseModel):
    """Request model for importing health data."""

    format: ImportFormat = Field(description="Format of the input data")
    organization_id: UUID = Field(description="Target organization ID")
    data: str = Field(description="Base64-encoded input data")
    patient_id: UUID | None = Field(
        default=None,
        description="Optional: existing patient to match",
    )
    metadata: dict[str, str] | None = Field(
        default=None,
        description="Optional metadata (source_system, document_type, etc.)",
    )


class ResourceCounts(BaseModel):
    """Counts of resources extracted from import."""

    Patient: int = 0
    Condition: int = 0
    MedicationUsage: int = 0
    AllergyIntolerance: int = 0
    Immunization: int = 0
    Observation: int = 0
    Procedure: int = 0
    Encounter: int = 0
    DiagnosticReport: int = 0
    DocumentReference: int = 0


class ImportResponse(BaseModel):
    """Response model for import operation."""

    import_id: UUID = Field(description="Unique identifier for this import job")
    status: ImportStatus = Field(description="Current status of the import")
    fhir_bundle: dict[str, Any] | None = Field(
        default=None,
        description="The converted FHIR R5 Bundle",
    )
    resources_extracted: ResourceCounts = Field(
        default_factory=ResourceCounts,
        description="Count of resources extracted by type",
    )
    warnings: list[str] = Field(
        default_factory=list,
        description="Non-fatal warnings during conversion",
    )
    errors: list[str] = Field(
        default_factory=list,
        description="Errors that occurred during conversion",
    )
