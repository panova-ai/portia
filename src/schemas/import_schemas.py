"""Schemas for import endpoints."""

from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

# Maximum size for imported data (10MB decoded, ~13.3MB base64-encoded)
# Typical C-CDA documents are 100KB-2MB
MAX_IMPORT_SIZE_BYTES = 10 * 1024 * 1024  # 10MB
MAX_BASE64_SIZE = int(MAX_IMPORT_SIZE_BYTES * 4 / 3) + 100  # base64 overhead + padding


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


class MatchingOptions(BaseModel):
    """Options for patient matching during import."""

    patient_id: UUID | None = Field(
        default=None,
        description="If provided, use this existing Patient (skips matching)",
    )
    person_id: UUID | None = Field(
        default=None,
        description="If provided, use this existing Person (skips person matching)",
    )
    create_if_not_found: bool = Field(
        default=True,
        description="Create new Person/Patient if no match found",
    )
    strict_matching: bool = Field(
        default=False,
        description="If true, fail import when no match found instead of creating",
    )


class ImportRequest(BaseModel):
    """Request model for importing health data."""

    format: ImportFormat = Field(description="Format of the input data")
    data: str = Field(description="Base64-encoded input data")

    @field_validator("data")
    @classmethod
    def validate_data_size(cls, v: str) -> str:
        """Validate that the data field doesn't exceed the maximum size."""
        if len(v) > MAX_BASE64_SIZE:
            max_mb = MAX_IMPORT_SIZE_BYTES / (1024 * 1024)
            raise ValueError(
                f"Import data exceeds maximum size of {max_mb:.0f}MB. "
                "Please split large documents or contact support."
            )
        return v

    # Context - defaults to current user's context if not provided
    organization_id: UUID | None = Field(
        default=None,
        description="Target organization ID. Defaults to current user's organization.",
    )
    practitioner_id: UUID | None = Field(
        default=None,
        description="Target practitioner ID. Defaults to current user.",
    )
    practitioner_role_id: UUID | None = Field(
        default=None,
        description="PractitionerRole ID for encounter participant. If not provided, will be looked up from practitioner_id and organization_id.",
    )

    # Patient matching options
    matching: MatchingOptions | None = Field(
        default=None,
        description="Patient matching options",
    )

    # Metadata
    metadata: dict[str, str] | None = Field(
        default=None,
        description="Optional metadata (source_system, document_type, etc.)",
    )

    # Legacy field - use matching.patient_id instead
    patient_id: UUID | None = Field(
        default=None,
        description="Legacy: use matching.patient_id instead",
    )


class ResourceCounts(BaseModel):
    """Counts of resources extracted from import."""

    Patient: int = 0
    Condition: int = 0
    MedicationStatement: int = 0  # GCP Healthcare R5 still uses this name
    AllergyIntolerance: int = 0
    Immunization: int = 0
    Observation: int = 0
    Procedure: int = 0
    Encounter: int = 0
    Composition: int = 0
    DiagnosticReport: int = 0
    DocumentReference: int = 0
    Practitioner: int = 0
    Organization: int = 0
    Medication: int = 0


class MatchingResult(BaseModel):
    """Result of patient/person matching during import."""

    person_id: UUID | None = Field(
        default=None,
        description="ID of the matched or created Person resource",
    )
    patient_id: UUID | None = Field(
        default=None,
        description="ID of the matched or created Patient resource",
    )
    person_created: bool = Field(
        default=False,
        description="True if a new Person was created",
    )
    patient_created: bool = Field(
        default=False,
        description="True if a new Patient was created",
    )
    match_method: str | None = Field(
        default=None,
        description="How the match was made (e.g., 'demographics', 'provided_id')",
    )


class PersistenceInfo(BaseModel):
    """Information about FHIR store persistence."""

    persisted: bool = Field(
        description="Whether resources were successfully persisted to FHIR store"
    )
    resources_created: int = Field(
        default=0,
        description="Number of resources created in FHIR store",
    )
    resources_updated: int = Field(
        default=0,
        description="Number of resources updated in FHIR store",
    )


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
    matching_result: MatchingResult | None = Field(
        default=None,
        description="Result of patient matching (if matching was performed)",
    )
    persistence: PersistenceInfo | None = Field(
        default=None,
        description="Result of FHIR store persistence (if persistence was enabled)",
    )
    warnings: list[str] = Field(
        default_factory=list,
        description="Non-fatal warnings during conversion",
    )
    errors: list[str] = Field(
        default_factory=list,
        description="Errors that occurred during conversion",
    )
