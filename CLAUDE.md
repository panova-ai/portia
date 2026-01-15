# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project Overview

Portia is Panova's Health Data Interchange Service. It enables importing and exporting patient health data across multiple formats:

- **Import:** C-CDA, HL7v2, FHIR R4 → FHIR R5
- **Export:** FHIR R5 → C-CDA, FHIR R4

The service uses the Microsoft FHIR Converter (deployed on Cloud Run) for C-CDA and HL7v2 to FHIR R4 conversion, then applies custom R4→R5 transformations.

## Project Structure

```
portia/
├── src/
│   ├── main.py                 # FastAPI application entry point
│   ├── settings.py             # Pydantic Settings configuration
│   ├── exceptions.py           # Custom exceptions
│   ├── clients/                # Dependency injection providers
│   ├── routers/                # API endpoints
│   ├── services/               # Business logic
│   ├── schemas/                # Pydantic request/response models
│   ├── import_/                # Import pipeline
│   └── transform/              # FHIR version transformers
├── tests/                      # Test suite
├── infrastructure/             # Pulumi deployment
└── docs/                       # Documentation
```

## Development Commands

### Setup

```bash
poetry install          # Install dependencies
```

### Running Locally

```bash
poetry run uvicorn src.main:app --reload  # Start dev server on port 8000
```

### Testing

```bash
poetry run pytest                    # Run all tests
poetry run pytest -v                 # Verbose output
poetry run pytest -k test_name       # Run specific test
```

### Code Quality

```bash
poetry run black src/ tests/         # Format code
poetry run ruff check --fix src/     # Lint and auto-fix
poetry run mypy src/                 # Type checking
poetry run pre-commit run --all      # Run all checks
```

## Architecture

### Import Flow

```
C-CDA/HL7v2 → Validation → MS FHIR Converter → FHIR R4 → R4→R5 Transform → FHIR R5 Bundle
```

### Key Components

- **MSConverterService:** HTTP client for Microsoft FHIR Converter
- **StorageService:** GCS operations for temp files and exports
- **R4→R5 Transformers:** Convert FHIR R4 resources to R5 format
- **Import Gateway:** Orchestrates the complete import pipeline

## Testing Patterns

- Use `client_factory` fixture for async test clients
- Mock `MSConverterService` and `StorageService` via dependency overrides
- Sample C-CDA files in `tests/fixtures/ccda/`

## Infrastructure

- **MS FHIR Converter:** Cloud Run service (internal-only)
- **Portia:** Cloud Run service (main API)
- **GCS Buckets:** Temp storage (1-day TTL), Exports (7-day TTL)

Deploy with Pulumi:

```bash
cd infrastructure/pulumi
pulumi up
```

## Key Transformations

### MedicationStatement → MedicationUsage (R4→R5)

The most significant change. Key mappings:
- `status`: active/completed → recorded
- `medication[x]` → `medication` (CodeableReference)
- `reasonCode/reasonReference` → `reason` (CodeableReference[])
