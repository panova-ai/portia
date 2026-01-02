# Portia - Health Data Interchange Service

> *Portia (Latin: gate, portal) - The gateway between health data formats*

## Overview

Portia is Panova's health data interchange service, enabling independent practitioners and small clinics to:
- **Import** patient records from external systems (C-CDA, HL7v2, other FHIR versions)
- **Export** patient data as FHIR R5 bundles (for CEHRT certification and data portability)

## Scope

**Target Users:** Independent practitioners, small outpatient clinics

### Import Capabilities

| Format | Use Case | Priority |
|--------|----------|----------|
| C-CDA | Patient records from other EHRs, HIEs | P0 |
| HL7v2 | Lab results, ADT messages | P1 |
| FHIR R4 | Data from R4-based systems | P1 |
| FHIR STU3 | Legacy FHIR data | P2 |
| PDF (with OCR) | Scanned records, faxes | P2 |

### Export Capabilities

| Format | Use Case | Priority |
|--------|----------|----------|
| FHIR R5 Bundle | Data portability, CEHRT certification | P0 |
| C-CDA | Referrals, care transfers | P1 |
| FHIR R4 Bundle | Interop with R4 systems | P1 |

### CEHRT Certification Requirements

For ONC certification (§170.315), Portia must support:
- **(b)(1) Transitions of Care** - Send/receive C-CDA documents
- **(b)(2) Clinical Information Reconciliation** - Import and reconcile meds, allergies, problems
- **(b)(6) Data Export** - Export patient data in computable format
- **(b)(10) Electronic Health Information Export** - Single patient and bulk export

---

## Architecture

```
                                    ┌─────────────────────────────────────────┐
                                    │              PORTIA                      │
                                    │         (Cloud Run Service)              │
┌──────────────┐                    │                                         │
│   External   │    ┌───────────────┼───────────────┐                         │
│   Systems    │───▶│    IMPORT     │    EXPORT     │◀───┐                    │
│  (C-CDA,     │    │    Gateway    │    Gateway    │    │                    │
│   HL7v2,     │    └───────┬───────┴───────┬───────┘    │                    │
│   FHIR R4)   │            │               │            │                    │
└──────────────┘            ▼               ▼            │                    │
                    ┌───────────────┐ ┌───────────────┐  │                    │
                    │  Format       │ │  Format       │  │                    │
                    │  Converters   │ │  Generators   │  │                    │
                    │  ┌─────────┐  │ │  ┌─────────┐  │  │                    │
                    │  │ C-CDA   │  │ │  │ C-CDA   │  │  │                    │
                    │  │ HL7v2   │  │ │  │ FHIR R4 │  │  │                    │
                    │  │ FHIR R4 │  │ │  │ FHIR R5 │  │  │                    │
                    │  └─────────┘  │ │  └─────────┘  │  │                    │
                    └───────┬───────┘ └───────┬───────┘  │                    │
                            │                 │          │                    │
                            ▼                 ▼          │                    │
                    ┌───────────────────────────────┐    │                    │
                    │      R4 ↔ R5 Transformer      │    │                    │
                    └───────────────┬───────────────┘    │                    │
                                    │                    │                    │
                                    ▼                    │                    │
                    ┌───────────────────────────────┐    │                    │
                    │   Provenance & Audit Trail    │    │                    │
                    └───────────────────────────────┘    │                    │
                                    │                    │                    │
                                    │                    │                    │
└───────────────────────────────────┼────────────────────┼────────────────────┘
                                    │                    │
                                    ▼                    │
                    ┌───────────────────────────────┐    │
                    │        Panova FHIR R5         │────┘
                    │     (Google Healthcare API)   │
                    └───────────────────────────────┘
```

### Components

1. **Import Gateway**
   - Receives files via API or GCS upload
   - Validates and identifies format
   - Routes to appropriate converter
   - Handles patient matching

2. **Export Gateway**
   - Accepts export requests (single patient or bulk)
   - Queries FHIR store for requested data
   - Routes to appropriate generator
   - Packages and delivers output

3. **Format Converters** (Import)
   - C-CDA → FHIR R4 (Microsoft FHIR Converter)
   - HL7v2 → FHIR R4 (Microsoft FHIR Converter)
   - FHIR STU3 → FHIR R4 (custom transformer)

4. **Format Generators** (Export)
   - FHIR R5 → FHIR R5 Bundle (passthrough with filtering)
   - FHIR R5 → FHIR R4 Bundle (R5→R4 transformer)
   - FHIR R5 → C-CDA (Microsoft FHIR Converter or custom)

5. **R4 ↔ R5 Transformer**
   - Bidirectional transformation
   - Based on HL7 official mappings
   - Handles breaking changes (MedicationStatement ↔ MedicationUsage)

6. **Provenance & Audit**
   - Tracks all imports/exports
   - Links transformed resources to sources
   - Audit trail for compliance

---

## Repository Structure

**Repository:** `panova-ai/portia`

```
portia/
├── src/
│   ├── main.py                     # FastAPI application
│   ├── config.py                   # Settings and configuration
│   │
│   ├── api/
│   │   ├── import_routes.py        # Import endpoints
│   │   ├── export_routes.py        # Export endpoints
│   │   └── schemas.py              # Request/response models
│   │
│   ├── import_/                    # Import pipeline
│   │   ├── gateway.py              # Import orchestration
│   │   ├── validators/
│   │   │   ├── ccda_validator.py
│   │   │   ├── hl7v2_validator.py
│   │   │   └── fhir_validator.py
│   │   ├── converters/
│   │   │   ├── ccda_converter.py   # Calls Microsoft FHIR Converter
│   │   │   ├── hl7v2_converter.py  # Calls Microsoft FHIR Converter
│   │   │   └── fhir_r4_converter.py
│   │   └── patient_matcher.py      # Match/create patients
│   │
│   ├── export/                     # Export pipeline
│   │   ├── gateway.py              # Export orchestration
│   │   ├── query_builder.py        # Build FHIR queries
│   │   ├── generators/
│   │   │   ├── fhir_bundle.py      # FHIR R5/R4 bundle generator
│   │   │   └── ccda_generator.py   # C-CDA document generator
│   │   └── packager.py             # Package for download
│   │
│   ├── transform/                  # Version transformers
│   │   ├── r4_to_r5/
│   │   │   ├── __init__.py
│   │   │   ├── patient.py
│   │   │   ├── condition.py
│   │   │   ├── medication.py       # MedicationStatement → MedicationUsage
│   │   │   ├── allergy.py
│   │   │   ├── observation.py
│   │   │   └── ...
│   │   ├── r5_to_r4/
│   │   │   ├── __init__.py
│   │   │   ├── patient.py
│   │   │   ├── medication.py       # MedicationUsage → MedicationStatement
│   │   │   └── ...
│   │   └── stu3_to_r4/
│   │       └── ...
│   │
│   ├── models/
│   │   ├── r4/                     # FHIR R4 Pydantic models
│   │   ├── r5/                     # FHIR R5 Pydantic models
│   │   └── ccda/                   # C-CDA models (if needed)
│   │
│   ├── services/
│   │   ├── fhir_client.py          # FHIR store client
│   │   ├── ms_converter_client.py  # Microsoft FHIR Converter client
│   │   ├── storage_service.py      # GCS for file handling
│   │   └── provenance_service.py   # Audit trail management
│   │
│   └── utils/
│       ├── xml_utils.py            # Safe XML parsing
│       └── bundle_utils.py         # Bundle manipulation
│
├── tests/
│   ├── fixtures/
│   │   ├── ccda/                   # Sample C-CDA files
│   │   ├── hl7v2/                  # Sample HL7v2 messages
│   │   └── fhir/                   # Sample FHIR bundles
│   ├── unit/
│   │   ├── test_transformers.py
│   │   ├── test_validators.py
│   │   └── test_generators.py
│   └── integration/
│       ├── test_import_ccda.py
│       ├── test_import_hl7v2.py
│       ├── test_export_bundle.py
│       └── test_export_ccda.py
│
├── infrastructure/
│   ├── pulumi/
│   │   ├── __main__.py
│   │   ├── Pulumi.yaml
│   │   ├── Pulumi.dev.yaml
│   │   ├── Pulumi.staging.yaml
│   │   └── Pulumi.prod.yaml
│   └── docker/
│       └── ms-fhir-converter/      # Microsoft converter config
│
├── docs/
│   ├── api.md
│   ├── import-formats.md
│   ├── export-formats.md
│   └── certification.md            # CEHRT compliance notes
│
├── Dockerfile
├── pyproject.toml
├── README.md
└── CLAUDE.md
```

---

## API Design

### Import Endpoints

#### POST /import
Import a health data file.

```json
Request:
{
  "format": "ccda",                 // ccda, hl7v2, fhir-r4, fhir-stu3
  "patient_id": "uuid",             // Optional: existing patient to match
  "organization_id": "uuid",        // Target organization
  "data": "base64-encoded-content",
  "metadata": {
    "source_system": "Epic",
    "document_type": "ccd",         // For C-CDA: ccd, referral, consultation
    "received_date": "2024-01-15"
  }
}

Response:
{
  "import_id": "uuid",
  "status": "completed",            // queued, processing, completed, partial, failed
  "patient_id": "uuid",
  "resources_created": {
    "Condition": 5,
    "MedicationUsage": 3,
    "AllergyIntolerance": 2,
    "Immunization": 8,
    "Observation": 12
  },
  "warnings": ["Unmapped code: 12345"],
  "errors": [],
  "provenance_id": "uuid",
  "document_reference_id": "uuid"
}
```

#### GET /import/{import_id}
Get import job status.

#### GET /import/{import_id}/resources
List resources created from import.

### Export Endpoints

#### POST /export
Request a data export.

```json
Request:
{
  "format": "fhir-r5-bundle",       // fhir-r5-bundle, fhir-r4-bundle, ccda
  "patient_id": "uuid",             // Single patient export
  "organization_id": "uuid",
  "resource_types": [               // Optional filter
    "Patient",
    "Condition",
    "MedicationUsage",
    "AllergyIntolerance"
  ],
  "date_range": {                   // Optional filter
    "start": "2023-01-01",
    "end": "2024-01-01"
  },
  "purpose": "patient-request"      // patient-request, referral, care-transfer
}

Response:
{
  "export_id": "uuid",
  "status": "completed",
  "format": "fhir-r5-bundle",
  "download_url": "https://...",    // Signed URL, expires in 1 hour
  "expires_at": "2024-01-15T11:30:00Z",
  "resource_count": 45,
  "bundle_size_bytes": 125000
}
```

#### POST /export/bulk
Bulk export for multiple patients (for data portability compliance).

```json
Request:
{
  "format": "fhir-r5-bundle",
  "organization_id": "uuid",
  "patient_ids": ["uuid1", "uuid2", ...],  // Or omit for all patients
  "since": "2023-01-01T00:00:00Z"          // Incremental export
}

Response:
{
  "export_id": "uuid",
  "status": "queued",
  "estimated_completion": "2024-01-15T10:45:00Z"
}
```

#### GET /export/{export_id}
Get export status and download URL.

---

## Implementation Phases

### Phase 1: Foundation (2 weeks)

- [ ] Create `panova-ai/portia` repository
- [ ] Project structure and CI/CD setup
- [ ] Deploy Microsoft FHIR Converter to Cloud Run
- [ ] Basic FastAPI application skeleton
- [ ] Configuration and secrets management
- [ ] GCS integration for file handling

### Phase 2: C-CDA Import (3 weeks)

- [ ] C-CDA validation
- [ ] Microsoft FHIR Converter client
- [ ] R4 → R5 transformers (core resources)
  - [ ] Patient
  - [ ] Condition
  - [ ] MedicationStatement → MedicationUsage
  - [ ] AllergyIntolerance
  - [ ] Immunization
  - [ ] Observation
  - [ ] Procedure
- [ ] Provenance generation
- [ ] DocumentReference for original
- [ ] POST /import endpoint
- [ ] Integration tests with sample CCDAs

### Phase 3: FHIR R5 Export (2 weeks)

- [ ] FHIR store query builder
- [ ] FHIR R5 Bundle generator
- [ ] Export filtering (resource types, date range)
- [ ] Signed download URLs
- [ ] POST /export endpoint
- [ ] Single patient export tests

### Phase 4: Additional Import Formats (2 weeks)

- [ ] HL7v2 → FHIR R4 (via Microsoft Converter)
- [ ] FHIR R4 → FHIR R5 (direct transform)
- [ ] FHIR STU3 → FHIR R4 → R5
- [ ] Format auto-detection

### Phase 5: Additional Export Formats (2 weeks)

- [ ] FHIR R5 → FHIR R4 transformer
- [ ] FHIR R4 Bundle export
- [ ] FHIR R5 → C-CDA generation (stretch goal)

### Phase 6: Production & Certification (2 weeks)

- [ ] Bulk export for data portability
- [ ] Patient matching improvements
- [ ] Performance optimization
- [ ] Security hardening
- [ ] CEHRT compliance documentation
- [ ] Load testing

---

## CEHRT Certification Mapping

| Certification Criterion | Portia Feature |
|------------------------|----------------|
| §170.315(b)(1) Transitions of Care | C-CDA import/export |
| §170.315(b)(2) Clinical Information Reconciliation | Import with patient matching |
| §170.315(b)(6) Data Export | FHIR R5 Bundle export |
| §170.315(b)(10) EHI Export | Bulk export endpoint |
| §170.315(g)(9) Application Access - All Data Request | FHIR Bundle with US Core profiles |
| §170.315(g)(10) Standardized API | FHIR R4 export (US Core) |

---

## Infrastructure

### Cloud Run Services

| Service | Runtime | Memory | CPU | Timeout | Notes |
|---------|---------|--------|-----|---------|-------|
| portia | Python 3.12 | 1GB | 2 | 10min | Main service |
| ms-fhir-converter | .NET | 2GB | 2 | 5min | Internal only |

### Storage

- **GCS Bucket:** `portia-{env}-temp` - Temporary file processing
- **GCS Bucket:** `portia-{env}-exports` - Export downloads (auto-expire)

### Secrets

- `portia-fhir-credentials` - Healthcare API access
- `portia-service-auth` - Service-to-service JWT
- `portia-ms-converter-url` - Internal converter URL

---

## Security Considerations

- All data contains PHI - encrypt at rest and in transit
- XML parsing with defusedxml to prevent XXE attacks
- Signed, expiring URLs for export downloads
- Rate limiting on import/export endpoints
- Audit logging for all operations (no PHI in logs)
- Service-to-service authentication
- Input validation and size limits

---

## Open Questions

1. **Patient Matching Strategy**
   - Create new vs. match on demographics?
   - What identifiers to use (MRN, SSN, name+DOB)?
   - How to handle conflicts?

2. **Reconciliation Workflow**
   - Auto-merge imported data?
   - Require manual review?
   - Flag duplicates for clinician decision?

3. **Export Scope**
   - Include all historical data or date-limited?
   - Include documents/attachments?
   - Size limits for single exports?

4. **C-CDA Export Priority**
   - Required for full CEHRT compliance
   - Build custom or use Microsoft Converter (FHIR→C-CDA)?

---

## References

- [Microsoft FHIR Converter](https://github.com/microsoft/FHIR-Converter)
- [HL7 C-CDA on FHIR](https://build.fhir.org/ig/HL7/ccda-on-fhir/)
- [FHIR R4 to R5 Mappings](https://hl7.org/fhir/R5/diff.html)
- [ONC CEHRT Certification Criteria](https://www.healthit.gov/topic/certification-ehrs/certification-criteria)
- [US Core Implementation Guide](https://www.hl7.org/fhir/us/core/)
- [HL7 C-CDA Examples](https://github.com/HL7/C-CDA-Examples)
