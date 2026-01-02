# Portia PR-FAQ

## Press Release

### Panova Launches Portia: Seamless Health Data Exchange for Independent Practices

*New service eliminates data silos, enabling small clinics to easily import patient records and meet federal data portability requirements*

**San Francisco, CA** — Panova today announced Portia, a health data interchange service that allows independent practitioners and small clinics to import and export patient records across multiple industry formats. Portia eliminates the technical barriers that have historically prevented smaller practices from participating in health information exchange.

"When a patient arrives with records from another provider, practices shouldn't need an IT department to access that information," said [CEO Name], CEO of Panova. "Portia makes health data exchange as simple as uploading a file."

**The Problem:** Independent practitioners and small clinics often receive patient records as C-CDA documents, HL7 messages, or FHIR bundles from hospitals, labs, and other providers. Without expensive integration infrastructure, this data sits in inboxes as unstructured files, forcing staff to manually re-enter information—wasting time and introducing errors.

**The Solution:** Portia automatically converts incoming health data into the practice's native format (FHIR R5), extracting problems, medications, allergies, immunizations, and lab results into discrete, searchable data. When patients request their records or providers need to send referrals, Portia exports data in any required format.

**Key Features:**
- **Universal Import:** Accept C-CDA, HL7v2, and FHIR documents from any source
- **Smart Extraction:** Automatically parse clinical data into discrete elements
- **Patient Matching:** Intelligently match incoming records to existing patients
- **One-Click Export:** Generate FHIR bundles or C-CDA documents for referrals and patient requests
- **Certification Ready:** Supports ONC CEHRT requirements for data portability

"I used to spend 20 minutes manually entering data from referral documents," said Dr. Sarah Chen, a family medicine physician in Portland. "Now I upload the file and everything appears in the chart. It's transformed how we handle new patients."

Portia is available today for all Panova EHR customers at no additional cost.

---

## Frequently Asked Questions

### Customer FAQs

**Q: What file formats can Portia import?**

A: Portia supports the most common health data exchange formats:
- **C-CDA** (Consolidated Clinical Document Architecture) - The standard format for patient summaries, referral notes, and care transitions
- **HL7v2** - Common for lab results and ADT (admit/discharge/transfer) messages
- **FHIR R4 and STU3** - Modern API-based health data from other FHIR-enabled systems

**Q: What data gets extracted from imported documents?**

A: Portia extracts and structures:
- Patient demographics
- Problems/diagnoses (conditions)
- Medications (current and historical)
- Allergies and intolerances
- Immunizations
- Vital signs and lab results
- Procedures
- Care team information

The original document is also preserved and linked to the extracted data for reference.

**Q: How does patient matching work?**

A: When you import a document, Portia attempts to match it to an existing patient using:
- Medical record numbers (MRN)
- Name and date of birth
- Other identifiers in the document

If no match is found, you can create a new patient or manually select an existing one. Portia never automatically merges data without your confirmation.

**Q: What if a document contains errors or data I don't want?**

A: Portia shows you what will be imported before committing changes. You can:
- Review extracted data before saving
- Exclude specific items (e.g., outdated medications)
- Flag items for clinical review
- Reject the entire import

**Q: Can patients request their data through Portia?**

A: Yes. Portia supports patient data export as required by the 21st Century Cures Act. Patients can request their complete health record as a FHIR bundle, which can be imported into another provider's system or a personal health app.

**Q: What export formats are available?**

A: Portia can export patient data as:
- **FHIR R5 Bundle** - Complete, structured data for modern systems
- **FHIR R4 Bundle** - Compatible with US Core for regulatory compliance
- **C-CDA** - For referrals to providers using traditional health IT systems

**Q: Is Portia certified for Meaningful Use / MIPS?**

A: Portia supports the ONC CEHRT certification criteria for:
- §170.315(b)(1) - Transitions of Care (C-CDA send/receive)
- §170.315(b)(2) - Clinical Information Reconciliation
- §170.315(b)(6) - Data Export
- §170.315(b)(10) - Electronic Health Information Export
- §170.315(g)(9) and (g)(10) - Standardized API for Patient and Population Services

**Q: How secure is health data during import/export?**

A: All data is:
- Encrypted in transit (TLS 1.3)
- Encrypted at rest (AES-256)
- Processed in HIPAA-compliant infrastructure (Google Cloud Healthcare API)
- Logged for audit purposes (without PHI in logs)
- Automatically purged from temporary storage after processing

**Q: What happens if an import fails?**

A: Portia provides detailed error messages explaining what went wrong:
- Invalid or corrupted files are rejected with specific validation errors
- Partial imports (some data extracted, some failed) show exactly what succeeded and what didn't
- All import attempts are logged for troubleshooting

You can retry failed imports or contact support for assistance with problematic files.

---

### Internal FAQs

**Q: Why build this instead of using an existing integration engine?**

A: Existing solutions (Mirth Connect, Rhapsody, etc.) are designed for enterprise IT departments with dedicated integration staff. They require significant configuration and maintenance. Portia is purpose-built for Panova's use case: small practices that need simple, automated data exchange without IT overhead.

**Q: Why not use a third-party conversion API like Metriport?**

A: We evaluated third-party options and chose to self-host the Microsoft FHIR Converter for several reasons:
- **Cost control** - Per-transaction pricing adds up quickly for high-volume practices
- **Data privacy** - PHI stays within our infrastructure
- **Customization** - We can modify conversion templates for our specific needs
- **Latency** - Local processing is faster than external API calls

**Q: Why is FHIR R5 our internal format when most of the industry uses R4?**

A: We adopted R5 early because:
- It's the latest normative release with improved data models
- MedicationUsage (R5) is cleaner than MedicationStatement (R4)
- We avoid a future migration from R4 to R5
- Google Healthcare API supports R5

Portia handles R4↔R5 conversion transparently, so external interoperability isn't affected.

**Q: What's the biggest technical risk?**

A: **Data fidelity during conversion.** Health data formats are complex, and conversions can lose nuance. Mitigations:
- Use HL7's official mapping guidance
- Preserve original documents for reference
- Extensive testing with real-world sample data
- Clear provenance linking converted data to sources
- Ability for clinicians to review before committing

**Q: How will we handle the variety of C-CDA implementations?**

A: C-CDA has many optional fields and implementation variations. Our approach:
- Start with the Microsoft FHIR Converter's battle-tested templates
- Test against HL7's C-CDA example repository
- Collect anonymized conversion metrics to identify edge cases
- Iteratively improve handling of common variations

**Q: What's the expected conversion accuracy?**

A: Based on industry benchmarks for C-CDA to FHIR conversion:
- **Demographics:** 99%+ (well-standardized)
- **Problems/Conditions:** 95%+ (some code mapping challenges)
- **Medications:** 90%+ (NDC/RxNorm mapping complexity)
- **Allergies:** 95%+
- **Labs/Vitals:** 90%+ (LOINC mapping variations)

We'll track actual metrics and publish them in our documentation.

**Q: How does this affect our certification timeline?**

A: Portia directly addresses several CEHRT criteria we need for certification:
- (b)(1), (b)(2): Transitions of Care - **Required**
- (b)(6), (b)(10): Data Export - **Required**
- (g)(9), (g)(10): API Access - **Required for 2015 Edition Cures Update**

Building Portia in-house gives us full control over the certification testing process.

**Q: What's the staffing requirement?**

A: Initial build: 1 senior engineer, ~3 months. Ongoing maintenance: Part-time (included in platform team rotation). The Microsoft FHIR Converter handles the heavy lifting; our work is primarily orchestration and R4↔R5 transformation.

**Q: When will this be available?**

A: Target timeline:
- **Phase 1-2 (C-CDA Import):** 5 weeks
- **Phase 3 (FHIR Export):** 2 weeks
- **Phase 4-5 (Additional Formats):** 4 weeks
- **Phase 6 (Production):** 2 weeks

MVP (C-CDA import + FHIR R5 export) in ~7 weeks. Full feature set in ~13 weeks.
