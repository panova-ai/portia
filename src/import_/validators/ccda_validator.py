"""
C-CDA document validator.

Uses defusedxml for secure XML parsing to prevent XXE attacks.
"""

from dataclasses import dataclass
from xml.etree.ElementTree import Element

import defusedxml.ElementTree as ET

from src.exceptions import ValidationError

# C-CDA namespace
CCDA_NS = "urn:hl7-org:v3"
NAMESPACES = {"cda": CCDA_NS}


@dataclass
class CcdaValidationResult:
    """Result of C-CDA validation."""

    is_valid: bool
    document_type: str | None = None
    patient_name: str | None = None
    errors: list[str] | None = None


def validate_ccda(xml_content: str) -> CcdaValidationResult:
    """
    Validate a C-CDA document.

    Args:
        xml_content: The C-CDA XML content as a string

    Returns:
        CcdaValidationResult with validation status and extracted metadata

    Raises:
        ValidationError: If the document is not valid XML or not a C-CDA
    """
    errors: list[str] = []

    # Parse XML securely
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        raise ValidationError(f"Invalid XML: {e}") from e

    # Check for C-CDA root element
    if not _is_ccda_document(root):
        raise ValidationError(
            "Document is not a valid C-CDA: missing ClinicalDocument root element"
        )

    # Extract document type
    document_type = _extract_document_type(root)
    if not document_type:
        errors.append("Could not determine C-CDA document type")

    # Extract patient name for verification
    patient_name = _extract_patient_name(root)

    # Validate required sections exist
    section_errors = _validate_required_sections(root)
    errors.extend(section_errors)

    return CcdaValidationResult(
        is_valid=len(errors) == 0,
        document_type=document_type,
        patient_name=patient_name,
        errors=errors if errors else None,
    )


def _is_ccda_document(root: Element) -> bool:
    """Check if the root element is a ClinicalDocument."""
    # Handle both namespaced and non-namespaced documents
    tag = root.tag
    if tag == f"{{{CCDA_NS}}}ClinicalDocument":
        return True
    if tag == "ClinicalDocument":
        return True
    return False


def _extract_document_type(root: Element) -> str | None:
    """Extract the document type from the templateId."""
    # Common C-CDA template OIDs
    template_oids = {
        "2.16.840.1.113883.10.20.22.1.1": "CCD",
        "2.16.840.1.113883.10.20.22.1.2": "CCD",
        "2.16.840.1.113883.10.20.22.1.4": "ConsultationNote",
        "2.16.840.1.113883.10.20.22.1.8": "DischargeSummary",
        "2.16.840.1.113883.10.20.22.1.3": "HistoryAndPhysical",
        "2.16.840.1.113883.10.20.22.1.7": "OperativeNote",
        "2.16.840.1.113883.10.20.22.1.6": "ProcedureNote",
        "2.16.840.1.113883.10.20.22.1.9": "ProgressNote",
        "2.16.840.1.113883.10.20.22.1.14": "ReferralNote",
        "2.16.840.1.113883.10.20.22.1.13": "TransferSummary",
    }

    # Look for templateId elements
    for template_id in root.findall("cda:templateId", NAMESPACES):
        oid = template_id.get("root")
        if oid in template_oids:
            return template_oids[oid]

    # Also check without namespace
    for template_id in root.findall("templateId"):
        oid = template_id.get("root")
        if oid in template_oids:
            return template_oids[oid]

    return None


def _extract_patient_name(root: Element) -> str | None:
    """Extract the patient name from the recordTarget."""
    # Try with namespace
    patient_role = root.find(
        ".//cda:recordTarget/cda:patientRole/cda:patient", NAMESPACES
    )
    if patient_role is None:
        # Try without namespace
        patient_role = root.find(".//recordTarget/patientRole/patient")

    if patient_role is None:
        return None

    # Try to find name element
    name_elem = patient_role.find("cda:name", NAMESPACES)
    if name_elem is None:
        name_elem = patient_role.find("name")

    if name_elem is None:
        return None

    # Extract name parts
    given = name_elem.findtext("cda:given", default="", namespaces=NAMESPACES)
    if not given:
        given = name_elem.findtext("given", default="")

    family = name_elem.findtext("cda:family", default="", namespaces=NAMESPACES)
    if not family:
        family = name_elem.findtext("family", default="")

    if given or family:
        return f"{given} {family}".strip()

    return None


def _validate_required_sections(root: Element) -> list[str]:
    """Validate that required C-CDA sections are present."""
    errors: list[str] = []

    # Check for structuredBody
    structured_body = root.find(".//cda:structuredBody", NAMESPACES)
    if structured_body is None:
        structured_body = root.find(".//structuredBody")

    if structured_body is None:
        errors.append("Missing structuredBody element")
        return errors

    # Count sections (basic validation)
    sections_ns = structured_body.findall(".//cda:section", NAMESPACES)
    sections_no_ns = structured_body.findall(".//section")
    total_sections = len(sections_ns) + len(sections_no_ns)

    if total_sections == 0:
        errors.append("No sections found in document")

    return errors
