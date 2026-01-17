"""
C-CDA pre-processor for sanitizing problematic content.

The MS FHIR Converter's Liquid templates can produce invalid JSON when
C-CDA documents contain non-standard values. This module sanitizes the
XML before conversion to prevent these issues.

Known issues handled:
- doseQuantity/@value with range values like "1-2" (produces invalid JSON)
- Other numeric attributes with non-numeric content
"""

import re
from xml.etree import ElementTree as ET

# C-CDA namespace
CDA_NS = "urn:hl7-org:v3"
NAMESPACES = {"cda": CDA_NS}

# Elements with 'value' attributes that must be numeric
NUMERIC_VALUE_ELEMENTS = [
    "doseQuantity",
    "rateQuantity",
    "maxDoseQuantity",
    "quantity",
]

# Pattern to detect non-numeric values (allows decimals and negative numbers)
NUMERIC_PATTERN = re.compile(r"^-?\d+(\.\d+)?$")

# Pattern to extract the first number from a range like "1-2"
FIRST_NUMBER_PATTERN = re.compile(r"^(-?\d+(?:\.\d+)?)")


def sanitize_ccda(content: str) -> tuple[str, list[str]]:
    """
    Sanitize a C-CDA document to fix values that cause MS Converter failures.

    Args:
        content: The raw C-CDA XML content

    Returns:
        Tuple of (sanitized XML string, list of warnings about changes made)
    """
    warnings: list[str] = []

    try:
        # Register namespace to preserve it in output
        ET.register_namespace("", CDA_NS)

        root = ET.fromstring(content)

        # Fix numeric value attributes
        fixes = _fix_numeric_value_attributes(root)
        warnings.extend(fixes)

        # Convert back to string
        sanitized = ET.tostring(root, encoding="unicode")

        # Restore XML declaration if present in original
        if content.strip().startswith("<?xml"):
            # Extract original declaration
            decl_match = re.match(r"(<\?xml[^?]*\?>)", content)
            if decl_match:
                sanitized = decl_match.group(1) + "\n" + sanitized

        return sanitized, warnings

    except ET.ParseError as e:
        # If XML parsing fails, return original content with warning
        return content, [f"XML parsing failed, skipping sanitization: {e}"]


def _fix_numeric_value_attributes(root: ET.Element) -> list[str]:
    """
    Find and fix elements with non-numeric 'value' attributes.

    For elements like doseQuantity that expect numeric values,
    this function:
    1. Detects non-numeric values like "1-2"
    2. Extracts the first number if possible (e.g., "1" from "1-2")
    3. Falls back to nullFlavor="NI" if no number can be extracted

    Returns:
        List of warning messages about fixes applied
    """
    warnings: list[str] = []

    for element_name in NUMERIC_VALUE_ELEMENTS:
        # Search with and without namespace
        for elem in root.iter():
            local_name = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

            if local_name == element_name:
                value = elem.get("value")

                if value is not None and not NUMERIC_PATTERN.match(value):
                    original_value = value

                    # Try to extract first number from range (e.g., "1-2" -> "1")
                    match = FIRST_NUMBER_PATTERN.match(value)

                    if match:
                        new_value = match.group(1)
                        elem.set("value", new_value)
                        warnings.append(
                            f"Sanitized {element_name}/@value: "
                            f"'{original_value}' -> '{new_value}'"
                        )
                    else:
                        # Can't extract a number, use nullFlavor instead
                        del elem.attrib["value"]
                        elem.set("nullFlavor", "NI")
                        warnings.append(
                            f"Sanitized {element_name}/@value: "
                            f"'{original_value}' -> nullFlavor='NI'"
                        )

    return warnings
