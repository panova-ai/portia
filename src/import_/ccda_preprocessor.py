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
from dataclasses import dataclass
from xml.etree import ElementTree as ET

# C-CDA namespace
CDA_NS = "urn:hl7-org:v3"
NAMESPACES = {"cda": CDA_NS}


@dataclass
class DoseRangeInfo:
    """Information about a dose range that was sanitized."""

    low: float
    high: float
    unit: str | None = None
    medication_code: str | None = None  # RxNorm or other code to match medication


# Elements with 'value' attributes that must be numeric
NUMERIC_VALUE_ELEMENTS = [
    "doseQuantity",
    "rateQuantity",
    "maxDoseQuantity",
    "quantity",
]

# Pattern to detect non-numeric values (allows decimals and negative numbers)
NUMERIC_PATTERN = re.compile(r"^-?\d+(\.\d+)?$")

# Pattern to detect a range like "1-2" or "0.5-1"
RANGE_PATTERN = re.compile(r"^(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)$")

# Pattern to extract the first number from other non-numeric values
FIRST_NUMBER_PATTERN = re.compile(r"^(-?\d+(?:\.\d+)?)")


def sanitize_ccda(content: str) -> tuple[str, list[str], list[DoseRangeInfo]]:
    """
    Sanitize a C-CDA document to fix values that cause MS Converter failures.

    Args:
        content: The raw C-CDA XML content

    Returns:
        Tuple of (sanitized XML string, warnings, dose_ranges)
        - sanitized: The sanitized XML content
        - warnings: List of warning messages about changes made
        - dose_ranges: List of DoseRangeInfo for ranges that were sanitized
                       (to be used for post-processing FHIR output)
    """
    warnings: list[str] = []
    dose_ranges: list[DoseRangeInfo] = []

    try:
        # Register namespace to preserve it in output
        ET.register_namespace("", CDA_NS)

        root = ET.fromstring(content)

        # Fix numeric value attributes
        fixes, ranges = _fix_numeric_value_attributes(root)
        warnings.extend(fixes)
        dose_ranges.extend(ranges)

        # Convert back to string
        sanitized = ET.tostring(root, encoding="unicode")

        # Restore XML declaration if present in original
        if content.strip().startswith("<?xml"):
            # Extract original declaration
            decl_match = re.match(r"(<\?xml[^?]*\?>)", content)
            if decl_match:
                sanitized = decl_match.group(1) + "\n" + sanitized

        return sanitized, warnings, dose_ranges

    except ET.ParseError as e:
        # If XML parsing fails, return original content with warning
        return content, [f"XML parsing failed, skipping sanitization: {e}"], []


def _fix_numeric_value_attributes(
    root: ET.Element,
) -> tuple[list[str], list[DoseRangeInfo]]:
    """
    Find and fix elements with non-numeric 'value' attributes.

    For elements like doseQuantity that expect numeric values,
    this function:
    1. Detects non-numeric values like "1-2"
    2. Uses average for MS Converter compatibility
    3. Records original range with medication code for FHIR post-processing
    4. Falls back to nullFlavor="NI" if no number can be extracted

    Returns:
        Tuple of (warnings, dose_ranges)
    """
    warnings: list[str] = []
    dose_ranges: list[DoseRangeInfo] = []

    # Build parent map for navigation
    parent_map = {child: parent for parent in root.iter() for child in parent}

    for element_name in NUMERIC_VALUE_ELEMENTS:
        # Search with and without namespace
        for elem in root.iter():
            local_name = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

            if local_name == element_name:
                value = elem.get("value")

                if value is not None and not NUMERIC_PATTERN.match(value):
                    original_value = value

                    # Try to detect a range like "1-2" and use average
                    range_match = RANGE_PATTERN.match(value)

                    if range_match:
                        low = float(range_match.group(1))
                        high = float(range_match.group(2))
                        avg = (low + high) / 2
                        # Format nicely: use integer if whole number
                        new_value = str(int(avg)) if avg == int(avg) else str(avg)
                        elem.set("value", new_value)

                        # Get unit if available
                        unit = elem.get("unit")

                        # Extract medication code for matching in FHIR post-processing
                        medication_code = None
                        if element_name == "doseQuantity":
                            medication_code = _find_medication_code(elem, parent_map)
                            dose_ranges.append(
                                DoseRangeInfo(
                                    low=low,
                                    high=high,
                                    unit=unit,
                                    medication_code=medication_code,
                                )
                            )

                        warnings.append(
                            f"Sanitized {element_name}/@value: "
                            f"'{original_value}' -> '{new_value}' (will use doseRange)"
                        )
                    else:
                        # Try to extract first number from other non-numeric values
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

    return warnings, dose_ranges


def _find_medication_code(
    dose_elem: ET.Element, parent_map: dict[ET.Element, ET.Element]
) -> str | None:
    """
    Find the medication code (RxNorm) for a doseQuantity element.

    Navigates up to substanceAdministration and down to
    consumable/manufacturedProduct/manufacturedMaterial/code.
    """
    # Navigate up to find substanceAdministration
    current = dose_elem
    substance_admin = None

    while current in parent_map:
        current = parent_map[current]
        local_name = current.tag.split("}")[-1] if "}" in current.tag else current.tag
        if local_name == "substanceAdministration":
            substance_admin = current
            break

    if substance_admin is None:
        return None

    # Navigate down to find the medication code
    # Path: consumable/manufacturedProduct/manufacturedMaterial/code
    for consumable in substance_admin:
        if consumable.tag.endswith("consumable"):
            for mfg_product in consumable:
                if mfg_product.tag.endswith("manufacturedProduct"):
                    for mfg_material in mfg_product:
                        if mfg_material.tag.endswith("manufacturedMaterial"):
                            for code_elem in mfg_material:
                                if code_elem.tag.endswith("code"):
                                    code_value: str | None = code_elem.get("code")
                                    return code_value

    return None
