"""
Organization transformer for R4 to R5 conversion.

Key changes in R5:
- 'telecom' and 'address' moved into 'contact' array
- 'contact.name' changed from HumanName to single string
"""

from typing import Any


def transform_organization(r4_organization: dict[str, Any]) -> dict[str, Any]:
    """
    Transform a FHIR R4 Organization to R5 format.

    Args:
        r4_organization: The R4 Organization resource

    Returns:
        R5-compatible Organization resource
    """
    r5_organization = r4_organization.copy()

    # In R5, telecom and address moved into contact array
    telecom = r5_organization.pop("telecom", None)
    address = r5_organization.pop("address", None)

    if telecom or address:
        # Create or update contact entry
        contacts = r5_organization.get("contact", [])

        # Create a new contact entry for organization-level telecom/address
        org_contact: dict[str, Any] = {}

        if telecom:
            telecom_list = telecom if isinstance(telecom, list) else [telecom]
            # Fix telecom use - organizations can't have "home" use
            for t in telecom_list:
                if t.get("use") == "home":
                    t["use"] = "work"
            org_contact["telecom"] = telecom_list

        if address:
            org_contact["address"] = (
                address[0] if isinstance(address, list) else address
            )

        if org_contact:
            contacts.insert(0, org_contact)
            r5_organization["contact"] = contacts

    return r5_organization
