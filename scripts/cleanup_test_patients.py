#!/usr/bin/env python3
"""
Clean up test patients and their related resources from the FHIR store.

This script finds patients by name and deletes them along with all related
resources (encounters, conditions, compositions, etc.).
"""

import argparse
import subprocess

import httpx


def get_fhir_access_token() -> str:
    """Get access token for GCP Healthcare FHIR API."""
    result = subprocess.run(
        ["gcloud", "auth", "print-access-token"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def get_fhir_base_url(
    project: str, location: str, dataset: str, fhir_store: str
) -> str:
    """Build the FHIR API base URL."""
    return (
        f"https://healthcare.googleapis.com/v1/projects/{project}/"
        f"locations/{location}/datasets/{dataset}/fhirStores/{fhir_store}/fhir"
    )


def search_patients_by_name(base_url: str, token: str, family_name: str) -> list[dict]:
    """Search for patients by family name."""
    headers = {"Authorization": f"Bearer {token}"}
    params = {"family": family_name, "_count": "100"}

    response = httpx.get(
        f"{base_url}/Patient",
        headers=headers,
        params=params,
        timeout=30.0,
    )
    response.raise_for_status()

    data = response.json()
    patients = []
    for entry in data.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Patient":
            patients.append(resource)

    return patients


def get_related_resources(
    base_url: str, token: str, patient_id: str
) -> dict[str, list[str]]:
    """Find all resources related to a patient."""
    headers = {"Authorization": f"Bearer {token}"}
    related: dict[str, list[str]] = {}

    # Resource types and their patient reference field
    resource_searches = [
        ("Encounter", "subject"),
        ("Condition", "subject"),
        ("MedicationStatement", "subject"),
        ("Composition", "subject"),
        ("Observation", "subject"),
        ("AllergyIntolerance", "patient"),
        ("Immunization", "patient"),
        ("Procedure", "subject"),
        ("DiagnosticReport", "subject"),
        ("DocumentReference", "subject"),
    ]

    for resource_type, field in resource_searches:
        try:
            params = {field: f"Patient/{patient_id}", "_count": "500"}
            response = httpx.get(
                f"{base_url}/{resource_type}",
                headers=headers,
                params=params,
                timeout=30.0,
            )
            if response.status_code == 200:
                data = response.json()
                ids = []
                for entry in data.get("entry", []):
                    resource = entry.get("resource", {})
                    if rid := resource.get("id"):
                        ids.append(rid)
                if ids:
                    related[resource_type] = ids
        except Exception as e:
            print(f"  Warning: Failed to search {resource_type}: {e}")

    return related


def delete_resource(
    base_url: str, token: str, resource_type: str, resource_id: str
) -> bool:
    """Delete a single resource."""
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = httpx.delete(
            f"{base_url}/{resource_type}/{resource_id}",
            headers=headers,
            timeout=30.0,
        )
        return response.status_code in (200, 204, 410)  # 410 = already deleted
    except Exception as e:
        print(f"  Error deleting {resource_type}/{resource_id}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Clean up test patients from FHIR store"
    )
    parser.add_argument("family_name", help="Family name of patients to delete")
    parser.add_argument("--project", default="panova-dev", help="GCP project ID")
    parser.add_argument("--location", default="us-central1", help="GCP location")
    parser.add_argument("--dataset", default="panova", help="Healthcare dataset")
    parser.add_argument(
        "--fhir-store", default="panova-fhir-store", help="FHIR store name"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without deleting",
    )
    args = parser.parse_args()

    print("Getting access token...")
    token = get_fhir_access_token()

    base_url = get_fhir_base_url(
        args.project, args.location, args.dataset, args.fhir_store
    )
    print(f"FHIR store: {base_url}")

    print(f"\nSearching for patients with family name '{args.family_name}'...")
    patients = search_patients_by_name(base_url, token, args.family_name)

    if not patients:
        print("No patients found.")
        return

    print(f"Found {len(patients)} patient(s):")
    for patient in patients:
        name = patient.get("name", [{}])[0]
        family = name.get("family", "?")
        given = name.get("given", ["?"])[0] if name.get("given") else "?"
        print(f"  - {family}, {given} (id: {patient.get('id')})")

    if args.dry_run:
        print("\n[DRY RUN] Would delete the following resources:")
    else:
        print("\nDeleting resources...")

    total_deleted = 0
    for patient in patients:
        patient_id = patient.get("id")
        if not patient_id:
            continue

        print(f"\nPatient {patient_id}:")

        # Get related resources
        related = get_related_resources(base_url, token, patient_id)

        # Delete related resources first (in order)
        delete_order = [
            "Composition",
            "DiagnosticReport",
            "DocumentReference",
            "Observation",
            "Procedure",
            "MedicationStatement",
            "Immunization",
            "AllergyIntolerance",
            "Condition",
            "Encounter",
        ]

        for resource_type in delete_order:
            if resource_type in related:
                ids = related[resource_type]
                print(f"  {resource_type}: {len(ids)} resource(s)")
                if not args.dry_run:
                    for rid in ids:
                        if delete_resource(base_url, token, resource_type, rid):
                            total_deleted += 1
                        else:
                            print(f"    Failed to delete {resource_type}/{rid}")

        # Delete the patient
        print("  Patient: 1 resource")
        if not args.dry_run:
            if delete_resource(base_url, token, "Patient", patient_id):
                total_deleted += 1
            else:
                print(f"    Failed to delete Patient/{patient_id}")

    if args.dry_run:
        print("\n[DRY RUN] No resources were deleted.")
    else:
        print(f"\nDeleted {total_deleted} resource(s) total.")


if __name__ == "__main__":
    main()
