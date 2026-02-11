#!/usr/bin/env python3
"""
Import CHARM EHR appointment CSV exports into Portia.

This script is used by Panova staff to import appointment records exported from
CHARM EHR. It resolves practitioner context from Sentia and uses service JWT
authentication.

The import process:
1. Parses CSV and matches/creates patients
2. Creates FHIR Encounters with pending-import confirmation status
3. Creates Google Calendar events for provider review

Patients are NOT activated (no Firebase identity, no SMS).
Use activate_imported_patients.py after provider review.

Usage:
    python scripts/import_charm_appointments.py <csv_file> <practitioner_email> --env dev
    python scripts/import_charm_appointments.py appointments.csv john.doe@panova.health --env staging

Requirements:
    - gcloud CLI authenticated with appropriate permissions
    - Access to Secret Manager for JWT secret
"""

import argparse
import base64
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import google.auth
import google.auth.transport.requests
import httpx
import jwt
from google.cloud import secretmanager

# Environment configuration
ENVIRONMENTS = {
    "dev": {
        "portia_url": "https://portia-mzpxirbrgq-uc.a.run.app",
        "sentia_url": "https://backend-mzpxirbrgq-uc.a.run.app",
        "project_id": "panova-dev",
        "fhir_store": "projects/panova-dev/locations/us-central1/datasets/healthcare_dataset/fhirStores/fhir_store",
    },
    "staging": {
        "portia_url": "https://portia-yqadsx4ooa-uc.a.run.app",
        "sentia_url": "https://backend-yqadsx4ooa-uc.a.run.app",
        "project_id": "panova-staging",
        "fhir_store": "projects/panova-staging/locations/us-central1/datasets/healthcare_dataset/fhirStores/fhir_store",
    },
    "prod": {
        "portia_url": "https://portia-prod.panova.health",
        "sentia_url": "https://backend-prod.panova.health",
        "project_id": "panova-prod",
        "fhir_store": "projects/panova-prod/locations/us-central1/datasets/healthcare_dataset/fhirStores/fhir_store",
    },
}

# Service auth configuration
SERVICE_AUTH_ISSUER = "panova-services"
SERVICE_AUTH_AUDIENCE_PORTIA = "panova-portia"
SERVICE_AUTH_AUDIENCE_SENTIA = "panova-backend"


def get_jwt_secret(project_id: str) -> str:
    """Fetch JWT secret from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/service-auth-jwt-secret/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


def create_service_token(
    secret: str, audience: str, service_name: str = "portia-cli"
) -> str:
    """Create a service JWT token."""
    now = datetime.now(timezone.utc)
    expires = now + timedelta(hours=1)

    payload = {
        "service_name": service_name,
        "iss": SERVICE_AUTH_ISSUER,
        "sub": f"service:{service_name}",
        "aud": audience,
        "iat": int(now.timestamp()),
        "exp": int(expires.timestamp()),
        "permissions": ["import.write", "fhir.write"],
    }

    return jwt.encode(payload, secret, algorithm="HS256")


def lookup_practitioner_by_email(
    sentia_url: str,
    token: str,
    email: str,
) -> dict[str, str | None]:
    """
    Look up practitioner by email via Sentia API.

    Returns dict with practitioner_id and organization_id.
    """
    headers = {"Authorization": f"Bearer {token}"}

    response = httpx.get(
        f"{sentia_url}/practitioners",
        headers=headers,
        params={"email": email},
        timeout=30.0,
    )

    if response.status_code == 404:
        raise ValueError(f"Practitioner with email {email} not found")

    response.raise_for_status()
    data = response.json()

    practitioners = data.get("entries", [data] if "id" in data else [])

    if not practitioners:
        raise ValueError(f"Practitioner with email {email} not found")

    practitioner = practitioners[0]
    practitioner_id = practitioner.get("id") or practitioner.get("fhir_practitioner_id")

    if not practitioner_id:
        raise ValueError(f"Could not get practitioner ID for {email}")

    # Get practitioner's organizations
    org_response = httpx.get(
        f"{sentia_url}/practitioners/{practitioner_id}/organizations",
        headers=headers,
        timeout=30.0,
    )

    if org_response.status_code == 200:
        org_data = org_response.json()
        organizations = org_data.get("entries", [])
        if organizations:
            organization_id = organizations[0].get("id")
        else:
            organization_id = None
    else:
        organization_id = None

    return {
        "practitioner_id": practitioner_id,
        "organization_id": organization_id,
        "practitioner_name": practitioner.get("name"),
    }


def get_fhir_access_token() -> str:
    """Get Google Cloud access token for FHIR API."""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-healthcare"]
    )
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    return credentials.token  # type: ignore[return-value]


def lookup_practitioner_role(
    fhir_store: str,
    practitioner_id: str,
    organization_id: str | None = None,
) -> dict[str, str | None]:
    """
    Look up PractitionerRole for a practitioner via FHIR.

    Args:
        fhir_store: Full FHIR store path
        practitioner_id: Practitioner resource ID (UUID)
        organization_id: Optional organization to filter by

    Returns:
        Dict with practitioner_role_id and organization_id
    """
    access_token = get_fhir_access_token()
    fhir_url = f"https://healthcare.googleapis.com/v1/{fhir_store}/fhir"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/fhir+json",
    }

    params: dict[str, str] = {"practitioner": f"Practitioner/{practitioner_id}"}
    if organization_id:
        params["organization"] = f"Organization/{organization_id}"

    response = httpx.get(
        f"{fhir_url}/PractitionerRole",
        headers=headers,
        params=params,
        timeout=30.0,
    )

    if response.status_code != 200:
        return {"practitioner_role_id": None, "organization_id": None}

    bundle = response.json()
    entries = bundle.get("entry", [])

    if not entries:
        return {"practitioner_role_id": None, "organization_id": None}

    role = entries[0].get("resource", {})
    role_id = role.get("id")
    org_ref = role.get("organization", {}).get("reference", "")

    org_id = None
    if org_ref.startswith("Organization/"):
        org_id = org_ref.replace("Organization/", "")

    return {"practitioner_role_id": role_id, "organization_id": org_id}


def import_appointments(
    portia_url: str,
    token: str,
    file_content: bytes,
    organization_id: str,
    practitioner_role_id: str,
) -> dict[str, Any]:
    """Import appointment CSV content to Portia."""
    encoded_data = base64.b64encode(file_content).decode("utf-8")

    payload: dict[str, Any] = {
        "data": encoded_data,
        "organization_id": organization_id,
        "practitioner_role_id": practitioner_role_id,
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    response = httpx.post(
        f"{portia_url}/import/appointments",
        headers=headers,
        json=payload,
        timeout=120.0,
    )

    response.raise_for_status()
    result: dict[str, Any] = response.json()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import CHARM EHR appointment CSV exports into Portia",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Import a CHARM appointment CSV for a practitioner in dev environment
    python scripts/import_charm_appointments.py appointments.csv john.doe@panova.health --env dev

    # Import to staging environment
    python scripts/import_charm_appointments.py appointments.csv jane.smith@panova.health --env staging

    # Import with explicit IDs (skips practitioner lookup)
    python scripts/import_charm_appointments.py appointments.csv --env dev \\
        --org-id 12345678-1234-1234-1234-123456789abc \\
        --practitioner-role-id 87654321-4321-4321-4321-cba987654321

    # Dry run to validate inputs
    python scripts/import_charm_appointments.py appointments.csv john.doe@panova.health --env dev --dry-run
        """,
    )

    parser.add_argument(
        "file_path",
        type=Path,
        help="Path to the Charm appointment CSV file to import",
    )

    parser.add_argument(
        "practitioner_email",
        nargs="?",
        help="Practitioner email (firstname.lastname@panova.health)",
    )

    parser.add_argument(
        "--env",
        choices=["dev", "staging", "prod"],
        default="dev",
        help="Target environment (default: dev)",
    )

    parser.add_argument(
        "--org-id",
        dest="organization_id",
        help="Organization ID (required if practitioner_email not provided)",
    )

    parser.add_argument(
        "--practitioner-role-id",
        dest="practitioner_role_id",
        help="PractitionerRole ID (required if practitioner_email not provided)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs without actually importing",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    # Validate inputs
    if not args.file_path.exists():
        print(f"Error: File not found: {args.file_path}", file=sys.stderr)
        sys.exit(1)

    if not args.file_path.suffix.lower() == ".csv":
        print(f"Error: Expected CSV file, got: {args.file_path}", file=sys.stderr)
        sys.exit(1)

    if not args.practitioner_email and not (
        args.organization_id and args.practitioner_role_id
    ):
        print(
            "Error: Provide practitioner_email or both --org-id and --practitioner-role-id",
            file=sys.stderr,
        )
        sys.exit(1)

    env_config = ENVIRONMENTS[args.env]

    if args.verbose:
        print(f"Environment: {args.env}")
        print(f"Portia URL: {env_config['portia_url']}")
        print(f"Sentia URL: {env_config['sentia_url']}")
        print(f"File: {args.file_path}")

    # Read file content
    file_content = args.file_path.read_bytes()
    print(f"Read {len(file_content):,} bytes from {args.file_path}")

    # Count rows (excluding header)
    lines = file_content.decode("utf-8").strip().split("\n")
    row_count = len(lines) - 1  # Subtract header
    print(f"Found {row_count} appointment(s) in CSV")

    # Get JWT secret
    print(f"Fetching JWT secret from {env_config['project_id']}...")
    try:
        jwt_secret = get_jwt_secret(env_config["project_id"])
    except Exception as e:
        print(f"Error fetching JWT secret: {e}", file=sys.stderr)
        print(
            "Make sure you're authenticated with gcloud and have Secret Manager access.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Resolve practitioner context
    organization_id = args.organization_id
    practitioner_id = None
    practitioner_role_id = args.practitioner_role_id

    if args.practitioner_email and not (organization_id and practitioner_role_id):
        print(f"Looking up practitioner: {args.practitioner_email}...")

        sentia_token = create_service_token(
            jwt_secret,
            SERVICE_AUTH_AUDIENCE_SENTIA,
        )

        try:
            practitioner_info = lookup_practitioner_by_email(
                env_config["sentia_url"],
                sentia_token,
                args.practitioner_email,
            )

            if not organization_id:
                organization_id = practitioner_info.get("organization_id")
            practitioner_id = practitioner_info.get("practitioner_id")

            if args.verbose:
                print(f"  Practitioner ID: {practitioner_id}")
                print(f"  Organization ID: {organization_id}")
                if practitioner_info.get("practitioner_name"):
                    print(f"  Name: {practitioner_info['practitioner_name']}")

        except Exception as e:
            print(f"Error looking up practitioner: {e}", file=sys.stderr)
            if not organization_id:
                print("Provide --org-id to continue.", file=sys.stderr)
                sys.exit(1)

    # Look up PractitionerRole from FHIR (unless provided directly)
    if practitioner_role_id:
        print(f"Using provided PractitionerRole: {practitioner_role_id}")
    elif practitioner_id:
        print(f"Looking up PractitionerRole for practitioner {practitioner_id}...")
        try:
            role_info = lookup_practitioner_role(
                env_config["fhir_store"],
                practitioner_id,
                organization_id,
            )
            practitioner_role_id = role_info.get("practitioner_role_id")
            if practitioner_role_id:
                print(f"  Found PractitionerRole: {practitioner_role_id}")
            else:
                print("  No PractitionerRole found", file=sys.stderr)

            if not organization_id and role_info.get("organization_id"):
                organization_id = role_info.get("organization_id")
                print(f"  Found organization: {organization_id}")
        except Exception as e:
            print(f"Error looking up PractitionerRole: {e}", file=sys.stderr)

    if not organization_id:
        print("Error: Could not determine organization ID", file=sys.stderr)
        print("Provide --org-id explicitly.", file=sys.stderr)
        sys.exit(1)

    if not practitioner_role_id:
        print("Error: Could not determine PractitionerRole ID", file=sys.stderr)
        print("Provide --practitioner-role-id explicitly.", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        print("\n[DRY RUN] Would import with:")
        print(f"  Organization ID: {organization_id}")
        print(f"  PractitionerRole ID: {practitioner_role_id}")
        print(f"  Appointments: {row_count}")
        print(f"  File size: {len(file_content):,} bytes")
        sys.exit(0)

    # Create token for Portia
    portia_token = create_service_token(
        jwt_secret,
        SERVICE_AUTH_AUDIENCE_PORTIA,
    )

    # Import the file
    print(f"Importing to {args.env}...")
    try:
        result = import_appointments(
            env_config["portia_url"],
            portia_token,
            file_content,
            organization_id,
            practitioner_role_id,
        )

        print("\nImport complete!")
        print(f"  Total rows: {result.get('total_rows', 0)}")
        print(f"  Successful: {result.get('successful', 0)}")
        print(f"  Failed: {result.get('failed', 0)}")
        print(f"  Skipped: {result.get('skipped', 0)}")

        if result.get("warnings"):
            print(f"  Warnings: {len(result['warnings'])}")
            if args.verbose:
                for warning in result["warnings"]:
                    print(f"    - {warning}")

        # Print per-appointment results
        if args.verbose and result.get("results"):
            print("\n  Per-appointment results:")
            for r in result["results"]:
                status = "OK" if r.get("success") else "FAILED"
                charm_id = r.get("charm_appointment_id", "?")
                print(f"    [{status}] {charm_id}")
                if r.get("encounter_id"):
                    print(f"        Encounter: {r['encounter_id']}")
                if r.get("gcal_event_id"):
                    print(f"        GCal: {r['gcal_event_id']}")
                if r.get("error"):
                    print(f"        Error: {r['error']}")
                if r.get("warnings"):
                    for w in r["warnings"]:
                        print(f"        Warning: {w}")

    except httpx.HTTPStatusError as e:
        print(
            f"Error: Import failed with status {e.response.status_code}",
            file=sys.stderr,
        )
        try:
            error_detail = e.response.json()
            print(
                f"  Detail: {error_detail.get('detail', e.response.text)}",
                file=sys.stderr,
            )
        except Exception:
            print(f"  Response: {e.response.text}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
