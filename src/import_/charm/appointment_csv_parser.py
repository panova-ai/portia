"""
CSV parser for Charm appointment exports.

Parses appointment CSV files and extracts appointment data for import.
"""

import csv
import io
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo


@dataclass
class ParsedCharmAppointment:
    """Parsed and validated appointment data from Charm CSV."""

    # Patient demographics (for PatientMatcher)
    given_name: str
    family_name: str
    birth_date: Optional[date]
    gender: Optional[str]
    phone: Optional[str]
    email: Optional[str]
    address_line: Optional[str]
    address_city: Optional[str]
    address_state: Optional[str]
    address_postal_code: Optional[str]

    # Appointment details
    start: datetime  # Timezone-aware
    end: datetime  # Calculated from start + duration
    duration_minutes: int
    visit_type: str
    is_virtual: bool
    reason: Optional[str]

    # Identifiers
    charm_appointment_id: str
    charm_record_id: str


def parse_appointment_csv(csv_content: str) -> list[ParsedCharmAppointment]:
    """
    Parse Charm appointment CSV content.

    Args:
        csv_content: Raw CSV content as string

    Returns:
        List of parsed appointments

    Raises:
        ValueError: If CSV is malformed or missing required fields
    """
    appointments: list[ParsedCharmAppointment] = []

    reader = csv.DictReader(io.StringIO(csv_content))

    for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
        try:
            appointment = _parse_row(row)
            appointments.append(appointment)
        except ValueError as e:
            raise ValueError(f"Row {row_num}: {e}") from e

    return appointments


def _parse_row(row: dict[str, str]) -> ParsedCharmAppointment:
    """Parse a single CSV row into a ParsedCharmAppointment."""
    # Required fields
    patient_name = row.get("Patient Name", "").strip()
    if not patient_name:
        raise ValueError("Patient Name is required")

    charm_appointment_id = row.get("Appointment ID", "").strip()
    if not charm_appointment_id:
        raise ValueError("Appointment ID is required")

    date_time_str = row.get("Date/Time", "").strip()
    if not date_time_str:
        raise ValueError("Date/Time is required")

    timezone_str = row.get("Timezone", "").strip() or "US/Pacific"

    # Parse patient name
    given_name, family_name = _parse_patient_name(patient_name)

    # Parse date of birth
    dob_str = row.get("DOB", "").strip()
    birth_date = _parse_dob(dob_str) if dob_str else None

    # Parse appointment datetime
    start = _parse_datetime(date_time_str, timezone_str)

    # Parse duration
    duration_str = row.get("Duration(mins)", "").strip()
    duration_minutes = int(duration_str) if duration_str else 30
    end = start + timedelta(minutes=duration_minutes)

    # Parse gender
    gender_str = row.get("Gender", "").strip().lower()
    gender = _normalize_gender(gender_str) if gender_str else None

    # Parse phone - normalize to E.164 format
    phone_str = row.get("Mobile Phone", "").strip()
    phone = _normalize_phone(phone_str) if phone_str else None

    # Parse appointment mode
    appointment_mode = row.get("Appointment Mode", "").strip().lower()
    is_virtual = appointment_mode in ("video consult", "phone call", "virtual")

    # Build address
    address_line = row.get("Address", "").strip() or None
    address_city = row.get("City", "").strip() or None
    address_state = row.get("State", "").strip() or None
    address_postal_code = row.get("Zip Code", "").strip() or None

    return ParsedCharmAppointment(
        given_name=given_name,
        family_name=family_name,
        birth_date=birth_date,
        gender=gender,
        phone=phone,
        email=row.get("Email", "").strip() or None,
        address_line=address_line,
        address_city=address_city,
        address_state=address_state,
        address_postal_code=address_postal_code,
        start=start,
        end=end,
        duration_minutes=duration_minutes,
        visit_type=row.get("Visit Type", "").strip() or "Follow-up Visit",
        is_virtual=is_virtual,
        reason=row.get("Reason", "").strip() or None,
        charm_appointment_id=charm_appointment_id,
        charm_record_id=row.get("Record ID", "").strip() or "",
    )


def _parse_patient_name(full_name: str) -> tuple[str, str]:
    """
    Parse patient name into given and family names.

    Examples:
        "Test 1" -> ("Test", "1")
        "John Smith" -> ("John", "Smith")
        "Mary Jane Watson" -> ("Mary Jane", "Watson")
    """
    parts = full_name.strip().split()
    if len(parts) == 1:
        return (parts[0], "")
    elif len(parts) == 2:
        return (parts[0], parts[1])
    else:
        # Assume last part is family name, rest is given name
        return (" ".join(parts[:-1]), parts[-1])


def _parse_dob(dob_str: str) -> Optional[date]:
    """
    Parse date of birth from Charm format.

    Formats supported:
        "26-Sep-44" -> 1944-09-26
        "25-Nov-79" -> 1979-11-25
        "18-Jan-96" -> 1996-01-18
        "30-Mar-01" -> 2001-03-30

    Uses 2-digit year logic: years 00-30 are 2000s, 31-99 are 1900s.
    """
    try:
        # Parse the date string
        parsed = datetime.strptime(dob_str, "%d-%b-%y")

        # Adjust century for 2-digit years
        # strptime uses 1969-2068 range, but we want to adjust for DOBs
        # If parsed year is in future or very recent (likely meant 1900s), adjust
        current_year = datetime.now().year
        if parsed.year > current_year - 10:
            # Person would be less than 10 years old - likely meant 1900s
            parsed = parsed.replace(year=parsed.year - 100)

        return parsed.date()
    except ValueError:
        # Try alternative formats
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(dob_str, fmt).date()
            except ValueError:
                continue
        return None


def _parse_datetime(date_time_str: str, timezone_str: str) -> datetime:
    """
    Parse appointment datetime from Charm format.

    Format: "1/22/26 12:00" with timezone "US/Pacific"

    Returns timezone-aware datetime.
    """
    # Normalize timezone string
    tz = _parse_timezone(timezone_str)

    # Try various formats
    formats = [
        "%m/%d/%y %H:%M",  # "1/22/26 12:00"
        "%m/%d/%Y %H:%M",  # "1/22/2026 12:00"
        "%Y-%m-%d %H:%M",  # "2026-01-22 12:00"
        "%m/%d/%y %I:%M %p",  # "1/22/26 12:00 PM"
    ]

    for fmt in formats:
        try:
            naive_dt = datetime.strptime(date_time_str, fmt)

            # Adjust 2-digit year if needed (strptime uses 1969-2068)
            # For appointments, we want future dates to stay in 2000s
            if naive_dt.year < 100:
                naive_dt = naive_dt.replace(year=naive_dt.year + 2000)

            return naive_dt.replace(tzinfo=tz)
        except ValueError:
            continue

    raise ValueError(f"Could not parse datetime: {date_time_str}")


def _parse_timezone(timezone_str: str) -> ZoneInfo:
    """Parse timezone string to ZoneInfo."""
    # Map common timezone names
    tz_map = {
        "US/Pacific": "America/Los_Angeles",
        "US/Eastern": "America/New_York",
        "US/Central": "America/Chicago",
        "US/Mountain": "America/Denver",
        "PST": "America/Los_Angeles",
        "EST": "America/New_York",
        "CST": "America/Chicago",
        "MST": "America/Denver",
    }

    tz_name = tz_map.get(timezone_str, timezone_str)

    try:
        return ZoneInfo(tz_name)
    except KeyError:
        # Default to Pacific if timezone is invalid
        return ZoneInfo("America/Los_Angeles")


def _normalize_gender(gender_str: str) -> Optional[str]:
    """Normalize gender string to FHIR values."""
    gender_map = {
        "male": "male",
        "m": "male",
        "female": "female",
        "f": "female",
        "other": "other",
        "unknown": "unknown",
    }
    return gender_map.get(gender_str.lower())


def _normalize_phone(phone_str: str) -> Optional[str]:
    """
    Normalize phone number to E.164 format.

    Examples:
        "561-132-5132" -> "+15611325132"
        "(561) 132-5132" -> "+15611325132"
        "5611325132" -> "+15611325132"
    """
    # Remove all non-digit characters
    digits = re.sub(r"\D", "", phone_str)

    if not digits:
        return None

    # Add country code if missing (assume US)
    if len(digits) == 10:
        digits = "1" + digits
    elif len(digits) == 11 and digits[0] == "1":
        pass  # Already has US country code
    else:
        # Unknown format, return as-is with +
        return f"+{digits}"

    return f"+{digits}"
