"""
Patient and Person matching for imports.

Matches imported patient data to existing FHIR Person/Patient resources
following Panova's two-resource model:
- Person: Organization-independent identity (name, DOB, demographics)
- Patient: Organization-specific care relationship

Matching strategy:
1. Search for Person by demographics (given name + family name + birthDate)
2. If Person found, search for Patient linked to Person within the target organization
3. If no Patient found, create one and link to Person
4. If no Person found, create both Person and Patient
"""

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import TYPE_CHECKING
from uuid import UUID

from fhir_client.datatypes.address import Address
from fhir_client.datatypes.contact_point import ContactPoint
from fhir_client.datatypes.human_name import HumanName
from fhir_client.resources.person import Person
from fhir_client.valuesets.address_use import AddressUse
from fhir_client.valuesets.administrative_gender import AdministrativeGender
from fhir_client.valuesets.contact_point_system import ContactPointSystem
from fhir_client.valuesets.contact_point_use import ContactPointUse
from fhir_client.valuesets.name_use import NameUse

if TYPE_CHECKING:
    from fhir_client.client import FHIRClient

    from src.import_.charm.extractor import CharmExtractionResult


class MatchStatus(str, Enum):
    """Status of the patient matching operation."""

    EXISTING_PERSON_EXISTING_PATIENT = "existing_person_existing_patient"
    EXISTING_PERSON_NEW_PATIENT = "existing_person_new_patient"
    NEW_PERSON_NEW_PATIENT = "new_person_new_patient"
    MULTIPLE_MATCHES = "multiple_matches"  # Requires manual resolution
    MATCH_FAILED = "match_failed"


@dataclass
class PatientDemographics:
    """Demographics extracted from import source for matching."""

    given_name: str
    family_name: str
    birth_date: date
    gender: str | None = None
    phone: str | None = None
    email: str | None = None
    address_line: str | None = None
    address_city: str | None = None
    address_state: str | None = None
    address_postal_code: str | None = None


@dataclass
class MatchResult:
    """Result of patient matching operation."""

    status: MatchStatus
    person_id: UUID | None = None
    patient_id: UUID | None = None
    person_created: bool = False
    patient_created: bool = False
    warnings: list[str] | None = None


class PatientMatcher:
    """
    Matches imported patient data to existing FHIR resources.

    Implements Panova's Person/Patient model where:
    - Person captures organization-independent identity
    - Patient captures the care relationship with a specific organization
    """

    def __init__(self, fhir_client: "FHIRClient"):
        self.fhir_client = fhir_client

    async def match_or_create(
        self,
        demographics: PatientDemographics,
        organization_id: UUID,
    ) -> MatchResult:
        """
        Match imported patient to existing resources or create new ones.

        Args:
            demographics: Patient demographics from import source
            organization_id: Target organization for the Patient resource

        Returns:
            MatchResult with status and resource IDs
        """
        warnings: list[str] = []

        # Step 1: Search for existing Person by demographics
        persons = await self._search_person_by_demographics(demographics)

        if len(persons) > 1:
            return MatchResult(
                status=MatchStatus.MULTIPLE_MATCHES,
                warnings=[
                    f"Found {len(persons)} Person resources matching demographics. "
                    "Manual resolution required."
                ],
            )

        if len(persons) == 1:
            person = persons[0]
            assert person.id is not None

            # Use the fhir_client's find_or_create method
            patient = await self.fhir_client.patients.find_or_create_for_person_and_organization(
                person, organization_id
            )
            assert patient.id is not None

            # Check if the patient was just created by comparing IDs in person.link
            patient_existed = any(
                link.target and link.target.reference == f"Patient/{patient.id}"
                for link in (person.link or [])
            )

            if patient_existed:
                return MatchResult(
                    status=MatchStatus.EXISTING_PERSON_EXISTING_PATIENT,
                    person_id=person.id,
                    patient_id=patient.id,
                    person_created=False,
                    patient_created=False,
                )
            else:
                warnings.append(
                    "Created new Patient for existing Person in organization"
                )
                return MatchResult(
                    status=MatchStatus.EXISTING_PERSON_NEW_PATIENT,
                    person_id=person.id,
                    patient_id=patient.id,
                    person_created=False,
                    patient_created=True,
                    warnings=warnings,
                )

        # No Person found - create both
        new_person = await self._create_person(demographics)
        assert new_person.id is not None

        new_patient = (
            await self.fhir_client.patients.find_or_create_for_person_and_organization(
                new_person, organization_id
            )
        )
        assert new_patient.id is not None

        warnings.append("Created new Person and Patient resources")

        return MatchResult(
            status=MatchStatus.NEW_PERSON_NEW_PATIENT,
            person_id=new_person.id,
            patient_id=new_patient.id,
            person_created=True,
            patient_created=True,
            warnings=warnings,
        )

    async def _search_person_by_demographics(
        self, demographics: PatientDemographics
    ) -> list[Person]:
        """Search for Person resources matching the demographics."""
        search_params = {
            "given": demographics.given_name,
            "family": demographics.family_name,
            "birthdate": demographics.birth_date.isoformat(),
        }

        # Use the base client's search directly via a custom search
        response = await self.fhir_client.persons.client.get(
            f"{self.fhir_client.persons.base_url}/Person",
            headers=self.fhir_client.persons._get_auth_headers(),  # type: ignore[no-untyped-call]
            params=search_params,
        )
        response.raise_for_status()
        data = response.json()

        persons = []
        for entry in data.get("entry", []):
            resource = entry.get("resource", {})
            if resource.get("resourceType") == "Person":
                persons.append(Person(**resource))

        return persons

    async def _create_person(self, demographics: PatientDemographics) -> Person:
        """Create a new Person resource from demographics."""
        name = HumanName(
            use=NameUse.OFFICIAL,
            given=[demographics.given_name],
            family=demographics.family_name,
        )

        person = Person(
            name=[name],
            birthDate=demographics.birth_date.isoformat(),
            active=True,
        )

        if demographics.gender:
            gender_map = {
                "male": AdministrativeGender.MALE,
                "female": AdministrativeGender.FEMALE,
                "other": AdministrativeGender.OTHER,
                "unknown": AdministrativeGender.UNKNOWN,
            }
            person.gender = gender_map.get(
                demographics.gender.lower(), AdministrativeGender.UNKNOWN
            )

        telecom = []
        if demographics.phone:
            telecom.append(
                ContactPoint(
                    system=ContactPointSystem.PHONE,
                    value=demographics.phone,
                    use=ContactPointUse.MOBILE,
                )
            )
        if demographics.email:
            telecom.append(
                ContactPoint(
                    system=ContactPointSystem.EMAIL,
                    value=demographics.email,
                    use=ContactPointUse.HOME,
                )
            )
        if telecom:
            person.telecom = telecom

        if demographics.address_line:
            person.address = [
                Address(
                    use=AddressUse.HOME,
                    line=[demographics.address_line],
                    city=demographics.address_city,
                    state=demographics.address_state,
                    postalCode=demographics.address_postal_code,
                )
            ]

        return await self.fhir_client.persons.create(person)


def demographics_from_extraction(
    extraction_result: "CharmExtractionResult",
) -> PatientDemographics | None:
    """
    Extract patient demographics from CHARM extraction result.

    Args:
        extraction_result: CharmExtractionResult from the extractor

    Returns:
        PatientDemographics or None if required fields missing
    """
    from src.import_.charm.extractor import CharmExtractionResult

    if not isinstance(extraction_result, CharmExtractionResult):
        return None

    demo = extraction_result.patient_demographics
    if not demo:
        return None

    # Require at minimum name and birth date
    if not demo.given_name or not demo.family_name or not demo.birth_date:
        return None

    return PatientDemographics(
        given_name=demo.given_name,
        family_name=demo.family_name,
        birth_date=demo.birth_date,
        gender=demo.gender,
        phone=demo.phone,
        email=demo.email,
        address_line=demo.address_line,
        address_city=demo.address_city,
        address_state=demo.address_state,
        address_postal_code=demo.address_postal_code,
    )
