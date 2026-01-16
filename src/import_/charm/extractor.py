"""
CHARM C-CDA extractor.

Extracts encounter dates, clinical notes, and linkage data from CHARM C-CDA exports.
CHARM exports have a pattern where:
- Problems are documented per-encounter with date ranges
- Clinical notes (HPI, PMH) are stored in the Notes section with dates
- Each therapy session should become an Encounter with linked resources
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from xml.etree.ElementTree import Element

import defusedxml.ElementTree as ET

# C-CDA namespaces
CCDA_NS = "urn:hl7-org:v3"
NAMESPACES = {"cda": CCDA_NS, "sdtc": "urn:hl7-org:sdtc"}


@dataclass
class ClinicalNote:
    """A clinical note extracted from the C-CDA."""

    date: date
    note_type: str  # e.g., "History of Present Illness", "Past Medical History"
    content: str
    note_id: str | None = None


@dataclass
class ProblemEntry:
    """A problem/condition entry with date range."""

    code: str  # SNOMED code
    display: str
    start_date: date
    end_date: date | None
    ccda_id: str  # Original ID from C-CDA for linking


@dataclass
class MedicationEntry:
    """A medication entry with date information."""

    code: str  # RxNorm code
    display: str
    start_date: date | None
    end_date: date | None
    dosage: str | None
    route: str | None
    ccda_id: str  # Original ID from C-CDA


@dataclass
class EncounterData:
    """Data for a synthesized encounter from CHARM export."""

    date: date
    notes: list[ClinicalNote] = field(default_factory=list)
    problem_ids: list[str] = field(
        default_factory=list
    )  # C-CDA IDs of related problems
    medication_ids: list[str] = field(default_factory=list)  # C-CDA IDs of related meds


@dataclass
class PatientDemographicsData:
    """Patient demographics extracted from C-CDA."""

    given_name: str | None = None
    family_name: str | None = None
    birth_date: date | None = None
    gender: str | None = None
    phone: str | None = None
    email: str | None = None
    address_line: str | None = None
    address_city: str | None = None
    address_state: str | None = None
    address_postal_code: str | None = None


@dataclass
class CharmExtractionResult:
    """Complete extraction result from CHARM C-CDA."""

    # Patient info
    patient_id: str | None
    patient_name: str | None
    patient_demographics: PatientDemographicsData | None

    # Practitioner info
    practitioner_name: str | None
    organization_name: str | None

    # Synthesized encounters (one per therapy session)
    encounters: list[EncounterData]

    # All problems with their date ranges
    problems: list[ProblemEntry]

    # All medications
    medications: list[MedicationEntry]

    # All clinical notes
    notes: list[ClinicalNote]


class CharmCcdaExtractor:
    """Extracts structured data from CHARM C-CDA exports."""

    def __init__(self, xml_content: str):
        """Initialize with C-CDA XML content."""
        self.root = ET.fromstring(xml_content)
        self._ns = NAMESPACES

    def extract(self) -> CharmExtractionResult:
        """Extract all relevant data from the C-CDA."""
        # Extract basic info
        patient_id = self._extract_patient_id()
        patient_name = self._extract_patient_name()
        patient_demographics = self._extract_patient_demographics()
        practitioner_name = self._extract_practitioner_name()
        organization_name = self._extract_organization_name()

        # Extract clinical data
        notes = self._extract_notes()
        problems = self._extract_problems()
        medications = self._extract_medications()

        # Synthesize encounters from notes (each unique date = one encounter)
        encounters = self._synthesize_encounters(notes, problems, medications)

        return CharmExtractionResult(
            patient_id=patient_id,
            patient_name=patient_name,
            patient_demographics=patient_demographics,
            practitioner_name=practitioner_name,
            organization_name=organization_name,
            encounters=encounters,
            problems=problems,
            medications=medications,
            notes=notes,
        )

    def _find(self, path: str, element: Element | None = None) -> Element | None:
        """Find element, trying both namespaced and non-namespaced paths."""
        root = element if element is not None else self.root

        # Try with namespace
        result = root.find(path, self._ns)
        if result is not None:
            return result

        # Try without namespace (replace cda: prefix)
        path_no_ns = path.replace("cda:", "").replace(".//", ".//")
        return root.find(path_no_ns)

    def _findall(self, path: str, element: Element | None = None) -> list[Element]:
        """Find all elements, trying both namespaced and non-namespaced paths."""
        root = element if element is not None else self.root

        # Try with namespace
        results = root.findall(path, self._ns)
        if results:
            return results

        # Try without namespace
        path_no_ns = path.replace("cda:", "")
        return root.findall(path_no_ns)

    def _findtext(
        self, path: str, element: Element | None = None, default: str = ""
    ) -> str:
        """Find text content, trying both namespaced and non-namespaced."""
        root = element if element is not None else self.root

        # Try with namespace
        result = root.findtext(path, namespaces=self._ns)
        if result:
            return result

        # Try without namespace
        path_no_ns = path.replace("cda:", "")
        return root.findtext(path_no_ns, default=default) or default

    def _extract_patient_id(self) -> str | None:
        """Extract patient ID from recordTarget."""
        patient_role = self._find(".//cda:recordTarget/cda:patientRole")
        if patient_role is None:
            return None

        id_elem = self._find("cda:id", patient_role)
        if id_elem is not None:
            return id_elem.get("extension")
        return None

    def _extract_patient_name(self) -> str | None:
        """Extract patient name from recordTarget."""
        patient = self._find(".//cda:recordTarget/cda:patientRole/cda:patient")
        if patient is None:
            return None

        name_elem = self._find("cda:name", patient)
        if name_elem is None:
            return None

        given = self._findtext("cda:given", name_elem)
        family = self._findtext("cda:family", name_elem)

        if given or family:
            return f"{given} {family}".strip()
        return None

    def _extract_patient_demographics(self) -> PatientDemographicsData | None:
        """Extract full patient demographics from recordTarget."""
        patient_role = self._find(".//cda:recordTarget/cda:patientRole")
        if patient_role is None:
            return None

        patient = self._find("cda:patient", patient_role)
        if patient is None:
            return None

        demographics = PatientDemographicsData()

        # Extract name
        name_elem = self._find("cda:name", patient)
        if name_elem is not None:
            demographics.given_name = self._findtext("cda:given", name_elem) or None
            demographics.family_name = self._findtext("cda:family", name_elem) or None

        # Extract birth date
        birth_time = self._find("cda:birthTime", patient)
        if birth_time is not None:
            demographics.birth_date = self._parse_date(birth_time.get("value"))

        # Extract gender
        gender_code = self._find("cda:administrativeGenderCode", patient)
        if gender_code is not None:
            code = gender_code.get("code")
            if code == "F":
                demographics.gender = "female"
            elif code == "M":
                demographics.gender = "male"
            else:
                demographics.gender = gender_code.get("displayName", "").lower() or None

        # Extract telecom (phone, email)
        for telecom in self._findall("cda:telecom", patient_role):
            value = telecom.get("value", "")
            if value.startswith("tel:"):
                demographics.phone = value[4:]
            elif value.startswith("mailto:"):
                demographics.email = value[7:]

        # Extract address
        addr = self._find("cda:addr", patient_role)
        if addr is not None:
            street = self._find("cda:streetAddressLine", addr)
            if street is not None and street.text:
                demographics.address_line = street.text
            city = self._find("cda:city", addr)
            if city is not None and city.text:
                demographics.address_city = city.text
            state = self._find("cda:state", addr)
            if state is not None and state.text:
                demographics.address_state = state.text
            postal = self._find("cda:postalCode", addr)
            if postal is not None and postal.text:
                demographics.address_postal_code = postal.text

        return demographics

    def _extract_practitioner_name(self) -> str | None:
        """Extract author/practitioner name."""
        author = self._find(".//cda:author/cda:assignedAuthor/cda:assignedPerson")
        if author is None:
            return None

        name_elem = self._find("cda:name", author)
        if name_elem is None:
            return None

        prefix = self._findtext("cda:prefix", name_elem)
        given = self._findtext("cda:given", name_elem)
        family = self._findtext("cda:family", name_elem)

        parts = [p for p in [prefix, given, family] if p]
        return " ".join(parts) if parts else None

    def _extract_organization_name(self) -> str | None:
        """Extract organization name from author."""
        org = self._find(".//cda:author/cda:assignedAuthor/cda:representedOrganization")
        if org is None:
            return None

        return self._findtext("cda:name", org)

    def _parse_date(self, value: str | None) -> date | None:
        """Parse C-CDA date format (YYYYMMDD or YYYYMMDDHHMMSS±ZZZZ)."""
        if not value:
            return None

        try:
            # Try full datetime format first
            if len(value) >= 8:
                return datetime.strptime(value[:8], "%Y%m%d").date()
        except ValueError:
            pass

        return None

    def _parse_display_date(self, value: str | None) -> date | None:
        """Parse display date format (MM/DD/YYYY)."""
        if not value:
            return None

        try:
            return datetime.strptime(value.strip(), "%m/%d/%Y").date()
        except ValueError:
            pass

        return None

    def _extract_notes(self) -> list[ClinicalNote]:
        """Extract clinical notes from the Notes section."""
        notes: list[ClinicalNote] = []

        # Find the Notes section by title or LOINC code
        for section in self._findall(".//cda:component/cda:section"):
            title = self._findtext("cda:title", section)
            if title and "notes" in title.lower():
                # Found the notes section - parse the table
                notes.extend(self._parse_notes_table(section))
                break

        return notes

    def _parse_notes_table(self, section: Element) -> list[ClinicalNote]:
        """Parse notes from the HTML table in the section text."""
        notes: list[ClinicalNote] = []

        # Find tbody rows
        text_elem = self._find("cda:text", section)
        if text_elem is None:
            return notes

        # Find all tr elements (table rows)
        for tr in (
            self._findall(".//cda:tr", text_elem)
            or text_elem.findall(".//{http://www.w3.org/1999/xhtml}tr")
            or text_elem.findall(".//tr")
        ):
            tds = (
                self._findall("cda:td", tr)
                or tr.findall("{http://www.w3.org/1999/xhtml}td")
                or tr.findall("td")
            )

            if len(tds) >= 3:
                # Format: Date | Note Type | Content
                date_text = self._get_element_text(tds[0])
                note_type = self._get_element_text(tds[1])
                content = self._get_element_text(tds[2])

                note_date = self._parse_display_date(date_text)
                if note_date and content:
                    # Get the ID attribute if present
                    note_id = tds[2].get("ID")

                    notes.append(
                        ClinicalNote(
                            date=note_date,
                            note_type=note_type or "Note",
                            content=content,
                            note_id=note_id,
                        )
                    )

        return notes

    def _get_element_text(self, elem: Element) -> str:
        """Get all text content from an element, including nested elements."""
        texts = []
        if elem.text:
            texts.append(elem.text)
        for child in elem:
            if child.text:
                texts.append(child.text)
            if child.tail:
                texts.append(child.tail)
        if elem.tail:
            texts.append(elem.tail)
        return " ".join(texts).strip()

    def _extract_problems(self) -> list[ProblemEntry]:
        """Extract problems/conditions from the Problems section."""
        problems: list[ProblemEntry] = []

        # Find the Problems section
        for section in self._findall(".//cda:component/cda:section"):
            code = self._find("cda:code", section)
            if code is not None and code.get("code") == "11450-4":  # Problem list LOINC
                # Found problems section - parse entries
                for entry in self._findall("cda:entry", section):
                    problem = self._parse_problem_entry(entry)
                    if problem:
                        problems.append(problem)
                break

        return problems

    def _parse_problem_entry(self, entry: Element) -> ProblemEntry | None:
        """Parse a single problem entry (act/observation)."""
        # Navigate to the observation within the act
        act = self._find("cda:act", entry)
        if act is None:
            return None

        # Get the act ID
        act_id_elem = self._find("cda:id", act)
        ccda_id = act_id_elem.get("root") if act_id_elem is not None else None

        # Get effective time from act
        effective_time = self._find("cda:effectiveTime", act)
        start_date = None
        end_date = None

        if effective_time is not None:
            low = self._find("cda:low", effective_time)
            high = self._find("cda:high", effective_time)

            if low is not None:
                start_date = self._parse_date(low.get("value"))
            if high is not None and high.get("nullFlavor") is None:
                end_date = self._parse_date(high.get("value"))

        # Navigate to the observation for the code
        observation = self._find("cda:entryRelationship/cda:observation", act)
        if observation is None:
            return None

        # Get the condition code from value element
        value_elem = self._find("cda:value", observation)
        if value_elem is None:
            return None

        code = value_elem.get("code")
        display = value_elem.get("displayName")

        if not code or not start_date:
            return None

        return ProblemEntry(
            code=code,
            display=display or "",
            start_date=start_date,
            end_date=end_date,
            ccda_id=ccda_id or "",
        )

    def _extract_medications(self) -> list[MedicationEntry]:
        """Extract medications from the Medications section."""
        medications: list[MedicationEntry] = []

        # Find the Medications section
        for section in self._findall(".//cda:component/cda:section"):
            code = self._find("cda:code", section)
            # LOINC code for medication history
            if code is not None and code.get("code") == "10160-0":
                # Found medications section - parse entries
                for entry in self._findall("cda:entry", section):
                    med = self._parse_medication_entry(entry)
                    if med:
                        medications.append(med)
                break

        return medications

    def _parse_medication_entry(self, entry: Element) -> MedicationEntry | None:
        """Parse a single medication entry (substanceAdministration)."""
        subst_admin = self._find("cda:substanceAdministration", entry)
        if subst_admin is None:
            return None

        # Get ID
        id_elem = self._find("cda:id", subst_admin)
        ccda_id = id_elem.get("root") if id_elem is not None else None

        # Get effective time (medication period)
        effective_time = self._find("cda:effectiveTime", subst_admin)
        start_date = None
        end_date = None

        if effective_time is not None:
            low = self._find("cda:low", effective_time)
            high = self._find("cda:high", effective_time)

            if low is not None:
                start_date = self._parse_date(low.get("value"))
            if high is not None and high.get("nullFlavor") is None:
                end_date = self._parse_date(high.get("value"))

        # Get the medication code
        manufactured_material = self._find(
            "cda:consumable/cda:manufacturedProduct/cda:manufacturedMaterial",
            subst_admin,
        )
        if manufactured_material is None:
            return None

        code_elem = self._find("cda:code", manufactured_material)
        if code_elem is None:
            return None

        code = code_elem.get("code")
        display = code_elem.get("displayName")

        # Get dosage info
        dosage = None
        dose_elem = self._find("cda:doseQuantity", subst_admin)
        if dose_elem is not None:
            dose_value = dose_elem.get("value")
            if dose_value:
                dosage = dose_value

        # Get route
        route = None
        route_elem = self._find("cda:routeCode", subst_admin)
        if route_elem is not None:
            route = route_elem.get("displayName")

        if not code:
            return None

        return MedicationEntry(
            code=code,
            display=display or "",
            start_date=start_date,
            end_date=end_date,
            dosage=dosage,
            route=route,
            ccda_id=ccda_id or "",
        )

    def _synthesize_encounters(
        self,
        notes: list[ClinicalNote],
        problems: list[ProblemEntry],
        medications: list[MedicationEntry],
    ) -> list[EncounterData]:
        """
        Synthesize Encounter resources from the extracted data.

        Each unique date in the notes becomes an Encounter.
        Problems and medications are linked based on their date ranges.
        """
        # Get unique dates from notes
        encounter_dates = sorted(set(note.date for note in notes))

        encounters: list[EncounterData] = []

        for enc_date in encounter_dates:
            # Find notes for this date
            enc_notes = [n for n in notes if n.date == enc_date]

            # Find problems that were active on this date
            problem_ids = [
                p.ccda_id
                for p in problems
                if p.start_date <= enc_date
                and (p.end_date is None or p.end_date >= enc_date)
            ]

            # Find medications that were active on this date
            medication_ids = [
                m.ccda_id
                for m in medications
                if m.start_date
                and m.start_date <= enc_date
                and (m.end_date is None or m.end_date >= enc_date)
            ]

            encounters.append(
                EncounterData(
                    date=enc_date,
                    notes=enc_notes,
                    problem_ids=problem_ids,
                    medication_ids=medication_ids,
                )
            )

        return encounters
