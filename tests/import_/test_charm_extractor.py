"""Tests for CHARM C-CDA extractor."""

from pathlib import Path

import pytest

from src.import_.charm.extractor import CharmCcdaExtractor


@pytest.fixture
def sample_charm_ccda() -> str:
    """Load the sample CHARM C-CDA file."""
    ccda_path = (
        Path(__file__).parent.parent
        / "data"
        / "PANOVA_TEST_PAT0015_ClinicalSummary.xml"
    )
    return ccda_path.read_text()


class TestCharmCcdaExtractor:
    """Tests for CharmCcdaExtractor."""

    def test_extract_patient_info(self, sample_charm_ccda: str) -> None:
        """Test extraction of patient information."""
        extractor = CharmCcdaExtractor(sample_charm_ccda)
        result = extractor.extract()

        assert result.patient_id == "PAT0015"
        assert result.patient_name == "Elkind Testpatientone"

    def test_extract_practitioner_info(self, sample_charm_ccda: str) -> None:
        """Test extraction of practitioner information."""
        extractor = CharmCcdaExtractor(sample_charm_ccda)
        result = extractor.extract()

        assert result.practitioner_name is not None
        assert "Jae" in result.practitioner_name or "Elkind" in result.practitioner_name
        assert result.organization_name == "Sofia Elkind MD"

    def test_extract_notes(self, sample_charm_ccda: str) -> None:
        """Test extraction of clinical notes."""
        extractor = CharmCcdaExtractor(sample_charm_ccda)
        result = extractor.extract()

        assert len(result.notes) > 0, "Should extract clinical notes"

        # Check for HPI notes
        hpi_notes = [
            n for n in result.notes if "History of Present Illness" in n.note_type
        ]
        assert len(hpi_notes) > 0, "Should have HPI notes"

        # Check note content
        first_hpi = hpi_notes[0]
        assert first_hpi.content, "HPI note should have content"
        assert first_hpi.date, "HPI note should have date"

    def test_extract_problems(self, sample_charm_ccda: str) -> None:
        """Test extraction of problems/conditions."""
        extractor = CharmCcdaExtractor(sample_charm_ccda)
        result = extractor.extract()

        assert len(result.problems) > 0, "Should extract problems"

        # Check for panic disorder
        panic_problems = [p for p in result.problems if "371631005" in p.code]
        assert len(panic_problems) > 0, "Should have panic disorder entries"

        # Each should have a start date
        for problem in panic_problems:
            assert problem.start_date is not None, "Problem should have start date"
            assert problem.code == "371631005", "Should be panic disorder SNOMED code"

    def test_extract_medications(self, sample_charm_ccda: str) -> None:
        """Test extraction of medications."""
        extractor = CharmCcdaExtractor(sample_charm_ccda)
        result = extractor.extract()

        assert len(result.medications) > 0, "Should extract medications"

        # Check for known medications (sertraline, alprazolam)
        med_codes = [m.code for m in result.medications]

        # RxNorm codes from the sample file
        assert any(
            "312938" in code or "312940" in code for code in med_codes
        ), "Should have sertraline"
        assert any("308047" in code for code in med_codes), "Should have alprazolam"

    def test_synthesize_encounters(self, sample_charm_ccda: str) -> None:
        """Test synthesis of encounters from notes."""
        extractor = CharmCcdaExtractor(sample_charm_ccda)
        result = extractor.extract()

        # Should create encounters from unique note dates
        assert len(result.encounters) > 0, "Should synthesize encounters"

        # Each encounter should have notes
        encounters_with_notes = [e for e in result.encounters if len(e.notes) > 0]
        assert len(encounters_with_notes) > 0, "Encounters should have associated notes"

        # Encounters should be sorted by date
        dates = [e.date for e in result.encounters]
        assert dates == sorted(dates), "Encounters should be sorted by date"

    def test_encounters_link_to_problems(self, sample_charm_ccda: str) -> None:
        """Test that encounters link to active problems."""
        extractor = CharmCcdaExtractor(sample_charm_ccda)
        result = extractor.extract()

        # Find encounters with linked problems
        encounters_with_problems = [
            e for e in result.encounters if len(e.problem_ids) > 0
        ]
        assert (
            len(encounters_with_problems) > 0
        ), "Some encounters should have linked problems"

    def test_note_content_preserved(self, sample_charm_ccda: str) -> None:
        """Test that clinical note content is fully preserved."""
        extractor = CharmCcdaExtractor(sample_charm_ccda)
        result = extractor.extract()

        # Find an HPI note and verify content
        hpi_notes = [
            n for n in result.notes if "History of Present Illness" in n.note_type
        ]

        if hpi_notes:
            # Should contain therapy-related content
            therapy_notes = [
                n
                for n in hpi_notes
                if "Therapy performed" in n.content or "HPI" in n.content
            ]
            assert len(therapy_notes) > 0, "Should have therapy session notes"

    def test_extract_patient_demographics(self, sample_charm_ccda: str) -> None:
        """Test extraction of full patient demographics for matching."""
        extractor = CharmCcdaExtractor(sample_charm_ccda)
        result = extractor.extract()

        demographics = result.patient_demographics
        assert demographics is not None, "Should extract demographics"

        # Check name
        assert demographics.given_name == "Elkind", "Should extract given name"
        assert (
            demographics.family_name == "Testpatientone"
        ), "Should extract family name"

        # Check birth date
        from datetime import date

        assert demographics.birth_date == date(
            1980, 12, 23
        ), "Should extract birth date"

        # Check gender
        assert demographics.gender == "female", "Should extract gender"

        # Check contact info
        assert demographics.phone is not None, "Should extract phone"
        assert demographics.email == "britt@bigandbrightpr.com", "Should extract email"

        # Check address
        assert demographics.address_city == "Carlsbad", "Should extract city"
        assert demographics.address_state == "CA", "Should extract state"
        assert demographics.address_postal_code == "92008", "Should extract postal code"
