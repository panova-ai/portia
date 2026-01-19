"""Tests for import endpoint."""

import base64
from unittest.mock import AsyncMock

import pytest

from src.schemas.import_schemas import MAX_BASE64_SIZE
from tests.conftest import ClientFactory


class TestImportEndpoint:
    """Tests for the /import endpoint."""

    @pytest.mark.anyio
    async def test_import_ccda_success(
        self,
        client_factory: ClientFactory,
        mock_ms_converter_service: AsyncMock,
        sample_ccda: str,
    ) -> None:
        """Successfully import a C-CDA document."""
        # Encode the C-CDA as base64
        encoded_data = base64.b64encode(sample_ccda.encode()).decode()

        async with client_factory() as client:
            response = await client.post(
                "/import",
                json={
                    "format": "ccda",
                    "organization_id": "12345678-1234-1234-1234-123456789012",
                    "data": encoded_data,
                },
            )

        assert response.status_code == 201
        data = response.json()
        assert data["status"] in ["completed", "partial"]
        assert data["import_id"] is not None
        assert data["fhir_bundle"] is not None
        assert data["fhir_bundle"]["resourceType"] == "Bundle"

        # Verify the converter was called
        mock_ms_converter_service.convert_ccda.assert_called_once()

    @pytest.mark.anyio
    async def test_import_ccda_extracts_resources(
        self,
        client_factory: ClientFactory,
        mock_ms_converter_service: AsyncMock,
        sample_ccda: str,
    ) -> None:
        """Import extracts and counts resources correctly."""
        # Set up a more complete mock response
        mock_ms_converter_service.convert_ccda.return_value = {
            "resourceType": "Bundle",
            "type": "collection",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Patient",
                        "id": "p1",
                    }
                },
                {
                    "resource": {
                        "resourceType": "Condition",
                        "id": "c1",
                    }
                },
                {
                    "resource": {
                        "resourceType": "Condition",
                        "id": "c2",
                    }
                },
                {
                    "resource": {
                        "resourceType": "MedicationStatement",
                        "id": "m1",
                        "status": "active",
                        "medicationCodeableConcept": {
                            "text": "Aspirin",
                        },
                    }
                },
            ],
        }

        encoded_data = base64.b64encode(sample_ccda.encode()).decode()

        async with client_factory() as client:
            response = await client.post(
                "/import",
                json={
                    "format": "ccda",
                    "organization_id": "12345678-1234-1234-1234-123456789012",
                    "data": encoded_data,
                },
            )

        assert response.status_code == 201
        data = response.json()

        # Verify resource counts
        counts = data["resources_extracted"]
        assert counts["Patient"] == 1
        assert counts["Condition"] == 2
        assert (
            counts["MedicationStatement"] == 1
        )  # GCP Healthcare R5 uses MedicationStatement

    @pytest.mark.anyio
    async def test_import_invalid_base64_returns_400(
        self,
        client_factory: ClientFactory,
    ) -> None:
        """Invalid base64 data returns 400 error."""
        async with client_factory() as client:
            response = await client.post(
                "/import",
                json={
                    "format": "ccda",
                    "organization_id": "12345678-1234-1234-1234-123456789012",
                    "data": "not-valid-base64!!!",
                },
            )

        assert response.status_code == 400
        assert "decode" in response.json()["detail"].lower()

    @pytest.mark.anyio
    async def test_import_hl7v2_not_implemented(
        self,
        client_factory: ClientFactory,
    ) -> None:
        """HL7v2 import returns not implemented error."""
        encoded_data = base64.b64encode(b"MSH|^~\\&|").decode()

        async with client_factory() as client:
            response = await client.post(
                "/import",
                json={
                    "format": "hl7v2",
                    "organization_id": "12345678-1234-1234-1234-123456789012",
                    "data": encoded_data,
                },
            )

        assert response.status_code == 400
        assert "not yet implemented" in response.json()["detail"].lower()

    @pytest.mark.anyio
    async def test_import_converter_error_returns_422(
        self,
        client_factory: ClientFactory,
        mock_ms_converter_service: AsyncMock,
        sample_ccda: str,
    ) -> None:
        """Converter error returns 422 error."""
        mock_ms_converter_service.convert_ccda.side_effect = Exception(
            "Conversion failed"
        )

        encoded_data = base64.b64encode(sample_ccda.encode()).decode()

        async with client_factory() as client:
            response = await client.post(
                "/import",
                json={
                    "format": "ccda",
                    "organization_id": "12345678-1234-1234-1234-123456789012",
                    "data": encoded_data,
                },
            )

        assert response.status_code == 422
        assert "converter" in response.json()["detail"].lower()

    @pytest.mark.anyio
    async def test_import_oversized_payload_returns_422(
        self,
        client_factory: ClientFactory,
    ) -> None:
        """Payload exceeding size limit returns 422 validation error."""
        # Create data just over the limit
        oversized_data = "A" * (MAX_BASE64_SIZE + 1000)

        async with client_factory() as client:
            response = await client.post(
                "/import",
                json={
                    "format": "ccda",
                    "organization_id": "12345678-1234-1234-1234-123456789012",
                    "data": oversized_data,
                },
            )

        assert response.status_code == 422
        assert "exceeds maximum size" in str(response.json()["detail"])
