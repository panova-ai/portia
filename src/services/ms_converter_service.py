"""
Microsoft FHIR Converter client service.

Converts C-CDA and HL7v2 messages to FHIR R4 bundles using the
Microsoft FHIR Converter Docker image (mcr.microsoft.com/healthcareapis/fhir-converter).
"""

from enum import Enum
from typing import Any

import google.auth.transport.requests  # type: ignore[import-untyped]
import httpx
from google.auth import default as google_auth_default  # type: ignore[import-untyped]
from google.oauth2 import id_token  # type: ignore[import-untyped]
from pydantic import BaseModel

from src.settings import settings


class InputDataFormat(str, Enum):
    """Supported input data formats for the MS FHIR Converter."""

    CCDA = "Ccda"
    HL7V2 = "Hl7v2"


class CcdaTemplate(str, Enum):
    """C-CDA root templates supported by MS FHIR Converter."""

    CCD = "CCD"
    CONSULTATION_NOTE = "ConsultationNote"
    DISCHARGE_SUMMARY = "DischargeSummary"
    HISTORY_AND_PHYSICAL = "HistoryandPhysical"
    OPERATIVE_NOTE = "OperativeNote"
    PROCEDURE_NOTE = "ProcedureNote"
    PROGRESS_NOTE = "ProgressNote"
    REFERRAL_NOTE = "ReferralNote"
    TRANSFER_SUMMARY = "TransferSummary"


class Hl7v2Template(str, Enum):
    """HL7v2 root templates supported by MS FHIR Converter."""

    ADT_A01 = "ADT_A01"
    ADT_A02 = "ADT_A02"
    ADT_A03 = "ADT_A03"
    ADT_A04 = "ADT_A04"
    ADT_A05 = "ADT_A05"
    ADT_A08 = "ADT_A08"
    ADT_A11 = "ADT_A11"
    ADT_A13 = "ADT_A13"
    ADT_A14 = "ADT_A14"
    ADT_A15 = "ADT_A15"
    ADT_A16 = "ADT_A16"
    ADT_A25 = "ADT_A25"
    ADT_A26 = "ADT_A26"
    ADT_A27 = "ADT_A27"
    ADT_A28 = "ADT_A28"
    ADT_A29 = "ADT_A29"
    ADT_A31 = "ADT_A31"
    ADT_A40 = "ADT_A40"
    ADT_A41 = "ADT_A41"
    ADT_A45 = "ADT_A45"
    ADT_A47 = "ADT_A47"
    ADT_A60 = "ADT_A60"
    ORM_O01 = "ORM_O01"
    ORU_R01 = "ORU_R01"
    OML_O21 = "OML_O21"
    VXU_V04 = "VXU_V04"
    SIU_S12 = "SIU_S12"
    SIU_S13 = "SIU_S13"
    SIU_S14 = "SIU_S14"
    SIU_S15 = "SIU_S15"
    SIU_S16 = "SIU_S16"
    SIU_S17 = "SIU_S17"
    SIU_S26 = "SIU_S26"
    MDM_T02 = "MDM_T02"


class ConversionRequest(BaseModel):
    """Request model for the MS FHIR Converter API."""

    InputDataFormat: InputDataFormat
    RootTemplateName: str
    InputDataString: str


class ConversionResult(BaseModel):
    """Wrapper for FHIR Bundle result from conversion."""

    resourceType: str
    type: str
    entry: list[dict[str, Any]] = []


class ConversionResponse(BaseModel):
    """Response model from the MS FHIR Converter API."""

    result: dict[str, Any]


class MSConverterService:
    """
    Client for Microsoft FHIR Converter.

    The converter is deployed as a separate Cloud Run service and provides
    conversion from C-CDA and HL7v2 to FHIR R4 format.
    """

    def __init__(self, base_url: str | None = None, timeout: float | None = None):
        self.base_url = base_url or settings.ms_converter_url
        self.timeout = timeout or settings.ms_converter_timeout
        self._client: httpx.AsyncClient | None = None

    def _get_identity_token(self) -> str | None:
        """Get an ID token for Cloud Run service-to-service auth."""
        try:
            # For Cloud Run service-to-service auth, we need an ID token
            # with the target service URL as the audience
            auth_request = google.auth.transport.requests.Request()
            token = id_token.fetch_id_token(auth_request, self.base_url)
            return str(token)
        except Exception:
            # Fallback: try using default credentials (works in some environments)
            try:
                credentials, _ = google_auth_default()
                auth_request = google.auth.transport.requests.Request()
                credentials.refresh(auth_request)
                if hasattr(credentials, "token"):
                    return str(credentials.token)
            except Exception:
                pass
        return None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create httpx async client with auth headers."""
        if self._client is None:
            headers = {}
            token = self._get_identity_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout),
                headers=headers,
            )
        return self._client

    async def close(self) -> None:
        """Close the httpx client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def convert_ccda(
        self,
        ccda_content: str,
        template: CcdaTemplate = CcdaTemplate.CCD,
    ) -> dict[str, Any]:
        """
        Convert C-CDA document to FHIR R4 Bundle.

        Args:
            ccda_content: The C-CDA XML content as a string
            template: The C-CDA template type (default: CCD)

        Returns:
            FHIR R4 Bundle as a dictionary
        """
        return await self._convert(
            input_format=InputDataFormat.CCDA,
            template_name=template.value,
            content=ccda_content,
        )

    async def convert_hl7v2(
        self,
        hl7v2_message: str,
        template: Hl7v2Template,
    ) -> dict[str, Any]:
        """
        Convert HL7v2 message to FHIR R4 Bundle.

        Args:
            hl7v2_message: The HL7v2 message content
            template: The HL7v2 message type template

        Returns:
            FHIR R4 Bundle as a dictionary
        """
        return await self._convert(
            input_format=InputDataFormat.HL7V2,
            template_name=template.value,
            content=hl7v2_message,
        )

    async def _convert(
        self,
        input_format: InputDataFormat,
        template_name: str,
        content: str,
    ) -> dict[str, Any]:
        """
        Internal method to perform conversion.

        Args:
            input_format: The input data format
            template_name: The root template name
            content: The input content to convert

        Returns:
            FHIR R4 Bundle as a dictionary

        Raises:
            httpx.HTTPStatusError: If the converter returns an error response
        """
        client = await self._get_client()

        request = ConversionRequest(
            InputDataFormat=input_format,
            RootTemplateName=template_name,
            InputDataString=content,
        )

        response = await client.post(
            "/convertToFhir",
            params={"api-version": "2024-05-01-preview"},
            json=request.model_dump(),
        )
        response.raise_for_status()

        result = ConversionResponse.model_validate(response.json())
        return result.result

    async def health_check(self) -> bool:
        """Check if the MS FHIR Converter service is healthy."""
        try:
            client = await self._get_client()
            # MS FHIR Converter doesn't have a /health endpoint
            # Check the root path which returns 200 or the swagger endpoint
            response = await client.get("/")
            # Accept 200 (success) or 404 (service is running but no root handler)
            return response.status_code in (200, 404)
        except Exception:
            return False
