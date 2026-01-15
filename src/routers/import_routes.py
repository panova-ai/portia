"""Import endpoint for health data."""

from fastapi import APIRouter, HTTPException, status

from src.exceptions import ConversionError, ValidationError
from src.import_.gateway import process_import
from src.routers.deps import MSConverterServiceDep
from src.schemas.import_schemas import ImportRequest, ImportResponse

router = APIRouter(prefix="/import", tags=["Import"])


@router.post("", response_model=ImportResponse, status_code=status.HTTP_201_CREATED)
async def import_data(
    request: ImportRequest,
    ms_converter: MSConverterServiceDep,
) -> ImportResponse:
    """
    Import health data from various formats.

    Supported formats:
    - C-CDA: Clinical documents from other EHRs
    - HL7v2: Lab results, ADT messages (coming soon)
    - FHIR R4: Data from R4-based systems (coming soon)

    The data is converted to FHIR R5 format and returned in the response.
    """
    try:
        response = await process_import(request, ms_converter)
        return response

    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e

    except ConversionError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        ) from e
