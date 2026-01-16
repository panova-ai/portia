"""
Application settings for Portia service.

- Defaults are intended for development use.
- For testing, override via pyproject.toml [tool.pytest.ini_options].
- For production, set environment variables to override fields.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Portia service configuration."""

    # GCP Configuration
    gcp_project_id: str = Field(description="GCP project ID")
    gcp_region: str = Field(default="us-central1", description="GCP region")

    # Microsoft FHIR Converter Configuration
    ms_converter_url: str = Field(
        default="http://localhost:8080",
        description="URL of the MS FHIR Converter service",
    )
    ms_converter_timeout: float = Field(
        default=60.0,
        description="Timeout for MS Converter requests in seconds",
    )

    # Storage Configuration
    temp_bucket: str | None = Field(
        default=None,
        description="GCS bucket for temporary file processing",
    )
    exports_bucket: str | None = Field(
        default=None,
        description="GCS bucket for export downloads",
    )

    # FHIR Store Configuration (for future phases)
    gcp_healthcare_dataset: str = Field(
        default="healthcare_dataset",
        description="GCP Healthcare API dataset name",
    )
    gcp_fhir_store: str = Field(
        default="fhir_store_r5",
        description="FHIR store name within the dataset",
    )

    # Sentia API Configuration
    sentia_url: str = Field(
        default="http://localhost:8002",
        description="URL of the Sentia backend service",
    )
    sentia_timeout: float = Field(
        default=30.0,
        description="Timeout for Sentia API requests in seconds",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    def model_post_init(self, __context: object) -> None:
        """Initialize derived settings after model construction."""
        if self.temp_bucket is None:
            self.temp_bucket = f"{self.gcp_project_id}-portia-temp"
        if self.exports_bucket is None:
            self.exports_bucket = f"{self.gcp_project_id}-portia-exports"


settings = Settings()
