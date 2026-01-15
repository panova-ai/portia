"""
Cloud Storage service for Portia file handling.

Handles temporary storage for import processing and export file delivery.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

from google.cloud import storage  # type: ignore[attr-defined]
from google.cloud.exceptions import NotFound

from src.settings import settings


class StorageService:
    """Service for managing files in Google Cloud Storage."""

    def __init__(self) -> None:
        self.client = storage.Client(project=settings.gcp_project_id)
        self.temp_bucket = self.client.bucket(settings.temp_bucket)
        self.exports_bucket = self.client.bucket(settings.exports_bucket)

    def upload_temp_file(
        self,
        content: bytes,
        filename: str,
        content_type: str = "application/octet-stream",
        import_id: UUID | None = None,
    ) -> str:
        """
        Upload a file to temporary storage for processing.

        Args:
            content: File content as bytes
            filename: Original filename (for extension)
            content_type: MIME type of the content
            import_id: Optional import job ID for organization

        Returns:
            Storage key for the uploaded file
        """
        ext = Path(filename).suffix.lower() if filename else ".bin"
        file_id = uuid4().hex[:8]
        import_prefix = str(import_id) if import_id else "unassigned"
        key = f"imports/{import_prefix}/{file_id}{ext}"

        blob = self.temp_bucket.blob(key)
        blob.upload_from_string(content, content_type=content_type)

        return key

    def get_temp_file(self, key: str) -> bytes:
        """
        Retrieve a file from temporary storage.

        Args:
            key: Storage key for the file

        Returns:
            File content as bytes
        """
        blob = self.temp_bucket.blob(key)
        result: bytes = blob.download_as_bytes()
        return result

    def delete_temp_file(self, key: str) -> bool:
        """
        Delete a file from temporary storage.

        Args:
            key: Storage key for the file

        Returns:
            True if deleted, False if not found
        """
        blob = self.temp_bucket.blob(key)
        try:
            blob.delete()
            return True
        except NotFound:
            return False

    def upload_export_file(
        self,
        content: bytes,
        export_id: UUID,
        filename: str,
        content_type: str = "application/fhir+json",
    ) -> str:
        """
        Upload an export file for download.

        Args:
            content: Export content as bytes
            export_id: Export job ID
            filename: Filename for the export
            content_type: MIME type of the content

        Returns:
            Storage key for the uploaded file
        """
        key = f"exports/{export_id}/{filename}"

        blob = self.exports_bucket.blob(key)
        blob.upload_from_string(content, content_type=content_type)

        return key

    def generate_export_download_url(
        self,
        key: str,
        expiration_minutes: int = 60,
    ) -> str:
        """
        Generate a signed URL for downloading an export.

        Args:
            key: Storage key for the export file
            expiration_minutes: How long the URL is valid (default 60 min)

        Returns:
            Signed URL for download
        """
        blob = self.exports_bucket.blob(key)
        expiration = datetime.now(timezone.utc) + timedelta(minutes=expiration_minutes)

        url: str = blob.generate_signed_url(
            version="v4",
            expiration=expiration,
            method="GET",
        )
        return url

    def exists(self, key: str, is_export: bool = False) -> bool:
        """Check if a file exists in storage."""
        bucket = self.exports_bucket if is_export else self.temp_bucket
        blob = bucket.blob(key)
        result: bool = blob.exists()
        return result
