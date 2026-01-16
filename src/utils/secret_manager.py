"""Utility for accessing Google Secret Manager secrets."""

from functools import lru_cache

from google.cloud import secretmanager

from src.settings import settings


class SecretManagerClient:
    """Client for accessing secrets from Google Secret Manager."""

    def __init__(self) -> None:
        """Initialize the Secret Manager client."""
        self.project_id = settings.gcp_project_id
        if not self.project_id:
            raise ValueError(
                "GCP project ID is required. Set GCP_PROJECT_ID environment variable."
            )
        self.client = secretmanager.SecretManagerServiceClient()

    @lru_cache(maxsize=32)
    def get_secret(self, secret_id: str, version: str = "latest") -> str:
        """Get a secret value from Secret Manager.

        Args:
            secret_id: The secret ID (name)
            version: The secret version (defaults to "latest")

        Returns:
            The secret value as a string
        """
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version}"

        try:
            response = self.client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            raise RuntimeError(f"Failed to access secret {secret_id}: {e}") from e


_secret_client: SecretManagerClient | None = None


def get_secret_manager_client() -> SecretManagerClient:
    """Get or create the global Secret Manager client."""
    global _secret_client
    if _secret_client is None:
        _secret_client = SecretManagerClient()
    return _secret_client


def get_secret(secret_id: str, version: str = "latest") -> str:
    """Convenience function to get a secret value.

    Args:
        secret_id: The secret ID (name)
        version: The secret version (defaults to "latest")

    Returns:
        The secret value as a string
    """
    client = get_secret_manager_client()
    return client.get_secret(secret_id, version)
