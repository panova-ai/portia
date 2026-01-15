"""Tests for health endpoint."""

from unittest.mock import AsyncMock

import pytest

from tests.conftest import ClientFactory


class TestHealthEndpoint:
    """Tests for the /health endpoint."""

    @pytest.mark.anyio
    async def test_health_returns_healthy_when_converter_available(
        self,
        client_factory: ClientFactory,
        mock_ms_converter_service: AsyncMock,
    ) -> None:
        """Health check returns healthy when MS Converter is available."""
        mock_ms_converter_service.health_check.return_value = True

        async with client_factory() as client:
            response = await client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["ms_converter"] is True

    @pytest.mark.anyio
    async def test_health_returns_degraded_when_converter_unavailable(
        self,
        client_factory: ClientFactory,
        mock_ms_converter_service: AsyncMock,
    ) -> None:
        """Health check returns degraded when MS Converter is unavailable."""
        mock_ms_converter_service.health_check.return_value = False

        async with client_factory() as client:
            response = await client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "degraded"
        assert data["ms_converter"] is False
