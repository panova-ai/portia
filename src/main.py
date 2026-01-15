"""Portia - Health Data Interchange Service."""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from httpx import HTTPError, HTTPStatusError
from pydantic import ValidationError

from src.routers import health, import_routes


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan management."""
    # Startup
    yield
    # Shutdown - cleanup resources if needed


app = FastAPI(
    title="Portia",
    description="Panova Health Data Interchange Service - Import and export patient health data across formats",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS middleware - restrict to Panova domains and localhost for development
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^(http://(localhost|127\.0\.0\.1)(:\d+)?|https://([a-zA-Z0-9-]+\.)*panova\.(ai|health))$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPStatusError)
async def handle_httpx_status_error(
    request: Request, exc: HTTPStatusError
) -> HTTPException:
    """Handle HTTP status errors from httpx clients (e.g., MS Converter)."""
    content = None
    if exc.response.content:
        try:
            content = exc.response.json()
        except (ValueError, UnicodeDecodeError):
            content = exc.response.text
    raise HTTPException(status_code=exc.response.status_code, detail=content)


@app.exception_handler(HTTPError)
async def handle_httpx_error(request: Request, exc: HTTPError) -> HTTPException:
    """Handle network/connection errors from httpx clients."""
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Service temporarily unavailable",
    )


@app.exception_handler(ValidationError)
async def handle_validation_error(
    request: Request, exc: ValidationError
) -> JSONResponse:
    """Handle Pydantic ValidationError and return 422."""
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors()},
    )


@app.exception_handler(Exception)
async def handle_unhandled_exceptions(request: Request, exc: Exception) -> JSONResponse:
    """Catch and log all unhandled exceptions."""
    # In production, log to Cloud Logging
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error"},
    )


# Register routers
app.include_router(health.router)
app.include_router(import_routes.router)


@app.get("/")
def root() -> dict[str, str]:
    """Root endpoint."""
    return {"service": "portia", "version": "0.1.0"}
