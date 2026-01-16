"""
Hybrid authentication system supporting both Firebase and service JWT tokens.

This module provides authentication for Portia APIs that accepts:
1. Firebase ID tokens (for user-facing requests)
2. Service JWT tokens (for service-to-service communication)

Usage:
    from src.core.auth import get_current_user, CurrentUserDep

    @router.post("/endpoint")
    async def endpoint(user: CurrentUserDep):
        # user contains auth info
        pass
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any

import cachecontrol
import google.auth.transport.requests
import jwt
import requests
from fastapi import Depends, HTTPException, Request, status
from google.oauth2 import id_token
from pydantic import BaseModel
from typing_extensions import NotRequired, Required, TypedDict

from src.settings import settings
from src.utils.secret_manager import get_secret

# Firebase token verification setup
_cached_session = cachecontrol.CacheControl(requests.session())
_cached_request = google.auth.transport.requests.Request(  # type: ignore[no-untyped-call]
    session=_cached_session
)


class FirebaseTokenPayload(TypedDict):
    """Firebase ID token payload structure."""

    iss: Required[str]
    aud: Required[str]
    user_id: Required[str]
    sub: Required[str]
    iat: Required[int]
    exp: Required[int]
    auth_time: NotRequired[int]
    email: NotRequired[str]
    email_verified: NotRequired[bool]
    firebase: NotRequired[dict[str, Any]]


class ServiceTokenPayload(BaseModel):
    """Service authentication token payload."""

    service_name: str
    iss: str  # issuer
    sub: str  # subject (service identifier)
    aud: str  # audience
    iat: int  # issued at
    exp: int  # expires at
    permissions: list[str] = []
    environment: str = "production"


class AuthenticatedUser(BaseModel):
    """Represents an authenticated user from any auth method."""

    auth_type: str  # "firebase" or "service"
    user_id: str | None = None
    email: str | None = None
    service_name: str | None = None
    permissions: list[str] = []

    # Full payloads for detailed access
    firebase_payload: dict[str, Any] | None = None
    service_payload: ServiceTokenPayload | None = None


# Service authentication configuration
SERVICE_AUTH_ISSUER = "panova-services"
SERVICE_AUTH_AUDIENCE = "panova-portia"

# Lazy-loaded JWT secret
_SERVICE_AUTH_SECRET: str | None = None


def _get_service_auth_secret() -> str:
    """Get the JWT secret, loading from Secret Manager on first access."""
    global _SERVICE_AUTH_SECRET
    if _SERVICE_AUTH_SECRET is None:
        try:
            _SERVICE_AUTH_SECRET = get_secret("service-auth-jwt-secret")
        except Exception as e:
            raise RuntimeError(
                f"Failed to get JWT secret from Secret Manager: {e}. "
                f"Ensure the service has secretmanager.secretAccessor role for "
                f"projects/{settings.gcp_project_id}/secrets/service-auth-jwt-secret"
            ) from e
    return _SERVICE_AUTH_SECRET


def verify_firebase_token(token: str) -> FirebaseTokenPayload:
    """Verify a Firebase ID token.

    Args:
        token: The Firebase ID token to verify

    Returns:
        The decoded token payload

    Raises:
        HTTPException: If the token is invalid or expired
    """
    try:
        payload: FirebaseTokenPayload = id_token.verify_firebase_token(  # type: ignore[no-untyped-call]
            token,
            _cached_request,
            audience=settings.gcp_project_id,
            clock_skew_in_seconds=10,
        )
        return payload
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid or expired Firebase token: {e}",
        ) from e


def verify_service_token(token: str) -> ServiceTokenPayload:
    """Verify a service JWT token.

    Args:
        token: The JWT token to verify

    Returns:
        The decoded token payload

    Raises:
        HTTPException: If the token is invalid or expired
    """
    try:
        payload = jwt.decode(
            token,
            _get_service_auth_secret(),
            algorithms=["HS256"],
            audience=SERVICE_AUTH_AUDIENCE,
            issuer=SERVICE_AUTH_ISSUER,
            options={"verify_exp": True},
        )
        return ServiceTokenPayload(**payload)
    except jwt.ExpiredSignatureError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Service token has expired",
        ) from e
    except jwt.InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid service token: {e}",
        ) from e


def create_service_token(
    service_name: str,
    permissions: list[str] | None = None,
    expires_hours: int = 24,
) -> str:
    """Create a service JWT token for service-to-service auth.

    Args:
        service_name: Name of the calling service
        permissions: List of permission strings
        expires_hours: Token validity in hours

    Returns:
        The signed JWT token
    """
    now = datetime.now(timezone.utc)
    expires = now + timedelta(hours=expires_hours)

    payload = {
        "service_name": service_name,
        "iss": SERVICE_AUTH_ISSUER,
        "sub": f"service:{service_name}",
        "aud": SERVICE_AUTH_AUDIENCE,
        "iat": int(now.timestamp()),
        "exp": int(expires.timestamp()),
        "permissions": permissions or [],
        "environment": os.getenv("ENVIRONMENT", "production"),
    }

    return jwt.encode(payload, _get_service_auth_secret(), algorithm="HS256")


def _get_bearer_token(request: Request) -> str | None:
    """Extract Bearer token from Authorization header."""
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.startswith("Bearer "):
        return auth_header[7:]
    return None


def _get_service_token_header(request: Request) -> str | None:
    """Extract service token from X-Service-Token header."""
    return request.headers.get("X-Service-Token")


def _get_firebase_cookie(request: Request) -> str | None:
    """Extract Firebase token from cookies (PRVID for practitioners)."""
    return request.cookies.get("PRVID") or request.cookies.get("PATID")


async def get_current_user(request: Request) -> AuthenticatedUser:
    """Get the current authenticated user.

    Supports multiple authentication methods:
    1. Firebase ID token via Authorization Bearer header
    2. Firebase ID token via cookie (PRVID/PATID)
    3. Service JWT via Authorization Bearer header
    4. Service JWT via X-Service-Token header

    Args:
        request: The incoming FastAPI request

    Returns:
        AuthenticatedUser with auth information

    Raises:
        HTTPException: If no valid authentication is found
    """
    # Try Bearer token (could be Firebase or service token)
    bearer_token = _get_bearer_token(request)
    if bearer_token:
        # Try Firebase first
        try:
            firebase_payload = verify_firebase_token(bearer_token)
            return AuthenticatedUser(
                auth_type="firebase",
                user_id=firebase_payload.get("user_id"),
                email=firebase_payload.get("email"),
                firebase_payload=dict(firebase_payload),
            )
        except HTTPException:
            pass  # Try service token

        # Try service token
        try:
            service_payload = verify_service_token(bearer_token)
            return AuthenticatedUser(
                auth_type="service",
                service_name=service_payload.service_name,
                permissions=service_payload.permissions,
                service_payload=service_payload,
            )
        except HTTPException:
            pass  # Continue to other methods

    # Try X-Service-Token header
    service_token = _get_service_token_header(request)
    if service_token:
        try:
            service_payload = verify_service_token(service_token)
            return AuthenticatedUser(
                auth_type="service",
                service_name=service_payload.service_name,
                permissions=service_payload.permissions,
                service_payload=service_payload,
            )
        except HTTPException:
            pass

    # Try Firebase cookie
    cookie_token = _get_firebase_cookie(request)
    if cookie_token:
        try:
            firebase_payload = verify_firebase_token(cookie_token)
            return AuthenticatedUser(
                auth_type="firebase",
                user_id=firebase_payload.get("user_id"),
                email=firebase_payload.get("email"),
                firebase_payload=dict(firebase_payload),
            )
        except HTTPException:
            pass

    # No valid authentication found
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required. Provide Firebase ID token or service token.",
        headers={"WWW-Authenticate": "Bearer"},
    )


# Type alias for dependency injection
CurrentUserDep = Annotated[AuthenticatedUser, Depends(get_current_user)]


def require_permission(permission: str) -> Any:
    """Create a dependency that requires a specific permission.

    Args:
        permission: The required permission string

    Returns:
        A FastAPI dependency function
    """

    async def check_permission(user: CurrentUserDep) -> AuthenticatedUser:
        if user.auth_type == "service":
            if permission not in user.permissions:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Missing required permission: {permission}",
                )
        # Firebase users are assumed to have permissions based on their role
        # (could be extended to check claims)
        return user

    return Depends(check_permission)


# Common permissions
class Permissions:
    """Common permission constants."""

    IMPORT_READ = "import.read"
    IMPORT_WRITE = "import.write"
    FHIR_READ = "fhir.read"
    FHIR_WRITE = "fhir.write"
