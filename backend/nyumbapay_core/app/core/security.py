"""Clerk Authentication- JWT verification via JWKS

Clerk issues RS256 JWTs. We verify them using Clerk's public JWKs endpoint.
The `sub` claim is the Clerk User ID (e.g user_2abc...)
Roles are stored in our own DB - not clerk JWT claims

PyJWT's PyJWKClient caches public keys automatically
"""

from functools import lru_cache
from typing import Any
import structlog
import jwt
from jwt import PyJWKClient, PyJWKClientError

from app.core.exceptions import AuthError

logger = structlog.get_logger(__name__)


class ClerkTokenVerifier:
    """Verifies Clerk-Issued JWT access tokens"""

    def __init__(self, jwks_url: str) -> None:
        # cache_keys=True caches public keys in-progress - reduces JWKs roundtrips
        self._jwks_client = PyJWKClient(jwks_url, cache_keys=True)

    def verify(self, token: str) -> dict[str, Any]:
        """Verify a clerk JWT and return its payload"""

        try:
            signing_key = self._jwks_client.get_signing_key_from_jwt(token)
        except PyJWKClientError as exc:
            raise AuthError(message=f"Could not fetch signing key: {exc}") from exc
        except Exception as exc:
            raise AuthError(message=f"Invalid token header: {exc}") from exc

        try:
            payload = jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                options={
                    "verify_aud": False,  # clerk doesnt always set aud
                    "verify_exp": True,
                    "verify_nbf": True,
                },
            )
        except jwt.ExpiredSignatureError as exc:
            logger.warning("token_expired")
            raise AuthError(message="Token has expired") from exc
        except jwt.InvalidTokenError as exc:
            logger.warning("token_invalid", reason=str(exc))
            raise AuthError(message=f"Invalid token: {exc}") from exc

        clerk_user_id = payload.get("sub")
        if not clerk_user_id:
            raise AuthError(message="Token missing sub claim")

        return payload


@lru_cache(maxsize=1)
def get_token_verifier(jwks_url: str) -> ClerkTokenVerifier:
    """Singleton verifier - JWKS client initialised once at startup"""
    return ClerkTokenVerifier(jwks_url)
