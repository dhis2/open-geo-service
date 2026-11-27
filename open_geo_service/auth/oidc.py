"""People who sign in through an OIDC provider: checking the provider's tokens, and web sign-in.

Open Geo Service is a resource server, not an identity provider (docs/authentication.md, People:
OIDC). It reads a provider's discovery document, checks each JWT against the keys the provider
publishes, and maps the issuer and subject to the principal `oidc:<issuer>#<subject>`, the same
whether the person arrives with a bearer token or signs in to the web interface. People get the
authenticated tier; operators use named tokens until provider roles map to tiers (CLIM-1481).
Access tokens must be JWTs: a provider that issues opaque tokens is not supported.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import secrets
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

import httpx
from joserfc import jwt
from joserfc.errors import InvalidKeyIdError, JoseError
from joserfc.jwk import KeySet

from open_geo_service.auth.tokens import OidcProvider, Principal, Tier

logger = logging.getLogger(__name__)

# Asymmetric only: a symmetric algorithm would let anyone holding the client secret mint tokens.
ALGORITHMS = ["RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA"]
_LEEWAY_SECONDS = 60
_DISCOVERY_TTL = 60 * 60
# Keys are fetched again after this long, so a key the provider withdrew stops being trusted.
_KEYS_TTL = 60 * 60
# A token naming a key we do not have refetches the keys sooner, at most this often, so a stream of
# forged key ids cannot make every request call the provider.
_KEY_REFRESH_INTERVAL = 60
# The claims RFC 9068 requires of a JWT access token.
_RFC9068_CLAIMS = ("iss", "exp", "aud", "sub", "client_id", "iat", "jti")
_HTTP_TIMEOUT = 10.0
_PENDING_SECONDS = 10 * 60
# Anyone may start a sign-in, so past this many open ones the oldest is dropped.
_PENDING_LIMIT = 1000


class TokenRejected(Exception):
    """A token that is not valid for this instance; the message says why, for the log only."""


class ProviderUnavailable(TokenRejected):
    """The provider could not be reached to check a token, which may well be valid."""


@dataclass(frozen=True)
class Person:
    """Someone a provider vouched for: its issuer, their subject there, and a name to show."""

    issuer: str
    subject: str
    name: str


def person_id(issuer: str, subject: str) -> str:
    """The principal id of a subject at an issuer: `oidc:<issuer>#<subject>`.

    Unambiguous, since an issuer has no fragment. Opaque to callers: it identifies the owner of a
    record, and nothing should be read out of it.
    """
    return f"oidc:{issuer}#{subject}"


def principal_for_person(person: Person, providers: tuple[OidcProvider, ...]) -> Principal | None:
    """The principal a person runs as, the authenticated tier; None once no provider has their issuer."""
    if not any(provider.issuer == person.issuer for provider in providers):
        return None
    return Principal(id=person_id(person.issuer, person.subject), tier=Tier.AUTHENTICATED, name=person.name)


# --- discovery and keys -------------------------------------------------------------------


@dataclass
class _Cached[T]:
    value: T
    fetched: float


_discovery: dict[str, _Cached[dict[str, Any]]] = {}
_keys: dict[str, _Cached[KeySet]] = {}
_cache_lock = threading.Lock()


def _get_json(url: str) -> Any:
    response = httpx.get(url, timeout=_HTTP_TIMEOUT, follow_redirects=True)
    response.raise_for_status()
    return response.json()


def discovery_url(provider: OidcProvider) -> str:
    """Where the provider publishes its discovery document (OpenID Connect Discovery 1.0, section 4)."""
    return f"{provider.issuer.rstrip('/')}/.well-known/openid-configuration"


def discovery(provider: OidcProvider) -> dict[str, Any]:
    """The provider's discovery document, fetched once an hour; its issuer must be the configured one."""
    now = time.monotonic()
    with _cache_lock:
        cached = _discovery.get(provider.id)
        if cached is not None and now - cached.fetched < _DISCOVERY_TTL:
            return dict(cached.value)
    document = _get_json(discovery_url(provider))
    if not isinstance(document, dict) or document.get("issuer") != provider.issuer:
        raise TokenRejected(f"the discovery document of {provider.id} names another issuer")
    with _cache_lock:
        _discovery[provider.id] = _Cached(document, now)
    return dict(document)


def _key_set(provider: OidcProvider, *, refresh: bool = False) -> KeySet:
    now = time.monotonic()
    with _cache_lock:
        cached = _keys.get(provider.id)
        if cached is not None:
            age = now - cached.fetched
            if age < _KEYS_TTL and (not refresh or age < _KEY_REFRESH_INTERVAL):
                return cached.value
    key_set = KeySet.import_key_set(_get_json(str(discovery(provider)["jwks_uri"])))
    with _cache_lock:
        _keys[provider.id] = _Cached(key_set, now)
    return key_set


def reset_caches() -> None:
    """Forget discovery documents and keys, for a test that changes a provider."""
    with _cache_lock:
        _discovery.clear()
        _keys.clear()


# --- tokens -------------------------------------------------------------------------------


def unverified_issuer(token: str) -> str | None:
    """The `iss` a JWT claims, read without checking it, to pick the provider that checks it."""
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
    except (ValueError, UnicodeDecodeError):
        return None
    issuer = payload.get("iss") if isinstance(payload, dict) else None
    return issuer if isinstance(issuer, str) else None


def _decoded(token: str, provider: OidcProvider) -> tuple[dict[str, Any], dict[str, Any]]:
    """The header and claims of a JWT whose signature checks out against the provider's keys."""
    try:
        try:
            decoded = jwt.decode(token, _key_set(provider), algorithms=ALGORITHMS)
        except InvalidKeyIdError:
            # The provider may have rotated its keys since we fetched them.
            decoded = jwt.decode(token, _key_set(provider, refresh=True), algorithms=ALGORITHMS)
    except JoseError as exc:
        raise TokenRejected(f"{provider.id}: {exc}") from exc
    except httpx.HTTPError as exc:
        raise ProviderUnavailable(f"{provider.id}: could not read its keys: {exc}") from exc
    except ValueError as exc:
        raise TokenRejected(f"{provider.id}: could not read its keys: {exc}") from exc
    return dict(decoded.header), dict(decoded.claims)


def _check(claims: dict[str, Any], provider: OidcProvider, audiences: tuple[str, ...]) -> None:
    registry = jwt.JWTClaimsRegistry(
        leeway=_LEEWAY_SECONDS,
        iss={"essential": True, "value": provider.issuer},
        aud={"essential": True, "values": list(audiences)},
        sub={"essential": True},
        exp={"essential": True},
    )
    try:
        registry.validate(claims)
    except JoseError as exc:
        raise TokenRejected(f"{provider.id}: {exc}") from exc


# A name to show for a person, the first of these a token carries, else the subject (DHIS2's is the
# username); never used to decide access.
_NAME_CLAIMS = ("preferred_username", "username")


def _person(claims: dict[str, Any], provider: OidcProvider) -> Person:
    subject = claims.get("sub")
    if not isinstance(subject, str) or not subject:
        raise TokenRejected(f"{provider.id}: the subject is not a non-empty string")
    name = next((v for c in _NAME_CLAIMS if isinstance(v := claims.get(c), str) and v), subject)
    return Person(provider.issuer, subject, name)


def person_from_access_token(token: str, provider: OidcProvider) -> Person:
    """The person an access token names, or TokenRejected.

    Checked: the signature against the provider's keys, the issuer, an audience this instance
    accepts, the subject, the expiry, and that the token is an access token under the provider's
    `access_tokens` profile. An ID token is refused: it is meant for the client that asked for it,
    not for Open Geo Service.
    """
    header, claims = _decoded(token, provider)
    _check(claims, provider, provider.audience)
    _require_access_token(header, claims, provider)
    return _person(claims, provider)


def _require_access_token(header: dict[str, Any], claims: dict[str, Any], provider: OidcProvider) -> None:
    """Refuse a token that is not an access token under the provider's profile (docs/instance_guide.md).

    - `rfc9068`, the default: the header's `typ` is `at+jwt`, and the claims RFC 9068 requires are there.
    - `keycloak`: Keycloak marks access tokens `typ: Bearer` in the claims, and ID tokens `typ: ID`.
    - `dhis2`: DHIS2 2.43 marks neither. Its access tokens carry `scope` and no `azp`; its ID tokens
      carry `azp`, and `nonce` when one was asked for, and no `scope`.
    """
    profile = provider.access_tokens
    if profile == "rfc9068":
        if str(header.get("typ", "")).lower() not in ("at+jwt", "application/at+jwt"):
            raise TokenRejected(f"{provider.id}: not an RFC 9068 access token (typ {header.get('typ')!r})")
        if missing := [claim for claim in _RFC9068_CLAIMS if claim not in claims]:
            raise TokenRejected(f"{provider.id}: the access token lacks {missing}")
    elif profile == "keycloak":
        if claims.get("typ") != "Bearer":
            raise TokenRejected(f"{provider.id}: not a Keycloak access token (typ {claims.get('typ')!r})")
    elif "scope" not in claims or "azp" in claims or "nonce" in claims:
        raise TokenRejected(f"{provider.id}: not a DHIS2 access token")


# --- web sign-in --------------------------------------------------------------------------


@dataclass(frozen=True)
class PendingSignIn:
    """A sign-in started in a browser, waiting for the provider to send it back."""

    provider_id: str
    transaction: str = field(repr=False)
    """The digest of the secret the starting browser holds in its transaction cookie."""
    verifier: str = field(repr=False)
    nonce: str = field(repr=False)
    redirect_uri: str
    next_path: str
    expires: float


_pending: dict[str, PendingSignIn] = {}
_pending_lock = threading.Lock()


def _s256(verifier: str) -> str:
    """The PKCE S256 challenge of *verifier* (RFC 7636)."""
    return base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()


def _digest(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def begin_sign_in(provider: OidcProvider, *, redirect_uri: str, next_path: str) -> tuple[str, str]:
    """Start a sign-in: the provider's authorization URL, with PKCE, a state and a nonce, and a transaction secret.

    The caller gives the secret to the browser in a cookie; the callback must bring it back, so a
    callback URL opened in another browser cannot complete the sign-in (login CSRF).
    """
    transaction = secrets.token_urlsafe(32)
    verifier, nonce, state = secrets.token_urlsafe(48), secrets.token_urlsafe(24), secrets.token_urlsafe(24)
    query = urllib.parse.urlencode(
        {
            "response_type": "code",
            "client_id": provider.client_id,
            "redirect_uri": redirect_uri,
            "scope": " ".join(provider.scopes),
            "state": state,
            "nonce": nonce,
            "code_challenge": _s256(verifier),
            "code_challenge_method": "S256",
        }
    )
    endpoint = str(discovery(provider)["authorization_endpoint"])
    now = time.monotonic()
    with _pending_lock:
        # Abandoned sign-ins are dropped here, so the table stays bounded by recent attempts.
        for stale in [key for key, item in _pending.items() if item.expires <= now]:
            del _pending[stale]
        while len(_pending) >= _PENDING_LIMIT:
            del _pending[next(iter(_pending))]
        _pending[state] = PendingSignIn(
            provider.id, _digest(transaction), verifier, nonce, redirect_uri, next_path, now + _PENDING_SECONDS
        )
    return f"{endpoint}{'&' if '?' in endpoint else '?'}{query}", transaction


def claim_sign_in(state: str, *, transaction: str | None, provider_id: str) -> PendingSignIn:
    """Take the sign-in *state* names, which this browser started with *provider_id*; it is used once.

    Its *transaction* cookie and the provider are checked before the sign-in is used up, so a stray
    callback cannot spend someone else's. Raises TokenRejected for that, or an unknown or expired state.
    """
    with _pending_lock:
        pending = _pending.get(state)
        if pending is None or pending.expires <= time.monotonic():
            _pending.pop(state, None)
            raise TokenRejected("the sign-in is unknown or has expired")
        if not transaction or not hmac.compare_digest(pending.transaction, _digest(transaction)):
            raise TokenRejected("the callback did not come from the browser that started the sign-in")
        if pending.provider_id != provider_id:
            raise TokenRejected(f"the sign-in was started with {pending.provider_id}")
        del _pending[state]
    return pending


def finish_sign_in(pending: PendingSignIn, code: str, *, providers: tuple[OidcProvider, ...]) -> Person:
    """Exchange the code the provider returned for its ID token, and return the person it names.

    Raises TokenRejected for a provider no longer configured, a failed exchange, or an ID token
    that does not check out.
    """
    provider = next((p for p in providers if p.id == pending.provider_id), None)
    if provider is None:
        raise TokenRejected(f"provider {pending.provider_id} is no longer configured")
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": pending.redirect_uri,
        "code_verifier": pending.verifier,
        "client_id": provider.client_id,
    }
    # A confidential client authenticates with its secret (client_secret_basic); a public one has only PKCE.
    auth = (provider.client_id, provider.client_secret) if provider.client_secret else None
    try:
        response = httpx.post(str(discovery(provider)["token_endpoint"]), data=form, auth=auth, timeout=_HTTP_TIMEOUT)
        response.raise_for_status()
        token = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise TokenRejected(f"{provider.id}: the code exchange failed: {exc}") from exc
    if not isinstance(token, dict):
        raise TokenRejected(f"{provider.id}: the token response is not an object")
    id_token = token.get("id_token")
    if not isinstance(id_token, str):
        raise TokenRejected(f"{provider.id}: the provider returned no ID token")
    _, claims = _decoded(id_token, provider)
    _check(claims, provider, (provider.client_id,))
    if claims.get("nonce") != pending.nonce:
        raise TokenRejected(f"{provider.id}: the ID token answers another sign-in")
    return _person(claims, provider)
