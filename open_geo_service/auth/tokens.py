"""The ``authentication`` block: named API tokens, OIDC providers, and the principal a request runs as."""

from __future__ import annotations

import base64
import binascii
import hmac
import os
import urllib.parse
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from open_geo_service import config as api_config

MIN_SECRET_LENGTH = 32


class Tier(IntEnum):
    """What a principal may do; a higher tier includes every lower one."""

    PUBLIC = 0
    AUTHENTICATED = 1
    OPERATOR = 2


@dataclass(frozen=True)
class NamedToken:
    """One machine client's token, with the secret read from its environment variable."""

    id: str
    tier: Tier
    secret: str = field(repr=False)


@dataclass(frozen=True)
class OidcProvider:
    """An OIDC provider people sign in through, with the client this instance is registered as."""

    id: str
    issuer: str
    client_id: str
    title: str
    audience: tuple[str, ...]
    access_tokens: Literal["rfc9068", "keycloak", "dhis2"]
    """How the provider marks an access token apart from an ID token (`oidc._require_access_token`)."""
    scopes: tuple[str, ...]
    openeo_client_id: str | None
    openeo_grant_types: tuple[str, ...]
    client_secret: str | None = field(default=None, repr=False)


@dataclass(frozen=True)
class Principal:
    """Who a request runs as, and the tier it was granted."""

    id: str
    tier: Tier
    token: NamedToken | None = None
    name: str | None = None
    """The username a person signed in with; None for a token or the anonymous caller."""

    @property
    def label(self) -> str:
        """How the web interface names the principal: the token id or the person's username."""
        return self.token.id if self.token is not None else self.name or self.id

    @property
    def owner_id(self) -> str | None:
        """The owner recorded on what this principal creates.

        None while authentication is off: `OPEN` shares its id with `ANONYMOUS`, so a record it
        owned would become readable without a token once authentication is turned on. An
        ownerless record is reached only by operators.
        """
        return None if self is OPEN else self.id

    def may_access(self, owner: str | None) -> bool:
        """Whether this principal may read or change a record `owner` created.

        A record from before principals were recorded has no owner, and only operators reach it.
        """
        return self.tier is Tier.OPERATOR or (owner is not None and owner == self.id)


def principal_for(token: NamedToken) -> Principal:
    """The principal a configured token runs as."""
    return Principal(id=f"token:{token.id}", tier=token.tier, token=token)


ANONYMOUS = Principal(id="anonymous", tier=Tier.PUBLIC)
"""A caller without a credential while authentication is on."""

OPEN = Principal(id="anonymous", tier=Tier.OPERATOR)
"""Every caller while authentication is off: tiers and ownership are not enforced."""


class _TokenConfig(BaseModel):
    # A secret pasted into `token_env` by mistake must not be echoed in the startup error.
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    token_env: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    tier: Literal["authenticated", "operator"]


_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
# Each list entry is one OAuth 2.0 scope-token (RFC 6749 section 3.3).
_Scope = Annotated[str, Field(pattern=r"^[\x21\x23-\x5B\x5D-\x7E]+$")]


class _OidcConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    issuer: str
    client_id: str = Field(min_length=1)
    client_secret_env: str | None = Field(default=None, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    title: str | None = None
    audience: list[str] | None = None
    access_tokens: Literal["rfc9068", "keycloak", "dhis2"] = "rfc9068"
    scopes: list[_Scope] = ["openid", "profile"]
    openeo_client_id: str | None = None
    openeo_grant_types: list[str] = ["authorization_code+pkce", "refresh_token"]

    @field_validator("issuer")
    @classmethod
    def _https(cls, issuer: str) -> str:
        # Over plain HTTP anyone on the path could serve their own keys; a local provider is fine.
        split = urllib.parse.urlsplit(issuer)
        if split.scheme != "https" and not (split.scheme == "http" and split.hostname in _LOOPBACK_HOSTS):
            raise ValueError("an issuer must be an https URL, or http on this machine")
        # OpenID Connect issuers have neither, and a principal id `oidc:<issuer>#<subject>` relies on it.
        if split.query or split.fragment or "#" in issuer:
            raise ValueError("an issuer has no query or fragment")
        return issuer

    @field_validator("scopes")
    @classmethod
    def _require_openid(cls, scopes: list[str]) -> list[str]:
        if "openid" not in scopes:
            raise ValueError("OIDC scopes must include openid")
        return scopes


class _AuthenticationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    tokens: list[_TokenConfig] = []
    oidc: list[_OidcConfig] = []
    require_for_read: bool = False


@dataclass(frozen=True)
class AccessSettings:
    """What the middleware needs on every request, from one read of the `authentication` block."""

    tokens: tuple[NamedToken, ...] = ()
    providers: tuple[OidcProvider, ...] = ()
    require_for_read: bool = False

    @property
    def on(self) -> bool:
        """Whether authentication is on: at least one token or OIDC provider is configured."""
        return bool(self.tokens or self.providers)


def _settings() -> _AuthenticationConfig | None:
    raw = api_config.get_config().get("authentication")
    if raw is None:
        return None
    try:
        return _AuthenticationConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"authentication in GEOSERVICE_CONFIG is invalid: {exc}") from exc


def auth_required_for_read() -> bool:
    """Whether `authentication.require_for_read` asks for a credential on reads too (see `route_table.gated`)."""
    settings = _settings()
    return settings is not None and settings.require_for_read


def configured_tokens() -> tuple[NamedToken, ...]:
    """The named tokens, read from config and the environment; empty when none are configured.

    Raises as :func:`access_settings` does.
    """
    return access_settings().tokens


def authentication_on() -> bool:
    """Whether authentication is on: at least one token or OIDC provider is configured."""
    return access_settings().on


def access_settings() -> AccessSettings:
    """The named tokens, the OIDC providers and whether reads are gated.

    Raises ValueError on an invalid block, an unset variable, a secret shorter than
    MIN_SECRET_LENGTH, or a repeated id or secret, so startup refuses a configuration that would
    leave a client unable to authenticate or two clients indistinguishable. Also raises when
    `require_for_read` is set without a token or provider, which would leave reads open while seeming
    private.
    """
    settings = _settings()
    if settings is None:
        return AccessSettings()
    if settings.require_for_read and not (settings.tokens or settings.oidc):
        raise ValueError("authentication.require_for_read needs a token in authentication.tokens or a provider in oidc")
    tokens: list[NamedToken] = []
    for index, item in enumerate(settings.tokens):
        where = f"authentication.tokens[{index}] ({item.id})"
        secret = os.environ.get(item.token_env, "").strip()
        if not secret:
            raise ValueError(f"{where}: environment variable {item.token_env} is not set")
        if len(secret) < MIN_SECRET_LENGTH:
            raise ValueError(f"{where}: the secret in {item.token_env} is shorter than {MIN_SECRET_LENGTH} characters")
        if any(token.id == item.id for token in tokens):
            raise ValueError(f"{where}: the id is used by an earlier token")
        if any(hmac.compare_digest(token.secret, secret) for token in tokens):
            raise ValueError(f"{where}: the secret in {item.token_env} is used by an earlier token")
        tokens.append(NamedToken(id=item.id, tier=Tier[item.tier.upper()], secret=secret))
    return AccessSettings(tuple(tokens), _providers(settings.oidc), settings.require_for_read)


def _providers(items: list[_OidcConfig]) -> tuple[OidcProvider, ...]:
    providers: list[OidcProvider] = []
    for index, item in enumerate(items):
        where = f"authentication.oidc[{index}] ({item.id})"
        if any(provider.id == item.id for provider in providers):
            raise ValueError(f"{where}: the id is used by an earlier provider")
        client_secret = None
        if item.client_secret_env is not None:
            client_secret = os.environ.get(item.client_secret_env, "").strip()
            if not client_secret:
                raise ValueError(f"{where}: environment variable {item.client_secret_env} is not set")
        providers.append(
            OidcProvider(
                id=item.id,
                issuer=item.issuer,
                client_id=item.client_id,
                title=item.title or item.id,
                # An explicit list is the whole rule; without one, tokens for this instance's client
                # and for the openEO client it advertises.
                audience=tuple(item.audience or [item.client_id, *filter(None, [item.openeo_client_id])]),
                access_tokens=item.access_tokens,
                scopes=tuple(item.scopes),
                openeo_client_id=item.openeo_client_id,
                openeo_grant_types=tuple(item.openeo_grant_types),
                client_secret=client_secret,
            )
        )
    return tuple(providers)


def owner_principal(owner: str | None) -> Principal | None:
    """The principal a recorded owner runs as when no request carries it, as a batch job does.

    None, which limits nothing, for work no client started (a pipeline run, a derived dataset
    refresh) and while authentication is off. A person runs with the authenticated tier, as they do
    in a request. An owner whose token or provider is no longer configured keeps its id and the
    public tier, so it still reaches only its own records.
    """
    settings = access_settings()
    if owner is None or not settings.on:
        return None
    for token in settings.tokens:
        if owner == f"token:{token.id}":
            return principal_for(token)
    if any(owner.startswith(f"oidc:{provider.issuer}#") for provider in settings.providers):
        return Principal(id=owner, tier=Tier.AUTHENTICATED)
    return Principal(id=owner, tier=Tier.PUBLIC)


def authenticate(authorization: str | None, tokens: tuple[NamedToken, ...]) -> Principal | None:
    """The principal an ``Authorization`` header names: anonymous without one, None when invalid.

    Accepts ``Bearer <secret>``, openEO's ``Bearer basic//<secret>``, and ``Basic`` with the
    token id and secret, which is what openEO clients send to ``GET /credentials/basic``.
    """
    if not authorization:
        return ANONYMOUS
    scheme, _, credential = authorization.partition(" ")
    credential = credential.strip()
    token_id: str | None = None
    if scheme.lower() == "bearer":
        secret = credential.removeprefix("basic//")
    elif scheme.lower() == "basic":
        try:
            token_id, _, secret = base64.b64decode(credential, validate=True).decode().partition(":")
        except (binascii.Error, UnicodeDecodeError):
            return None
    else:
        return None
    # Every token is compared, so the time taken does not say which one came close.
    matches = [token for token in tokens if hmac.compare_digest(token.secret.encode(), secret.encode())]
    if not matches or (token_id is not None and matches[0].id != token_id):
        return None
    return principal_for(matches[0])
