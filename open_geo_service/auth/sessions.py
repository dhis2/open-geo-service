"""Web interface sessions: a random id in a cookie, mapped in memory to who signed in.

Who signed in is a named token or a person from an OIDC provider. The cookie never holds a
token. Sessions live in this process, so a restart signs everyone out; Open Geo Service runs as
one process. A session ends at sign-out, after `LIFETIME`, or when its token is no longer
configured with the same secret or its person's provider is no longer configured. A person's
tier is decided on each request from the configuration, not kept in the session.
"""

from __future__ import annotations

import secrets
import threading
import time
from dataclasses import dataclass
from http.cookies import SimpleCookie

from open_geo_service.auth.oidc import Person, principal_for_person
from open_geo_service.auth.tokens import AccessSettings, NamedToken, Principal, principal_for

COOKIE = "geoservice_session"
LIFETIME_SECONDS = 8 * 60 * 60


@dataclass(frozen=True)
class _Session:
    who: NamedToken | Person
    expires: float


_sessions: dict[str, _Session] = {}
_lock = threading.Lock()


def start(who: NamedToken | Person) -> str:
    """Open a session for a token or a person that signed in, and return the id the cookie carries."""
    session_id = secrets.token_urlsafe(32)
    now = time.monotonic()
    with _lock:
        # Expired sessions are dropped here, so the table stays bounded by recent sign-ins.
        for stale in [key for key, session in _sessions.items() if session.expires <= now]:
            del _sessions[stale]
        _sessions[session_id] = _Session(who, now + LIFETIME_SECONDS)
    return session_id


def resolve(session_id: str, settings: AccessSettings) -> Principal | None:
    """The principal a session id stands for; None when it is unknown, expired, or its token or provider changed."""
    with _lock:
        session = _sessions.get(session_id)
        if session is None:
            return None
        principal: Principal | None = None
        if session.expires > time.monotonic():
            if isinstance(session.who, Person):
                principal = principal_for_person(session.who, settings.providers)
            elif session.who in settings.tokens:
                principal = principal_for(session.who)
        if principal is None:
            del _sessions[session_id]
    return principal


def end(session_id: str) -> None:
    """End a session at sign-out; an unknown id is ignored."""
    with _lock:
        _sessions.pop(session_id, None)


def cookie(session_id: str, path: str) -> str:
    """The `Set-Cookie` value for a session, scoped to *path*."""
    return set_cookie(COOKIE, session_id, path, LIFETIME_SECONDS)


def expired_cookie(path: str) -> str:
    """The `Set-Cookie` value that removes the session cookie at *path*."""
    return set_cookie(COOKIE, "", path, 0)


def set_cookie(name: str, value: str, path: str, max_age: int) -> str:
    """A `Set-Cookie` value: host-only, `Secure`, `HttpOnly`, `SameSite=Lax`, scoped to *path*."""
    # Secure: browsers keep it over https and on localhost, so sign-in needs one of the two.
    jar = SimpleCookie()
    jar[name] = value
    morsel = jar[name]
    morsel["path"] = path
    morsel["max-age"] = max_age
    morsel["httponly"] = True
    morsel["secure"] = True
    morsel["samesite"] = "Lax"
    return morsel.OutputString()
