"""Web interface sign-in: a token posted once, or a person back from their OIDC provider, becomes a session cookie.

See `auth/sessions.py` for the session and `auth/oidc.py` for the provider's side.
"""

from __future__ import annotations

import logging
import urllib.parse
from typing import Annotated

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from open_geo_service.auth import oidc, sessions
from open_geo_service.auth.middleware import log_rejected_credential, page_origins, session_cookie_path
from open_geo_service.auth.tokens import (
    OidcProvider,
    access_settings,
    authenticate,
    authentication_on,
    configured_tokens,
)
from open_geo_service.shared.urls import absolute_url, mount_prefix

logger = logging.getLogger(__name__)

# Holds the secret that ties a provider sign-in to the browser that started it (`oidc.begin_sign_in`).
_TRANSACTION_COOKIE = "geoservice_oidc"
_TRANSACTION_SECONDS = 10 * 60

router = APIRouter(include_in_schema=False)


def _require_authentication() -> None:
    # With authentication off there is nothing to sign in to.
    if not authentication_on():
        raise HTTPException(status_code=404)


def _provider_or_404(provider_id: str) -> OidcProvider:
    provider = next((p for p in access_settings().providers if p.id == provider_id), None)
    if provider is None:
        raise HTTPException(status_code=404)
    return provider


_LOOPBACK = frozenset({"localhost", "127.0.0.1", "::1"})


def _secure_page(request: Request) -> bool:
    """Whether the browser reached this page over HTTPS or on this machine, as far as the server can tell.

    Elsewhere the form would send the token in clear, and the browser would not keep the
    `Secure` cookie anyway. The origin is the one the request addressed, or
    `GEOSERVICE_BASE_URL`'s when it names the same host, as behind a TLS proxy. A proxy that
    rewrites `Host` to a loopback address passes this check, so the page also shows the form
    only in a secure context, which only the browser can see.
    """
    host = request.headers.get("host", "")
    for origin in page_origins(request.scope):
        split = urllib.parse.urlsplit(origin)
        name = split.hostname or ""
        if split.netloc == host and (split.scheme == "https" or name in _LOOPBACK or name.endswith(".localhost")):
            return True
    return False


def _local_path(candidate: str | None, mount: str) -> str:
    """A path on this instance to return to after signing in; the landing page for anything else."""
    split = urllib.parse.urlsplit(candidate or "")
    path = split.path
    # Only a path under the mount, never another origin or a scheme-relative `//host`; browsers
    # read a backslash as a slash, so `/\host` is one too.
    if split.scheme or split.netloc or not path.startswith(f"{mount}/") or path.startswith("//") or "\\" in path:
        return f"{mount}/"
    if path in (f"{mount}/sign-in", f"{mount}/sign-out"):
        return f"{mount}/"
    return path + (f"?{split.query}" if split.query else "")


@router.get("/sign-in", response_class=HTMLResponse)
def sign_in_page(request: Request, next: str | None = None) -> HTMLResponse:
    """The form that takes a token; it returns to *next*, or else the page the visitor came from."""
    _require_authentication()
    from open_geo_service.system.templates import SIGN_IN_NEEDS_HTTPS, render_sign_in_page

    mount = mount_prefix(request)
    referer = urllib.parse.urlsplit(request.headers.get("referer", ""))
    came_from = referer.path + (f"?{referer.query}" if referer.query else "")
    same_host = referer.netloc == request.headers.get("host")
    next_path = _local_path(next if next is not None else came_from if same_host else None, mount)
    if not _secure_page(request):
        return HTMLResponse(render_sign_in_page(mount, next_path=next_path, error=SIGN_IN_NEEDS_HTTPS, form=False))
    return HTMLResponse(render_sign_in_page(mount, next_path=next_path))


@router.post("/sign-in", response_class=HTMLResponse)
def sign_in(request: Request, token: Annotated[str, Form()], next: Annotated[str, Form()] = "") -> Response:
    """Exchange a token for a session cookie, then return to *next*."""
    if not configured_tokens():
        raise HTTPException(status_code=404)
    from open_geo_service.system.templates import SIGN_IN_NEEDS_HTTPS, render_sign_in_page

    mount = mount_prefix(request)
    if not _secure_page(request):
        page = render_sign_in_page(mount, next_path=_local_path(next, mount), error=SIGN_IN_NEEDS_HTTPS, form=False)
        return HTMLResponse(page, status_code=403)
    principal = authenticate(f"Bearer {token.strip()}", configured_tokens())
    if principal is None or principal.token is None:
        log_rejected_credential()
        page = render_sign_in_page(mount, next_path=_local_path(next, mount), error="That token is not valid here.")
        return HTMLResponse(page, status_code=401)
    response = RedirectResponse(_local_path(next, mount), status_code=303)
    session_id = sessions.start(principal.token)
    response.headers.append("set-cookie", sessions.cookie(session_id, session_cookie_path(request)))
    return response


@router.get("/sign-in/oidc/{provider_id}")
def sign_in_with_provider(request: Request, provider_id: str, next: str | None = None) -> Response:
    """Send the browser to the provider to sign in; it comes back to the callback below."""
    _require_authentication()
    provider = _provider_or_404(provider_id)
    from open_geo_service.system.templates import SIGN_IN_NEEDS_HTTPS, render_sign_in_page

    mount = mount_prefix(request)
    next_path = _local_path(next, mount)
    if not _secure_page(request):
        page = render_sign_in_page(mount, next_path=next_path, error=SIGN_IN_NEEDS_HTTPS, form=False)
        return HTMLResponse(page, status_code=403)
    redirect_uri = absolute_url(request, f"/sign-in/oidc/{provider.id}/callback")
    try:
        url, transaction = oidc.begin_sign_in(provider, redirect_uri=redirect_uri, next_path=next_path)
    except Exception as exc:  # noqa: BLE001 - an unreachable or misconfigured provider, whatever the cause
        logger.warning("Could not start sign-in with %s: %s", provider.id, exc)
        error = f"Could not reach {provider.title}. Try again later, or ask the operator to check its configuration."
        return HTMLResponse(render_sign_in_page(mount, next_path=next_path, error=error), status_code=502)
    response = RedirectResponse(url, status_code=303)
    callback = _callback_path(mount, provider)
    cookie = sessions.set_cookie(_TRANSACTION_COOKIE, transaction, callback, _TRANSACTION_SECONDS)
    response.headers.append("set-cookie", cookie)
    return response


def _callback_path(mount: str, provider: OidcProvider) -> str:
    """Where the transaction cookie is sent: only the provider's callback, under the deployment prefix."""
    return f"{mount}/sign-in/oidc/{provider.id}/callback"


@router.get("/sign-in/oidc/{provider_id}/callback")
def provider_callback(
    request: Request, provider_id: str, state: str = "", code: str | None = None, error: str | None = None
) -> Response:
    """Where the provider returns the browser: exchange its code for the person, and start their session."""
    _require_authentication()
    provider = _provider_or_404(provider_id)
    from open_geo_service.system.templates import render_sign_in_page

    mount = mount_prefix(request)
    failed = f"Signing in with {provider.title} did not succeed. Try again."
    response: Response
    if error is not None or code is None:
        # The person cancelled, or the provider refused; its `error` is a short code such as access_denied.
        logger.info("Sign-in with %s returned %s", provider.id, error or "no code")
        response = HTMLResponse(render_sign_in_page(mount, next_path=f"{mount}/", error=failed), status_code=401)
    else:
        try:
            person, next_path = oidc.finish_sign_in(
                state,
                code,
                transaction=request.cookies.get(_TRANSACTION_COOKIE),
                provider_id=provider.id,
                providers=access_settings().providers,
            )
        except oidc.TokenRejected as exc:
            log_rejected_credential()
            logger.info("Sign-in with %s rejected: %s", provider.id, exc)
            response = HTMLResponse(render_sign_in_page(mount, next_path=f"{mount}/", error=failed), status_code=401)
        else:
            response = RedirectResponse(next_path, status_code=303)
            session_id = sessions.start(person)
            response.headers.append("set-cookie", sessions.cookie(session_id, session_cookie_path(request)))
    # The transaction ends here, whatever the outcome.
    ended = sessions.set_cookie(_TRANSACTION_COOKIE, "", _callback_path(mount, provider), 0)
    response.headers.append("set-cookie", ended)
    return response


@router.post("/sign-out")
def sign_out(request: Request) -> Response:
    """End the session and clear its cookie."""
    _require_authentication()
    session_id = request.cookies.get(sessions.COOKIE)
    if session_id:
        sessions.end(session_id)
    response = RedirectResponse(f"{mount_prefix(request)}/", status_code=303)
    response.headers.append("set-cookie", sessions.expired_cookie(session_cookie_path(request)))
    return response
