"""Tests for named API tokens, tiers and the route table (CLIM-1292), web sign-in (CLIM-1293), and OIDC (CLIM-1480)."""

from __future__ import annotations

import asyncio
import base64
import re
import secrets
import time
import urllib.parse
from dataclasses import dataclass
from datetime import UTC, datetime
from http.cookiejar import DefaultCookiePolicy
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import Response
from fastapi.testclient import TestClient
from joserfc import jwt
from joserfc.jwk import KeySet, RSAKey
from starlette.types import Message

from open_geo_service import config as api_config
from open_geo_service.auth import middleware, oidc, sessions
from open_geo_service.auth.route_table import ROUTE_TABLE, UNGATED, route_tier
from open_geo_service.auth.tokens import Tier, configured_tokens
from open_geo_service.ingestions import services as ingestion_services
from open_geo_service.ingestions.schemas import ArtifactFormat
from open_geo_service.main import app
from open_geo_service.openeo.jobs import get_openeo_job_service
from open_geo_service.stac import services as stac_services
from open_geo_service.stac.services import AUTHENTICATION_EXTENSION
from open_geo_service.system.templates import actions_closed

from .conftest import MountedClientFactory

OPERATOR_SECRET = "o" * 32
CLIENT_SECRET = "c" * 40
OTHER_SECRET = "d" * 40
DECLARED = {route: tier for tier, routes in ROUTE_TABLE.items() for route in routes}


def _live_routes() -> tuple[set[str], set[str]]:
    """Every method and path the application serves, including routes left out of OpenAPI, and
    the paths of entries that serve any method below them, such as a `Mount`."""
    live: set[str] = set()
    mounts: set[str] = set()
    # FastAPI 0.162 keeps an included router as one entry in `app.routes`; its routes, with the
    # prefix applied, come from `effective_route_contexts()`.
    for entry in app.routes:
        included = getattr(entry, "effective_route_contexts", None)
        for route in included() if included else [entry]:
            path = getattr(route, "path", "")
            methods = getattr(route, "methods", None)
            if not methods:
                mounts.add(path)
            for method in methods or set():
                live.add(f"{'GET' if method == 'HEAD' else method} {path}")
    return live, mounts


def _under(route: str, mount: str) -> bool:
    path = route.split(" ", 1)[1]
    return path == mount or path.startswith(f"{mount.rstrip('/')}/")


def _concrete(path: str) -> str:
    return re.sub(r"\{[^}]+\}", "abc", path)


def _configure(monkeypatch: pytest.MonkeyPatch, tokens: list[dict[str, Any]], *, gate_reads: bool = False) -> None:
    authentication = {"tokens": tokens, "gate_reads": gate_reads}
    monkeypatch.setattr(api_config, "_cache", {**api_config.get_config(), "authentication": authentication})


@pytest.fixture
def auth_on(monkeypatch: pytest.MonkeyPatch) -> None:
    _turn_on(monkeypatch)


@pytest.fixture
def reads_gated(monkeypatch: pytest.MonkeyPatch) -> None:
    _turn_on(monkeypatch, gate_reads=True)


def _turn_on(monkeypatch: pytest.MonkeyPatch, *, gate_reads: bool = False) -> None:
    monkeypatch.setenv("GEOSERVICE_TEST_OPERATOR_TOKEN", OPERATOR_SECRET)
    monkeypatch.setenv("GEOSERVICE_TEST_CLIENT_TOKEN", CLIENT_SECRET)
    monkeypatch.setenv("GEOSERVICE_TEST_OTHER_TOKEN", OTHER_SECRET)
    _configure(
        monkeypatch,
        [
            {"id": "caps", "token_env": "GEOSERVICE_TEST_OPERATOR_TOKEN", "tier": "operator"},
            {"id": "notebook", "token_env": "GEOSERVICE_TEST_CLIENT_TOKEN", "tier": "authenticated"},
            {"id": "other", "token_env": "GEOSERVICE_TEST_OTHER_TOKEN", "tier": "authenticated"},
        ],
        gate_reads=gate_reads,
    )


def _bearer(secret: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {secret}"}


ISSUER = "https://idp.example"
CLIENT_ID = "open-geo-service"
_SIGNING_KEY = RSAKey.generate_key(2048, parameters={"kid": "k1", "alg": "RS256"})


class FakeProvider:
    """An OIDC provider for `auth/oidc.py`: discovery, its keys and a token endpoint, without a network."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.keys = KeySet([_SIGNING_KEY])
        self.codes: dict[str, dict[str, Any]] = {}
        oidc.reset_caches()
        monkeypatch.setattr(oidc, "_get_json", self._get_json)
        monkeypatch.setattr(httpx, "post", self._post)

    def _get_json(self, url: str) -> Any:
        if url == f"{ISSUER}/.well-known/openid-configuration":
            return {
                "issuer": ISSUER,
                "authorization_endpoint": f"{ISSUER}/authorize",
                "token_endpoint": f"{ISSUER}/token",
                "jwks_uri": f"{ISSUER}/keys",
            }
        assert url == f"{ISSUER}/keys"
        return self.keys.as_dict(private=False)

    def token(
        self,
        username: str,
        *,
        header_typ: str | None = "at+jwt",
        aud: str = CLIENT_ID,
        expires_in: int = 300,
        key: RSAKey = _SIGNING_KEY,
        **claims: Any,
    ) -> str:
        """An RFC 9068 access token for *username*, with *header_typ* in its header; a claim set to None is left out."""
        now = int(time.time())
        payload = {
            "iss": ISSUER,
            "sub": f"sub-{username}",
            "aud": aud,
            "iat": now,
            "exp": now + expires_in,
            "jti": secrets.token_hex(8),
            "client_id": CLIENT_ID,
            "scope": "openid profile",
            "preferred_username": username,
            **claims,
        }
        header = {"alg": "RS256", "kid": key.kid, **({"typ": header_typ} if header_typ else {})}
        return jwt.encode(header, {k: v for k, v in payload.items() if v is not None}, key)

    def bearer(self, username: str) -> dict[str, str]:
        """openEO's form of an OIDC access token."""
        return {"Authorization": f"Bearer oidc/idp/{self.token(username)}"}

    def _post(self, url: str, *, data: dict[str, str], **_: Any) -> httpx.Response:
        # The token endpoint: answers a code it issued, once, with an ID token bound to the sign-in's nonce.
        assert url == f"{ISSUER}/token" and data["grant_type"] == "authorization_code"
        issued = self.codes.pop(data["code"], None)
        request = httpx.Request("POST", url)
        if issued is None or oidc._s256(data["code_verifier"]) != issued["challenge"]:
            return httpx.Response(400, json={"error": "invalid_grant"}, request=request)
        id_token = self.token(issued["username"], header_typ="JWT", nonce=issued["nonce"], scope=None, client_id=None)
        return httpx.Response(200, json={"access_token": "opaque", "id_token": id_token}, request=request)

    def approve_query(self, query: dict[str, str], username: str) -> str:
        """Sign *username* in at the provider for an authorization request; return the code it sends back."""
        code = f"code-{username}"
        self.codes[code] = {"username": username, "nonce": query["nonce"], "challenge": query["code_challenge"]}
        return code


def _configure_oidc(
    monkeypatch: pytest.MonkeyPatch, *, tokens: list[dict[str, Any]], gate_reads: bool, **settings: Any
) -> None:
    provider = {"id": "idp", "issuer": ISSUER, "client_id": CLIENT_ID, "title": "Example ID", **settings}
    authentication = {"tokens": tokens, "oidc": [provider], "gate_reads": gate_reads}
    monkeypatch.setattr(api_config, "_cache", {**api_config.get_config(), "authentication": authentication})


@dataclass(frozen=True)
class People:
    """Credentials of callers: one who creates records, someone else, and an operator where there is one."""

    mine: dict[str, str]
    other: dict[str, str]
    operator: dict[str, str] | None


@pytest.fixture(params=["tokens", "oidc"])
def people(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> People:
    """Named tokens on an open instance, or people from an OIDC provider on a gated one with no tokens.

    The second is where a resumed batch job finds its owner from the provider configuration alone.
    It has no operator: people get the authenticated tier, and operators use named tokens.
    """
    if request.param == "tokens":
        _turn_on(monkeypatch)
        return People(_bearer(CLIENT_SECRET), _bearer(OTHER_SECRET), _bearer(OPERATOR_SECRET))
    _configure_oidc(monkeypatch, tokens=[], gate_reads=True)
    provider = FakeProvider(monkeypatch)
    return People(provider.bearer("alice"), provider.bearer("bob"), None)


def _finished(client: TestClient, job_id: str, headers: dict[str, str]) -> str:
    """Start a batch job and return the status it ends in."""
    client.post(f"/jobs/{job_id}/results", headers=headers)
    deadline = time.monotonic() + 30
    while (status := client.get(f"/jobs/{job_id}", headers=headers).json()["status"]) not in ("error", "finished"):
        assert time.monotonic() < deadline
        time.sleep(0.1)
    return str(status)


def _basic(token_id: str, secret: str) -> dict[str, str]:
    return {"Authorization": "Basic " + base64.b64encode(f"{token_id}:{secret}".encode()).decode()}


# --- the route table ------------------------------------------------------------------


def test_every_served_route_is_in_the_route_table() -> None:
    """A new route fails here until it is given a tier; until then it needs the operator tier."""
    live, mounts = _live_routes()
    assert sorted(live - DECLARED.keys()) == [], "add each route to ROUTE_TABLE in auth/route_table.py"
    # A mount (/mcp, say) is listed by the methods and paths it serves, e.g. "POST /mcp/{path:path}".
    assert sorted(m for m in mounts if not any(_under(r, m) for r in DECLARED)) == [], "give each mount a tier"
    mounted = {route for route in DECLARED if any(_under(route, m) for m in mounts)}
    assert sorted(DECLARED.keys() - live - mounted) == [], "routes listed in ROUTE_TABLE that the app no longer serves"
    decided = {route: route_tier(route.split(" ")[0], _concrete(route.split(" ")[1])) for route in DECLARED}
    assert {route: tier for route, tier in decided.items() if tier != DECLARED[route]} == {}
    assert route_tier("GET", "/not-a-route") == Tier.OPERATOR
    public = {route for route, tier in DECLARED.items() if tier is Tier.PUBLIC}
    assert sorted(UNGATED - public) == [], "UNGATED lists only public routes"


@pytest.mark.parametrize("route", sorted(route for route, tier in DECLARED.items() if tier > Tier.PUBLIC))
def test_gated_routes_need_a_token(auth_on: None, client: TestClient, route: str) -> None:
    method, path = route.split(" ", 1)
    response = client.request(method, _concrete(path), json={})

    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"
    assert response.json()["code"] == "AuthenticationRequired"


# --- the middleware -------------------------------------------------------------------


@pytest.mark.parametrize(
    "path", ["/", "/.well-known/openeo", "/openapi.json", "/credentials/oidc", "/stac/catalog.json"]
)
def test_reads_and_discovery_stay_public(auth_on: None, client: TestClient, path: str) -> None:
    response = client.get(path)
    assert response.status_code == 200
    if path == "/stac/catalog.json":
        assert "auth:schemes" not in response.json()


def test_refuses_before_reading_the_body(auth_on: None) -> None:
    sent: list[Message] = []

    async def receive() -> Message:
        raise AssertionError("the body was read")

    async def send(message: Message) -> None:
        sent.append(message)

    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "path": "/jobs",
        "raw_path": b"/jobs",
        "root_path": "",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json"), (b"content-length", b"1000000000")],
        "server": ("testserver", 80),
        "client": ("testclient", 50000),
        "scheme": "http",
    }
    asyncio.run(app(scope, receive, send))

    assert sent[0]["status"] == 401


def test_a_token_is_granted_its_tier(auth_on: None, client: TestClient) -> None:
    assert client.get("/me", headers=_bearer(CLIENT_SECRET)).json()["user_id"] == "token:notebook"
    assert client.get("/jobs", headers=_bearer(CLIENT_SECRET)).status_code == 200
    # openEO clients prefix the secret with the method they authenticated by.
    assert client.get("/jobs", headers=_bearer(f"basic//{CLIENT_SECRET}")).status_code == 200

    refused = client.post("/manage/sync", headers=_bearer(CLIENT_SECRET))
    assert refused.status_code == 403
    assert refused.json()["code"] == "PermissionsInsufficient"
    assert client.post("/schedules/sync/abc/pause", headers=_bearer(OPERATOR_SECRET)).status_code not in (401, 403)


def test_an_invalid_credential_is_refused_even_on_a_public_route(auth_on: None, client: TestClient) -> None:
    for headers in (_bearer("x" * 32), {"Authorization": "Token abc"}, {"Authorization": "Basic !!"}):
        response = client.get("/datasets", headers=headers)
        assert response.status_code == 401
        assert response.json()["code"] == "TokenInvalid"


def test_a_path_no_route_serves_is_not_found(auth_on: None, client: TestClient) -> None:
    # The openEO client asks for job cost estimates, which Open Geo Service does not serve.
    assert client.get("/jobs/abc/estimate").status_code == 404
    assert client.post("/datasets").status_code == 401


def test_without_tokens_nothing_is_checked(client: TestClient) -> None:
    assert client.get("/jobs", headers=_bearer("anything")).status_code == 200
    assert client.get("/me").json()["user_id"] == "anonymous"


@pytest.mark.parametrize("root_path", ["/ocs", "/ocs/"])
def test_a_mount_prefix_does_not_hide_a_gated_route(
    auth_on: None, mounted_client_factory: MountedClientFactory, root_path: str
) -> None:
    mounted = mounted_client_factory(root_path)

    assert mounted.get("/jobs").status_code == 401
    assert mounted.get("/stac/catalog.json").status_code == 200


def test_read_only_applies_before_the_tier(auth_on: None, monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    monkeypatch.setitem(api_config._cache or {}, "read_only", True)

    assert client.post("/manage/sync", headers=_bearer(OPERATOR_SECRET)).json()["id"] == "read-only"
    # Read-only lets POST /result through, but never lowers its tier.
    assert client.post("/result", json={}).status_code == 401
    assert client.post("/result", json={}, headers=_bearer(CLIENT_SECRET)).status_code != 401


# --- ownership --------------------------------------------------------------------------


def test_a_job_is_reached_only_by_its_owner_and_operators(people: People, client: TestClient) -> None:
    graph = {"add": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": True}}
    created = client.post("/jobs", json={"process": {"process_graph": graph}}, headers=people.mine)
    job_id = created.json()["id"]

    def listed(headers: dict[str, str]) -> bool:
        return job_id in {job["id"] for job in client.get("/jobs", headers=headers).json()["jobs"]}

    assert listed(people.mine) and not listed(people.other)
    assert people.operator is None or listed(people.operator)
    other = people.other
    for method, path in [
        ("GET", f"/jobs/{job_id}"),
        ("PATCH", f"/jobs/{job_id}"),
        ("POST", f"/jobs/{job_id}/results"),
        ("GET", f"/jobs/{job_id}/results/result.geojson"),
        ("DELETE", f"/jobs/{job_id}"),
    ]:
        assert client.request(method, path, json={}, headers=other).status_code == 404, f"{method} {path}"
    if people.operator is not None:
        assert client.get(f"/jobs/{job_id}", headers=people.operator).status_code == 200
    assert client.delete(f"/jobs/{job_id}", headers=people.mine).status_code == 204

    # The route table cannot see a `dataset_id` inside the job body: publishing is refused at the write.
    publishes = {
        "sum": {"process_id": "add", "arguments": {"x": 1, "y": 2}},
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "sum"}, "format": "ZARR", "options": {"dataset_id": "mine"}},
            "result": True,
        },
    }
    mine = people.mine
    job_id = client.post("/jobs", json={"process": {"process_graph": publishes}}, headers=mine).json()["id"]
    assert _finished(client, job_id, mine) == "error"
    assert "needs the operator tier" in (get_openeo_job_service().get_job_or_404(job_id).error_message or "")


def test_a_stored_workflow_is_reached_only_by_its_owner_and_operators(people: People, client: TestClient) -> None:
    graph = {"process_graph": {"add": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": True}}}
    assert client.put("/process_graphs/mine", json=graph, headers=people.mine).status_code == 200

    def listed(headers: dict[str, str]) -> bool:
        response = client.get("/process_graphs", headers=headers)
        return response.status_code == 200 and "mine" in {item["id"] for item in response.json()["processes"]}

    assert listed(people.mine) and (people.operator is None or listed(people.operator))
    assert not listed(people.other) and not listed({})
    other = people.other
    assert "aggregate_by_features" in {
        item["id"] for item in client.get("/process_graphs", headers=other).json()["processes"]
    }
    assert client.get("/process_graphs/mine", headers=other).status_code == 404
    assert client.get("/workflows/mine", headers=other).status_code == 404
    assert client.put("/process_graphs/mine", json=graph, headers=other).status_code == 403
    assert client.delete("/process_graphs/mine", headers=other).status_code == 404
    # Nor can it be run by id, synchronously or in a batch job.
    calls_mine = {"process_graph": {"m": {"process_id": "mine", "arguments": {}, "result": True}}}
    assert client.post("/result", json={"process": calls_mine}, headers=people.mine).json() == 3
    assert client.post("/result", json={"process": calls_mine}, headers=other).status_code == 400
    job_id = client.post("/jobs", json={"process": calls_mine}, headers=other).json()["id"]
    assert _finished(client, job_id, other) == "error"
    assert "mine not found" in (get_openeo_job_service().get_job_or_404(job_id).error_message or "")
    # Taking a built-in id would replace it for every caller and pipeline.
    built_in = "/process_graphs/aggregate_by_features"
    assert client.put(built_in, json=graph, headers=people.mine).status_code == 403
    assert client.delete("/process_graphs/mine", headers=people.operator or people.mine).status_code == 204


def test_records_stored_with_authentication_off_are_operator_only(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    # Authentication off stores no owner: its principal shares the anonymous caller's id.
    graph = {"process_graph": {"add": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": True}}}
    assert client.put("/process_graphs/before", json=graph).status_code == 200
    job_id = client.post("/jobs", json={"process": graph}).json()["id"]
    _turn_on(monkeypatch)

    def listed(headers: dict[str, str]) -> bool:
        return "before" in {item["id"] for item in client.get("/process_graphs", headers=headers).json()["processes"]}

    assert not listed({}) and not listed(_bearer(CLIENT_SECRET)) and listed(_bearer(OPERATOR_SECRET))
    assert client.get("/process_graphs/before").status_code == 404
    assert client.get(f"/jobs/{job_id}", headers=_bearer(CLIENT_SECRET)).status_code == 404
    assert client.get(f"/jobs/{job_id}", headers=_bearer(OPERATOR_SECRET)).status_code == 200
    assert client.delete("/process_graphs/before", headers=_bearer(OPERATOR_SECRET)).status_code == 204


# --- openEO -----------------------------------------------------------------------------


def test_openeo_basic_exchange_returns_the_secret(auth_on: None, client: TestClient) -> None:
    assert client.get("/credentials/basic", headers=_basic("notebook", CLIENT_SECRET)).json() == {
        "access_token": CLIENT_SECRET
    }
    assert client.get("/credentials/basic", headers=_basic("caps", CLIENT_SECRET)).status_code == 401
    for headers in ({}, _bearer(CLIENT_SECRET)):
        challenge = client.get("/credentials/basic", headers=headers)
        assert challenge.status_code == 401
        assert challenge.headers["WWW-Authenticate"].startswith("Basic")

    endpoints = {endpoint["path"] for endpoint in client.get("/").json()["endpoints"]}
    assert "/credentials/basic" in endpoints


def test_capabilities_omit_basic_auth_when_authentication_is_off(client: TestClient) -> None:
    endpoints = {endpoint["path"] for endpoint in client.get("/").json()["endpoints"]}
    assert "/credentials/basic" not in endpoints


# --- web sign-in --------------------------------------------------------------------------

SAME_ORIGIN = {"Origin": "https://testserver"}
HTML = {"Accept": "text/html"}


@pytest.fixture
def browser(client: TestClient) -> TestClient:
    """The client over HTTPS, which sign-in needs. It keeps no cookies: each test sends the
    session cookie by hand, so which requests carry it is visible in the test."""
    client.base_url = httpx.URL("https://testserver")
    client.cookies.jar.set_policy(DefaultCookiePolicy(blocked_domains=["testserver.local"]))
    return client


def _session(client: TestClient, secret: str) -> dict[str, str]:
    """Sign in through the form and return the cookie header the browser would send."""
    signed_in = client.post(
        "/sign-in", data={"token": secret, "next": "/datasets"}, headers=SAME_ORIGIN, follow_redirects=False
    )
    assert signed_in.status_code == 303 and signed_in.headers["location"] == "/datasets"
    set_cookie = signed_in.headers["set-cookie"]
    assert {"HttpOnly", "Secure", "SameSite=Lax", "Path=/"} <= {part.strip() for part in set_cookie.split(";")}
    assert secret not in set_cookie
    return {"Cookie": set_cookie.split(";")[0]}


def test_web_sign_in_opens_the_forms_its_tier_allows(
    auth_on: None, browser: TestClient, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    client = browser
    pause = "/schedules/sync/nothing/pause"
    # A page rendered outside a request shows no operator forms.
    assert actions_closed() == "sign-in"
    signed_out = client.get("/data-sources", headers=HTML).text
    assert 'href="/sign-in">Sign in</a>' in signed_out and "Sign out" not in signed_out
    assert "Sign in</a> as an operator to" in client.get("/", headers=HTML).text
    assert client.post(pause, headers=SAME_ORIGIN).status_code == 401

    came_from = client.get("/sign-in", headers={"Referer": "https://testserver/data-sources?q=1"})
    assert 'name="next" value="/data-sources?q=1"' in came_from.text
    monkeypatch.setattr(middleware, "_last_failure_log", float("-inf"))
    wrong = client.post("/sign-in", data={"token": "x" * 40}, headers=SAME_ORIGIN)
    assert wrong.status_code == 401 and "not valid" in wrong.text
    assert "invalid credential" in caplog.text
    other_site = {"Origin": "https://elsewhere.example"}
    refused = client.post("/sign-in", data={"token": OPERATOR_SECRET}, headers=other_site)
    assert refused.status_code == 403 and "GEOSERVICE_BASE_URL" in refused.json()["message"]
    # Only a path on this instance is returned to.
    off_site = client.post(
        "/sign-in", data={"token": CLIENT_SECRET, "next": "//elsewhere.example/"}, headers=SAME_ORIGIN
    )
    assert off_site.url.path == "/"
    assert 'name="next" value="/"' in client.get("/sign-in?next=/%5Celsewhere.example/").text

    # An authenticated token signs in, but the pages' forms need an operator.
    client_cookie = _session(client, CLIENT_SECRET)
    page = client.get("/", headers={**client_cookie, **HTML}).text
    assert "Sign out" in page and "You are signed in as notebook, who is not an operator" in page
    assert client.post(pause, headers={**client_cookie, **SAME_ORIGIN}).status_code == 403

    cookie = _session(client, OPERATOR_SECRET)
    assert "as an operator to" not in client.get("/", headers={**cookie, **HTML}).text
    assert client.post(pause, headers={**cookie, **SAME_ORIGIN}).status_code not in (401, 403)
    assert client.post(pause, headers={**cookie, "Sec-Fetch-Site": "same-origin"}).status_code not in (401, 403)
    # A write from another site, the same host on another scheme, or with no sign of where it
    # came from, is refused.
    assert client.post(pause, headers={**cookie, **other_site}).status_code == 403
    assert client.post(pause, headers={**cookie, "Origin": "http://testserver"}).status_code == 403
    assert client.post(pause, headers=cookie).status_code == 403
    # The page never gets the token back.
    assert client.get("/credentials/basic", headers=cookie).status_code == 401

    signed_out_now = client.post("/sign-out", headers={**cookie, **SAME_ORIGIN}, follow_redirects=False)
    assert signed_out_now.status_code == 303 and "Max-Age=0" in signed_out_now.headers["set-cookie"]
    # The ended session is no credential: pages stay readable and the stale cookie is cleared.
    stale = client.get("/datasets", headers={**cookie, **HTML})
    assert stale.status_code == 200 and "Max-Age=0" in stale.headers["set-cookie"]
    assert client.post(pause, headers={**cookie, **SAME_ORIGIN}).status_code == 401
    # Signing in again with the stale cookie sets only the new one.
    again = client.post(
        "/sign-in", data={"token": OPERATOR_SECRET}, headers={**cookie, **SAME_ORIGIN}, follow_redirects=False
    )
    set_cookies = again.headers.get_list("set-cookie")
    assert len(set_cookies) == 1 and "Max-Age=28800" in set_cookies[0]


def test_a_session_ends_when_it_expires_or_its_token_changes(
    auth_on: None, monkeypatch: pytest.MonkeyPatch, browser: TestClient
) -> None:
    client = browser
    cookie = _session(client, OPERATOR_SECRET)
    assert "caps" in client.get("/", headers={**cookie, **HTML}).text
    monkeypatch.setenv("GEOSERVICE_TEST_OPERATOR_TOKEN", "n" * 32)
    assert "caps" not in client.get("/", headers={**cookie, **HTML}).text

    cookie = _session(client, "n" * 32)
    monkeypatch.setattr(sessions, "LIFETIME_SECONDS", -1)
    expiring = _session(client, "n" * 32)
    assert "caps" in client.get("/", headers={**cookie, **HTML}).text
    assert "caps" not in client.get("/", headers={**expiring, **HTML}).text


def test_sign_in_is_offered_only_over_https_or_on_this_machine(auth_on: None, client: TestClient) -> None:
    over_http = client.get("/sign-in")
    assert "needs HTTPS" in over_http.text and 'name="token"' not in over_http.text
    refused = client.post("/sign-in", data={"token": OPERATOR_SECRET}, headers={"Origin": "http://testserver"})
    assert refused.status_code == 403 and "set-cookie" not in refused.headers
    client.base_url = httpx.URL("http://localhost:8000")
    # The server offers the form; the browser shows it only in a secure context, which a proxy
    # rewriting Host to a loopback address cannot fake.
    local = client.get("/sign-in").text
    assert 'name="token"' in local and 'id="sign-in" hidden' in local and "window.isSecureContext" in local


def test_a_person_is_one_principal_by_web_sign_in_and_by_bearer_token(
    monkeypatch: pytest.MonkeyPatch, browser: TestClient, mounted_client_factory: MountedClientFactory
) -> None:
    client = browser
    monkeypatch.setenv("GEOSERVICE_TEST_CLIENT_TOKEN", CLIENT_SECRET)
    notebook = {"id": "notebook", "token_env": "GEOSERVICE_TEST_CLIENT_TOKEN", "tier": "authenticated"}
    _configure_oidc(monkeypatch, tokens=[notebook], gate_reads=True, openeo_client_id="openeo")
    provider = FakeProvider(monkeypatch)
    advertised = client.get("/credentials/oidc").json()["providers"]
    assert [(p["id"], p["scopes"]) for p in advertised] == [("idp", ["openid", "profile"])]
    assert "Sign in with Example ID" in client.get("/sign-in").text

    def start() -> tuple[str, dict[str, str], dict[str, str]]:
        """Start a sign-in; return the state, the transaction cookie the browser holds, and the authorize query."""
        started = client.get("/sign-in/oidc/idp?next=/datasets", follow_redirects=False)
        query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(started.headers["location"]).query))
        set_cookie = started.headers["set-cookie"]
        attributes = {part.strip() for part in set_cookie.split(";")}
        assert {"HttpOnly", "Secure", "SameSite=Lax", "Path=/sign-in/oidc/idp/callback"} <= attributes
        assert not any(a.lower().startswith("domain=") for a in attributes), "host-only"
        return query["state"], {"Cookie": set_cookie.split(";")[0]}, query

    # To the provider with PKCE, a state and a nonce; back with a code; into a session.
    state, transaction, query = start()
    assert query["code_challenge_method"] == "S256"
    assert query["redirect_uri"] == "https://testserver/sign-in/oidc/idp/callback"
    assert query["scope"] == "openid profile"
    callback = f"/sign-in/oidc/idp/callback?state={state}&code={provider.approve_query(query, 'alice')}"
    # Opened in a browser that did not start it, or in one that started another, the callback fails
    # and leaves the sign-in to its own browser (login CSRF).
    _, someone_elses, _ = start()
    for stray in ({}, someone_elses):
        assert client.get(callback, headers=stray).status_code == 401
    back = client.get(callback, headers=transaction, follow_redirects=False)
    assert back.status_code == 303 and back.headers["location"] == "/datasets"
    set_cookies = back.headers.get_list("set-cookie")
    assert any(c.startswith("geoservice_oidc=") and "Max-Age=0" in c for c in set_cookies), "the transaction ends"
    cookie = {"Cookie": next(c for c in set_cookies if c.startswith("geoservice_session=")).split(";")[0]}
    assert client.get(callback, headers=transaction).status_code == 401, "a sign-in is used once"
    # A cancelled sign-in offers to try again for the page the person was going to.
    state, transaction, _ = start()
    cancelled = client.get(f"/sign-in/oidc/idp/callback?state={state}&error=access_denied", headers=transaction)
    assert cancelled.status_code == 401 and 'href="/sign-in/oidc/idp?next=/datasets"' in cancelled.text
    # Anyone may start a sign-in, so past the limit the oldest open one is dropped.
    monkeypatch.setattr(oidc, "_PENDING_LIMIT", 1)
    state, transaction, query = start()
    start()
    dropped = f"/sign-in/oidc/idp/callback?state={state}&code={provider.approve_query(query, 'alice')}"
    assert client.get(dropped, headers=transaction).status_code == 401
    # Under a deployment prefix the cookie and the redirect follow the prefix.
    mounted = mounted_client_factory("/geoservice")
    mounted.base_url = httpx.URL("https://testserver")
    under_prefix = mounted.get("/sign-in/oidc/idp", follow_redirects=False)
    assert "Path=/geoservice/sign-in/oidc/idp/callback" in under_prefix.headers["set-cookie"]
    assert "redirect_uri=https%3A%2F%2Ftestserver%2Fgeoservice%2Fsign-in" in under_prefix.headers["location"]

    # One person, one principal, whichever way they arrive: their issuer and subject, at the authenticated tier.
    alice = {"user_id": f"oidc:{ISSUER}#sub-alice", "name": "alice", "links": []}
    assert client.get("/me", headers=cookie).json() == alice
    assert client.get("/me", headers=provider.bearer("alice")).json() == alice
    assert client.get("/me", headers=_bearer(provider.token("alice"))).json() == alice
    assert client.get("/datasets", headers=cookie).status_code == 200
    assert "alice" in client.get("/", headers={**cookie, **HTML}).text
    assert client.post("/ingestions", json={}, headers=provider.bearer("alice")).status_code == 403, "not an operator"
    # The openEO client it advertises is an accepted audience; another application is not.
    assert client.get("/me", headers=_bearer(provider.token("alice", aud="openeo"))).json() == alice
    assert client.get("/me", headers=_bearer(provider.token("alice", aud="another-service"))).status_code == 401
    # Named tokens work beside the provider.
    assert client.get("/me", headers=_bearer(CLIENT_SECRET)).json()["user_id"] == "token:notebook"

    # Refused: an expired token, one without a subject, and a forgery under the provider's key id.
    forged = provider.token("alice", key=RSAKey.generate_key(2048, parameters={"kid": "k1"}))
    for token in (provider.token("alice", expires_in=-600), provider.token("alice", sub=""), forged):
        assert client.get("/datasets", headers={"Authorization": f"Bearer oidc/idp/{token}"}).status_code == 401
    # A key the provider rotates in is fetched when a token first names it, and one it withdraws stops
    # being trusted once the key set is fetched again.
    monkeypatch.setattr(oidc, "_KEY_REFRESH_INTERVAL", 0)
    rotated = RSAKey.generate_key(2048, parameters={"kid": "k2"})
    provider.keys = KeySet([rotated])
    assert client.get("/me", headers=_bearer(provider.token("alice", key=rotated))).json() == alice
    monkeypatch.setattr(oidc, "_KEYS_TTL", 0)
    assert client.get("/me", headers=_bearer(provider.token("alice"))).status_code == 401

    # A provider that cannot be reached is no refusal, so a client keeps its token and tries again.
    def unreachable(url: str) -> Any:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(oidc, "_get_json", unreachable)
    assert client.get("/me", headers=provider.bearer("alice")).status_code == 503
    monkeypatch.setattr(oidc, "_get_json", provider._get_json)

    # An explicit list replaces the default for both clients.
    _configure_oidc(monkeypatch, tokens=[notebook], gate_reads=True, scopes=["openid", "email"])
    assert client.get("/credentials/oidc").json()["providers"][0]["scopes"] == ["openid", "email"]
    assert start()[2]["scope"] == "openid email"

    # A provider removed from the configuration ends its people's sessions.
    _configure(monkeypatch, [notebook])
    assert client.get("/me", headers=cookie).json()["user_id"] == "anonymous"


# Access and ID tokens as each provider shapes them: the header's typ, then the claims beyond the defaults.
_PROFILES: dict[str, list[tuple[dict[str, Any], bool]]] = {
    "rfc9068": [
        ({"typ": "at+jwt"}, True),
        ({"typ": "application/at+jwt"}, True),
        ({"typ": "at+jwt", "client_id": None}, False),  # a claim RFC 9068 requires
        ({"typ": "JWT", "nonce": "n", "scope": None}, False),  # an ID token
    ],
    "keycloak": [
        ({"typ": "JWT", "_typ": "Bearer"}, True),
        ({"typ": "JWT", "_typ": "ID", "scope": None}, False),
    ],
    "dhis2": [
        ({"typ": None, "scope": ["openid", "profile"], "client_id": None, "jti": None}, True),
        ({"typ": None, "azp": CLIENT_ID, "scope": None, "client_id": None}, False),  # ID token, no nonce asked
        ({"typ": None, "azp": CLIENT_ID, "nonce": "n", "scope": None, "client_id": None}, False),
    ],
}


@pytest.mark.parametrize("profile", sorted(_PROFILES))
def test_each_provider_profile_takes_its_access_tokens_and_refuses_its_id_tokens(
    profile: str, monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    _configure_oidc(monkeypatch, tokens=[], gate_reads=False, access_tokens=profile)
    provider = FakeProvider(monkeypatch)
    for shape, accepted in _PROFILES[profile]:
        claims = {("typ" if key == "_typ" else key): value for key, value in shape.items() if key != "typ"}
        token = provider.token("alice", header_typ=shape["typ"], **claims)
        status = client.get("/me", headers=_bearer(token)).status_code
        assert (status == 200) is accepted, (profile, shape, status)


def test_without_tokens_there_is_no_sign_in(client: TestClient) -> None:
    assert client.get("/sign-in").status_code == 404
    page = client.get("/data-sources", headers=HTML).text
    assert "Sign in" not in page and "Sign out" not in page


# --- gated reads --------------------------------------------------------------------------

JSON = {"Accept": "application/json"}


@pytest.mark.parametrize("route", sorted(route for route, tier in DECLARED.items() if tier is Tier.PUBLIC))
def test_gated_reads_need_a_token(reads_gated: None, client: TestClient, route: str) -> None:
    method, path = route.split(" ", 1)
    anonymous = client.request(method, _concrete(path), headers=JSON, follow_redirects=False)
    if route in UNGATED:
        # Not refused by the middleware; `/credentials/basic` answers 401 itself without Basic.
        assert "needs a token" not in anonymous.text
        return
    assert anonymous.status_code == 401 and anonymous.headers["WWW-Authenticate"] == "Bearer"
    # Any token reads, as Basic with its id too, which is what STAC Browser and QGIS send.
    for credential in (_bearer(CLIENT_SECRET), _basic("notebook", CLIENT_SECRET)):
        assert client.request(method, _concrete(path), headers={**JSON, **credential}).status_code not in (401, 403)


def test_gated_reads_send_a_browser_to_sign_in_and_declare_the_schemes(
    reads_gated: None, browser: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = browser
    # A browser is sent to sign in and back; openEO discovery at `/` stays public.
    for page, back in (("/datasets?q=1", "%2Fdatasets%3Fq%3D1"), ("/", "%2F")):
        redirected = client.get(page, headers=HTML, follow_redirects=False)
        assert redirected.status_code == 303 and redirected.headers["location"] == f"/sign-in?next={back}"
    assert client.get("/", headers=JSON).json()["api_version"]
    assert 'name="next" value="/datasets?q=1"' in client.get("/sign-in?next=/datasets?q=1").text

    # A session reads what the pages and the map viewer load.
    cookie = _session(client, CLIENT_SECRET)
    assert client.get("/datasets", headers={**cookie, **HTML}).status_code == 200
    # A store's root too, which GDAL asks for with a trailing slash.
    for store in ("/zarr/abc/zarr.json", "/zarr/abc/", "/icechunk/abc/"):
        assert client.get(store, headers=cookie).status_code not in (401, 403)
    assert client.get("/datasets", headers=cookie).headers["cache-control"] == "private"
    # A route's own caching keeps for the browser, never for a shared cache in front.
    for own, sent in (("max-age=60", "private, max-age=60"), ("no-store", "no-store")):
        own_caching = Response(headers={"Cache-Control": own})
        monkeypatch.setattr(ingestion_services, "serve_icechunk_file", lambda *_, r=own_caching: r)
        assert client.get("/icechunk/abc/repo", headers=cookie).headers["cache-control"] == sent

    # Read-only refuses writes by the table's tiers, so gated reads still answer a token.
    monkeypatch.setitem(api_config._cache or {}, "read_only", True)
    assert client.get("/datasets", headers={**JSON, **_bearer(CLIENT_SECRET)}).status_code == 200

    # STAC declares both schemes and marks what is on this instance, not a source elsewhere.
    raster: dict[str, Any] = {
        "type": "Collection",
        "id": "rain",
        "stac_extensions": [],
        "links": [
            {"rel": "via", "href": "https://source.example/rain"},
            {"rel": "license", "href": "//source.example/licence"},
        ],
        "assets": {"zarr": {"href": "https://testserver/zarr/rain"}},
    }
    synced = datetime(2026, 1, 1, tzinfo=UTC)
    published = {"rain": SimpleNamespace(dataset_name="Rain", format=ArtifactFormat.ICECHUNK, created_at=synced)}
    monkeypatch.setattr(stac_services, "_eligible_artifacts_by_dataset", lambda: published)
    monkeypatch.setattr(stac_services, "_build_raster_collection", lambda *_: raster)
    catalog = client.get("/stac/catalog.json", headers=_bearer(CLIENT_SECRET)).json()
    assert set(catalog["auth:schemes"]) == {"basic", "bearer"}
    assert AUTHENTICATION_EXTENSION in catalog["stac_extensions"]
    assert all(link["auth:refs"] == ["basic", "bearer"] for link in catalog["links"])
    collection = client.get("/stac/rain/collection.json", headers=_bearer(CLIENT_SECRET)).json()
    assert collection["assets"]["zarr"]["auth:refs"] == ["basic", "bearer"]
    assert not any("auth:refs" in link for link in collection["links"][:2])
    assert "auth:refs" not in raster["assets"]["zarr"]


# --- configuration -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("tokens", "gate_reads", "error"),
    [
        ([{"id": "a", "token_env": "GEOSERVICE_TEST_UNSET", "tier": "operator"}], False, "UNSET is not set"),
        ([{"id": "a", "token_env": "GEOSERVICE_TEST_SHORT", "tier": "operator"}], False, "shorter than 32"),
        ([{"id": "a", "token_env": "GEOSERVICE_TEST_OPERATOR_TOKEN", "tier": "admin"}], False, "invalid"),
        (
            [
                {"id": "a", "token_env": "GEOSERVICE_TEST_OPERATOR_TOKEN", "tier": "operator"},
                {"id": "a", "token_env": "GEOSERVICE_TEST_CLIENT_TOKEN", "tier": "operator"},
            ],
            False,
            "id is used by an earlier token",
        ),
        (
            [
                {"id": "a", "token_env": "GEOSERVICE_TEST_OPERATOR_TOKEN", "tier": "operator"},
                {"id": "b", "token_env": "GEOSERVICE_TEST_SAME", "tier": "operator"},
            ],
            False,
            "secret in GEOSERVICE_TEST_SAME is used by an earlier token",
        ),
        # Gating with no token would leave reads open while seeming private.
        ([], True, "gate_reads needs a token"),
    ],
)
def test_an_unusable_token_refuses_startup(
    monkeypatch: pytest.MonkeyPatch, tokens: list[dict[str, Any]], gate_reads: bool, error: str
) -> None:
    monkeypatch.setenv("GEOSERVICE_TEST_OPERATOR_TOKEN", OPERATOR_SECRET)
    monkeypatch.setenv("GEOSERVICE_TEST_CLIENT_TOKEN", CLIENT_SECRET)
    monkeypatch.setenv("GEOSERVICE_TEST_SHORT", "short")
    monkeypatch.setenv("GEOSERVICE_TEST_SAME", OPERATOR_SECRET)
    monkeypatch.delenv("GEOSERVICE_TEST_UNSET", raising=False)
    _configure(monkeypatch, tokens, gate_reads=gate_reads)

    with pytest.raises(ValueError, match=error) as raised:
        configured_tokens()
    assert OPERATOR_SECRET not in str(raised.value)
    with pytest.raises(ValueError, match=error), TestClient(app):
        pass


@pytest.mark.parametrize("scopes", [[], ["profile"], ["openid", "profile email"]])
def test_invalid_oidc_scopes_refuse_startup(monkeypatch: pytest.MonkeyPatch, scopes: list[str]) -> None:
    _configure_oidc(monkeypatch, tokens=[], gate_reads=True, scopes=scopes)
    with pytest.raises(ValueError, match=r"oidc\.0\.scopes"), TestClient(app):
        pass
