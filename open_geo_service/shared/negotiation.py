"""Content negotiation: whether a request asks for an HTML page or JSON."""

from __future__ import annotations

from starlette.requests import Request


def _media_type_q(accept: str, media_type: str) -> float:
    """Return the effective q-value for media_type in an Accept header, or -1.0 if absent.

    Handles RFC 7231 wildcards: exact matches beat type/* beats */*. An API client
    sending Accept: */* (e.g. the requests library default) therefore matches any
    media type at q=1.0, and JSON wins over HTML when both match equally.
    """
    media_type_family = media_type.split("/", 1)[0]
    exact_q = -1.0
    family_q = -1.0
    wildcard_q = -1.0
    for item in accept.split(","):
        parts = item.strip().split(";")
        token = parts[0].strip()
        q = 1.0
        for param in parts[1:]:
            param = param.strip()
            if param.startswith("q="):
                try:
                    q = float(param[2:])
                except ValueError:
                    pass
        if token == media_type:
            exact_q = max(exact_q, q)
        elif token == f"{media_type_family}/*":
            family_q = max(family_q, q)
        elif token == "*/*":
            wildcard_q = max(wildcard_q, q)
    if exact_q >= 0:
        return exact_q
    if family_q >= 0:
        return family_q
    return wildcard_q


def wants_json(request: Request) -> bool:
    """Return True if the client prefers a JSON response over HTML.

    JSON is preferred when application/json is present with a q-value greater
    than or equal to text/html. HTML wins only when text/html has a strictly
    higher q-value, matching RFC 7231 content negotiation semantics.
    """
    if request.query_params.get("f") == "json":
        return True
    accept = request.headers.get("accept", "")
    if not accept:
        return False
    json_q = _media_type_q(accept, "application/json")
    html_q = _media_type_q(accept, "text/html")
    return json_q >= 0 and (html_q < 0 or json_q >= html_q)


def prefers_html(request: Request) -> bool:
    """Whether a client asked for HTML over JSON, for an endpoint that is JSON by default.

    Stricter than `wants_json` on purpose. `/` has always defaulted to HTML; a data endpoint
    has always answered JSON, and scripts calling it may send no Accept header or `*/*`.
    Those keep getting JSON, and only a client that ranks `text/html` above JSON — a browser —
    gets the page. `?f=html` and `?f=json` override either way.
    """
    requested = request.query_params.get("f")
    if requested in {"html", "json"}:
        return requested == "html"
    accept = request.headers.get("accept", "")
    if not accept:
        return False
    return _media_type_q(accept, "text/html") > _media_type_q(accept, "application/json")
