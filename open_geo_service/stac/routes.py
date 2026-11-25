"""Routes for the STAC catalogue.

Laid out as a static catalog, so a client that follows links reads it as files: the root
`catalog.json`, a sub-catalog `{kind}/catalog.json` per kind of dataset, and one directory per
collection, `{kind}/{id}/`, holding `collection.json`. Each catalog and collection has its
`README.md` and `AGENTS.md` beside it. Each
document has one URL, because its `describedby` and `agents` links are relative to it. Every route
answers HEAD, as a static host does and a link checker expects; HEAD is kept out of the OpenAPI
schema so each operation is listed once.
"""

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse

from open_geo_service.shared import layout
from open_geo_service.stac import documents
from open_geo_service.stac import services as stac_services
from open_geo_service.stac.schemas import StacCatalogResponse

router = APIRouter()


# `response_model_exclude_none`: a link with no title or type leaves the field out. The core
# catalog schema allows `title` only as a string, so a `null` one fails validation.
@router.get("/catalog.json", response_model=StacCatalogResponse, response_model_exclude_none=True)
@router.head(
    "/catalog.json", response_model=StacCatalogResponse, response_model_exclude_none=True, include_in_schema=False
)
def get_stac_catalog_json(request: Request) -> dict[str, object]:
    """Return the root STAC catalog."""
    return stac_services.build_catalog(request)


def _kind(kind: str) -> layout.Kind:
    """The kind a path names, or 404, as a static host answers a folder that is not there."""
    try:
        return layout.Kind(kind)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"No sub-catalog '{kind}'") from None


@router.get("/{kind}/catalog.json", response_model=StacCatalogResponse, response_model_exclude_none=True)
@router.head(
    "/{kind}/catalog.json",
    response_model=StacCatalogResponse,
    response_model_exclude_none=True,
    include_in_schema=False,
)
def get_stac_subcatalog(kind: str, request: Request) -> dict[str, object]:
    """Return the sub-catalog of one kind of dataset: `rasters`, `vectors` or `tables`."""
    return stac_services.build_subcatalog(_kind(kind), request)


@router.get("/{kind}/{dataset_id}/collection.json")
@router.head("/{kind}/{dataset_id}/collection.json", include_in_schema=False)
def get_stac_collection(kind: str, dataset_id: str, request: Request) -> dict[str, object]:
    """Return one STAC collection document."""
    return stac_services.build_collection(_kind(kind), dataset_id, request)


def _markdown(text: str) -> PlainTextResponse:
    return PlainTextResponse(text, media_type=stac_services.MARKDOWN_MEDIA_TYPE)


@router.get("/README.md", response_class=PlainTextResponse)
@router.head("/README.md", response_class=PlainTextResponse, include_in_schema=False)
def get_catalog_readme(request: Request) -> PlainTextResponse:
    """What the catalog holds, as the catalog's `describedby` link."""
    return _markdown(documents.catalog_readme(request))


@router.get("/AGENTS.md", response_class=PlainTextResponse)
@router.head("/AGENTS.md", response_class=PlainTextResponse, include_in_schema=False)
def get_catalog_agents(request: Request) -> PlainTextResponse:
    """How an agent reads the catalog, as its `agents` link."""
    return _markdown(documents.catalog_agents(request))


@router.get("/{kind}/README.md", response_class=PlainTextResponse)
@router.head("/{kind}/README.md", response_class=PlainTextResponse, include_in_schema=False)
def get_subcatalog_readme(kind: str, request: Request) -> PlainTextResponse:
    """What one sub-catalog holds."""
    return _markdown(documents.subcatalog_readme(_kind(kind), request))


@router.get("/{kind}/AGENTS.md", response_class=PlainTextResponse)
@router.head("/{kind}/AGENTS.md", response_class=PlainTextResponse, include_in_schema=False)
def get_subcatalog_agents(kind: str, request: Request) -> PlainTextResponse:
    """How an agent reads one sub-catalog."""
    return _markdown(documents.subcatalog_agents(_kind(kind), request))


@router.get("/{kind}/{dataset_id}/README.md", response_class=PlainTextResponse)
@router.head("/{kind}/{dataset_id}/README.md", response_class=PlainTextResponse, include_in_schema=False)
def get_collection_readme(kind: str, dataset_id: str, request: Request) -> PlainTextResponse:
    """A collection's title, description, licence and provenance."""
    return _markdown(documents.collection_readme(_kind(kind), dataset_id, request))


@router.get("/{kind}/{dataset_id}/AGENTS.md", response_class=PlainTextResponse)
@router.head("/{kind}/{dataset_id}/AGENTS.md", response_class=PlainTextResponse, include_in_schema=False)
def get_collection_agents(kind: str, dataset_id: str, request: Request) -> PlainTextResponse:
    """How an agent reads a collection's data."""
    return _markdown(documents.collection_agents(_kind(kind), dataset_id, request))
