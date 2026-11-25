"""The STAC catalog written as files into `catalog/`, which `/stac` serves and any static host can.

The documents are the ones the builders make for a request, with their links rewritten for a
folder: a link between documents becomes relative, a link to a dataset's data becomes the file
beside its `collection.json`, and a link to another GeoService route is dropped, so a copied
`catalog/` is a complete catalog wherever it is hosted. What GeoService generates on request
stays absolute and is written only when `GEOSERVICE_BASE_URL` is set: the `zarr` asset, a feature
collection's `style` asset, and the instance's own address as a provider. The written layout is
described in `docs/data_folder_layout.md`.
"""

from __future__ import annotations

import logging
import posixpath
import re
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from starlette.requests import Request

from open_geo_service import writer_lease
from open_geo_service.features import tiles
from open_geo_service.ingestions import services as ingestion_services
from open_geo_service.ingestions.schemas import ArtifactFormat, ArtifactRecord
from open_geo_service.shared import layout
from open_geo_service.shared.geoparquet import PARQUET_MEDIA_TYPE
from open_geo_service.shared.urls import absolute_base, configured_base, configured_root_path
from open_geo_service.stac import documents, portolan
from open_geo_service.stac import services as stac_services

logger = logging.getLogger(__name__)

_writing = threading.Lock()

_PLACEHOLDER_HOST = "catalog.invalid"
"""The host documents are built for when no public address is configured; never written."""


def write_catalog() -> None:
    """Rewrite every document in `catalog/` from the records, and remove those no longer published.

    Called by the writer at startup and after every change to the records, so a publish, sync or
    refresh is in the files before it returns. A collection that cannot be described is left out,
    with the links to it, and logged.
    """
    writer_lease.ensure_held()
    # Built and written under one lock, so of two writes the later one reads the later records.
    with _writing:
        root = layout.catalog_root()
        operations = portolan.plan(catalog_documents(), portolan.managed_keys(root))
        portolan.apply(operations, portolan.FolderSink(root, layout.staging_dir()))


def write_catalog_after_change() -> None:
    """`write_catalog`, logging rather than raising: the records it follows are already written."""
    try:
        write_catalog()
    except Exception:
        logger.exception("Could not rewrite the STAC catalog in the data folder; /stac is stale until the next write")


def catalog_documents() -> dict[str, portolan.Document]:
    """Every document of the catalog, keyed by its path in `catalog/`."""
    request = _catalog_request()
    base = absolute_base(request)
    published = stac_services._eligible_artifacts_by_dataset()
    built: dict[str, portolan.Document] = {}
    files: dict[str, str] = {}
    described: dict[str, ArtifactRecord] = {}
    for dataset_id, artifact in published.items():
        kind = layout.kind_of(artifact.format)
        folder = layout.dataset_key(kind, dataset_id)
        try:
            collection = stac_services.build_collection_for_artifact(dataset_id, artifact, request)
            readme = documents.collection_readme(kind, dataset_id, request)
            agents = documents.collection_agents(kind, dataset_id, request)
        except Exception:
            logger.exception("Could not describe '%s' for the catalog; leaving it out", folder)
            continue
        built |= {f"{folder}/collection.json": collection, f"{folder}/README.md": readme, f"{folder}/AGENTS.md": agents}
        files |= _data_files(dataset_id, artifact)
        described[dataset_id] = artifact
    # Only kinds with a collection written, so no sub-catalog is empty.
    for kind in stac_services.published_kinds(described):
        built |= {
            f"{kind.value}/catalog.json": stac_services.build_subcatalog(kind, request),
            f"{kind.value}/README.md": documents.subcatalog_readme(kind, request),
            f"{kind.value}/AGENTS.md": documents.subcatalog_agents(kind, request),
        }
    built |= {
        "catalog.json": stac_services.build_catalog(request),
        "README.md": documents.catalog_readme(request),
        "AGENTS.md": documents.catalog_agents(request),
    }
    written = set(built)
    return {
        key: _localized_markdown(document, key, base, files, written)
        if isinstance(document, str)
        else _localized_json(dict(document), key, base, files, written)
        for key, document in built.items()
    }


def _catalog_request() -> Request:
    """A request for the instance's public address, or for a placeholder host when none is configured.

    Under the configured `ROOT_PATH`, as uvicorn serves the app, so an origin-only address gains
    the same prefix a request-built URL has.
    """
    return Request(
        {
            "type": "http",
            "scheme": "https",
            "server": (_PLACEHOLDER_HOST, 443),
            "path": "/",
            "root_path": configured_root_path(),
            "query_string": b"",
            "headers": [(b"host", _PLACEHOLDER_HOST.encode())],
        }
    )


def _data_files(dataset_id: str, artifact: ArtifactRecord) -> dict[str, str]:
    """The data routes of one dataset and the keys of the files in `catalog/` they serve.

    A file recorded outside the dataset's folder has no key, so links to it are dropped.
    """
    kind = layout.kind_of(artifact.format)
    folder = layout.dataset_dir(kind, dataset_id)
    key = layout.dataset_key(kind, dataset_id)
    files = {f"/datasets/{dataset_id}/thumbnail.png": f"{key}/thumbnail.png"}
    # A table's record names its folder, and the current file as its one asset.
    recorded = artifact.asset_paths[0] if artifact.format == ArtifactFormat.PARQUET and artifact.asset_paths else None
    raw = recorded or artifact.path
    path = Path(raw) if raw else None
    if path is None or path.parent.resolve() != folder.resolve():
        return files
    if artifact.format == ArtifactFormat.GEOPARQUET:
        files[f"/features/{dataset_id}/data.parquet"] = f"{key}/{path.name}"
        files[f"/features/{dataset_id}/data.pmtiles"] = f"{key}/{tiles.archive_path(path).name}"
    elif artifact.format == ArtifactFormat.PARQUET:
        files[f"/tables/{dataset_id}/data.parquet"] = f"{key}/{path.name}"
    elif artifact.format == ArtifactFormat.ICECHUNK:
        files[f"/icechunk/{dataset_id}"] = f"{key}/{path.name}/"
    return files


def _localizer(key: str, base: str, files: dict[str, str]) -> Callable[[str], str | None]:
    """Where an href of the document at *key* points in the catalog, relative to it; None to drop it.

    The configured address stays absolute for the routes a static host cannot serve.
    """
    here = posixpath.dirname(key) or "."
    keep_base = bool(configured_base())

    def localize(href: str) -> str | None:
        if not href.startswith(f"{base}/") and href != base:
            return href  # relative already, or on another host
        route, hash_, fragment = href.removeprefix(base).partition("#")
        target = route.removeprefix("/stac/") if route.startswith("/stac/") else files.get(route)
        if target is None:
            return href if keep_base and _served_by_the_instance_only(route) else None
        relative = posixpath.relpath(target.rstrip("/"), here) + ("/" if target.endswith("/") else "")
        return ("" if relative.startswith("..") else "./") + relative + hash_ + fragment

    return localize


def _served_by_the_instance_only(route: str) -> bool:
    """The routes a written document may name with the configured address: generated on request.

    Zarr, translated from Icechunk; a feature collection's MapLibre style, which names its tiles
    by absolute URL; and the instance itself, as a provider.
    """
    return route.startswith("/zarr/") or (route.startswith("/features/") and "/styles/" in route) or route in ("", "/")


def _localized_json(
    document: dict[str, Any], key: str, base: str, files: dict[str, str], written: set[str]
) -> dict[str, Any]:
    localize = _localizer(key, base, files)
    here = posixpath.dirname(key)
    links: list[dict[str, Any]] = []
    for link in document.get("links", []):
        href = localize(str(link.get("href", "")))
        # No `self`: a document in a folder is wherever it is read from (PORTO-CORE-081).
        if link.get("rel") == "self" or href is None:
            continue
        if (
            href.endswith(".json")
            and "://" not in href
            and posixpath.normpath(posixpath.join(here, href)) not in written
        ):
            continue  # a collection left out of the catalog, or an input that is not published
        links.append({**link, "href": href})
    document["links"] = links
    if isinstance(document.get("assets"), dict):
        assets: dict[str, Any] = {}
        for name, asset in document["assets"].items():
            href = localize(str(asset.get("href", "")))
            if href is not None:
                assets[name] = {**asset, "href": href}
        document["assets"] = assets
    if isinstance(document.get("providers"), list):
        document["providers"] = [_provider(provider, localize) for provider in document["providers"]]
    return document


def _provider(provider: dict[str, Any], localize: Callable[[str], str | None]) -> dict[str, Any]:
    url = provider.get("url")
    if not isinstance(url, str):
        return provider
    localized = localize(url)
    if localized is None:
        return {name: value for name, value in provider.items() if name != "url"}
    return {**provider, "url": localized}


def _localized_markdown(text: str, key: str, base: str, files: dict[str, str], written: set[str]) -> str:
    """*text* with every URL on the instance localized, and the instance's own address left out without one.

    A line linking a document that is not written, a collection left out, is dropped, as its JSON link is.
    """
    localize = _localizer(key, base, files)

    def replace(match: re.Match[str]) -> str:
        # Punctuation that ends the sentence a URL closes is not part of it.
        url = match.group(0).rstrip(".,:;")
        localized = localize(url)
        shown = localized if localized is not None else url.removeprefix(base) or "/"
        return shown + match.group(0)[len(url) :]

    localized = re.sub(re.escape(base) + r"(?:/[^\s)\"'`<>]*)?", replace, text)
    here = posixpath.dirname(key)

    def missing(target: str) -> bool:
        in_folder = target.endswith(".json") and "://" not in target
        return in_folder and posixpath.normpath(posixpath.join(here, target)) not in written

    lines = localized.split("\n")
    return "\n".join(line for line in lines if not any(missing(t) for t in re.findall(r"\]\(([^)\s]+)\)", line)))


_MEDIA_TYPES = {
    ".json": "application/json",
    ".md": stac_services.MARKDOWN_MEDIA_TYPE,
    ".parquet": PARQUET_MEDIA_TYPE,
    ".pmtiles": tiles.PMTILES_MEDIA_TYPE,
    ".png": "image/png",
}


def served_file(path: str) -> tuple[Path, str]:
    """The file at *path* in `catalog/` and its media type, or 404.

    A document is served wherever it is, as the writer writes one only for what the catalog
    lists. A data file is served only from the folder of a published dataset of its kind, so a
    dataset written before it is published stays out of reach, as it stays out of the catalog.
    """
    root = layout.catalog_root().resolve()
    target = (root / path).resolve()
    if not target.is_relative_to(root) or not target.is_file():
        raise HTTPException(status_code=404, detail=f"No file '{path}' in the catalog")
    parts = target.relative_to(root).parts
    if not (target.name in portolan.DOCUMENT_NAMES and len(parts) <= 3) and not _published_folder(parts):
        raise HTTPException(status_code=404, detail=f"No file '{path}' in the catalog")
    return target, _MEDIA_TYPES.get(target.suffix, "application/octet-stream")


def _published_folder(parts: tuple[str, ...]) -> bool:
    if len(parts) < 3:
        return False
    artifact = ingestion_services.stac_eligible_artifacts_cached().get(parts[1])
    return artifact is not None and layout.kind_of(artifact.format).value == parts[0]
