"""The STAC catalog written into `catalog/`: what `/stac` serves, and what a static host can.

Written through the real paths: every record write rewrites the catalog here, as in production,
rather than the `client` fixture writing it before a `/stac` request.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from fastapi.testclient import TestClient

from open_geo_service import config as api_config
from open_geo_service.data_manager.services import downloader
from open_geo_service.features import services as feature_services
from open_geo_service.features import templates as feature_templates
from open_geo_service.ingestions import services as ingestion_services
from open_geo_service.ingestions.schemas import (
    ArtifactCoverage,
    ArtifactFormat,
    ArtifactProvenance,
    ArtifactPublication,
    ArtifactRecord,
    ArtifactRequestScope,
    CoverageSpatial,
    CoverageTemporal,
    PublicationStatus,
)
from open_geo_service.main import app
from open_geo_service.shared import layout
from open_geo_service.stac import catalog_files
from open_geo_service.stac import services as stac_services
from open_geo_service.tables import store as table_store

PUBLIC = "https://geo.example.org"
PRODUCER = [{"name": "Example Agency", "url": "https://agency.example.org", "roles": ["producer", "licensor"]}]
SOURCE = "https://agency.example.org/data"
RAIN = {
    "id": "rain",
    "name": "Rain",
    "period_type": "daily",
    "license": "CC-BY-4.0",
    "providers": PRODUCER,
    "source_url": SOURCE,
}
DISTRICTS = {
    "id": "districts",
    "name": "Districts",
    "id_property": "orgUnitCode",
    "license": "CC-BY-4.0",
    "providers": PRODUCER,
    "source_url": SOURCE,
}


@pytest.fixture(autouse=True)
def _no_catalog_files_on_record_writes() -> None:
    """Overrides the conftest fixture: here a record write rewrites the catalog, as in production."""


@pytest.fixture(autouse=True)
def _no_vector_tiles() -> None:
    """Overrides the conftest fixture: the tiles bring a style, which Portolan checks for."""


def _box(code: str) -> dict[str, Any]:
    return {
        "type": "Feature",
        "properties": {"orgUnitCode": code},
        "geometry": {"type": "Polygon", "coordinates": [[[10, 0], [12, 0], [12, 2], [10, 2], [10, 0]]]},
    }


def write_one_of_each(data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A data folder with a raster, a feature collection and a table, written through the real paths."""
    monkeypatch.setattr(api_config, "get_data_root", lambda: data)
    monkeypatch.setattr(ingestion_services, "ARTIFACTS_DIR", data / "artifacts")
    monkeypatch.setattr(ingestion_services, "ARTIFACTS_INDEX_PATH", data / "artifacts" / "records.json")
    monkeypatch.setattr(
        stac_services.registry_datasets, "get_dataset", lambda dataset_id: RAIN if dataset_id == "rain" else None
    )
    monkeypatch.setattr(
        feature_templates, "get_feature_template", lambda dataset_id: DISTRICTS if dataset_id == "districts" else None
    )

    cube = xr.Dataset(
        {"precip": (("t", "y", "x"), np.ones((2, 2, 2), dtype="float32"))},
        coords={"t": pd.date_range("2026-01-01", periods=2, freq="D"), "y": [1.5, 0.5], "x": [10.5, 11.5]},
    )
    store = layout.raster_store("rain")
    downloader.write_to_icechunk_store(cube, store, commit_message="test")
    ingestion_services.register_artifact_record(
        ArtifactRecord(
            artifact_id="a1",
            dataset_id="rain",
            dataset_name="Rain",
            variable="precip",
            period_type="daily",
            format=ArtifactFormat.ICECHUNK,
            path=str(store),
            asset_paths=[str(store)],
            variables=["precip"],
            request_scope=ArtifactRequestScope(start="2026-01-01", end="2026-01-02"),
            coverage=ArtifactCoverage(
                temporal=CoverageTemporal(start="2026-01-01", end="2026-01-02"),
                spatial=CoverageSpatial(xmin=10.0, ymin=0.0, xmax=12.0, ymax=2.0),
            ),
            created_at=datetime(2026, 1, 2, tzinfo=UTC),
            publication=ArtifactPublication(status=PublicationStatus.PUBLISHED, collection_id="rain"),
        ),
        publish=True,
    )
    feature_services.refresh_feature_collection(
        template=DISTRICTS, features={"type": "FeatureCollection", "features": [_box("D1")]}
    )
    # As a job writes it, so its licence and producers are its sources'.
    table_store.write_rows(
        dataset_id="rain_districts",
        dataset_name="Rain by district",
        collection_id="districts",
        rows=[{"feature_id": "D1", "period_start": "2026-01-01", "value": 1.0, "reducer": "mean"}],
        provenance=ArtifactProvenance(
            producer_job_id="job-1",
            execution={
                "process_sha256": "f" * 64,
                "sources": [{"collection_id": "rain"}, {"collection_id": "districts"}],
            },
        ),
    )


@pytest.fixture
def data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("GEOSERVICE_BASE_URL", PUBLIC)
    folder = tmp_path / "data"
    write_one_of_each(folder, monkeypatch)
    return folder


def _rashid(catalog: Path) -> list[dict[str, Any]]:
    """The findings of `rashid check` on *catalog*, reading the data beside the documents."""
    rashid = Path(sys.executable).with_name("rashid")
    result = subprocess.run(
        # Only the files in the folder: the Zarr asset names an address that does not exist here.
        [str(rashid), "check", "--json", "--schema", "--data-scope", "local", str(catalog)],
        capture_output=True,
        text=True,
        check=False,
    )
    return json.loads(result.stdout)["findings"]


def _documents(catalog: Path) -> dict[str, Any]:
    """Every JSON document under *catalog*, parsed, by its path in it."""
    return {
        path.relative_to(catalog).as_posix(): json.loads(path.read_text())
        for path in catalog.rglob("*.json")
        if path.name in ("catalog.json", "collection.json")
    }


def _hrefs(document: dict[str, Any]) -> list[str]:
    links = [link["href"] for link in document.get("links", [])]
    return links + [asset["href"] for asset in document.get("assets", {}).values()]


def test_the_written_catalog_passes_rashid_and_links_only_inside_itself(data: Path) -> None:
    """Read as a static host would serve it, with GeoService not involved: rashid reads the folder."""
    catalog = data / "catalog"

    errors = [finding for finding in _rashid(catalog) if finding["severity"] == "error"]

    assert errors == []
    assert set(_documents(catalog)["vectors/districts/collection.json"]["assets"]) == {
        "data",
        "tiles",
        "style",
        "thumbnail",
    }
    for key, document in _documents(catalog).items():
        for href in _hrefs(document):
            # Inside the folder, a source elsewhere, or a route the configured address generates.
            generated = href.startswith(f"{PUBLIC}/zarr/") or href.endswith("/styles/default.json")
            assert not href.startswith(PUBLIC) or generated, (key, href)
            if "://" not in href:
                assert (catalog / key).parent.joinpath(href).resolve().exists(), (key, href)
    for path in catalog.rglob("*.md"):
        # The Zarr route, and this instance named as a provider, as in the JSON.
        assert PUBLIC not in path.read_text().replace(f"{PUBLIC}/zarr/", "").replace(f"({PUBLIC}/)", ""), path
    # A table's example reads the file beside it, and says what its path is relative to.
    agents = (catalog / "tables" / "rain_districts" / "AGENTS.md").read_text()
    assert 'pd.read_parquet("./' in agents and catalog_files.RELATIVE_PATHS_NOTE in agents


def test_a_write_is_in_the_files_before_it_returns_and_an_unpublished_folder_has_no_collection(
    data: Path,
) -> None:
    catalog = data / "catalog"
    regions = {**DISTRICTS, "id": "regions", "name": "Regions"}

    feature_services.refresh_feature_collection(
        template=regions, features={"type": "FeatureCollection", "features": [_box("R1")]}, publish=False
    )

    # Its data is in its folder from the first write, but the catalog does not list it, so a
    # checker finds no unlinked collection (PTL-LNK-002).
    assert list((catalog / "vectors" / "regions").glob("*.parquet"))
    assert not (catalog / "vectors" / "regions" / "collection.json").exists()
    assert not [finding for finding in _rashid(catalog) if finding["rule_id"] == "PTL-LNK-002"]

    first = feature_services.refresh_feature_collection(
        template=regions, features={"type": "FeatureCollection", "features": [_box("R1")]}
    )
    written = _documents(catalog)
    assert "./regions/collection.json" in _hrefs(written["vectors/catalog.json"])
    # Its tiles, style and thumbnail, made after its record, are in the files when the refresh returns.
    assert set(written["vectors/regions/collection.json"]["assets"]) == {"data", "tiles", "style", "thumbnail"}
    assert written["vectors/regions/collection.json"]["assets"]["data"]["href"] == f"./{Path(str(first.path)).name}"

    second = feature_services.refresh_feature_collection(
        template=regions, features={"type": "FeatureCollection", "features": [_box("R1"), _box("R2")]}
    )
    data_href = _documents(catalog)["vectors/regions/collection.json"]["assets"]["data"]["href"]
    assert data_href == f"./{Path(str(second.path)).name}", "a refresh rewrites the collection"


def test_the_zarr_asset_follows_the_configured_address(
    data: Path, monkeypatch: pytest.MonkeyPatch, restart_writer: Callable[[], None]
) -> None:
    collection = data / "catalog" / "rasters" / "rain" / "collection.json"
    assert json.loads(collection.read_text())["assets"]["zarr"]["href"] == f"{PUBLIC}/zarr/rain"

    # A restart with another address rewrites every document.
    monkeypatch.setenv("GEOSERVICE_BASE_URL", "https://moved.example.org")
    restart_writer()
    assert json.loads(collection.read_text())["assets"]["zarr"]["href"] == "https://moved.example.org/zarr/rain"

    # An origin-only address behind a proxy prefix composes as a request-built URL does.
    monkeypatch.setenv("ROOT_PATH", "/geoservice")
    catalog_files.write_catalog()
    assert (
        json.loads(collection.read_text())["assets"]["zarr"]["href"] == "https://moved.example.org/geoservice/zarr/rain"
    )
    monkeypatch.delenv("ROOT_PATH")

    # Without one, nothing absolute on the instance is written: no Zarr asset, no host address,
    # and no render drawing the Zarr asset. The rewrite reads the records once, not per document.
    monkeypatch.delenv("GEOSERVICE_BASE_URL")
    reads: list[None] = []
    read = ingestion_services._latest_published_artifacts_by_dataset
    monkeypatch.setattr(
        ingestion_services, "_latest_published_artifacts_by_dataset", lambda: reads.append(None) or read()
    )
    catalog_files.write_catalog()
    assert len(reads) == 1
    written = json.loads(collection.read_text())
    assert "zarr" not in written["assets"] and written["assets"]["icechunk"]["href"] == "./rain.icechunk/"
    for render in written.get("renders", {}).values():
        assert set(render["assets"]) <= set(written["assets"]), render
    assert "renders" in written or stac_services.RENDER_EXTENSION not in written["stac_extensions"]
    assert [provider.get("url") for provider in written["providers"] if "host" in provider["roles"]] == [None]


def test_stac_serves_the_folder_as_a_static_host_would(data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    feature_services.refresh_feature_collection(
        template={**DISTRICTS, "id": "regions", "name": "Regions"},
        features={"type": "FeatureCollection", "features": [_box("R1")]},
        publish=False,
    )
    client = TestClient(app)  # not the fixture that writes the catalog before each request

    served = client.get("/stac/vectors/districts/collection.json")
    assert served.content == (data / "catalog" / "vectors" / "districts" / "collection.json").read_bytes()
    # A document and a store's `repo` change in place, so they are revalidated; a chunk never changes.
    assert served.headers["cache-control"] == "no-cache"
    assert client.get("/stac/rasters/rain/rain.icechunk/repo").headers["cache-control"] == "no-cache"
    snapshot = next((data / "catalog" / "rasters" / "rain" / "rain.icechunk" / "snapshots").iterdir())
    immutable = client.get(f"/stac/rasters/rain/rain.icechunk/snapshots/{snapshot.name}")
    assert immutable.headers["cache-control"] == "max-age=31536000, immutable"
    assert client.head("/stac/README.md").headers["content-type"].startswith("text/markdown")
    data_file = next((data / "catalog" / "vectors" / "districts").glob("*.parquet"))
    ranged = client.get(f"/stac/vectors/districts/{data_file.name}", headers={"Range": "bytes=0-3"})
    assert ranged.status_code == 206 and ranged.content == b"PAR1"
    # A reader of a store sends a request per chunk, so a file is served without reading the records.
    monkeypatch.setattr(ingestion_services, "_load_records", lambda: pytest.fail("a file request read the records"))
    assert client.get(f"/stac/vectors/districts/{data_file.name}").status_code == 200

    # As from any copy of the folder: not ready rather than private, and nothing links to it.
    unpublished = next((data / "catalog" / "vectors" / "regions").glob("*.parquet"))
    assert client.get(f"/stac/vectors/regions/{unpublished.name}").status_code == 200
    assert client.get("/stac/vectors/regions/collection.json").status_code == 404
    assert client.get("/stac/../artifacts/records.json").status_code == 404
    assert client.get("/stac/rasters/districts/README.md").status_code == 404


def test_the_client_lists_every_kind_through_the_relative_links(data: Path) -> None:
    from open_geo_service.client import GeoService

    service = GeoService("http://testserver")
    service._http = TestClient(app)

    assert [link["id"] for link in service.datasets()] == [
        "rasters/rain",
        "vectors/districts",
        "tables/rain_districts",
    ]


def test_a_collection_that_cannot_be_described_leaves_no_link_to_it(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The only vector collection fails, so no vector sub-catalog is written, and nothing links to either."""
    from open_geo_service.stac import documents

    describe = documents.collection_readme

    def failing(kind: layout.Kind, dataset_id: str, request: Any) -> str:
        if dataset_id == "districts":
            raise ValueError("an unreadable footer")
        return describe(kind, dataset_id, request)

    monkeypatch.setattr(documents, "collection_readme", failing)
    catalog_files.write_catalog()
    catalog = data / "catalog"

    assert not (catalog / "vectors" / "catalog.json").exists()
    assert not (catalog / "vectors" / "districts" / "collection.json").exists()
    for path in catalog.rglob("*.md"):
        assert "vectors/districts/" not in path.read_text(), path
        assert "vectors/catalog.json" not in path.read_text(), path
    assert [finding for finding in _rashid(catalog) if finding["severity"] == "error"] == []
