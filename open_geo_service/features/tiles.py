"""Vector tiles for a stored feature collection, for drawing it in a browser (CLIM-1177).

Every refresh builds a PMTiles archive beside the collection's GeoParquet, with tylertoo, and
the map viewer draws the collection from it. Tiles carry geometry already simplified per zoom,
so a browser zoomed out over a country fetches a few small tiles instead of every vertex.

The archive is derived output, for display only. Aggregation reads the exact geometry in the
GeoParquet, and a refresh can always rebuild the archive from it. So a failed build is logged
and never fails the refresh; the next refresh builds it again.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Any
from uuid import uuid4

from open_geo_service.shared import geoparquet, layout
from open_geo_service.shared.crs import canonical_crs_code

logger = logging.getLogger(__name__)

PMTILES_MEDIA_TYPE = "application/vnd.pmtiles"
STYLE_MEDIA_TYPE = "application/vnd.mapbox.style+json"

TILE_LAYER = "features"
"""The MVT layer name. One name for every collection, so the viewer never has to look it up."""

MIN_MAX_ZOOM = 10
"""The finest zoom is at least this, about 10 m per tile unit at the default 4096 extent.

tylertoo's `auto` estimates the finest zoom from the data. That suits small features, where it
goes as deep as zoom 16 for buildings, but for boundaries it chose zoom 4 for Norway's
municipalities, too coarse to show when zoomed in on one. The viewer overzooms past the finest
zoom, so 10 is fine enough for boundaries."""

_TILEABLE_CRS = ("EPSG:4326", "EPSG:3857")

_POLYGON_TYPES = frozenset({"Polygon", "MultiPolygon"})


def archive_path(collection_path: Path) -> Path:
    """The archive for one stored version of a collection: its GeoParquet path, `.pmtiles`.

    Named after the versioned file rather than the collection, so a new version never changes
    an archive in place, and removing a superseded file removes its archive with it.
    """
    return collection_path.with_suffix(".pmtiles")


def build_archive(collection_path: Path, collection_id: str, crs: str) -> Path | None:
    """Build the PMTiles archive for one stored collection file, returning it, or None.

    **Never raises.** A collection that cannot be tiled is still registered.

    Written in the staging folder and moved into place, so the route never serves a partial
    archive. tylertoo reads WGS 84 or Web Mercator; a collection stored in another CRS is
    reprojected into a temporary copy first.

    A polygon collection is built without tylertoo's per-level feature budget, which thins dense
    areas to keep tiles small. Boundaries are what GeoService stores, and a zoomed-out map with
    whole countries' districts missing looks like missing data. Polygons too small to draw at a
    zoom are still left out. Points and lines keep the budget.
    """
    target = archive_path(collection_path)
    staging: Path | None = None
    try:
        import tylertoo

        staging = layout.staging_dir() / f"{target.name}.{uuid4().hex}.writing"
        with tempfile.TemporaryDirectory(prefix="geoservice-tiles-") as scratch:
            source = collection_path
            if canonical_crs_code(crs) not in _TILEABLE_CRS:
                import geopandas as gpd

                source = Path(scratch) / "wgs84.parquet"
                gpd.read_parquet(collection_path).to_crs("EPSG:4326").to_parquet(source)
            overview = Path(scratch) / "overview.parquet"
            # GeoParquet may say "Polygon Z"; the dimension does not change how it is tiled.
            geometry_types = {name.split(" ")[0] for name in geoparquet.stored_geometry_types(collection_path)}
            density_drop = not geometry_types or not geometry_types <= _POLYGON_TYPES
            report = tylertoo.overview(str(source), str(overview), max_zoom="auto", density_drop=density_drop)
            if max(level["zoom"] for level in report["levels"]) < MIN_MAX_ZOOM:
                tylertoo.overview(str(source), str(overview), max_zoom=MIN_MAX_ZOOM, density_drop=density_drop)
            tylertoo.export_pmtiles(str(overview), str(staging), layer_name=TILE_LAYER)
        layout.move_into_place(staging, target)
    except Exception:
        logger.warning("Could not build vector tiles for feature collection '%s'", collection_id, exc_info=True)
        return None
    finally:
        if staging is not None:
            staging.unlink(missing_ok=True)
    return target


# Both spellings: MapLibre reports a multi-part feature as `MultiPolygon` and so on.
_AREA_TYPES = ["Polygon", "MultiPolygon"]
_LINE_TYPES = ["LineString", "MultiLineString"]
_POINT_TYPES = ["Point", "MultiPoint"]


def style(name: str, tiles_url: str, attribution: str | None) -> dict[str, Any]:
    """The MapLibre style a collection is drawn with: its tiles as filled areas, lines and points.

    Self-contained, so any MapLibre client can load it as it is, and the map viewer draws from
    it too, so the two never disagree. The attribution travels with the source: an open
    licence such as ODbL requires it wherever the data is shown.
    """
    source: dict[str, Any] = {"type": "vector", "url": f"pmtiles://{tiles_url}"}
    if attribution:
        source["attribution"] = attribution
    on_tiles = {"source": TILE_LAYER, "source-layer": TILE_LAYER}
    return {
        "version": 8,
        "name": name,
        "sources": {TILE_LAYER: source},
        "layers": [
            {
                "id": "features-fill",
                "type": "fill",
                **on_tiles,
                "filter": ["in", ["geometry-type"], ["literal", _AREA_TYPES]],
                "paint": {"fill-color": "#0066cc", "fill-opacity": 0.15},
            },
            {
                "id": "features-line",
                "type": "line",
                **on_tiles,
                "filter": ["in", ["geometry-type"], ["literal", _AREA_TYPES + _LINE_TYPES]],
                "paint": {"line-color": "#1e3a5f", "line-width": 1},
            },
            {
                "id": "features-point",
                "type": "circle",
                **on_tiles,
                "filter": ["in", ["geometry-type"], ["literal", _POINT_TYPES]],
                "paint": {
                    "circle-radius": 4,
                    "circle-color": "#0066cc",
                    "circle-stroke-color": "#fff",
                    "circle-stroke-width": 1,
                },
            },
        ],
    }
