"""MCP tools and resources over the OGS API: the first, read-only slice of CLIM-851.

Every tool calls OGS's own HTTP routes in-process, never its services, so a call is subject to
the read-only middleware and the same rules as any outside client. The numbers come from a
process graph OGS executed, and each result carries that graph and the dataset's links, so an
answer can be reproduced without the assistant.
"""

import csv
import io
import json
import logging
import math
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Literal
from urllib.parse import quote, unquote

import httpx
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.types as pa_types
import shapely
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field
from pyproj import CRS, Transformer
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request

from open_geo_service.mcp_server.endpoint import current_scope
from open_geo_service.shared import layout
from open_geo_service.shared.time import iso_step_to_approx_hours
from open_geo_service.shared.urls import absolute_base, is_segment_safe_id, path_segment
from open_geo_service.stac.services import collection_path, subcatalog_path

logger = logging.getLogger(__name__)

# A result is read by a model, so its size is bounded by what fits usefully in a context window
# rather than by what the server can compute: a daily series of about three weeks over a hundred
# districts, or of five years at one point.
MAX_ROWS = 2000

# Half the side of the square a point query aggregates over, in degrees: far smaller than any
# grid cell, so a square around a cell's centre returns that cell's value. A square rather than
# a point because the weighted aggregation takes polygons only.
_POINT_HALF_SIDE = 1e-5

Bounds = tuple[float, float, float, float]

Statistic = Literal["mean", "sum", "min", "max", "median"]

INSTRUCTIONS = """\
Climate and earth observation data held by this Open Geo Service instance, for one country.

Start with list_datasets to see what the instance holds: raster datasets (climate variables over
time) and area collections (districts or other administrative units). To answer a question about
a place, call list_areas on an area collection, pick the area by name, then call
get_zonal_statistics with its id. For a single location, use get_timeseries.

Every value is computed by the instance, not estimated. Each result includes the process graph
that produced it and links to the dataset; cite the dataset link in answers.
"""


class DatasetSummary(BaseModel):
    """One published dataset, as list_datasets reports it."""

    id: str
    title: str
    kind: Literal["raster", "areas"] = Field(
        description="raster: values on a grid, queried with get_timeseries or get_zonal_statistics. "
        "areas: a collection of areas, listed with list_areas."
    )
    description: str | None = None
    variable: str | None = Field(default=None, description="Name of the raster variable.")
    unit: str | None = None
    dimensions: list[str] = Field(
        default_factory=list,
        description="Non-spatial dimensions of a raster: 't' for time, or e.g. 'month' for a climate normal.",
    )
    time_start: str | None = None
    time_end: str | None = None
    time_step: str | None = Field(default=None, description="ISO 8601 duration between time steps, e.g. P1M.")
    area_count: int | None = Field(default=None, description="Number of areas in an area collection.")
    last_updated: str | None = Field(default=None, description="When the instance last updated this dataset.")
    dataset_url: str
    stac_url: str
    map_url: str = Field(description="The dataset in the instance's map viewer, to show the user.")


class Area(BaseModel):
    """One area of a collection."""

    id: str
    name: str
    properties: dict[str, str | int | float | bool | None] = Field(
        default_factory=dict, description="Other attributes, such as the parent region or level."
    )


class AreaList(BaseModel):
    """The areas of one collection, sorted by name."""

    collection_id: str
    areas: list[Area]


class TimeseriesRow(BaseModel):
    """One period of a point time series."""

    period: str
    value: float | None


class ZonalRow(BaseModel):
    """One period of one area."""

    period: str
    area_id: str
    area_name: str
    value: float | None


class _Provenance(BaseModel):
    dataset_id: str
    variable: str
    unit: str | None
    period_dimension: str | None = Field(
        description="The dimension `period` comes from: 't' for time, or e.g. 'month' for a climate normal."
    )
    process_graph: dict[str, Any] = Field(
        description="The openEO process graph the instance executed, for reproduction."
    )
    dataset_url: str
    stac_url: str
    map_url: str = Field(description="The dataset in the instance's map viewer, to show the user.")


class TimeseriesResult(_Provenance):
    """A point time series, with the process graph that produced it."""

    lon: float
    lat: float
    rows: list[TimeseriesRow]


class ZonalResult(_Provenance):
    """Statistics per area and period, with the process graph that produced them."""

    collection_id: str
    statistic: Statistic
    rows: list[ZonalRow]


@asynccontextmanager
async def _ocs() -> AsyncGenerator[httpx.AsyncClient]:
    """A client for this instance's API, reached in-process at the address the call came in on.

    It carries the caller's credential, so a tool may do what the caller may and no more.
    """
    scope = current_scope.get()
    prefix = str(scope.get("root_path", "")).rstrip("/")
    request = Request(scope)
    url = request.url
    authorization = request.headers.get("authorization")
    # A route that fails comes back as its 500 response, reported like any other refusal.
    transport = httpx.ASGITransport(app=scope["app"], root_path=prefix, raise_app_exceptions=False)
    async with httpx.AsyncClient(
        transport=transport,
        base_url=f"{url.scheme}://{url.netloc}{prefix}",
        headers={"Authorization": authorization} if authorization else None,
        timeout=None,
    ) as client:
        yield client


def _error_detail(response: httpx.Response) -> str:
    try:
        detail = response.json().get("detail")
    except (ValueError, AttributeError):
        detail = None
    return str(detail) if detail else response.text


async def _get_json(client: httpx.AsyncClient, path: str) -> Any:
    response = await client.get(path)
    if response.status_code == 404:
        raise ToolError(f"Not found: {path}. Use list_datasets to see what this instance holds.")
    if not response.is_success:
        raise ToolError(f"The instance refused {path} ({response.status_code}): {_error_detail(response)}")
    return response.json()


def _checked_id(dataset_id: str) -> str:
    """An id the assistant passed, refused when no dataset can have it: each id is one URL segment."""
    if not is_segment_safe_id(dataset_id):
        raise ToolError(f"No dataset {dataset_id!r}. Use list_datasets to see what this instance holds.")
    return dataset_id


async def _raster(client: httpx.AsyncClient, dataset_id: str) -> dict[str, Any]:
    """A raster's STAC collection."""
    collection: dict[str, Any] = await _get_json(client, collection_path(layout.Kind.RASTER, _checked_id(dataset_id)))
    return collection


def _links(kind: layout.Kind, dataset_id: str) -> dict[str, str]:
    """Links to show the user, at the instance's public address.

    Behind a proxy that address may differ from the one the call came in on.
    """
    base = absolute_base(Request(current_scope.get()))
    return {
        "dataset_url": f"{base}/datasets/{path_segment(dataset_id)}",
        "stac_url": f"{base}{collection_path(kind, dataset_id)}",
        "map_url": f"{base}/map?dataset={quote(dataset_id, safe='')}",
    }


def _raster_variable(collection: dict[str, Any]) -> tuple[str, str | None]:
    """The variable a raster collection renders, and its unit."""
    variables: dict[str, Any] = collection.get("cube:variables") or {}
    render = (collection.get("renders") or {}).get("default") or {}
    variable = render.get("ogs:variable") or next(iter(variables), "")
    return variable, (variables.get(variable) or {}).get("unit")


def _period_dimensions(collection: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """A raster's dimensions other than space and bands: what a result has one row per value of."""
    return {
        key: dim
        for key, dim in (collection.get("cube:dimensions") or {}).items()
        if dim.get("type") not in ("spatial", "bands", "geometry")
    }


def _summarise(
    dataset_id: str, collection: dict[str, Any], kind: Literal["raster", "areas"], links: dict[str, str]
) -> DatasetSummary:
    summary = DatasetSummary(
        id=dataset_id,  # the collection's id is `{kind}/{id}`; every tool takes the dataset's own
        title=collection.get("title") or dataset_id,
        kind=kind,
        description=collection.get("description"),
        dataset_url=links["dataset_url"],
        stac_url=links["stac_url"],
        map_url=links["map_url"],
    )
    if summary.kind == "areas":
        summary.area_count = collection.get("table:row_count")
        return summary
    summary.variable, summary.unit = _raster_variable(collection)
    dimensions = _period_dimensions(collection)
    summary.dimensions = list(dimensions)
    # Stores keep their source's name for time: `t`, or `time` and `valid_time` for ERA5.
    time: dict[str, Any] = next((dim for dim in dimensions.values() if dim.get("type") == "temporal"), {})
    summary.time_start, summary.time_end = (time.get("extent") or [None, None])[:2]
    step = time.get("step")
    summary.time_step = step if isinstance(step, str) else None
    return summary


def _is_areas(record: dict[str, Any]) -> bool:
    """Whether a feature collection holds areas: polygons, which a raster can be aggregated over."""
    types: list[str] = record.get("geometry_types") or []
    return bool(types) and all("Polygon" in kind for kind in types)


async def _published(client: httpx.AsyncClient, kind: layout.Kind) -> list[str]:
    """The ids of the published datasets of *kind*, from its sub-catalog, which exists once one is published."""
    response = await client.get(subcatalog_path(kind))
    if response.status_code == 404:
        return []
    if not response.is_success:
        raise ToolError(
            f"The instance refused {subcatalog_path(kind)} ({response.status_code}): {_error_detail(response)}"
        )
    # A child link is `./{id}/collection.json`.
    links: list[dict[str, Any]] = response.json().get("links", [])
    return [unquote(str(link["href"]).rsplit("/", 2)[-2]) for link in links if link.get("rel") == "child"]


async def _catalogue(client: httpx.AsyncClient) -> list[DatasetSummary]:
    ids = [
        (kind, dataset_id)
        for kind in (layout.Kind.RASTER, layout.Kind.VECTOR)
        for dataset_id in await _published(client, kind)
    ]
    # The records, not the STAC documents, know what each dataset holds and when it was last
    # updated. Tables, and features other than areas, are left out: no tool reads them yet.
    records = {item["dataset_id"]: item for item in (await _get_json(client, "/datasets?f=json")).get("items", [])}
    features = {item["id"]: item for item in (await _get_json(client, "/features")).get("items", [])}
    summaries: list[DatasetSummary] = []
    for stored_as, dataset_id in ids:
        record = records.get(dataset_id, {})
        if stored_as is layout.Kind.RASTER:
            kind: Literal["raster", "areas"] = "raster"
        elif _is_areas(features.get(dataset_id, {})):
            kind = "areas"
        else:
            continue
        try:
            collection = await _get_json(client, collection_path(stored_as, dataset_id))
        except ToolError as error:
            # One broken or just-removed dataset must not hide the rest of the catalogue.
            logger.warning("Leaving %s out of the MCP catalogue: %s", dataset_id, error)
            continue
        summary = _summarise(dataset_id, collection, kind, _links(stored_as, dataset_id))
        summary.last_updated = record.get("last_updated")
        summaries.append(summary)
    return summaries


async def _area_file(client: httpx.AsyncClient, collection_id: str) -> tuple[dict[str, Any], bytes]:
    """An area collection's record and its GeoParquet file."""
    segment = path_segment(_checked_id(collection_id))
    record = await _get_json(client, f"/features/{segment}")
    if not _is_areas(record):
        held = ", ".join(record.get("geometry_types") or ["no geometries"])
        raise ToolError(f"{collection_id} holds {held}, not areas. Use list_datasets for the area collections.")
    response = await client.get(f"/features/{segment}/data.parquet")
    if not response.is_success:
        raise ToolError(
            f"The instance refused the areas of {collection_id} ({response.status_code}): {_error_detail(response)}"
        )
    return record, response.content


def _read_areas(content: bytes, record: dict[str, Any]) -> list[Area]:
    """The areas in a collection's GeoParquet file, sorted by name, without decoding a geometry."""
    file = pq.ParquetFile(io.BytesIO(content))
    schema = file.schema_arrow
    id_property = record["id_property"]
    name_property = next((name for name in ("name", "displayName") if name in schema.names), id_property)
    scalar = [
        field.name
        for field in schema
        if field.name not in (id_property, name_property, record.get("primary_geometry"))
        and not pa_types.is_nested(field.type)  # structs such as Overture's bbox
        and not pa_types.is_binary(field.type)
    ]
    rows = file.read(columns=list(dict.fromkeys([id_property, name_property, *scalar]))).to_pylist()
    areas = [
        Area(id=str(row[id_property]), name=str(row[name_property]), properties={key: row[key] for key in scalar})
        for row in rows
    ]
    return sorted(areas, key=lambda area: area.name)


def _bounds(content: bytes, record: dict[str, Any], ids: set[str]) -> Bounds:
    """The bounds of the areas with these ids, in WGS 84, the CRS a collection is stored in."""
    id_property, geometry = record["id_property"], record["primary_geometry"]
    columns = pq.read_table(io.BytesIO(content), columns=[id_property, geometry]).to_pydict()
    selected = [wkb for key, wkb in zip(columns[id_property], columns[geometry], strict=True) if str(key) in ids]
    west, south, east, north = shapely.total_bounds(shapely.from_wkb(selected))
    return float(west), float(south), float(east), float(north)


def _grid(collection: dict[str, Any]) -> tuple[Any, dict[str, Any], dict[str, Any]] | None:
    """A raster's CRS and its x and y dimensions, or None when the collection does not give its grid.

    Each dimension has a numeric `step` and an `extent` from the first cell centre to the last.
    """
    dimensions: dict[str, Any] = collection.get("cube:dimensions") or {}
    axes = {dim.get("axis"): dim for dim in dimensions.values() if dim.get("type") == "spatial"}
    x, y = axes.get("x") or {}, axes.get("y") or {}
    for dim in (x, y):
        extent = dim.get("extent")
        if not isinstance(dim.get("step"), int | float) or not isinstance(extent, list) or len(extent) != 2:
            return None
    return x.get("reference_system", 4326), x, y


def _raster_extent(collection: dict[str, Any], bounds: Bounds) -> dict[str, Any] | None:
    """WGS 84 bounds as a `load_collection` spatial extent in the raster's own CRS, one cell wider.

    `load_collection` keeps the cells whose centres lie in the extent; the margin keeps every
    cell the bounds touch. None when the collection does not give its grid: the whole raster is
    then read.
    """
    grid = _grid(collection)
    if grid is None:
        return None
    crs, x, y = grid
    transformer = Transformer.from_crs(4326, CRS.from_user_input(crs), always_xy=True)
    west, south, east, north = transformer.transform_bounds(*bounds)
    x_step, y_step = abs(x["step"]), abs(y["step"])
    extent = {"west": west - x_step, "south": south - y_step, "east": east + x_step, "north": north + y_step}
    return extent if crs == 4326 else {**extent, "crs": crs}


def _cell_centre(collection: dict[str, Any], lon: float, lat: float) -> tuple[float, float]:
    """The centre of the grid cell a point falls in, in WGS 84; the point itself without a grid.

    A point within a thousandth of a cell of an edge takes the cell to its north-east. Stored
    coordinates carry float noise, so a point given on an edge, as one with two decimals often
    is, would otherwise fall a hair to either side of it, or between cells.
    """
    grid = _grid(collection)
    if grid is None:
        return lon, lat
    crs, x, y = grid
    transformer = Transformer.from_crs(4326, CRS.from_user_input(crs), always_xy=True)
    centre: list[float] = []
    for value, dim in zip(transformer.transform(lon, lat), (x, y), strict=True):
        first, last = sorted(dim["extent"])
        # STAC rounds the step; the extent's cell count gives it back exactly.
        cells = round((last - first) / abs(dim["step"]))
        step = (last - first) / cells if cells else abs(dim["step"])
        centre.append(first + math.floor(round((value - first) / step + 0.5, 3)) * step)
    centre_lon, centre_lat = transformer.transform(centre[0], centre[1], direction="INVERSE")
    return float(centre_lon), float(centre_lat)


def _instant(text: str | None) -> pd.Timestamp | None:
    if not text:
        return None
    stamp = pd.Timestamp(text)
    return stamp.tz_convert(None) if stamp.tzinfo else stamp


def _period_count(collection: dict[str, Any], start: str | None, end: str | None) -> int | None:
    """About how many periods a query from start to end covers; None when the dimensions do not say."""
    count = 1
    for dim in _period_dimensions(collection).values():
        values = dim.get("values")
        if dim.get("type") != "temporal":
            if not isinstance(values, list):
                return None
            count *= len(values)
            continue
        extent = dim.get("extent") or [None, None]
        try:
            lower = max((t for t in (_instant(extent[0]), _instant(start)) if t is not None), default=None)
            upper = min((t for t in (_instant(extent[1]), _instant(end)) if t is not None), default=None)
            if isinstance(values, list):
                instants = [t for t in (_instant(str(value)) for value in values) if t is not None]
                count *= sum(1 for t in instants if (lower is None or t >= lower) and (upper is None or t <= upper))
                continue
            step = dim.get("step")
            if lower is None or upper is None or not isinstance(step, str):
                return None
            hours = (upper - lower).total_seconds() / 3600
            count *= max(0, math.floor(hours / iso_step_to_approx_hours(step)) + 1)
        except ValueError:
            return None
    return count


def _number(text: str) -> float | None:
    try:
        value = float(text)
    except ValueError:
        return None
    return None if math.isnan(value) else value


async def _execute_table(
    client: httpx.AsyncClient, graph: dict[str, Any], variable: str
) -> tuple[str | None, list[dict[str, str]]]:
    """Run a process graph that saves CSV; return the period column's name and the rows."""
    response = await client.post("/result", json={"process": {"process_graph": graph}})
    if not response.is_success:
        raise ToolError(f"The instance could not compute this ({response.status_code}): {_error_detail(response)}")
    reader = csv.DictReader(io.StringIO(response.text))
    columns = reader.fieldnames or []
    value_column = variable if variable in columns else columns[-1]
    period_column = next((c for c in columns if c not in ("feature_id", value_column, "x", "y")), None)
    rows = [
        {
            "period": row[period_column] if period_column else "",
            "feature_id": row.get("feature_id", ""),
            "value": row[value_column],
        }
        for row in reader
    ]
    return period_column, rows


def _check_size(rows: int | None) -> None:
    """Refuse a result too large to read. Checked on the estimate before computing, and on the result."""
    if rows is not None and rows > MAX_ROWS:
        raise ToolError(
            f"That is {rows} rows, more than the {MAX_ROWS} a result may hold. "
            "Narrow the period with start and end, or ask for fewer areas."
        )


def _load(dataset_id: str, start: str | None, end: str | None, spatial_extent: dict[str, Any] | None) -> dict[str, Any]:
    arguments: dict[str, Any] = {"id": dataset_id}
    if spatial_extent:
        arguments["spatial_extent"] = spatial_extent
    if start or end:
        arguments["temporal_extent"] = [start, end]
    return {"process_id": "load_collection", "arguments": arguments}


def build_server() -> MCPServer:
    """The MCP server: catalogue, areas, and point and zonal statistics."""
    server = MCPServer(name="Open Geo Service", instructions=INSTRUCTIONS)
    read_only = ToolAnnotations(read_only_hint=True, destructive_hint=False, open_world_hint=False)

    @server.resource(
        "ocs://catalogue",
        name="catalogue",
        description="Every published dataset and area collection on this instance.",
        mime_type="application/json",
    )
    async def catalogue() -> str:
        async with _ocs() as client:
            return json.dumps([summary.model_dump() for summary in await _catalogue(client)])

    @server.tool(annotations=read_only)
    async def list_datasets() -> list[DatasetSummary]:
        """List the datasets this instance publishes.

        Rasters are climate variables over time, with their unit, variable and time range. Area
        collections are districts or other administrative units.
        """
        async with _ocs() as client:
            return await _catalogue(client)

    @server.tool(annotations=read_only)
    async def list_areas(
        collection_id: str,
        name: str | None = Field(
            default=None, description="Part of an area's name, any case, to narrow a long collection."
        ),
    ) -> AreaList:
        """List the areas in an area collection, by name and id.

        Each comes with attributes such as its parent region. Pick an area by name here, then pass
        its id to get_zonal_statistics.
        """
        async with _ocs() as client:
            record, content = await _area_file(client, collection_id)
        areas = await run_in_threadpool(_read_areas, content, record)
        if name:
            areas = [area for area in areas if name.casefold() in area.name.casefold()]
        if len(areas) > MAX_ROWS:
            raise ToolError(
                f"{collection_id} has {len(areas)} areas, more than the {MAX_ROWS} a result may hold. "
                "Pass part of the name you are looking for."
            )
        return AreaList(collection_id=collection_id, areas=areas)

    @server.tool(annotations=read_only)
    async def get_timeseries(
        dataset_id: str,
        lon: float = Field(ge=-180, le=180, description="Longitude, WGS 84."),
        lat: float = Field(ge=-90, le=90, description="Latitude, WGS 84."),
        start: str | None = Field(default=None, description="First period, ISO 8601 date, e.g. 2024-01-01."),
        end: str | None = Field(default=None, description="Last period, ISO 8601 date."),
    ) -> TimeseriesResult:
        """Values of a raster dataset at one location over time.

        The value of the grid cell the point falls in, for each period between start and end. A
        point on the edge between cells takes the cell to its north-east.
        """
        async with _ocs() as client:
            collection = await _raster(client, dataset_id)
            variable, unit = _raster_variable(collection)
            _check_size(_period_count(collection, start, end))
            centre_lon, centre_lat = _cell_centre(collection, lon, lat)
            e = _POINT_HALF_SIDE
            west, south, east, north = centre_lon - e, centre_lat - e, centre_lon + e, centre_lat + e
            ring = [[west, south], [east, south], [east, north], [west, north], [west, south]]
            point = {"type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [ring]}}
            graph = {
                "load": _load(dataset_id, start, end, _raster_extent(collection, (west, south, east, north))),
                "point": {
                    "process_id": "aggregate_spatial_weighted",
                    "arguments": {
                        "data": {"from_node": "load"},
                        "geometries": {"type": "FeatureCollection", "features": [point]},
                        "reducer": "mean",
                    },
                },
                "save": {
                    "process_id": "save_result",
                    "arguments": {"data": {"from_node": "point"}, "format": "CSV"},
                    "result": True,
                },
            }
            period_column, rows = await _execute_table(client, graph, variable)
            _check_size(len(rows))
            return TimeseriesResult(
                dataset_id=dataset_id,
                variable=variable,
                unit=unit,
                period_dimension=period_column,
                lon=lon,
                lat=lat,
                rows=[TimeseriesRow(period=row["period"], value=_number(row["value"])) for row in rows],
                process_graph=graph,
                **_links(layout.Kind.RASTER, dataset_id),
            )

    @server.tool(annotations=read_only)
    async def get_zonal_statistics(
        dataset_id: str,
        collection_id: str = Field(description="The area collection, as list_datasets names it."),
        area_ids: list[str] | None = Field(
            default=None, description="Ids from list_areas. Omit to get every area, e.g. to rank them."
        ),
        start: str | None = Field(default=None, description="First period, ISO 8601 date, e.g. 2024-01-01."),
        end: str | None = Field(default=None, description="Last period, ISO 8601 date."),
        statistic: Statistic = "mean",
    ) -> ZonalResult:
        """A statistic of a raster dataset over each area of a collection, per period.

        Covers each period between start and end. Area-weighted: a grid cell counts in proportion
        to how much of it lies in the area.
        """
        if area_ids is not None and not area_ids:
            raise ToolError("area_ids is empty. Omit it for every area, or pass ids from list_areas.")
        async with _ocs() as client:
            collection = await _raster(client, dataset_id)
            variable, unit = _raster_variable(collection)
            record, content = await _area_file(client, collection_id)
            names = {area.id: area.name for area in await run_in_threadpool(_read_areas, content, record)}
            wanted = set(area_ids or names)
            unknown = sorted(wanted - names.keys())
            if unknown:
                raise ToolError(f"No area {', '.join(unknown)} in {collection_id}. Use list_areas for the ids.")
            periods = _period_count(collection, start, end)
            _check_size(None if periods is None else periods * len(wanted))
            # Areas asked for by id are computed with only their neighbours, over only the part
            # of the raster they cover; every area is computed over the whole raster.
            areas: dict[str, Any] = {"id": collection_id}
            raster_extent = None
            if area_ids:
                bounds = await run_in_threadpool(_bounds, content, record, wanted)
                areas["spatial_extent"] = dict(zip(("west", "south", "east", "north"), bounds, strict=True))
                raster_extent = _raster_extent(collection, bounds)
            graph = {
                "load": _load(dataset_id, start, end, raster_extent),
                "areas": {"process_id": "load_collection", "arguments": areas},
                "zonal": {
                    "process_id": "aggregate_spatial_weighted",
                    "arguments": {
                        "data": {"from_node": "load"},
                        "geometries": {"from_node": "areas"},
                        "reducer": statistic,
                    },
                },
                "save": {
                    "process_id": "save_result",
                    "arguments": {"data": {"from_node": "zonal"}, "format": "CSV"},
                    "result": True,
                },
            }
            period_column, rows = await _execute_table(client, graph, variable)
            rows = [row for row in rows if row["feature_id"] in wanted]
            _check_size(len(rows))
            return ZonalResult(
                dataset_id=dataset_id,
                collection_id=collection_id,
                statistic=statistic,
                variable=variable,
                unit=unit,
                period_dimension=period_column,
                rows=[
                    ZonalRow(
                        period=row["period"],
                        area_id=row["feature_id"],
                        area_name=names[row["feature_id"]],
                        value=_number(row["value"]),
                    )
                    for row in rows
                ],
                process_graph=graph,
                **_links(layout.Kind.RASTER, dataset_id),
            )

    return server
