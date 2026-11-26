"""README.md and AGENTS.md for the STAC catalog and each collection: raster, feature or table.

Linked from the STAC documents as `describedby` and `agents`, so a person or an agent can find
out what the data is and how to read it without knowing the API. Generated from the records and
templates the catalogue is already built from, so they never say anything the STAC does not.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import HTTPException, Request

from open_geo_service.data_registry.services import datasets as registry_datasets
from open_geo_service.features import store
from open_geo_service.features import templates as feature_templates
from open_geo_service.ingestions.schemas import ArtifactFormat, ArtifactRecord
from open_geo_service.shared import layout
from open_geo_service.shared.licences import parse_licence
from open_geo_service.shared.urls import absolute_url, configured_base, path_segment
from open_geo_service.stac import services
from open_geo_service.stac.media_types import data_group_open_kwargs

if TYPE_CHECKING:
    from open_geo_service.tables.schemas import TableSchema


def catalog_readme(request: Request) -> str:
    """What the catalog holds: every published collection, with the licence each declares."""
    lines = [
        f"# {_instance_name()}",
        "",
        "The published datasets of this instance: rasters as GeoZarr stores, vector datasets as "
        "GeoParquet, and tables as Parquet.",
        "",
        "Each collection declares its own licence in its STAC document, and its providers where they are known.",
        "",
    ]
    artifacts = services._eligible_artifacts_by_dataset()
    for kind in services.published_kinds(artifacts):
        title, _description = services.SUBCATALOGS[kind]
        lines += [f"## [{title}]({absolute_url(request, services.subcatalog_path(kind))})", ""]
        lines += _collection_list(kind, artifacts, request)
        lines.append("")
    return "\n".join(lines)


def subcatalog_readme(kind: layout.Kind, request: Request) -> str:
    """What one sub-catalog holds: its collections."""
    artifacts = services._eligible_artifacts_by_dataset()
    if kind not in services.published_kinds(artifacts):
        raise HTTPException(status_code=404, detail=f"No published collections in '{kind.value}'")
    title, description = services.SUBCATALOGS[kind]
    lines = [f"# {title}", "", f"{description} Part of the {_instance_name()} catalog.", "", "## Collections", ""]
    return "\n".join([*lines, *_collection_list(kind, artifacts, request)]) + "\n"


def _collection_list(kind: layout.Kind, artifacts: dict[str, ArtifactRecord], request: Request) -> list[str]:
    return [
        f"- [{artifact.dataset_name}]({absolute_url(request, services.collection_path(kind, dataset_id))})"
        for dataset_id, artifact in artifacts.items()
        if layout.kind_of(artifact.format) is kind
    ]


_READING: dict[layout.Kind, str] = {
    layout.Kind.RASTER: "A **raster** collection has an `icechunk` asset: an Icechunk repository of a Zarr v3 store "
    "with GeoZarr metadata, which its `AGENTS.md` link says how to open. On an instance with a public address it "
    'also has a `zarr` asset, the same data as a plain Zarr v3 store: `xr.open_zarr(asset["href"], '
    '**asset["xarray:open_kwargs"])`. Each asset\'s `xarray:open_kwargs` name the full-resolution group of a '
    "multiscale store.",
    layout.Kind.VECTOR: "A **vector** collection has a `data` asset: one GeoParquet 1.1 file with a `bbox` covering "
    "column, readable with DuckDB, GeoPandas or any Parquet reader. Its `AGENTS.md` link says how.",
    layout.Kind.TABLE: "A **table** collection has a `data` asset: one Parquet file, whose columns `table:columns` "
    "describes. Its `README.md` link says how to read it and what it joins to.",
}
"""How an agent reads a collection of each kind."""

_RANGE_REQUESTS = "Every asset is served with range requests, so a reader fetches only what it needs."


def catalog_agents(request: Request) -> str:
    """How an agent reads this catalog."""
    catalog = absolute_url(request, services.CATALOG_PATH)
    return "\n".join(
        [
            f"# Reading the {_instance_name()} catalog",
            "",
            f"Start at {catalog}. Each `child` link is a sub-catalog of one kind of dataset, and each of "
            "its `child` links is one collection.",
            "",
            *(f"- {text}" for text in _READING.values()),
            "",
            _RANGE_REQUESTS,
            "",
        ]
    )


def subcatalog_agents(kind: layout.Kind, request: Request) -> str:
    """How an agent reads one sub-catalog."""
    if kind not in services.published_kinds(services._eligible_artifacts_by_dataset()):
        raise HTTPException(status_code=404, detail=f"No published collections in '{kind.value}'")
    here = absolute_url(request, services.subcatalog_path(kind))
    return "\n".join(
        [
            f"# Reading {services.SUBCATALOGS[kind][0].lower()}",
            "",
            f"Start at {here}. Each `child` link is one collection.",
            "",
            _READING[kind],
            "",
            _RANGE_REQUESTS,
            "",
        ]
    )


def collection_readme(kind: layout.Kind, dataset_id: str, request: Request) -> str:
    """Title, description, licence, provenance and shape of one collection."""
    artifact, template = _published_collection(kind, dataset_id)
    if artifact.format == ArtifactFormat.PARQUET:
        return _table_readme(dataset_id, artifact, template, request)
    raster = artifact.format != ArtifactFormat.GEOPARQUET
    licence = parse_licence(template.get("license"))
    licence_text = licence.name or licence.identifier or licence.stac_license
    if licence.url:
        licence_text = f"[{licence_text}]({licence.url})"
    description = (
        services._collection_description(dataset_id, artifact, template)
        if raster
        else services._feature_collection_description(dataset_id, artifact, template)
    )
    lines = [f"# {artifact.dataset_name}", "", description, "", "## Licence", "", licence_text]
    attribution = template.get("attribution")
    if isinstance(attribution, str) and attribution.strip():
        lines += ["", f"Attribution: {attribution.strip()}"]
    lines += ["", "## Provenance", ""]
    # The providers the STAC document publishes, a workflow's processor before this instance as
    # the one host, so the two documents agree.
    processors = [services._processor_provider(artifact, request)] if artifact.provenance is not None else []
    for provider in services._published_providers(template, request, processors):
        name = f"[{provider['name']}]({provider['url']})" if provider.get("url") else provider["name"]
        roles = f" ({', '.join(provider['roles'])})" if provider.get("roles") else ""
        lines.append(f"- {name}{roles}")
    source_url = template.get("source_url")
    if isinstance(source_url, str) and source_url.strip():
        lines.append(f"- Source: {source_url.strip()}")
    if artifact.version is not None:
        lines.append(f"- Version: {artifact.version.value} ({artifact.version.authority})")
    lines.append(f"- Last refreshed: {artifact.created_at.isoformat()}")
    lines += ["", "## Data", ""]
    if raster:
        units = template.get("units")
        unit_text = f" in {units}" if isinstance(units, str) and units.strip() else ""
        temporal = artifact.coverage.temporal
        lines.append(f"- Variable `{artifact.variable}`{unit_text}.")
        if temporal.start and temporal.end:
            period = f"{artifact.period_type.capitalize()} data" if artifact.period_type else "Data"
            lines.append(f"- {period} from {temporal.start} to {temporal.end}.")
        lines.append("- A Zarr v3 store with GeoZarr metadata; its `AGENTS.md` link says how to open it.")
    elif artifact.features is not None:
        detail = artifact.features
        try:
            types = ", ".join(store.stored_geometry_types(artifact)) or "unknown"
        except (OSError, ValueError):  # an unreadable footer leaves the types unknown, as openEO does
            types = "unknown"
        lines += [
            f"- {detail.feature_count} features, geometry types: {types}.",
            f"- Identifier property: `{detail.id_property}`.",
            f"- Stored in {detail.crs}.",
        ]
    return "\n".join(lines) + "\n"


def collection_agents(kind: layout.Kind, dataset_id: str, request: Request) -> str:
    """How an agent reads one collection's data."""
    artifact, _template = _published_collection(kind, dataset_id)
    if artifact.format == ArtifactFormat.PARQUET:
        return _table_agents(dataset_id, artifact, request)
    if artifact.format != ArtifactFormat.GEOPARQUET:
        return _raster_agents(dataset_id, artifact, request)
    return _feature_agents(dataset_id, artifact, request)


def _raster_agents(dataset_id: str, artifact: ArtifactRecord, request: Request) -> str:
    """How an agent reads one raster collection: the store, how to open it, the variable.

    The Icechunk store is in the catalog folder beside this file; the Zarr route only on an
    instance that knows its public address (see `stac/catalog_files.py`).
    """
    # The same arguments the STAC `zarr` and `icechunk` assets carry as `xarray:open_kwargs`.
    data_group = data_group_open_kwargs(services._zarr_media_type(artifact))
    open_kwargs = {**services._zarr_open_kwargs(artifact), **data_group}
    arguments = "".join(f", {name}={value!r}" for name, value in open_kwargs.items() if value is not None)
    lines = [
        f"# Reading {artifact.dataset_name}",
        "",
        f"The values are in the variable `{artifact.variable}`, over time, latitude and longitude or the "
        "store's own projected axes; the store's `spatial:` and `proj:` attributes give the grid. Any "
        "reader fetches only the chunks it needs.",
        "",
    ]
    if artifact.format == ArtifactFormat.ICECHUNK:
        icechunk_href = absolute_url(request, f"/icechunk/{path_segment(dataset_id)}/")
        lines += [
            f"The data is an Icechunk repository of a Zarr v3 store with GeoZarr metadata: {icechunk_href}",
            "",
            "With the Icechunk library and xarray, from a copy of this folder; over HTTP, use "
            "`icechunk.http_storage` with the store's URL instead:",
            "",
            "```python",
            "import icechunk",
            "import xarray as xr",
            f'repo = icechunk.Repository.open(icechunk.local_filesystem_storage("{icechunk_href}"))',
            'session = repo.readonly_session("main")',
            "ds = xr.open_zarr(session.store, consolidated=False"
            + "".join(f", {name}={value!r}" for name, value in data_group.items())
            + ")",
            f'values = ds["{artifact.variable}"]',
            "```",
            "",
        ]
    if configured_base() or artifact.format != ArtifactFormat.ICECHUNK:
        store_href = absolute_url(request, f"/zarr/{path_segment(dataset_id)}")
        lines += [
            f"The same data as a plain Zarr v3 store, for any Zarr v3 reader over HTTP: {store_href}",
            "",
            "```python",
            "import xarray as xr",
            f'ds = xr.open_zarr("{store_href}"{arguments})',
            "```",
            "",
        ]
    lines += [
        f"The licence, providers and extent are in the STAC collection: "
        f"{absolute_url(request, services.collection_path(layout.kind_of(artifact.format), dataset_id))}",
        "",
    ]
    return "\n".join(lines)


def _feature_agents(dataset_id: str, artifact: ArtifactRecord, request: Request) -> str:
    """How an agent reads one feature collection: the file, its id column, a windowed query."""
    detail = artifact.features
    id_property = detail.id_property if detail is not None else "id"
    geometry = detail.primary_geometry if detail is not None else "geometry"
    crs = detail.crs if detail is not None else "EPSG:4326"
    # The covering bbox is in the stored CRS, which a collection can set to a projected one.
    units = "longitude and latitude" if crs == "EPSG:4326" else f"{crs} coordinates"
    data = absolute_url(request, f"/features/{path_segment(dataset_id)}/data.parquet")
    url_literal = data.replace("'", "''")
    return "\n".join(
        [
            f"# Reading {artifact.dataset_name}",
            "",
            f"The data is one GeoParquet 1.1 file: {data}",
            "",
            f"- One row per feature, identified by `{id_property}`; the geometry is in `{geometry}`.",
            "- A `bbox` covering column holds each feature's bounds, for filtering by area.",
            "",
            f"With DuckDB, reading only one area (replace the four bounds, in {units}):",
            "",
            "```sql",
            "INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;",
            f"SELECT {_sql_identifier(id_property)}, ST_AsText({_sql_identifier(geometry)}) FROM '{url_literal}'",
            "WHERE bbox.xmin < <east> AND bbox.xmax > <west> AND bbox.ymin < <north> AND bbox.ymax > <south>;",
            "```",
            "",
            f"With GeoPandas: `geopandas.read_parquet(path, bbox=(west, south, east, north))` on a downloaded "
            f"copy, with the bounds in {units}.",
            "",
            f"The licence, providers and extent are in the STAC collection: "
            f"{absolute_url(request, services.collection_path(layout.kind_of(artifact.format), dataset_id))}",
            "",
        ]
    )


def _table_readme(dataset_id: str, artifact: ArtifactRecord, template: dict[str, Any], request: Request) -> str:
    """A table's README: what it holds, its sources' licences and providers, its columns, and how to join it."""
    from open_geo_service.tables.sources import table_sources
    from open_geo_service.tables.store import period_label, schema_of, summarize

    sources = table_sources(artifact)
    summary = summarize(artifact)
    declared = schema_of(artifact)
    lines = [f"# {artifact.dataset_name}", "", sources.description(artifact)]
    lines += ["", "## Licence", ""]
    licences = [licence for licence in sources.licences if licence.identifier or licence.name]
    if not licences:
        lines.append("Not declared by the datasets it was computed from.")
    else:
        if len(sources.licences) > 1:
            lines += ["Computed from several datasets, so the licence of each applies:", ""]
        for licence in sources.licences:
            text = licence.name or licence.identifier or "Not declared"
            lines.append(f"- [{text}]({licence.url})" if licence.url else f"- {text}")
    lines += ["", "## Provenance", ""]
    for provider in services._published_providers(template, request, [services._processor_provider(artifact, request)]):
        name = f"[{provider['name']}]({provider['url']})" if provider.get("url") else provider["name"]
        roles = f" ({', '.join(provider['roles'])})" if provider.get("roles") else ""
        lines.append(f"- {name}{roles}")
    for link in services._derived_from(artifact, request):
        lines.append(f"- Computed from: [{link['title']}]({link['href']})")
    for source_url in services._source_pages(dataset_id, {}):
        lines.append(f"- Source: {source_url}")
    lines.append(f"- Last updated: {artifact.created_at.isoformat()}")

    data = absolute_url(request, f"/tables/{path_segment(dataset_id)}/data.parquet")
    lines += ["", "## Data", "", f"One Parquet file, {data}, of {declared.holds(artifact.period_type)}."]
    lines.append("")
    if summary.first_period is not None and summary.last_period is not None:
        period = f"{artifact.period_type.capitalize()} periods" if artifact.period_type else "Periods"
        first = period_label(summary.first_period, artifact.period_type)
        last = period_label(summary.last_period, artifact.period_type)
        lines.append(f"- {summary.row_count} rows. {period} from {first} to {last}.")
    lines += ["", "| Column | Type | Unit | Description |", "| --- | --- | --- | --- |"]
    for column in services._table_dataset_columns(artifact):
        cells = (column["type"], column.get("unit", ""), column.get("description", ""))
        lines.append(f"| `{column['name']}` | {' | '.join(cells)} |")

    join = _join_example(data, summary.collection_ids, declared, request)
    if join:
        collection, feature = declared.represented_by("collection_id"), declared.represented_by("feature_id")
        lines += [
            "",
            "## Joining to the features",
            "",
            f"`{collection}` names the feature collection a row reports on, and `{feature}` the feature, "
            "by the collection's identifier property. With DuckDB, the values with their features' geometry:",
            "",
            "```sql",
            *join,
            "```",
        ]
    return "\n".join(lines) + "\n"


def _join_example(data: str, collection_ids: list[str], declared: TableSchema, request: Request) -> list[str]:
    """A DuckDB query joining the table to the first published feature collection it reports on.

    None for a table without a column representing the feature: its rows report on no collection.
    """
    from open_geo_service.features.services import registered_collections

    collection, feature = declared.represented_by("collection_id"), declared.represented_by("feature_id")
    if collection is None or feature is None:
        return []
    shown = [
        f"t.{_sql_identifier(name)}"
        for name in (
            feature,
            declared.represented_by("feature_name"),
            declared.represented_by("period_start"),
            *declared.values,
        )
        if name
    ]
    order = ", ".join(
        f"t.{_sql_identifier(name)}" for name in (feature, declared.represented_by("period_start")) if name
    )
    published = services._eligible_artifacts_by_dataset()
    collections = registered_collections()
    for collection_id in collection_ids:
        record = collections.get(collection_id)
        if collection_id not in published or record is None or record.features is None:
            continue
        features = absolute_url(request, f"/features/{path_segment(collection_id)}/data.parquet")
        id_column = _sql_identifier(record.features.id_property)
        geometry = _sql_identifier(record.features.primary_geometry)
        return [
            "INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;",
            "SELECT " + ", ".join(shown) + ", ST_AsText(f." + geometry + ") AS geometry",
            "FROM '" + data.replace("'", "''") + "' AS t",
            "JOIN '" + features.replace("'", "''") + "' AS f",
            "  ON t." + _sql_identifier(collection) + " = '" + collection_id.replace("'", "''") + "'",
            "  AND CAST(f." + id_column + " AS VARCHAR) = t." + _sql_identifier(feature),
            "ORDER BY " + order + ";",
        ]
    return []


def _table_agents(dataset_id: str, artifact: ArtifactRecord, request: Request) -> str:
    """How an agent reads one table dataset: the file, its key columns, and the rows API."""
    from open_geo_service.tables.store import schema_of

    declared = schema_of(artifact)
    data = absolute_url(request, f"/tables/{path_segment(dataset_id)}/data.parquet")
    rows = absolute_url(request, f"/tables/{path_segment(dataset_id)}/rows")
    key = ", ".join(f"`{name}`" for name in declared.key)
    about = [f"- The rows are {declared.holds(artifact.period_type)}, one per key: {key}."]
    if declared.values:
        about.append(f"- The values are in {', '.join(f'`{name}`' for name in declared.values)}.")
    if (collection := declared.represented_by("collection_id")) and (feature := declared.represented_by("feature_id")):
        about.append(
            f"- Join it to the feature collection's GeoParquet on `{collection}` and `{feature}`; the README "
            "shows a DuckDB query."
        )
    return "\n".join(
        [
            f"# Reading {artifact.dataset_name}",
            "",
            f"The data is one Parquet file: {data}",
            "",
            *about,
            "",
            "With pandas:",
            "",
            "```python",
            "import pandas as pd",
            f'rows = pd.read_parquet("{data}")',
            "```",
            "",
            f"Or as JSON or CSV, filtered by feature, collection or period: {rows}?feature=<feature_id>"
            if declared.represented_by("feature_id")
            else f"Or as JSON or CSV: {rows}",
            "",
            f"The licence, providers and columns are in the STAC collection: "
            f"{absolute_url(request, services.collection_path(layout.kind_of(artifact.format), dataset_id))}",
            "",
        ]
    )


def _published_collection(kind: layout.Kind, dataset_id: str) -> tuple[ArtifactRecord, dict[str, Any]]:
    """The published collection's record and template, or 404 as the STAC route does."""
    artifact = services.published_artifact(kind, dataset_id)
    if artifact.format == ArtifactFormat.GEOPARQUET:
        return artifact, feature_templates.get_feature_template(artifact.dataset_id) or {}
    if artifact.format == ArtifactFormat.PARQUET:
        from open_geo_service.tables.sources import table_sources

        return artifact, table_sources(artifact).template()
    return artifact, registry_datasets.get_dataset(artifact.dataset_id) or {}


def _sql_identifier(name: str) -> str:
    """A column name quoted for SQL, so spaces, punctuation or a reserved word stay one name."""
    return '"' + name.replace('"', '""') + '"'


def _instance_name() -> str:
    from open_geo_service import config as api_config

    return api_config.get_name()
