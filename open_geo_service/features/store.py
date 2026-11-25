"""The feature collection store: where a collection is written, and how it is read back.

One file per collection version, in the collection's folder (`shared.layout`), written as GeoParquet
and read back through `read_feature_collection`. `ingestions.services.create_feature_artifact`
turns a written file into a registered collection; nothing here writes a record, and nothing
here discovers a file on disk. That split is deliberate: a record is what makes a collection
exist, so the store directory is not an inbox.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from open_geo_service import writer_lease
from open_geo_service.features import tiles
from open_geo_service.ingestions.schemas import ArtifactRecord
from open_geo_service.shared import geoparquet, layout
from open_geo_service.shared.crs import canonical_crs_code, transform_bbox
from open_geo_service.shared.urls import require_segment_safe_id
from open_geo_service.shared.versions import prune_superseded

if TYPE_CHECKING:
    import geopandas as gpd

logger = logging.getLogger(__name__)

WGS84 = "EPSG:4326"
"""The CRS a bbox is expressed in unless a caller says otherwise.

GeoJSON is WGS 84 by RFC 7946, every OGC API bbox defaults to it, and the configured instance
extent is stored in it — so it is what a caller means when it passes four numbers and says
nothing else.
"""

UNQUALIFIED_READ_LIMIT = 5000
"""Feature count above which a read must narrow itself with a bbox or say `max_unqualified_read=None`.

A DHIS2 hierarchy runs to the thousands, and pulling all of it is a real operation — just
never an accidental one. The guard catches the caller that forgot a bbox, not the caller that
means it, which is why `max_unqualified_read=None` is an ordinary argument rather than a
setting an operator has to find. The number is a backstop rather than a measured capacity: it
sits above a country's divisions at every level and below a national facility register.
"""

ROW_GROUP_SIZE = 100_000
"""The most rows one Parquet row group holds.

A bbox read skips whole row groups by their bbox statistics, so a collection in one row group is
read whole whatever the window. Portolan caps row groups at 150,000 rows; this stays under it.
"""


_collection_locks: dict[str, threading.Lock] = {}
_collection_locks_mutex = threading.Lock()


def collection_lock(dataset_id: str) -> threading.Lock:
    """Return the exclusive lock for one collection, creating it on first use.

    Serializes a refresh end to end — write, replace, register — rather than only the write.
    Those are separate operations on the same collection, so without this two refreshes can
    interleave: both replace the file, and whichever registers last stamps its count, extent and
    CRS onto whatever bytes happen to be on disk.

    In-process, like `ingestions.services._acquire_store_lock` for raster stores. The record
    index itself is guarded across processes by portalocker; a store is guarded within one, which
    is the deployment this serves — one instance owning its data directory, which the writer
    lease enforces: a process that is not the instance's writer is refused here.
    """
    writer_lease.ensure_held()
    with _collection_locks_mutex:
        return _collection_locks.setdefault(dataset_id, threading.Lock())


def validate_collection_id(dataset_id: str) -> str:
    """Return *dataset_id* unchanged, or refuse an id that cannot name a file or a URL segment.

    `dataset_id` reaches here from a template id or a provider, so it is checked rather than
    trusted — see `shared.urls.SEGMENT_SAFE_ID_PATTERN` for what the id has to satisfy and why.
    The same constraint governs raster template ids, because both become URL path segments.
    """
    return require_segment_safe_id(dataset_id, "feature collection id")


def new_collection_file(dataset_id: str) -> Path:
    """Return the path a *new* version of this collection will be written to.

    Each write lands on its own immutable path rather than replacing one stable file, and the
    record is what says which of them is current. That is what closes the gap between the bytes
    and the record describing them: replacing a stable path makes the new bytes visible to every
    reader the instant they land, while the record still says how many features the *old* file
    held — and a crash in that window leaves the mismatch for good, because both the record and
    the file are individually valid.

    With a version per write, a reader resolves record then path and gets bytes that record
    actually describes, whichever record it read. The cost is that old files outlive their
    records until `prune_superseded_files` removes them, which is the right way round: an
    unreferenced file wastes space, a misdescribed one gives wrong answers.
    """
    return _collection_root(dataset_id) / f"{dataset_id}.{uuid4().hex}.parquet"


def collection_files(dataset_id: str) -> list[Path]:
    """Return this collection's stored files, current and replaced alike.

    `<id>.*.parquet` matches only this collection's files: an id has no dots, so the files of
    `a` and `a_b` never match each other's pattern.
    """
    root = _collection_root(dataset_id)
    return sorted(root.glob(f"{dataset_id}.*.parquet")) if root.is_dir() else []


def prune_superseded_files(dataset_id: str, *, keep: Path | None) -> None:
    """Remove this collection's files that a newer one replaced more than the grace period ago.

    `keep` is the file the current record names after any rollback, or None when the collection
    has no record. Each file's vector tiles go with it.
    """
    prune_superseded(collection_files(dataset_id), keep=keep, remove=_remove_version)


def _remove_version(path: Path) -> None:
    # The archive first, so that if it cannot be removed, the Parquet file is still here for the
    # next prune to find, and both are tried again.
    tiles.archive_path(path).unlink(missing_ok=True)
    path.unlink()


def _collection_root(dataset_id: str) -> Path:
    """Return the collection's folder, having checked the id that will name files inside it."""
    validate_collection_id(dataset_id)
    return layout.dataset_dir(layout.Kind.VECTOR, dataset_id)


def validate_features_for_write(*, dataset_id: str, features: object, id_property: str) -> gpd.GeoDataFrame:
    """Return a frame that can be stored and found by bbox reads, before touching any file.

    Three rules, one place, so `write_feature_collection` refuses on the same terms as the
    refresh path that calls this ahead of backing the previous collection up:

    * identity — each feature's id is present and unique, from `validate_feature_ids`;
    * geometry — missing and empty shapes are invisible to bbox reads, so check the converted
      frame rather than enumerating GeoJSON spellings for them;
    * names — `bbox` conflicts with the covering column, while `geometry` conflicts with the
      geometry column; both are refused with a rename the provider can act on.
    """
    import geopandas as gpd

    from open_geo_service.shared.features import validate_feature_ids

    members = _members(features)
    validate_feature_ids(features, id_property=id_property)
    for index, feature in enumerate(members):
        properties = feature.get("properties")
        if isinstance(properties, dict):
            for reserved in ("bbox", "geometry"):
                if reserved in properties:
                    raise ValueError(
                        f"feature collection '{dataset_id}' has a property named '{reserved}' at index {index}; "
                        "rename the property before writing"
                    )
    try:
        frame = gpd.GeoDataFrame.from_features(members, crs=WGS84)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"feature collection '{dataset_id}' has malformed GeoJSON geometry: {exc}") from exc
    if frame.empty:
        raise ValueError(f"feature collection '{dataset_id}' has no features to write")
    invalid = frame.geometry.isna() | frame.geometry.is_empty
    for index, bad in enumerate(invalid):
        if bad:
            raise ValueError(
                f"feature collection '{dataset_id}' has a feature with no geometry at index {index}; "
                "missing or empty geometry cannot be found by any bbox read"
            )
    if id_property not in frame.columns:
        raise ValueError(
            f"feature collection '{dataset_id}' declares id_property '{id_property}', but no such "
            f"column survived conversion; got {sorted(frame.columns)}"
        )
    return frame


def write_feature_collection(
    *,
    dataset_id: str,
    features: object,
    id_property: str,
    store_crs: str = WGS84,
) -> tuple[Path, int, str]:
    """Write one GeoJSON FeatureCollection to the store, returning (path, count, geometry column).

    Returns exactly what `create_feature_artifact` needs to build a record, because those three
    facts are the store's to report: it is the thing that knows how many rows it wrote and what
    it called the geometry column. Nothing is registered here, and nothing existing is touched —
    each write lands on its own path (see `new_collection_file`), so a caller that writes and
    then fails to register has left an unreferenced file the listing ignores, with the previous
    collection still in place and still correctly described by its record.

    Identity is validated before anything is written. A duplicate identifier is not a dropped
    feature: two features map onto one org unit, DHIS2 keeps whichever value arrives last, and
    the result is silently wrong — so the write does not happen at all.

    `write_covering_bbox` is what makes `read_feature_collection`'s bbox argument a pushdown
    rather than a filter over everything: it writes the GeoParquet 1.1 bbox covering column, so
    a windowed read skips whole row groups instead of decoding them. For that, a row group must
    cover a small area, so rows are sorted along a Hilbert curve and row groups are bounded at
    `ROW_GROUP_SIZE`. Both are Portolan requirements as well.

    The input is GeoJSON, so its coordinates are WGS 84 by RFC 7946 whatever `store_crs` says.
    A different `store_crs` therefore *reprojects* rather than relabels: tagging WGS 84
    coordinates with a projected CRS would leave every later read windowing the wrong numbers,
    and nothing downstream could detect it.
    """
    frame = validate_features_for_write(dataset_id=dataset_id, features=features, id_property=id_property)
    target = canonical_crs_code(store_crs)
    if target != WGS84:
        frame = frame.to_crs(target)
    # Sorted in the stored CRS, so nearby rows are nearby in the coordinates a read windows on.
    # The index is reset, or the permuted one would be written as an extra column.
    frame = frame.iloc[frame.hilbert_distance().argsort(kind="stable")].reset_index(drop=True)

    path = new_collection_file(dataset_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Written in the staging folder and moved into place, so a reader never sees a partial file.
    # The target is new on every write, so this replaces nothing a record points at: an
    # interrupted write leaves an unreferenced file, never a truncated collection.
    staging = layout.staging_dir() / f"{path.name}.{uuid4().hex}.writing"
    try:
        frame.to_parquet(
            staging,
            write_covering_bbox=True,
            schema_version="1.1.0",
            row_group_size=ROW_GROUP_SIZE,
            compression=geoparquet.PARQUET_COMPRESSION,
        )
        staging.replace(path)
    finally:
        staging.unlink(missing_ok=True)
    return path, len(frame), str(frame.geometry.name)


def read_feature_collection(
    record: ArtifactRecord,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    bbox_crs: str = WGS84,
    max_unqualified_read: int | None = UNQUALIFIED_READ_LIMIT,
) -> gpd.GeoDataFrame:
    """Read a registered collection, optionally windowed to *bbox*.

    `bbox` is interpreted in `bbox_crs` and reprojected into the collection's own CRS before it
    reaches the file, so a WGS 84 window works against a collection stored in a projected CRS.
    Reprojecting the window rather than the rows is what keeps the read a pushdown: the rows
    stay in the CRS they were written in, and only four numbers cross CRSs.

    `max_unqualified_read` guards an *unqualified* read — one with no bbox — against pulling a
    whole hierarchy by accident. A windowed read is already narrowed by the window, so the guard
    does not apply to it; `max_unqualified_read=None` turns it off for a caller that means to
    read everything.
    """
    import geopandas as gpd

    detail = record.features
    if detail is None:
        raise ValueError(f"artifact '{record.artifact_id}' is not a feature collection, so it has no features to read")
    path = _stored_path(record)
    if bbox is None and max_unqualified_read is not None and detail.feature_count > max_unqualified_read:
        raise ValueError(
            f"feature collection '{record.dataset_id}' holds {detail.feature_count} features, above the "
            f"unqualified-read limit of {max_unqualified_read}; pass a bbox to narrow the read, or "
            "max_unqualified_read=None to read all of it deliberately"
        )
    window = transform_bbox(bbox, source=bbox_crs, target=detail.crs) if bbox is not None else None
    frame: gpd.GeoDataFrame = gpd.read_parquet(path, bbox=window)
    return frame


def read_feature_columns(record: ArtifactRecord, columns: Sequence[str]) -> list[dict[str, Any]]:
    """Some attribute columns of a registered collection, one row per feature, without its geometry.

    For a caller that needs names or codes, not shapes: only the named columns are read from the
    file. A column the collection does not have is left out of every row.
    """
    import pyarrow.parquet as pq

    path = _stored_path(record)
    present = set(pq.read_schema(path).names)
    wanted = [column for column in dict.fromkeys(columns) if column in present]
    if not wanted:
        return []
    rows: list[dict[str, Any]] = pq.read_table(path, columns=wanted).to_pylist()
    return rows


def stored_geometry_types(record: ArtifactRecord) -> list[str]:
    """Return the geometry types a stored collection declares, read from its file footer."""
    return geoparquet.stored_geometry_types(
        _stored_path(record), column=record.features.primary_geometry if record.features else None
    )


def _stored_path(record: ArtifactRecord) -> Path:
    """Return the file a record points at, or say plainly that it is gone."""
    raw = record.path or (record.asset_paths[0] if record.asset_paths else None)
    if raw is None:
        raise ValueError(f"feature collection '{record.dataset_id}' has no stored path on its record")
    path = Path(raw)
    if not path.is_file():
        raise ValueError(
            f"feature collection '{record.dataset_id}' is registered at {path}, but no file is there; "
            "the record and the store disagree"
        )
    return path


def _members(features: object) -> list[Any]:
    """Return the member features of an already-validated FeatureCollection.

    A non-string Sequence, matching `validate_feature_ids` and the ingestion path: a tuple is
    what a provider that built its features with a comprehension hands over, and the three
    checks are on one path, so they have to agree about what a collection is.
    """
    if isinstance(features, Mapping) and features.get("type") == "FeatureCollection":
        members = features.get("features")
        if isinstance(members, Sequence) and not isinstance(members, (str, bytes)):
            return list(members)
    raise ValueError("expected a GeoJSON FeatureCollection to write")
