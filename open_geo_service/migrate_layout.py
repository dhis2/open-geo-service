"""Move an instance onto the catalog layout (docs/data_folder_layout.md).

Run once per instance, with its server stopped, from the directory its ``.env`` is in. It
refuses while a server that holds the writer lease runs, but a server from before the lease
holds none, so during an upgrade stopping the server is up to you:

    python -m open_geo_service.migrate_layout [--dry-run]

Every dataset moves from ``rasters/``, ``vectors/``, ``tables/`` and ``thumbnails/`` into a folder
of its own, ``catalog/<kind>/<id>/``, with its thumbnail. Pending-collection markers move to
``gc-pending/``; the empty per-store lock files, the old ``.superseded`` markers and thumbnails no
record owns are deleted. The store paths in ``artifacts/records.json`` and
``openeo_jobs/jobs.json`` are rewritten to match. Each raster store written before the ``ingest``
branch existed then gets it, and ``main`` becomes its published version (``adopt_write_branch``);
until then no server reads the store. It refuses to run while a record's id breaks the id rule,
which the routes would refuse to serve from its new folder, and while ``catalog/`` or a folder in
it is on another filesystem than the folders it moves from, as it moves by renaming.

Open Geo Service reads only the catalog layout, so an instance that has not been migrated starts
with no stores. Running it again once done changes nothing. It refuses to run when a file or
folder it would move to already exists, rather than merging. An instance still on the layout
before ``rasters/`` and ``vectors/`` (CLIM-1253) runs ``migrate_layout`` of a release from before
November 2026 first; this one refuses until it has.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from contextlib import nullcontext
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

import open_geo_service.startup  # noqa: F401  # pyright: ignore[reportUnusedImport]  # loads .env
from open_geo_service import config as api_config
from open_geo_service import storage, writer_lease
from open_geo_service.shared import layout
from open_geo_service.shared.urls import ID_RULE, is_segment_safe_id

_PATH_FIELDS = ("path", "asset_paths")


class MigrationError(RuntimeError):
    """The layout cannot be migrated without a decision this command will not make."""


OLD_KIND_FOLDERS = tuple(kind.value for kind in layout.Kind)
_RASTER_STORE = re.compile(r"(?P<id>.+)\.(?:icechunk|zarr)")
_VECTOR_FILE = re.compile(r"(?P<id>.+)\.[0-9a-f]{32}\.(?:parquet|pmtiles)")
# Left beside the data by a write an earlier version was killed in, and by the old feature rule:
# the empty per-store lock files (unused since CLIM-1998), half-written files and `.superseded` markers.
# And the macOS folder metadata, so a folder holding only that is removed.
_DISCARDED = (".icechunk.lock", ".writing", ".tmp.png", ".superseded", ".DS_Store")


@dataclass
class CatalogPlan:
    """What moving into ``catalog/`` does, so a dry run can report it and a real run carry it out."""

    root: Path = field(default_factory=Path)
    invalid_ids: list[str] = field(default_factory=list)
    records: tuple[Path, list[Any]] | None = None
    jobs: tuple[Path, list[Any]] | None = None
    moves: list[tuple[Path, Path]] = field(default_factory=list)
    deletions: list[Path] = field(default_factory=list)
    leftovers: list[Path] = field(default_factory=list)
    old_folders: list[Path] = field(default_factory=list)
    # Raster stores, at their path after the moves, to adopt the write branch, and whether published.
    adoptions: list[tuple[Path, bool]] = field(default_factory=list)
    cross_device: list[tuple[Path, Path]] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not (self.records or self.jobs or self.moves or self.deletions or self.old_folders or self.adoptions)


def _catalog_key(key: PurePosixPath) -> PurePosixPath | None:
    """Where a file at *key* (relative to the data folder, old layout) lives in the catalog."""
    parts = key.parts
    if len(parts) < 2 or parts[0] not in OLD_KIND_FOLDERS:
        return None
    kind, name = parts[0], parts[1]
    if kind == layout.Kind.TABLE.value:
        return PurePosixPath(layout.CATALOG, *parts)
    pattern = _RASTER_STORE if kind == layout.Kind.RASTER.value else _VECTOR_FILE
    match = pattern.fullmatch(name)
    if match is None:
        return None
    return PurePosixPath(layout.CATALOG, layout.dataset_key(layout.Kind(kind), match["id"]), *parts[1:])


def _old_key(raw: str, root: Path) -> PurePosixPath | None:
    """The old-layout key of a recorded path, or None when it is not a dataset file of ours.

    A path outside root counts when it no longer exists there and the same file exists under
    root: a container's ``/app/data/rasters/x.icechunk`` seen from the host. One that still exists
    is a store deliberately kept elsewhere, and is left alone.
    """
    if not Path(raw).is_absolute():
        return PurePosixPath(raw)
    candidate = Path(raw)
    try:
        return PurePosixPath(candidate.resolve(strict=False).relative_to(root).as_posix())
    except ValueError:
        pass
    if candidate.exists():
        return None
    parts = candidate.parts
    for index in range(1, len(parts) - 1):
        if parts[index] in OLD_KIND_FOLDERS and root.joinpath(*parts[index:]).exists():
            return PurePosixPath(*parts[index:])
    return None


def _catalog_path(raw: str, root: Path, *, absolute: bool) -> str:
    """*raw* moved into the catalog, relative to root or absolute under it; unchanged if not ours."""
    old = _old_key(raw, root)
    new = _catalog_key(old) if old is not None else None
    if new is None:
        return raw
    return str(root.joinpath(*new.parts)) if absolute else new.as_posix()


def _rewrite_paths(item: Any, keys: tuple[str, ...], root: Path, *, absolute: bool) -> bool:
    """Move the paths under *keys* of the dict *item* into the catalog; return whether any changed."""
    if not isinstance(item, dict):
        return False
    changed = False
    for key in keys:
        value = item.get(key)
        if isinstance(value, str):
            new: Any = _catalog_path(value, root, absolute=absolute)
        elif isinstance(value, list):
            new = [
                _catalog_path(entry, root, absolute=absolute) if isinstance(entry, str) else entry for entry in value
            ]
        else:
            continue
        changed |= new != value
        item[key] = new
    return changed


def _plan_catalog_records(root: Path, plan: CatalogPlan) -> tuple[dict[str, layout.Kind], list[Any]]:
    """Rewrite the record paths and check the ids; return each dataset's kind, and the rewritten records."""
    index = root / "artifacts" / "records.json"
    if not index.is_file():
        return {}, []
    records = json.loads(index.read_text(encoding="utf-8"))
    kinds: dict[str, layout.Kind] = {}
    changed = False
    for record in records:
        dataset_id = str(record.get("dataset_id", ""))
        kinds.setdefault(dataset_id, layout.kind_of(str(record.get("format", ""))))
        if not is_segment_safe_id(dataset_id) and dataset_id not in plan.invalid_ids:
            plan.invalid_ids.append(dataset_id)
        changed |= _rewrite_paths(record, _PATH_FIELDS, root, absolute=False)
    if changed:
        plan.records = (index, records)
    return kinds, records


def _plan_catalog_jobs(root: Path, plan: CatalogPlan) -> None:
    """Rewrite the absolute store paths openEO jobs keep for a publication and its outcome."""
    index = root / "openeo_jobs" / "jobs.json"
    if not index.is_file():
        return
    jobs = json.loads(index.read_text(encoding="utf-8"))
    changed = False
    for job in jobs:
        publication = job.get("publication")
        if isinstance(publication, dict):
            changed |= _rewrite_paths(publication, ("store_path",), root, absolute=True)
            changed |= _rewrite_paths(publication.get("record"), _PATH_FIELDS, root, absolute=True)
        changed |= _rewrite_paths(job.get("usage"), ("output_path",), root, absolute=True)
    if changed:
        plan.jobs = (index, jobs)


def _plan_move(source: Path, target: Path, plan: CatalogPlan) -> None:
    if target.exists():
        raise MigrationError(f"both {source} and {target} exist; move their contents by hand, then run again")
    plan.moves.append((source, target))


def _plan_catalog_files(root: Path, kinds: dict[str, layout.Kind], plan: CatalogPlan) -> None:
    catalog = root / layout.CATALOG
    for kind in layout.Kind:
        folder = root / kind.value
        if not folder.is_dir():
            continue
        plan.old_folders.append(folder)
        for path in sorted(folder.iterdir()):
            name = path.name
            if name.endswith(_DISCARDED):
                plan.deletions.append(path)
            elif name.endswith(".icechunk.gc-pending") and kind is layout.Kind.RASTER:
                dataset_id = name.removesuffix(".icechunk.gc-pending")
                marker = root / "gc-pending" / dataset_id
                # The store may already be in the catalog, moved by a run that stopped before its marker.
                moved = root / layout.CATALOG / layout.dataset_key(kind, dataset_id) / f"{dataset_id}.icechunk"
                store_kept = (folder / f"{dataset_id}.icechunk").is_dir() or moved.is_dir()
                if store_kept and not marker.exists():
                    plan.moves.append((path, marker))
                else:
                    plan.deletions.append(path)  # its store is gone, or the marker was already moved
            else:
                new = _catalog_key(PurePosixPath(kind.value, name))
                dataset_id = new.parts[2] if new is not None else ""
                if new is None or not is_segment_safe_id(dataset_id):
                    plan.leftovers.append(path)
                    continue
                _plan_move(path, root.joinpath(*new.parts), plan)
                if path.is_dir() and kind is layout.Kind.TABLE:
                    plan.deletions += [file for file in sorted(path.iterdir()) if file.name.endswith(_DISCARDED)]
    thumbnails = root / "thumbnails"
    if thumbnails.is_dir():
        plan.old_folders.append(thumbnails)
        for path in sorted(thumbnails.iterdir()):
            owner = kinds.get(path.stem)
            if path.name.endswith(_DISCARDED) or (path.suffix == ".png" and owner is None):
                plan.deletions.append(path)  # a thumbnail no record owns could never be served
            elif path.suffix != ".png" or owner is None or not is_segment_safe_id(path.stem):
                plan.leftovers.append(path)
            else:
                _plan_move(path, catalog / layout.dataset_key(owner, path.stem) / "thumbnail.png", plan)


def _plan_adoptions(root: Path, records: list[Any], plan: CatalogPlan) -> None:
    """Find the raster stores written before the write branch existed: recorded, moved or in the catalog.

    *records* have their paths rewritten for the catalog already.
    """
    from open_geo_service.streaming.store import needs_adoption

    stores: set[Path] = set()
    published: set[Path] = set()
    for record in records:
        raw = record.get("path")
        if record.get("format") != "icechunk" or not isinstance(raw, str) or not raw:
            continue
        store = Path(raw) if Path(raw).is_absolute() else root / raw
        stores.add(store)
        if (record.get("publication") or {}).get("status") == "published":
            published.add(store)
    moved_from = {target: source for source, target in plan.moves}
    stores |= {target for target in moved_from if target.name.endswith(".icechunk")}
    catalog_rasters = root / layout.CATALOG / layout.Kind.RASTER.value
    if catalog_rasters.is_dir():
        stores |= {store for store in catalog_rasters.glob("*/*.icechunk") if store.is_dir()}
    for store in sorted(stores):
        now = moved_from.get(store, store)
        if (now / "repo").exists() and needs_adoption(now):
            plan.adoptions.append((store, store in published))


def _plan_same_device(plan: CatalogPlan) -> None:
    """Note the moves a rename cannot make: to `catalog/`, or a folder in it, on another filesystem."""
    for source, target in plan.moves:
        existing = target.parent
        while not existing.exists():
            existing = existing.parent
        if source.lstat().st_dev != existing.stat().st_dev:
            plan.cross_device.append((source, target))


def _cross_device_message(moves: list[tuple[Path, Path]]) -> str:
    listed = ", ".join(f"{source} -> {target}" for source, target in moves)
    return (
        f"these moves cross filesystems, which this migration does not copy: {listed}. Run it with "
        "catalog/ on the data folder's filesystem, then move catalog/ or its folders to the other disk."
    )


# The folders of the layout before ``rasters/`` and ``vectors/`` (CLIM-1253), which the migration
# of a release from before November 2026 renames.
EARLIER_DATA_FOLDERS = ("downloads", "features")
_EARLIER_PLUGIN_FOLDERS = ("datasets", "features")


def _earlier_layout_folders(root: Path) -> list[Path]:
    """The folders of the layout before CLIM-1253 in the data folder *root* and in ``plugins_dir``."""
    folders = [root / name for name in EARLIER_DATA_FOLDERS]
    raw = api_config.get_config().get("plugins_dir")
    if raw:
        config_path = api_config.get_config_path()
        plugins = ((config_path.parent if config_path else Path()) / str(raw)).resolve()
        folders += [plugins / name for name in _EARLIER_PLUGIN_FOLDERS]
    return [folder for folder in folders if folder.is_dir()]


def build_catalog_plan() -> CatalogPlan:
    """Work out every change of the move into ``catalog/``, without making any."""
    root = api_config.get_data_root().resolve()
    plan = CatalogPlan(root=root)
    if not root.is_dir():
        return plan
    if earlier := _earlier_layout_folders(root):
        raise MigrationError(
            f"{', '.join(str(folder) for folder in earlier)} are from the layout before rasters/ and vectors/. "
            "Run `python -m open_geo_service.migrate_layout` of a release from before November 2026 first, "
            "then this one."
        )
    kinds, records = _plan_catalog_records(root, plan)
    _plan_catalog_jobs(root, plan)
    _plan_catalog_files(root, kinds, plan)
    _plan_same_device(plan)
    _plan_adoptions(root, records, plan)
    # Removed once emptied; one holding a leftover stays, and the leftover is named.
    plan.old_folders = [folder for folder in plan.old_folders if all(path.parent != folder for path in plan.leftovers)]
    return plan


def pending_migration() -> str | None:
    """Why the data folder needs this migration before a writer may start on it, or None.

    A writer started first would sync into `catalog/` beside the old stores, downloading the data
    again, and the migration would then refuse to merge the two.
    """
    try:
        plan = build_catalog_plan()
    except MigrationError as exc:
        return str(exc)
    if plan.empty:
        return None
    folders = sorted(
        {path.relative_to(plan.root).parts[0] + "/" for path in [*plan.old_folders, *(s for s, _ in plan.moves)]}
    )
    if folders or plan.records or plan.jobs:
        return f"it holds {', '.join(folders) or 'records'} from the layout before catalog/"
    stores = ", ".join(store.name for store, _ in plan.adoptions)
    return f"its raster stores {stores} were written before their write branch, and are not read until moved to it"


def apply_catalog(plan: CatalogPlan) -> None:
    """Rewrite the paths, delete, move, then adopt the write branch, so a run cut short is completed by the next.

    Deleted first, so nothing discarded is carried into ``catalog/`` inside a table's folder.
    """
    if plan.invalid_ids:
        raise MigrationError(_invalid_ids_message(plan.invalid_ids))
    if plan.cross_device:
        raise MigrationError(_cross_device_message(plan.cross_device))
    for index, items in filter(None, (plan.records, plan.jobs)):
        storage.write_state(index, f"{json.dumps(items, indent=2)}\n".encode())
    for path in plan.deletions:
        path.unlink(missing_ok=True)
    for source, target in plan.moves:
        target.parent.mkdir(parents=True, exist_ok=True)
        source.rename(target)
    for folder in plan.old_folders:
        if folder.is_dir() and not any(folder.iterdir()):
            folder.rmdir()
    from open_geo_service.streaming.store import adopt_write_branch

    failed: list[str] = []
    for store, published in plan.adoptions:
        try:
            adopt_write_branch(store, published)
        except Exception as exc:
            failed.append(f"{store} ({exc})")
    if failed:
        # Each store picks up where it stopped when run again.
        raise MigrationError(
            f"could not move {', '.join(failed)} to the write branch; no server reads it until then. Run again."
        )


def _invalid_ids_message(ids: list[str]) -> str:
    listed = ", ".join(repr(dataset_id) for dataset_id in ids)
    return (
        f"datasets whose id breaks the id rule ({ID_RULE}): {listed}. "
        "Delete each, or re-ingest it under a valid id, then run again."
    )


def describe_catalog(plan: CatalogPlan) -> list[str]:
    """One line per change, in the order `apply_catalog` makes them, with paths in the data folder."""

    def shown(path: Path) -> str:
        return path.relative_to(plan.root).as_posix()

    lines = [f"rewrite store paths in {shown(index)}" for index, _ in filter(None, (plan.records, plan.jobs))]
    lines += [f"delete {shown(path)}" for path in plan.deletions]
    lines += [f"move {shown(source)} -> {shown(target)}" for source, target in plan.moves]
    lines += [
        f"give {shown(store) if store.is_relative_to(plan.root) else store} its write branch"
        + (", keeping main as published" if published else "")
        for store, published in plan.adoptions
    ]
    return lines


def main(argv: list[str] | None = None) -> int:
    """Migrate the configured instance, or report what would change with ``--dry-run``."""
    parser = argparse.ArgumentParser(description="Move an instance onto the catalog layout.")
    parser.add_argument("--dry-run", action="store_true", help="report the changes without making them")
    args = parser.parse_args(argv)
    prefix = "would " if args.dry_run else ""
    try:
        # The plan is built under the lease, so no writer changes what it read before it is applied.
        # Without a data folder there is nothing for a server to write, and taking the lease would create one.
        writing = not args.dry_run and api_config.get_data_root().is_dir()
        with writer_lease.hold("migrate_layout") if writing else nullcontext():
            catalog = build_catalog_plan()
            if catalog.invalid_ids:
                raise MigrationError(_invalid_ids_message(catalog.invalid_ids))
            if catalog.cross_device:
                raise MigrationError(_cross_device_message(catalog.cross_device))
            if catalog.empty:
                print("Nothing to migrate: this instance already uses the catalog layout.")
            else:
                print(f"In {catalog.root}:")
            for line in describe_catalog(catalog):
                print(prefix + line)
            for path in catalog.leftovers:
                print(f"{'would leave' if args.dry_run else 'leave'} {path}: not a dataset file this migration knows")
            if args.dry_run or catalog.empty:
                return 0
            apply_catalog(catalog)
    except (MigrationError, writer_lease.WriterLeaseHeld) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print("Done. Restart the server.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
