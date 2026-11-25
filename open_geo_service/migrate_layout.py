"""Move an instance onto the current data folder layout (docs/data_folder_layout.md).

Run once per instance, with its server stopped, from the directory its ``.env`` is in. It
refuses while a server that holds the writer lease runs, but a server from before the lease
holds none, so during an upgrade stopping the server is up to you:

    python -m open_geo_service.migrate_layout [--dry-run]

Two steps, each made only where it is still needed:

1. The rasters/vectors names (CLIM-1253). In the data directory, ``downloads/`` becomes
   ``rasters/`` and ``features/`` becomes ``vectors/``. Under ``plugins_dir``, ``datasets/``
   becomes ``rasters/`` and ``features/`` becomes ``vectors/``, and the ``ingestion.plugin``
   paths and plugin imports that name the old folders are rewritten.
2. The catalog (CLIM-1968). Every dataset moves into a folder of its own,
   ``catalog/<kind>/<id>/``, with its thumbnail. Pending-collection markers move to
   ``gc-pending/``; the empty per-store lock files and the old ``.superseded`` markers are
   deleted. It refuses to run while a record's id breaks the id rule, which the routes would
   refuse to serve from its new folder.

Both rewrite the store paths in ``artifacts/records.json`` to match, and the second also those
in ``openeo_jobs/jobs.json``. Open Geo Service reads only the current layout, so an instance
that has not been migrated starts with no stores. Running it again once done changes nothing.
It refuses to run when a file or folder it would move to already exists, rather than merging.
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

DATA_RENAMES = (("downloads", "rasters"), ("features", "vectors"))
PLUGIN_RENAMES = (("datasets", "rasters"), ("features", "vectors"))

_PATH_FIELDS = ("path", "asset_paths")


class MigrationError(RuntimeError):
    """The layout cannot be migrated without a decision this command will not make."""


@dataclass
class Plan:
    """What a migration does, so a dry run can report it and a real run can carry it out."""

    renames: list[tuple[Path, Path]] = field(default_factory=list)
    records: tuple[Path, list[Any]] | None = None
    rewrites: list[tuple[Path, str]] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not self.renames and self.records is None and not self.rewrites


def _plan_rename(parent: Path, old: str, new: str, plan: Plan) -> None:
    source, target = parent / old, parent / new
    if not source.exists():
        return
    if target.exists():
        raise MigrationError(f"both {source} and {target} exist; move their contents by hand, then run again")
    plan.renames.append((source, target))


def _migrated_path(raw: str, root: Path) -> str:
    """The same store path with its first folder renamed, written relative when it is ours.

    A path under root comes back relative, the portable form `artifact_paths.to_portable`
    writes, so a migrated data directory can be moved or copied. A path outside root is
    claimed only when it no longer exists here and the same store does exist under root:
    a container's `/app/data/downloads/x` seen from the host. One that still exists is a store
    deliberately kept elsewhere, even when root holds a store of the same name, and is left alone.
    """
    mapping = dict(DATA_RENAMES)
    # Absolute is decided by the native path, so a Windows record (`C:\\data\\downloads\\...`)
    # is recognised on Windows; relative records are always written with forward slashes.
    if not Path(raw).is_absolute():
        parts = PurePosixPath(raw).parts
        if parts and parts[0] in mapping:
            return PurePosixPath(mapping[parts[0]], *parts[1:]).as_posix()
        return raw
    candidate = Path(raw)
    try:
        # Resolved, as root is, so a data directory reached through a symlink still matches.
        relative = candidate.resolve(strict=False).relative_to(root)
    except ValueError:
        pass
    else:
        parts = relative.parts
        if parts and parts[0] in mapping:
            parts = (mapping[parts[0]], *parts[1:])
        return PurePosixPath(*parts).as_posix()
    if candidate.exists():
        return raw  # a store deliberately kept outside the data directory
    parts = candidate.parts
    for index in range(1, len(parts) - 1):
        if parts[index] in mapping and root.joinpath(*parts[index:]).exists():
            return PurePosixPath(mapping[parts[index]], *parts[index + 1 :]).as_posix()
    return raw  # nothing under root matches: keep what was recorded


def _plan_records(data_root: Path, plan: Plan) -> None:
    index = data_root / "artifacts" / "records.json"
    if not index.is_file():
        return
    records = json.loads(index.read_text(encoding="utf-8"))
    changed = False
    for record in records:
        for key in _PATH_FIELDS:
            value = record.get(key)
            if isinstance(value, str):
                new = _migrated_path(value, data_root)
                changed |= new != value
                record[key] = new
            elif isinstance(value, list):
                new_list = [_migrated_path(item, data_root) if isinstance(item, str) else item for item in value]
                changed |= new_list != value
                record[key] = new_list
    if changed:
        plan.records = (index, records)


# In a dataset template, the old folder as the first component of the plugin path or after the
# built-in package: `plugin: datasets.x.Class`, `plugin: open_geo_service.plugins.datasets.x.Class`.
# In a plugin module, only imports of the built-in folders (`from open_geo_service.plugins.datasets.x
# import y`): no plugin imports a sibling by the bare folder name, and a bare `datasets` or `features`
# is as likely another package. Nothing else is touched: the words are common.
_YAML_PLUGIN = re.compile(
    r"^(\s*plugin:\s*['\"]?)((?:open_geo_service\.plugins\.)?)(datasets|features)(?=\.)", re.MULTILINE
)
_PY_IMPORT = re.compile(
    r"^(\s*(?:from|import)\s+)(open_geo_service\.plugins\.)(datasets|features)(?=[.\s,]|$)", re.MULTILINE
)


def _rewritten(text: str, pattern: re.Pattern[str]) -> str:
    mapping = dict(PLUGIN_RENAMES)
    return pattern.sub(lambda m: f"{m.group(1)}{m.group(2)}{mapping[m.group(3)]}", text)


def _plan_plugin_files(plugins_root: Path, plan: Plan) -> None:
    """Dataset templates in the template folders; Python modules anywhere under plugins_dir.

    plugins_dir is on `sys.path`, so a module in `processes/` or `exports/` can import from
    `datasets` or `features` as readily as a plugin next to its template can.
    """
    candidates: list[tuple[Path, re.Pattern[str]]] = []
    for folder in {old for old, _ in PLUGIN_RENAMES} | {new for _, new in PLUGIN_RENAMES}:
        directory = plugins_root / folder
        if directory.is_dir():
            candidates += [(path, _YAML_PLUGIN) for path in directory.rglob("*") if path.suffix in {".yaml", ".yml"}]
    for path in plugins_root.rglob("*.py"):
        if not any(part.startswith(".") or part == "__pycache__" for part in path.relative_to(plugins_root).parts):
            candidates.append((path, _PY_IMPORT))
    for path, pattern in sorted(candidates):
        text = path.read_text(encoding="utf-8")
        new = _rewritten(text, pattern)
        if new != text:
            plan.rewrites.append((path, new))


def _plugins_root() -> Path | None:
    config = api_config.get_config()
    raw = config.get("plugins_dir") if config else None
    if not raw:
        return None
    config_path = api_config.get_config_path()
    base = config_path.parent if config_path else Path()
    root = (base / raw).resolve()
    return root if root.is_dir() else None


def build_plan() -> Plan:
    """Work out every change for the configured instance, without making any."""
    plan = Plan()
    data_root = api_config.get_data_root().resolve()
    if data_root.is_dir():
        for old, new in DATA_RENAMES:
            _plan_rename(data_root, old, new, plan)
        _plan_records(data_root, plan)
    plugins_root = _plugins_root()
    if plugins_root is not None:
        for old, new in PLUGIN_RENAMES:
            _plan_rename(plugins_root, old, new, plan)
        _plan_plugin_files(plugins_root, plan)
    return plan


def apply(plan: Plan) -> None:
    """Rewrite files first, while their paths are the ones planned, then rename the folders."""
    for path, text in plan.rewrites:
        path.write_text(text, encoding="utf-8")
    if plan.records is not None:
        index, records = plan.records
        storage.write(index, f"{json.dumps(records, indent=2)}\n".encode())
    for source, target in plan.renames:
        source.rename(target)


# --- step 2: the catalog -------------------------------------------------------------------------

OLD_KIND_FOLDERS = tuple(kind.value for kind in layout.Kind)
_RASTER_STORE = re.compile(r"(?P<id>.+)\.(?:icechunk|zarr)")
_VECTOR_FILE = re.compile(r"(?P<id>.+)\.[0-9a-f]{32}\.(?:parquet|pmtiles)")
# Left beside the data by a write an earlier version was killed in, and by the old feature rule:
# the empty per-store lock files (unused since CLIM-1998), half-written files and `.superseded` markers.
_DISCARDED = (".icechunk.lock", ".writing", ".tmp.png", ".superseded")


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

    @property
    def empty(self) -> bool:
        return not (self.records or self.jobs or self.moves or self.deletions)


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

    A path outside root counts when the same file exists under root: a container's
    ``/app/data/rasters/x.icechunk`` seen from the host.
    """
    if not Path(raw).is_absolute():
        return PurePosixPath(raw)
    candidate = Path(raw)
    try:
        return PurePosixPath(candidate.resolve(strict=False).relative_to(root).as_posix())
    except ValueError:
        pass
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


def _plan_catalog_records(root: Path, plan: CatalogPlan) -> dict[str, layout.Kind]:
    """Rewrite the record paths and check the ids; return each dataset's kind."""
    index = root / "artifacts" / "records.json"
    if not index.is_file():
        return {}
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
    return kinds


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
                if (folder / f"{dataset_id}.icechunk").is_dir() and not marker.exists():
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
            if path.name.endswith(_DISCARDED):
                plan.deletions.append(path)
            elif path.suffix != ".png" or owner is None or not is_segment_safe_id(path.stem):
                plan.leftovers.append(path)
            else:
                _plan_move(path, catalog / layout.dataset_key(owner, path.stem) / "thumbnail.png", plan)


def build_catalog_plan() -> CatalogPlan:
    """Work out every change of the move into ``catalog/``, without making any."""
    root = api_config.get_data_root().resolve()
    plan = CatalogPlan(root=root)
    if not root.is_dir():
        return plan
    kinds = _plan_catalog_records(root, plan)
    _plan_catalog_jobs(root, plan)
    _plan_catalog_files(root, kinds, plan)
    return plan


def apply_catalog(plan: CatalogPlan) -> None:
    """Rewrite the paths, delete, then move, so a run cut short is completed by the next.

    Deleted first, so nothing discarded is carried into ``catalog/`` inside a table's folder.
    """
    if plan.invalid_ids:
        raise MigrationError(_invalid_ids_message(plan.invalid_ids))
    for index, items in filter(None, (plan.records, plan.jobs)):
        storage.write(index, f"{json.dumps(items, indent=2)}\n".encode())
    for path in plan.deletions:
        path.unlink(missing_ok=True)
    for source, target in plan.moves:
        target.parent.mkdir(parents=True, exist_ok=True)
        source.rename(target)
    for folder in plan.old_folders:
        if folder.is_dir() and not any(folder.iterdir()):
            folder.rmdir()


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
    return lines


def describe(plan: Plan) -> list[str]:
    """One line per change, in the order `apply` makes them."""
    lines = [f"rename {source} -> {target.name}/" for source, target in plan.renames]
    if plan.records is not None:
        lines.append(f"rewrite store paths in {plan.records[0]}")
    lines += [f"rewrite plugin paths in {path}" for path, _ in plan.rewrites]
    return lines


def main(argv: list[str] | None = None) -> int:
    """Migrate the configured instance, or report what would change with ``--dry-run``."""
    parser = argparse.ArgumentParser(description="Move an instance onto the current data folder layout.")
    parser.add_argument("--dry-run", action="store_true", help="report the changes without making them")
    args = parser.parse_args(argv)
    prefix = "would " if args.dry_run else ""
    try:
        # The plan is built under the lease, so no writer changes what it read before it is applied.
        # Without a data folder there is nothing for a server to write, and taking the lease would create one.
        writing = not args.dry_run and api_config.get_data_root().is_dir()
        with writer_lease.hold("migrate_layout") if writing else nullcontext():
            # Checked before either step, so a refused migration moves nothing.
            if invalid := build_catalog_plan().invalid_ids:
                raise MigrationError(_invalid_ids_message(invalid))
            plan = build_plan()
            if not plan.empty:
                for line in describe(plan):
                    print(prefix + line)
                if args.dry_run:
                    print(
                        "then move the datasets into catalog/; run --dry-run again after migrating to list those moves"
                    )
                    return 0
                apply(plan)
            catalog = build_catalog_plan()
            if plan.empty and catalog.empty:
                print("Nothing to migrate: this instance already uses the current layout.")
            elif not catalog.empty:
                print(f"In {catalog.root}:")
            for line in describe_catalog(catalog):
                print(prefix + line)
            for path in catalog.leftovers:
                print(f"{'would leave' if args.dry_run else 'leave'} {path}: not a dataset file this migration knows")
            if args.dry_run or (plan.empty and catalog.empty):
                return 0
            apply_catalog(catalog)
    except (MigrationError, writer_lease.WriterLeaseHeld) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print("Done. Restart the server.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
