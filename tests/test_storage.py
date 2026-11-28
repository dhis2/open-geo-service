"""The object interface every record, data file and derived file is written through."""

from __future__ import annotations

import errno
import os
import stat
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from open_geo_service import storage
from open_geo_service.features import services as feature_services
from open_geo_service.features import store as feature_store
from open_geo_service.ingestions import services as ingestion_services
from open_geo_service.main import app
from open_geo_service.shared import layout
from open_geo_service.storage import local as storage_local
from open_geo_service.tables import store as table_store

from .test_catalog_files import DISTRICTS, _box, write_one_of_each


def test_a_put_is_whole_flushed_and_create_refuses_an_existing_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "data"
    objects = storage.Objects(root, staging=root / "tmp")
    files: list[int] = []
    folders: list[Path] = []
    fsync, flush_folder = os.fsync, storage_local._flush_folder
    monkeypatch.setattr(storage_local.os, "fsync", lambda descriptor: files.append(descriptor) or fsync(descriptor))
    monkeypatch.setattr(storage_local, "_flush_folder", lambda folder: folders.append(folder) or flush_folder(folder))

    objects.put("catalog/a/collection.json", b"{}")

    assert objects.get("catalog/a/collection.json") == b"{}"
    assert list((root / "tmp").iterdir()) == [], "nothing staged is left behind"
    if os.name != "nt":
        # The file, then every folder from its own up to the root, before the put returns.
        assert folders == [root / "catalog" / "a", root / "catalog", root]
        assert len(files) == 1 + len(folders), "the file itself, and each folder"
        objects.path("catalog/a/collection.json").chmod(0o640)
        objects.put("catalog/a/collection.json", b"{ }")
        assert stat.S_IMODE(objects.path("catalog/a/collection.json").stat().st_mode) == 0o640, (
            "a replace keeps its mode"
        )
    else:
        objects.put("catalog/a/collection.json", b"{ }")
    with pytest.raises(FileExistsError):
        objects.put("catalog/a/collection.json", b"[]", create=True)

    assert objects.get("catalog/a/collection.json") == b"{ }"

    # With catalog/ on another filesystem than the staging folder, as a mount or a symlinked disk.
    replace = storage_local.replace_file

    def across(source: str | Path, target: Path) -> None:
        if Path(source).parent != target.parent:
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        replace(source, target)

    monkeypatch.setattr(storage_local, "replace_file", across)
    objects.put("catalog/a/collection.json", b"{  }")
    assert objects.get("catalog/a/collection.json") == b"{  }"
    assert sorted(p.name for p in objects.path("catalog/a").iterdir()) == ["collection.json"], "nothing left beside it"
    assert [listed.key for listed in objects.scan("catalog")] == ["catalog/a/collection.json"]


def test_a_write_stopped_between_file_and_record_is_removed_at_the_writers_next_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, restart_writer: Callable[[], None]
) -> None:
    data = tmp_path / "data"
    write_one_of_each(data, monkeypatch)
    (replaced,) = [Path(str(r.path)) for r in ingestion_services._load_records() if r.dataset_id == "districts"]
    feature_services.refresh_feature_collection(
        template=DISTRICTS, features={"type": "FeatureCollection", "features": [_box("D1")]}
    )
    records = {record.dataset_id: record for record in ingestion_services._load_records()}
    districts = Path(str(records["districts"].path))
    table = Path(records["rain_districts"].asset_paths[0])

    # The first half of each write, the file, with no record after it: what a process stopped
    # between the two leaves. Made to look newer than the files the records name: no file time is read.
    stopped_vector, _count, _geometry = feature_store.write_feature_collection(
        dataset_id="districts",
        features={"type": "FeatureCollection", "features": [_box("D2")]},
        id_property="orgUnitCode",
    )
    stopped_table = table_store._write_file(
        layout.dataset_dir(layout.Kind.TABLE, "rain_districts"), pq.read_table(table), datetime.now(UTC)
    )
    for path in (districts, table, replaced):
        os.utime(path, (time.time() - 86400, time.time() - 86400))
    # A first write stopped the same way, in a folder no record names at all: left, as the
    # records may be what is missing.
    unrecorded, _count, _geometry = feature_store.write_feature_collection(
        dataset_id="regions",
        features={"type": "FeatureCollection", "features": [_box("R1")]},
        id_property="orgUnitCode",
    )

    restart_writer()

    assert not stopped_vector.exists() and not stopped_table.exists()
    assert unrecorded.exists(), "a folder no record names is left alone"
    assert replaced.exists(), "a file the record lists as replaced stays for its grace period"
    assert districts.exists() and table.exists(), "the files the records name stay"
    assert TestClient(app).get("/features/districts/data.parquet").status_code == 200

    # Without the records, as when `catalog/` is restored before them, no file is removed.
    index = ingestion_services.ARTIFACTS_INDEX_PATH
    index.rename(index.with_suffix(".away"))
    files = sorted((data / "catalog").rglob("*.p*"))
    restart_writer()
    assert sorted((data / "catalog").rglob("*.p*")) == files
