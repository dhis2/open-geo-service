"""The object interface every record, data file and derived file is written through."""

from __future__ import annotations

import os
import stat
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from open_geo_service import storage
from open_geo_service.features import store as feature_store
from open_geo_service.ingestions import services as ingestion_services
from open_geo_service.main import app
from open_geo_service.shared import layout
from open_geo_service.tables import store as table_store

from .test_catalog_files import _box, write_one_of_each


def test_a_put_is_whole_flushed_and_create_refuses_an_existing_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    objects = storage.Objects(tmp_path / "data", staging=tmp_path / "data" / "tmp")
    flushed: list[int] = []
    fsync = os.fsync
    monkeypatch.setattr(storage.os, "fsync", lambda descriptor: flushed.append(descriptor) or fsync(descriptor))

    objects.put("catalog/a/collection.json", b"{}")

    # The file, then the folder holding its new entry, before the put returns.
    assert len(flushed) == 2
    assert objects.get("catalog/a/collection.json") == b"{}"
    assert list((tmp_path / "data" / "tmp").iterdir()) == [], "nothing staged is left behind"
    objects.path("catalog/a/collection.json").chmod(0o640)
    objects.put("catalog/a/collection.json", b"{ }")
    assert stat.S_IMODE(objects.path("catalog/a/collection.json").stat().st_mode) == 0o640, "a replace keeps the mode"
    with pytest.raises(FileExistsError):
        objects.put("catalog/a/collection.json", b"[]", create=True)
    assert objects.get("catalog/a/collection.json") == b"{ }"
    assert [listed.key for listed in objects.scan("catalog")] == ["catalog/a/collection.json"]


def test_a_write_stopped_between_file_and_record_is_removed_at_the_writers_next_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "data"
    write_one_of_each(data, monkeypatch)
    records = {record.dataset_id: record for record in ingestion_services._load_records()}
    districts = Path(str(records["districts"].path))
    table = Path(records["rain_districts"].asset_paths[0])

    # The first half of each write, the file, with no record after it: what a process stopped
    # between the two leaves.
    stopped_vector, _count, _geometry = feature_store.write_feature_collection(
        dataset_id="districts",
        features={"type": "FeatureCollection", "features": [_box("D2")]},
        id_property="orgUnitCode",
    )
    stopped_table = table_store._write_file(
        layout.dataset_dir(layout.Kind.TABLE, "rain_districts"), pq.read_table(table), datetime.now(UTC)
    )
    # A first write stopped the same way, in a folder no record names at all.
    unrecorded, _count, _geometry = feature_store.write_feature_collection(
        dataset_id="regions",
        features={"type": "FeatureCollection", "features": [_box("R1")]},
        id_property="orgUnitCode",
    )

    with TestClient(app):  # the writer starts
        pass

    assert not stopped_vector.exists() and not stopped_table.exists() and not unrecorded.exists()
    assert districts.exists() and table.exists(), "the files the records name stay"
    assert TestClient(app).get("/features/districts/data.parquet").status_code == 200
