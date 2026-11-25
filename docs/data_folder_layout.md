# Data folder layout

Everything an instance stores lives in its data folder, the `data_dir` set in `geoservice.yaml`.
The datasets sit in `catalog/`, one folder per dataset, laid out as a
[Portolan](https://spec.portolan-sdi.org/) catalog. GeoService's own state sits beside it.

```
data/
  catalog/                       the datasets
    rasters/
      <id>/
        <id>.icechunk/           the Icechunk store
        thumbnail.png
    vectors/
      <id>/
        <id>.<uuid>.parquet      one GeoParquet file per version
        <id>.<uuid>.pmtiles      its vector tiles
        thumbnail.png
    tables/
      <id>/
        <stamp>-<uuid>.parquet   one Parquet file per version
  artifacts/records.json         the dataset records, which name each dataset's current files
  gc-pending/<id>                stores with unreachable data due for collection
  tmp/                           files being written
  jobs/  openeo_jobs/  ...       jobs, schedules, pipelines and other state
  writer.lock  writer.json       the writer lease
```

## Rules

- **A dataset has one folder, named by its id**, from its first write, published or not.
  Publishing changes the record, not the files. A dataset id starts with a lowercase letter and
  holds only lowercase letters, digits, `_` and `-`, at most 128 characters.
- **Nothing GeoService keeps for itself lives under `catalog/`**: no locks, markers or
  half-written files. A file is written in `tmp/` and moved into place, so a reader of
  `catalog/` never sees a partial file.
- **The data folder is one filesystem.** Moving a file from `tmp/` into `catalog/` is atomic
  only within one filesystem, so do not mount `catalog/` or one of its folders separately.
- **A new version of a vector or table dataset is a new file.** The record names the current
  one. A replaced file is removed once the file that replaced it is a minute old, so a reader
  that has just read the record can still open the file it named.
- **The records decide what exists.** A file in `catalog/` that no record names is not served.

The `/stac` routes describe the published datasets. The catalog documents (`catalog.json`,
`collection.json`) are not yet written into `catalog/`.

## Moving an instance to this layout

An instance written by an earlier version keeps its datasets in `rasters/`, `vectors/`,
`tables/` and `thumbnails/` at the top of the data folder, and this version does not read them:
the server logs a warning at startup. Move them with `migrate_layout`; nothing is downloaded
again. See [Upgrading](instance_guide.md#moving-to-a-new-data-folder-layout).
