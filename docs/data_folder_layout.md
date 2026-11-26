# Data folder layout

Everything an instance stores lives in its data folder, the `data_dir` set in `geoservice.yaml`.
The datasets sit in `catalog/`, one folder per dataset, laid out as a
[Portolan](https://spec.portolan-sdi.org/) catalog. GeoService's own state sits beside it.

```
data/
  catalog/                       the datasets, and the STAC catalog describing them
    catalog.json  README.md  AGENTS.md
    rasters/
      catalog.json  README.md  AGENTS.md
      <id>/
        collection.json  README.md  AGENTS.md
        <id>.icechunk/           the Icechunk store
        thumbnail.png
    vectors/
      catalog.json  README.md  AGENTS.md
      <id>/
        collection.json  README.md  AGENTS.md
        <id>.<uuid>.parquet      one GeoParquet file per version
        <id>.<uuid>.pmtiles      its vector tiles
        styles/default.json      its MapLibre style, with GEOSERVICE_BASE_URL set
        thumbnail.png
    tables/
      catalog.json  README.md  AGENTS.md
      <id>/
        collection.json  README.md  AGENTS.md
        <stamp>-<uuid>.parquet   one Parquet file per version
  artifacts/                     records.json, the dataset records, which name each dataset's
                                 current files, and the locks of their writes
  gc-pending/<id>                stores with unreachable data due for collection
  tmp/                           files being written
  cache/                         source downloads kept for reuse
  jobs/  openeo_jobs/  process_graphs/  pipelines/  derivations/  exports/
  schedules.json  schedule_results.json  notifications.json
                                 jobs, schedules, pipelines and other state
  writer.lock  writer.json       the writer lease
```

## Rules

- **A dataset has one folder, named by its id**, from its first write, published or not.
  Publishing changes the record, not the files. A dataset id starts with a lowercase letter and
  holds only lowercase letters, digits, `_` and `-`, at most 128 characters.
- **Nothing GeoService keeps for itself lives under `catalog/`**: no locks, markers or
  half-written files. A file is written in `tmp/` and moved into place, so a reader of
  `catalog/` never sees a partial file.
- **Every file is written through one object interface** (`storage.py`), flushed to disk before
  the write returns. At its start the writer removes the vector and table files a write stopped
  before their record left behind.
- **The data folder is one filesystem.** Moving a file from `tmp/` into `catalog/` is atomic
  only within one filesystem, so do not mount `catalog/` or one of its folders separately.
- **A new version of a vector or table dataset is a new file.** The record names the current
  one. A replaced file is removed once the file that replaced it is a minute old, so a reader
  that has just read the record can still open the file it named.
- **The records decide what is listed.** The listings and the catalog read the records, never
  the folder. They do not control access: `/stac`, like any static host of `catalog/`, serves a
  folder no published record names (see below).

## The catalog

The writer keeps a STAC catalog of the published datasets in `catalog/`, in the shape of a
Portolan catalog: the root `catalog.json`, a sub-catalog per kind of dataset, and a
`collection.json` beside each dataset's data, each with a `README.md` and an `AGENTS.md`. It
rewrites them whenever a record changes, before the write returns, and all of them when it
starts, which picks up an upgrade, a changed template or a changed public address. A read-only
server writes none of it, so after an upgrade the writer starts first.

- **Only what is published is listed.** A dataset written but not yet published has its folder
  and data, but no `collection.json`, and no catalog links to it.
- **Every link stays inside `catalog/`.** Links between documents are relative, and so are the
  data assets: the Icechunk store, the GeoParquet and PMTiles files, the Parquet file, the
  thumbnail. A copy of `catalog/`, served by any static host, is a complete catalog.
- **What needs the public address is written only with it.** A raster's `zarr` asset, which
  GeoService generates on request, and the host provider's address are written only when
  `GEOSERVICE_BASE_URL` is set. So is a feature collection's MapLibre style, a file in its
  `styles/` folder that its `style` asset links: MapLibre resolves the tiles URL in a style
  against the page rather than the style, so the style names its PMTiles file by absolute URL.
  Portolan asks for the host's address and for a style beside PMTiles, so a catalog meant to be
  published needs the setting.

`/stac` serves `catalog/` as a static host of it would: the documents and the data files beside
them, without reading the records. A dataset written but not yet published is reachable by its
path there, as in any copy of the folder, but nothing links to it: unpublished means not ready,
not private. The dataset pages and `/datasets/{id}` give the `/zarr` URL either way, and
openEO's `/collections` are still built on request, with absolute links.

## Moving an instance to this layout

An instance written by an earlier version keeps its datasets in `rasters/`, `vectors/`,
`tables/` and `thumbnails/` at the top of the data folder, and this version does not read them:
the server logs a warning at startup. Move them with `migrate_layout`; nothing is downloaded
again. See [Upgrading](instance_guide.md#moving-to-a-new-data-folder-layout).
