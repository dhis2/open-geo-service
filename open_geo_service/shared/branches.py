"""The Icechunk branches of a raster store (docs/managed_data_api_guide.md, under versions and rollback).

Writers commit to WRITE_BRANCH, and publishing moves PUBLISHED_BRANCH to a version on it.
PUBLISHED_BRANCH is `main`, the branch Icechunk readers open by default, so a reader outside
GeoService gets the published version without being told a branch.
"""

WRITE_BRANCH = "ingest"
PUBLISHED_BRANCH = "main"
