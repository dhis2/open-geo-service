"""Routes for the STAC catalogue, served from the files the writer keeps in `catalog/`.

Laid out as a static catalog, so a client that follows links reads it as files: the root
`catalog.json`, a sub-catalog `{kind}/catalog.json` per kind of dataset, and one folder per
collection, `{kind}/{id}/`, holding `collection.json` beside the dataset's data. Each catalog and
collection has its `README.md` and `AGENTS.md` beside it. The links are relative, so `/stac`
serves the datasets' data files too: it serves `catalog/` as a static host of the folder would.
Every route answers HEAD, as a static host does and a link checker expects.
"""

from fastapi import APIRouter
from fastapi.responses import FileResponse

from open_geo_service import storage
from open_geo_service.stac import catalog_files

router = APIRouter()


@router.get("/{path:path}", response_class=FileResponse)
@router.head("/{path:path}", response_class=FileResponse, include_in_schema=False)
def get_stac_file(path: str) -> FileResponse:
    """A document of the STAC catalog, or a dataset's data file, at its path in the catalog."""
    file, media_type = catalog_files.served_file(path)
    return storage.file_response(
        file, media_type=media_type, headers={"Cache-Control": catalog_files.cache_control(file)}
    )
